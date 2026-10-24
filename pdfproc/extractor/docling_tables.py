"""Extracción de tablas con Docling (TableFormer)."""

import logging
import subprocess
import time
from pathlib import Path

from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TableFormerMode,
    TesseractCliOcrOptions,
)
from docling.document_converter import DocumentConverter, PdfFormatOption

from pdfproc.extractor.pipeline import ExtractedTable, Region

log = logging.getLogger(__name__)

MODELS = ("layout", "tableformer")
READY_MARKER = ".models-ready"


def ensure_models(artifacts_path: str, attempts: int = 5) -> None:
    """Descarga los modelos de Docling al volumen la primera vez.

    Se hace al arrancar y no en el build: un corte de red no rompe la imagen
    y el volumen conserva los modelos entre reinicios y réplicas.
    """
    marker = Path(artifacts_path) / READY_MARKER
    if marker.exists():
        return
    for attempt in range(1, attempts + 1):
        log.info("Descargando modelos de Docling en %s (%d/%d)", artifacts_path, attempt, attempts)
        result = subprocess.run(
            ["docling-tools", "models", "download", *MODELS, "-o", artifacts_path],
            check=False,
        )
        if result.returncode == 0:
            marker.touch()
            return
        time.sleep(min(10 * attempt, 60))
    raise RuntimeError("No se pudieron descargar los modelos de Docling")


class DoclingTableExtractor:
    def __init__(self, *, do_ocr: bool = False, artifacts_path: str | None = None) -> None:
        opts = PdfPipelineOptions()
        opts.do_ocr = do_ocr
        if do_ocr:
            # Tesseract (binario del sistema) evita depender de easyocr/torch extra.
            opts.ocr_options = TesseractCliOcrOptions(lang=["spa", "eng"])
        opts.do_table_structure = True
        opts.table_structure_options.mode = TableFormerMode.ACCURATE
        opts.table_structure_options.do_cell_matching = True
        if artifacts_path:
            opts.artifacts_path = artifacts_path
        # El converter carga los modelos una sola vez y se reutiliza entre jobs.
        self._converter = DocumentConverter(
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=opts)}
        )

    def extract(self, path: str) -> list[ExtractedTable]:
        doc = self._converter.convert(path).document
        tables: list[ExtractedTable] = []
        for item in doc.tables:
            df = item.export_to_dataframe(doc=doc)
            if df.empty:
                continue
            tables.append(
                ExtractedTable(
                    columns=[str(c) for c in df.columns],
                    rows=df.fillna("").astype(str).values.tolist(),
                    markdown=item.export_to_markdown(doc=doc),
                    caption=item.caption_text(doc) or None,
                    pages=sorted({p.page_no for p in item.prov}),
                    regions=[_region(doc, prov) for prov in item.prov],
                )
            )
        log.info("Docling: %d tablas en %s", len(tables), path)
        return tables


def _region(doc, prov) -> Region:
    size = doc.pages[prov.page_no].size
    box = prov.bbox.to_top_left_origin(page_height=size.height)
    return Region(
        page=prov.page_no,
        left=box.l / size.width,
        top=box.t / size.height,
        right=box.r / size.width,
        bottom=box.b / size.height,
    )
