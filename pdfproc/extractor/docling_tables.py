"""Extracción de tablas con Docling (TableFormer)."""

import logging

from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
from docling.document_converter import DocumentConverter, PdfFormatOption

from pdfproc.extractor.pipeline import ExtractedTable

log = logging.getLogger(__name__)


class DoclingTableExtractor:
    def __init__(self, *, do_ocr: bool = False, artifacts_path: str | None = None) -> None:
        opts = PdfPipelineOptions()
        opts.do_ocr = do_ocr
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
                )
            )
        log.info("Docling: %d tablas en %s", len(tables), path)
        return tables
