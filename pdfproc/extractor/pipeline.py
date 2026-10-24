"""Tipos intermedios de extracción y ensamblado de chunks (sin dependencias pesadas)."""

from dataclasses import dataclass, field
from typing import Protocol

from pdfproc.schemas import Chunk, IngestRequest, TablePayload


@dataclass(frozen=True)
class Region:
    """Caja de una tabla en fracciones de página (0..1), origen arriba-izquierda.

    Normalizada para poder compararla con coordenadas de Unstructured, que
    usa puntos PDF en `fast` y píxeles en `hi_res`.
    """

    page: int
    left: float
    top: float
    right: float
    bottom: float

    def contains(self, page: int, x: float, y: float, margin: float = 0.005) -> bool:
        return (
            page == self.page
            and self.left - margin <= x <= self.right + margin
            and self.top - margin <= y <= self.bottom + margin
        )


def inside_any(regions: list[Region], page: int, x: float, y: float) -> bool:
    return any(region.contains(page, x, y) for region in regions)


@dataclass(frozen=True)
class ExtractedTable:
    columns: list[str]
    rows: list[list[str]]
    markdown: str
    caption: str | None = None
    pages: list[int] = field(default_factory=list)
    regions: list[Region] = field(default_factory=list)


@dataclass(frozen=True)
class ExtractedText:
    text: str
    pages: list[int] = field(default_factory=list)
    element_types: list[str] = field(default_factory=list)


class TableExtractor(Protocol):
    def extract(self, path: str) -> list[ExtractedTable]: ...


class TextExtractor(Protocol):
    def partition(self, path: str) -> list: ...

    def chunk(self, elements: list, exclude: list[Region]) -> list[ExtractedText]: ...


def build_chunks(
    req: IngestRequest, tables: list[ExtractedTable], texts: list[ExtractedText]
) -> list[Chunk]:
    """Convierte los resultados de Docling/Unstructured en mensajes Chunk.

    El chunk_id es determinista (job + tipo + secuencia), así que reprocesar un
    job sobrescribe los mismos documentos en vez de duplicarlos.
    """
    chunks: list[Chunk] = []
    for seq, item in enumerate(texts):
        chunks.append(
            Chunk(
                chunk_id=f"{req.job_id}-text-{seq:05d}",
                job_id=req.job_id,
                filename=req.filename,
                kind="text",
                seq=seq,
                text=item.text,
                page_numbers=item.pages,
                element_types=item.element_types,
            )
        )
    for seq, item in enumerate(tables):
        chunks.append(
            Chunk(
                chunk_id=f"{req.job_id}-table-{seq:05d}",
                job_id=req.job_id,
                filename=req.filename,
                kind="table",
                seq=seq,
                text=_table_text(item),
                page_numbers=item.pages,
                element_types=["Table"],
                table=TablePayload(
                    columns=item.columns,
                    rows=item.rows,
                    markdown=item.markdown,
                    caption=item.caption,
                ),
            )
        )
    return chunks


def _table_text(table: ExtractedTable) -> str:
    """Texto indexable de la tabla: caption + markdown, sin repetir el caption
    (según la versión, Docling ya lo incluye en el markdown)."""
    if table.caption and table.caption not in table.markdown:
        return f"{table.caption}\n\n{table.markdown}"
    return table.markdown
