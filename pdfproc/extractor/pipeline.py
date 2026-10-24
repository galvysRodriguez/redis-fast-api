"""Tipos intermedios de extracción y ensamblado de chunks (sin dependencias pesadas)."""

from dataclasses import dataclass, field
from typing import Protocol

from pdfproc.schemas import Chunk, IngestRequest, TablePayload


@dataclass(frozen=True)
class ExtractedTable:
    columns: list[str]
    rows: list[list[str]]
    markdown: str
    caption: str | None = None
    pages: list[int] = field(default_factory=list)


@dataclass(frozen=True)
class ExtractedText:
    text: str
    pages: list[int] = field(default_factory=list)
    element_types: list[str] = field(default_factory=list)


class TableExtractor(Protocol):
    def extract(self, path: str) -> list[ExtractedTable]: ...


class TextExtractor(Protocol):
    def extract(self, path: str) -> list[ExtractedText]: ...


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
                text="\n\n".join(part for part in (item.caption, item.markdown) if part),
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
