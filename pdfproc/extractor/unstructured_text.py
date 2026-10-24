"""Extracción y chunking semántico de texto con Unstructured."""

import logging

from unstructured.chunking.title import chunk_by_title
from unstructured.partition.pdf import partition_pdf

from pdfproc.extractor.pipeline import ExtractedText, Region, inside_any

log = logging.getLogger(__name__)

# Las tablas las resuelve Docling; cabeceras/pies de página solo meten ruido.
SKIPPED_CATEGORIES = {"Table", "Image", "PageBreak", "Header", "Footer"}


class UnstructuredTextExtractor:
    def __init__(
        self,
        *,
        strategy: str = "fast",
        languages: list[str] | None = None,
        max_characters: int = 1500,
        new_after_n_chars: int = 1200,
        combine_text_under_n_chars: int = 300,
    ) -> None:
        self._strategy = strategy
        self._languages = languages or ["spa", "eng"]
        self._max_characters = max_characters
        self._new_after_n_chars = new_after_n_chars
        self._combine_under = combine_text_under_n_chars

    def partition(self, path: str) -> list:
        elements = partition_pdf(filename=path, strategy=self._strategy, languages=self._languages)
        return [
            el for el in elements
            if el.category not in SKIPPED_CATEGORIES and (el.text or "").strip()
        ]

    def chunk(self, elements: list, exclude: list[Region]) -> list[ExtractedText]:
        """Agrupa por secciones, descartando los elementos dentro de `exclude`.

        La estrategia `fast` no clasifica tablas: sus celdas llegan como
        UncategorizedText y solo se pueden quitar por posición.
        """
        kept = [el for el in elements if not _inside_table(el, exclude)]
        if not kept:
            return []

        chunks = chunk_by_title(
            kept,
            max_characters=self._max_characters,
            new_after_n_chars=self._new_after_n_chars,
            combine_text_under_n_chars=self._combine_under,
        )
        results = [
            ExtractedText(
                text=chunk.text,
                pages=_pages(chunk),
                element_types=sorted({el.category for el in _origin(chunk)}),
            )
            for chunk in chunks
            if chunk.text.strip()
        ]
        log.info(
            "Unstructured: %d elementos (%d dentro de tablas) -> %d chunks",
            len(elements), len(elements) - len(kept), len(results),
        )
        return results


def _inside_table(element, regions: list[Region]) -> bool:
    coords = element.metadata.coordinates
    page = element.metadata.page_number
    if not regions or not coords or not coords.points or not page:
        return False
    xs = [x for x, _ in coords.points]
    ys = [y for _, y in coords.points]
    x = (min(xs) + max(xs)) / 2 / coords.system.width
    y = (min(ys) + max(ys)) / 2 / coords.system.height
    return inside_any(regions, page, x, y)


def _origin(chunk) -> list:
    return getattr(chunk.metadata, "orig_elements", None) or [chunk]


def _pages(chunk) -> list[int]:
    pages = {el.metadata.page_number for el in _origin(chunk) if el.metadata.page_number}
    return sorted(pages)
