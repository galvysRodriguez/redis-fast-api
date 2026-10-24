"""Extracción y chunking semántico de texto con Unstructured."""

import logging

from unstructured.chunking.title import chunk_by_title
from unstructured.partition.pdf import partition_pdf

from pdfproc.extractor.pipeline import ExtractedText

log = logging.getLogger(__name__)

# Las tablas las resuelve Docling; cabeceras/pies de página solo meten ruido.
SKIPPED_CATEGORIES = {"Table", "Image", "PageBreak", "Header", "Footer"}


class UnstructuredTextExtractor:
    def __init__(
        self,
        *,
        strategy: str = "fast",
        max_characters: int = 1500,
        new_after_n_chars: int = 1200,
        combine_text_under_n_chars: int = 300,
    ) -> None:
        self._strategy = strategy
        self._max_characters = max_characters
        self._new_after_n_chars = new_after_n_chars
        self._combine_under = combine_text_under_n_chars

    def extract(self, path: str) -> list[ExtractedText]:
        elements = partition_pdf(filename=path, strategy=self._strategy)
        elements = [
            el for el in elements
            if el.category not in SKIPPED_CATEGORIES and (el.text or "").strip()
        ]
        if not elements:
            return []

        chunks = chunk_by_title(
            elements,
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
        log.info("Unstructured: %d elementos -> %d chunks en %s", len(elements), len(results), path)
        return results


def _origin(chunk) -> list:
    return getattr(chunk.metadata, "orig_elements", None) or [chunk]


def _pages(chunk) -> list[int]:
    pages = {el.metadata.page_number for el in _origin(chunk) if el.metadata.page_number}
    return sorted(pages)
