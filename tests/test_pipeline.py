from pdfproc.extractor.pipeline import (
    ExtractedTable,
    ExtractedText,
    Region,
    build_chunks,
    inside_any,
)
from pdfproc.schemas import Chunk, IngestRequest, JobCompleted, chunk_topic_adapter
from pdfproc.messaging import encode

REQ = IngestRequest(job_id="job1", filename="q3.pdf", path="/data/uploads/job1.pdf")


def test_build_chunks_assigns_deterministic_ids_and_kinds():
    texts = [ExtractedText("Intro", [1], ["Title", "NarrativeText"]), ExtractedText("Más", [2])]
    tables = [ExtractedTable(["A", "B"], [["1", "2"]], "| A | B |", caption="Ventas", pages=[3])]

    chunks = build_chunks(REQ, tables, texts)

    assert [c.chunk_id for c in chunks] == ["job1-text-00000", "job1-text-00001", "job1-table-00000"]
    table = chunks[-1]
    assert table.kind == "table"
    assert table.text == "Ventas\n\n| A | B |"
    assert table.table.rows == [["1", "2"]]
    assert table.page_numbers == [3]


def test_build_chunks_is_stable_across_runs():
    texts = [ExtractedText("x")]
    first = [c.chunk_id for c in build_chunks(REQ, [], texts)]
    second = [c.chunk_id for c in build_chunks(REQ, [], texts)]
    assert first == second


def test_region_contains_point_on_same_page_only():
    region = Region(page=1, left=0.3, top=0.28, right=0.66, bottom=0.37)
    assert region.contains(1, 0.5, 0.33)
    assert not region.contains(2, 0.5, 0.33)
    assert not region.contains(1, 0.5, 0.20)


def test_inside_any_tolerates_small_margin():
    regions = [Region(page=1, left=0.3, top=0.3, right=0.6, bottom=0.4)]
    assert inside_any(regions, 1, 0.603, 0.35)
    assert not inside_any(regions, 1, 0.62, 0.35)
    assert not inside_any([], 1, 0.5, 0.35)


def test_chunk_topic_messages_roundtrip_through_discriminator():
    chunk = build_chunks(REQ, [], [ExtractedText("hola")])[0]
    done = JobCompleted(job_id="job1", total_chunks=1, text_chunks=1, table_chunks=0, extraction_seconds=0.1)

    assert isinstance(chunk_topic_adapter.validate_json(encode(chunk)), Chunk)
    assert isinstance(chunk_topic_adapter.validate_json(encode(done)), JobCompleted)
