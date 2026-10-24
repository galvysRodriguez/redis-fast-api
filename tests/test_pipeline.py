from pdfproc.extractor.pipeline import ExtractedTable, ExtractedText, build_chunks
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


def test_chunk_topic_messages_roundtrip_through_discriminator():
    chunk = build_chunks(REQ, [], [ExtractedText("hola")])[0]
    done = JobCompleted(job_id="job1", total_chunks=1, text_chunks=1, table_chunks=0, extraction_seconds=0.1)

    assert isinstance(chunk_topic_adapter.validate_json(encode(chunk)), Chunk)
    assert isinstance(chunk_topic_adapter.validate_json(encode(done)), JobCompleted)
