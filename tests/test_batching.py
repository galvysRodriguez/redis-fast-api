import asyncio
from datetime import UTC, datetime

from pdfproc.indexer.batching import (
    build_bulk_operations,
    chunk_to_document,
    classify_bulk_response,
    collect_batch,
)
from pdfproc.schemas import Chunk


class FakeConsumer:
    """Simula getmany(): entrega los registros en tandas pequeñas."""

    def __init__(self, records, per_call):
        self._records = list(records)
        self._per_call = per_call
        self.calls = 0

    async def getmany(self, timeout_ms, max_records):
        self.calls += 1
        if not self._records:
            await asyncio.sleep(timeout_ms / 1000)
            return {}
        take = min(self._per_call, max_records)
        batch, self._records = self._records[:take], self._records[take:]
        return {"tp0": batch}


def test_collect_batch_fills_up_to_max_records_across_polls():
    consumer = FakeConsumer(range(10), per_call=3)
    records = asyncio.run(collect_batch(consumer, max_records=7, max_wait_ms=1000))
    assert records == list(range(7))
    assert consumer.calls == 3


def test_collect_batch_returns_partial_batch_when_window_expires():
    consumer = FakeConsumer(range(2), per_call=5)
    records = asyncio.run(collect_batch(consumer, max_records=100, max_wait_ms=50))
    assert records == [0, 1]


def test_collect_batch_stops_early_when_stop_is_set():
    async def scenario():
        stop = asyncio.Event()
        stop.set()
        return await collect_batch(FakeConsumer(range(5), 5), 100, 1000, stop)

    assert asyncio.run(scenario()) == []


def test_build_bulk_operations_pairs_action_and_source():
    ops = build_bulk_operations("idx", {"a": {"x": 1}, "b": {"x": 2}})
    assert ops == [
        {"index": {"_index": "idx", "_id": "a"}}, {"x": 1},
        {"index": {"_index": "idx", "_id": "b"}}, {"x": 2},
    ]


def test_chunk_to_document_drops_message_type_and_stamps_indexed_at():
    chunk = Chunk(chunk_id="c1", job_id="j", filename="f.pdf", kind="text", seq=0, text="t")
    doc = chunk_to_document(chunk, datetime(2026, 1, 1, tzinfo=UTC))
    assert "type" not in doc
    assert doc["indexed_at"] == "2026-01-01T00:00:00+00:00"


def test_classify_bulk_response_splits_retryable_and_permanent():
    response = {
        "errors": True,
        "items": [
            {"index": {"_id": "ok", "status": 201}},
            {"index": {"_id": "busy", "status": 429, "error": "es_rejected_execution_exception"}},
            {"index": {"_id": "down", "status": 503, "error": "unavailable"}},
            {"index": {"_id": "bad", "status": 400, "error": "mapper_parsing_exception"}},
        ],
    }
    outcome = classify_bulk_response(response)
    assert set(outcome.retryable) == {"busy", "down"}
    assert set(outcome.permanent) == {"bad"}


def test_classify_bulk_response_without_errors_is_empty():
    outcome = classify_bulk_response({"errors": False, "items": [{"index": {"_id": "a", "status": 201}}]})
    assert not outcome.retryable and not outcome.permanent
