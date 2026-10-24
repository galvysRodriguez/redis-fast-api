"""Piezas puras del indexador: acumulación de micro-batches y bulk de Elasticsearch."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pdfproc.schemas import Chunk

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, ConsumerRecord


async def collect_batch(
    consumer: "AIOKafkaConsumer",
    max_records: int,
    max_wait_ms: int,
    stop: asyncio.Event | None = None,
) -> list["ConsumerRecord"]:
    """Acumula registros hasta llenar `max_records` o agotar `max_wait_ms`.

    `getmany` por sí solo devuelve en cuanto hay *algún* dato; aquí se sigue
    leyendo hasta cerrar la ventana para formar lotes de tamaño útil. El orden
    por partición se conserva.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_wait_ms / 1000
    buffer: list[ConsumerRecord] = []
    while len(buffer) < max_records and not (stop and stop.is_set()):
        remaining_ms = int((deadline - loop.time()) * 1000)
        if remaining_ms <= 0:
            break
        fetched = await consumer.getmany(
            timeout_ms=remaining_ms, max_records=max_records - len(buffer)
        )
        for records in fetched.values():
            buffer.extend(records)
    return buffer


def chunk_to_document(chunk: Chunk, indexed_at: datetime) -> dict[str, Any]:
    doc = chunk.model_dump(mode="json", exclude={"type"})
    doc["indexed_at"] = indexed_at.isoformat()
    return doc


def build_bulk_operations(index: str, docs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    for doc_id, source in docs.items():
        operations.append({"index": {"_index": index, "_id": doc_id}})
        operations.append(source)
    return operations


@dataclass
class BulkOutcome:
    retryable: dict[str, str] = field(default_factory=dict)
    permanent: dict[str, str] = field(default_factory=dict)


def is_retryable_status(status: int) -> bool:
    return status == 429 or status >= 500


def classify_bulk_response(response: dict[str, Any]) -> BulkOutcome:
    """Separa los items fallidos en reintentables (429/5xx) y permanentes (4xx)."""
    outcome = BulkOutcome()
    if not response.get("errors"):
        return outcome
    for item in response.get("items", []):
        result = next(iter(item.values()))
        status = result.get("status", 500)
        if status < 300:
            continue
        error = str(result.get("error", f"HTTP {status}"))
        target = outcome.retryable if is_retryable_status(status) else outcome.permanent
        target[result["_id"]] = error
    return outcome
