"""Helpers de Kafka (aiokafka): creación de clientes con reintentos y DLQ."""

import asyncio
import logging
from collections.abc import Callable
from typing import TypeVar

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from pydantic import BaseModel

from pdfproc.config import Settings
from pdfproc.schemas import DeadLetter

log = logging.getLogger(__name__)

# Las tablas grandes pueden superar el 1 MB por defecto de Kafka; el broker
# está configurado con message.max.bytes acorde (ver docker-compose.yml).
MAX_MESSAGE_BYTES = 10 * 1024 * 1024

ClientT = TypeVar("ClientT", AIOKafkaProducer, AIOKafkaConsumer)


def encode(model: BaseModel) -> bytes:
    return model.model_dump_json().encode()


def _encode_key(key: str | None) -> bytes | None:
    return key.encode() if key is not None else None


async def _start_with_retry(
    factory: Callable[[], ClientT], name: str, attempts: int = 30, delay: float = 2.0
) -> ClientT:
    for attempt in range(1, attempts + 1):
        client = factory()
        try:
            await client.start()
            log.info("%s conectado a Kafka", name)
            return client
        except KafkaConnectionError as exc:
            await client.stop()
            log.warning("%s: Kafka no disponible (%d/%d): %s", name, attempt, attempts, exc)
            await asyncio.sleep(delay)
    raise RuntimeError(f"{name}: no se pudo conectar a Kafka")


async def start_producer(settings: Settings) -> AIOKafkaProducer:
    return await _start_with_retry(
        lambda: AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            compression_type="gzip",
            linger_ms=20,
            max_request_size=MAX_MESSAGE_BYTES,
            key_serializer=_encode_key,
        ),
        "producer",
    )


async def start_consumer(
    settings: Settings, topic: str, group_id: str, **kwargs
) -> AIOKafkaConsumer:
    return await _start_with_retry(
        lambda: AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_partition_fetch_bytes=MAX_MESSAGE_BYTES,
            fetch_max_bytes=MAX_MESSAGE_BYTES * 5,
            **kwargs,
        ),
        f"consumer[{group_id}]",
    )


async def send_to_dlq(
    producer: AIOKafkaProducer,
    settings: Settings,
    *,
    stage: str,
    source_topic: str,
    key: str | None,
    payload: bytes | str,
    error: str,
) -> None:
    if isinstance(payload, bytes):
        payload = payload.decode(errors="replace")
    letter = DeadLetter(
        stage=stage, source_topic=source_topic, key=key, payload=payload, error=error
    )
    await producer.send_and_wait(settings.topic_dlq, encode(letter), key=key)
    log.error("Mensaje enviado a DLQ (stage=%s key=%s): %s", stage, key, error)
