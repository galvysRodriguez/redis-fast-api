from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuración compartida por los tres servicios (api, extractor, indexer).

    Cada campo se puede sobrescribir con una variable de entorno en mayúsculas,
    p. ej. KAFKA_BOOTSTRAP_SERVERS o BATCH_MAX_RECORDS.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:29092"
    topic_ingest: str = "pdf.ingest"
    topic_chunks: str = "pdf.chunks"
    topic_dlq: str = "pdf.dlq"

    # Almacenamiento de los PDF subidos (volumen compartido api <-> extractor)
    upload_dir: str = "/data/uploads"
    max_upload_mb: int = 100

    # Elasticsearch
    elasticsearch_url: str = "http://localhost:9200"
    index_chunks: str = "report-chunks"
    index_jobs: str = "report-jobs"

    # Extractor
    unstructured_strategy: str = "fast"  # fast | hi_res | ocr_only | auto
    chunk_max_chars: int = 1500
    chunk_new_after_chars: int = 1200
    chunk_combine_under_chars: int = 300
    docling_do_ocr: bool = False
    docling_artifacts_path: str | None = None
    extractor_max_poll_interval_ms: int = 60 * 60 * 1000

    # Indexer (micro-batches)
    batch_max_records: int = 500
    batch_max_wait_ms: int = 2000
    bulk_max_retries: int = 5


@lru_cache
def get_settings() -> Settings:
    return Settings()
