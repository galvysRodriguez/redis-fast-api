# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_RETRIES=10 PIP_DEFAULT_TIMEOUT=120
WORKDIR /app

# poppler/tesseract: necesarios para UNSTRUCTURED_STRATEGY=hi_res y OCR.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 libglib2.0-0 libmagic1 poppler-utils tesseract-ocr tesseract-ocr-spa \
    && rm -rf /var/lib/apt/lists/*

# torch CPU primero para no arrastrar ~5 GB de CUDA. La caché de pip persiste
# entre builds: si la red corta una descarga, el reintento no empieza de cero.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
COPY requirements/base.txt requirements/extractor.txt requirements/
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements/extractor.txt
# Unstructured necesita este modelo de spaCy; si falta intenta bajarlo a
# site-packages en runtime, falla sin permisos y devuelve 0 elementos sin error.
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m spacy download en_core_web_sm

RUN useradd --uid 1000 --create-home app \
    && mkdir -p /data/uploads /models && chown -R app:app /data/uploads /models
USER app

# Los modelos de Docling se descargan al arrancar en el volumen /models
# (ver ensure_models), así un corte de red no rompe el build.
ENV HF_HOME=/models/hf HF_HUB_DISABLE_XET=1 DOCLING_ARTIFACTS_PATH=/models/docling

COPY --chown=app:app pdfproc ./pdfproc
CMD ["python", "-m", "pdfproc.extractor.worker"]
