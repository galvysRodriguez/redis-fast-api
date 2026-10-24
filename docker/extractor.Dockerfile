FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HOME=/models/hf \
    NLTK_DATA=/models/nltk_data \
    DOCLING_ARTIFACTS_PATH=/models/docling
WORKDIR /app

# poppler/tesseract: necesarios para UNSTRUCTURED_STRATEGY=hi_res y OCR.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 libglib2.0-0 libmagic1 poppler-utils tesseract-ocr tesseract-ocr-spa \
    && rm -rf /var/lib/apt/lists/*

# torch CPU primero para no arrastrar ~5 GB de CUDA.
RUN pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu
COPY requirements/base.txt requirements/extractor.txt requirements/
RUN pip install --no-cache-dir -r requirements/extractor.txt

RUN useradd --uid 1000 --create-home app \
    && mkdir -p /data/uploads /models && chown -R app:app /data/uploads /models
USER app

# Modelos horneados en la imagen: el worker arranca sin descargar nada.
RUN docling-tools models download layout tableformer easyocr -o "$DOCLING_ARTIFACTS_PATH" \
    && python -c "import nltk; [nltk.download(p, download_dir='$NLTK_DATA') for p in ('punkt_tab', 'averaged_perceptron_tagger_eng')]"

COPY --chown=app:app pdfproc ./pdfproc
CMD ["python", "-m", "pdfproc.extractor.worker"]
