FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY requirements/base.txt requirements/indexer.txt requirements/
RUN pip install --no-cache-dir -r requirements/indexer.txt

RUN useradd --uid 1000 --create-home app
COPY pdfproc ./pdfproc
USER app

CMD ["python", "-m", "pdfproc.indexer.worker"]
