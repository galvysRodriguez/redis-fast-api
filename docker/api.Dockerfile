FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY requirements/base.txt requirements/api.txt requirements/
RUN pip install --no-cache-dir -r requirements/api.txt

RUN useradd --uid 1000 --create-home app && mkdir -p /data/uploads && chown app:app /data/uploads
COPY pdfproc ./pdfproc
USER app

EXPOSE 8000
CMD ["uvicorn", "pdfproc.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
