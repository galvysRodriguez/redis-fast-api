#!/usr/bin/env bash
# Envía un PDF y sigue el estado del job hasta que termine.
# Uso: scripts/submit.sh reporte.pdf [http://localhost:8000]
set -euo pipefail

FILE="${1:?Uso: $0 archivo.pdf [api_url]}"
API="${2:-http://localhost:8000}"

JOB_ID=$(curl -fsS -F "file=@${FILE};type=application/pdf" "${API}/reports" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')
echo "job_id: ${JOB_ID}"

while true; do
  STATUS_JSON=$(curl -fsS "${API}/reports/${JOB_ID}")
  STATUS=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"${STATUS_JSON}")
  echo "estado: ${STATUS}"
  case "${STATUS}" in
    completed|completed_with_errors|failed) echo "${STATUS_JSON}" | python3 -m json.tool; break ;;
  esac
  sleep 2
done
