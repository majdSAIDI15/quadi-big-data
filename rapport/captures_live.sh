#!/usr/bin/env bash
# Captures du dashboard pendant un fonctionnement réel : Spark, puis le producteur, puis Playwright.
set -u
cd "$(dirname "$0")/.."
P="$(pwd -W 2>/dev/null || pwd)"
docker compose start spark-streaming >/dev/null 2>&1
for i in $(seq 1 60); do
  docker compose logs --since 10m spark-streaming 2>&1 | grep -q "\[start\]" && break
  sleep 10
done
echo "spark prêt"
docker compose start producer >/dev/null 2>&1
sleep 420
MSYS_NO_PATHCONV=1 docker run --rm --network fraud-graph_default -v "$P/rapport:/r" \
  -e PERIODE="15 dernières minutes" -e ALERTE="Carrousel" \
  mcr.microsoft.com/playwright/python:v1.49.0-noble \
  sh -c "pip install -q playwright==1.49.0 >/dev/null 2>&1; python /r/captures_rapport.py"
docker compose stop producer spark-streaming >/dev/null 2>&1
echo termine
