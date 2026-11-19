#!/usr/bin/env bash
# Test de panne rejoué pour le rapport : démarrage, panne brutale de Spark, reprise, intégrité.
set -u
cd "$(dirname "$0")/.."
OUT=rapport/donnees
docker compose start spark-streaming producer >/dev/null 2>&1
date -u +%Y-%m-%dT%H:%M:%SZ > $OUT/panne_debut.txt
sleep 240
date -u +%Y-%m-%dT%H:%M:%SZ > $OUT/panne_kill.txt
docker compose kill spark-streaming >/dev/null 2>&1
sleep 45
date -u +%Y-%m-%dT%H:%M:%SZ > $OUT/panne_restart.txt
docker compose start spark-streaming >/dev/null 2>&1
sleep 210
docker compose logs --since 5m spark-streaming 2>&1 | grep -E "recover|start\]" > $OUT/panne_journal.txt
docker compose exec -T dashboard python scripts/check_integrity.py > $OUT/panne_integrite.txt 2>&1
date -u +%Y-%m-%dT%H:%M:%SZ > $OUT/panne_fin.txt
docker compose stop producer spark-streaming >/dev/null 2>&1
echo termine
