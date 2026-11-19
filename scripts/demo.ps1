<#
.SYNOPSIS
  Pilote de la démo (Windows / PowerShell).

.EXAMPLE
  .\scripts\demo.ps1 start        # construit et démarre toute la plateforme, ouvre le dashboard
  .\scripts\demo.ps1 inject CARROUSEL
  .\scripts\demo.ps1 crash        # panne de Spark + redémarrage + contrôle d'intégrité
  .\scripts\demo.ps1 evaluate     # rappel / précision / latence -> docs\resultats.md
  .\scripts\demo.ps1 ablation     # étude d'ablation (arrête le producteur pendant la mesure)
  .\scripts\demo.ps1 benchmark    # montée en charge -> docs\benchmark.md
  .\scripts\demo.ps1 tests        # ruff + pytest dans le conteneur
  .\scripts\demo.ps1 logs         # journaux du job Spark (alertes)
  .\scripts\demo.ps1 stop         # arrêt (données conservées)
  .\scripts\demo.ps1 reset        # arrêt + suppression des données
#>
param(
    [Parameter(Position = 0)][string]$Action = "start",
    [Parameter(Position = 1)][string]$Arg = "CARROUSEL"
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$docs = "$((Get-Location).Path)\docs:/app/docs"

function Wait-Url($url, $label) {
    Write-Host "Attente de $label ..." -NoNewline
    for ($i = 0; $i -lt 90; $i++) {
        try { Invoke-WebRequest -UseBasicParsing -Uri $url -TimeoutSec 2 | Out-Null; Write-Host " prêt."; return }
        catch { Start-Sleep -Seconds 2; Write-Host "." -NoNewline }
    }
    Write-Host " délai dépassé."
}

switch ($Action) {
    "start" {
        docker compose up -d --build
        Wait-Url "http://localhost:8501" "le dashboard"
        Wait-Url "http://localhost:4040" "la Spark UI"
        Start-Process "http://localhost:8501"
        Write-Host "Dashboard : http://localhost:8501   Spark UI : http://localhost:4040"
        Write-Host "Kafka UI (optionnel) : docker compose --profile tools up -d kafka-ui -> http://localhost:8080"
    }
    "inject" {
        docker compose exec producer python financial_graph_producer.py --inject $Arg --tempo 0.05
    }
    "crash" {
        Write-Host "1) Panne brutale du job Spark (SIGKILL)" -ForegroundColor Yellow
        docker compose kill spark-streaming
        Start-Sleep -Seconds 20
        Write-Host "2) Redémarrage : reprise depuis les checkpoints" -ForegroundColor Yellow
        docker compose start spark-streaming
        Write-Host "   Attente de la reprise (90 s)..."
        Start-Sleep -Seconds 90
        docker compose logs --tail 40 spark-streaming | Select-String "recover|start\]|ALERTE"
        Write-Host "3) Contrôle d'intégrité du lakehouse" -ForegroundColor Yellow
        docker compose exec dashboard python scripts/check_integrity.py
    }
    "evaluate" {
        docker compose run --rm --no-deps -v $docs spark-streaming python scripts/evaluate.py
    }
    "ablation" {
        docker compose stop producer spark-streaming
        docker compose run --rm --no-deps -v $docs spark-streaming python scripts/evaluate.py --ablation --minutes 30
        docker compose start spark-streaming producer
    }
    "benchmark" {
        docker compose stop producer
        docker compose run --rm --no-deps -v $docs producer python scripts/benchmark.py --rates 25 50 100 150
        docker compose start producer
    }
    "tests" {
        docker compose run --rm --no-deps -e CHECKPOINT_DIR=/tmp/ckpt spark-streaming sh -c "ruff check . && pytest -q"
    }
    "logs" { docker compose logs -f spark-streaming | Select-String "ALERTE|slow-path|recover|start\]" }
    "stop" { docker compose stop }
    "reset" { docker compose down -v }
    default { Get-Help $PSCommandPath -Detailed }
}
