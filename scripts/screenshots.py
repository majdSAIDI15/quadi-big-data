"""Captures d'écran du dashboard (pour le README et le rapport).

    docker run --rm --network fraud-graph_default -v "<projet>/docs/img:/out" -v "<projet>/scripts:/s" \
        mcr.microsoft.com/playwright/python:v1.49.0-noble python /s/screenshots.py
"""
from __future__ import annotations

import os
import sys

from playwright.sync_api import sync_playwright

URL = os.environ.get("DASHBOARD_URL", "http://dashboard:8501")
OUT = os.environ.get("OUT_DIR", "/out")
TABS = ["Vue d'ensemble", "Alertes & investigation", "Explorateur de réseau", "Santé du pipeline",
        "Audit Delta Lake"]
FILES = ["dashboard_vue_ensemble", "dashboard_alertes", "dashboard_reseau", "dashboard_sante", "dashboard_audit"]


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1600, "height": 1000}, device_scale_factor=1.5)
        page.goto(URL, wait_until="networkidle", timeout=120_000)
        page.wait_for_selector("text=Graph Analytics", timeout=120_000)
        page.wait_for_timeout(12_000)  # graphes PyVis : stabilisation de la simulation physique
        for label, name in zip(TABS, FILES, strict=True):
            page.get_by_role("tab", name=label).click()
            page.wait_for_timeout(9_000)
            page.screenshot(path=os.path.join(OUT, f"{name}.png"), full_page=True)
            print("capture", name, flush=True)
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
