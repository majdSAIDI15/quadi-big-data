"""Captures du dashboard pour le rapport (rafraîchissement coupé, période élargie).

    docker run --rm --network fraud-graph_default -v "<projet>/rapport:/r" \
        mcr.microsoft.com/playwright/python:v1.49.0-noble python /r/captures_rapport.py
"""
from __future__ import annotations

import os

from playwright.sync_api import sync_playwright

URL = os.environ.get("DASHBOARD_URL", "http://dashboard:8501")
OUT = "/r/figures/captures"


def choisir(page, libelle: str, option: str) -> None:
    boite = page.locator('[data-testid="stSelectbox"]').filter(has_text=libelle).first
    boite.click()
    page.get_by_role("option").filter(has_text=option).first.click()
    page.wait_for_timeout(4_000)


def main() -> None:
    os.makedirs(OUT, exist_ok=True)
    with sync_playwright() as p:
        nav = p.chromium.launch()
        page = nav.new_page(viewport={"width": 1600, "height": 1100}, device_scale_factor=1.5)
        page.goto(URL, wait_until="networkidle", timeout=120_000)
        page.wait_for_selector("text=Graph Analytics", timeout=120_000)
        page.wait_for_timeout(6_000)
        page.get_by_text("Rafraîchissement temps réel").click()  # plus de rerun pendant les captures
        page.wait_for_timeout(2_000)
        choisir(page, "Période", os.environ.get("PERIODE", "6 dernières heures"))
        page.wait_for_timeout(15_000)  # stabilisation de la simulation physique PyVis
        page.screenshot(path=f"{OUT}/dashboard_vue_ensemble.png")
        print("vue d'ensemble", flush=True)

        page.get_by_role("tab", name="Alertes & investigation").click()
        page.wait_for_timeout(6_000)
        try:
            choisir(page, "Alerte à investiguer", os.environ.get("ALERTE", "Compte pivot"))
        except Exception as exc:  # garde l'alerte par défaut
            print("sélection d'alerte impossible :", exc)
        page.wait_for_timeout(12_000)
        page.screenshot(path=f"{OUT}/dashboard_alertes.png")
        page.mouse.wheel(0, 900)
        page.wait_for_timeout(3_000)
        page.screenshot(path=f"{OUT}/dashboard_alertes_bas.png")
        print("alertes", flush=True)

        for onglet, nom in (("Explorateur de réseau", "dashboard_reseau"), ("Santé du pipeline", "dashboard_sante"),
                            ("Audit Delta Lake", "dashboard_audit")):
            page.get_by_role("tab", name=onglet).click()
            page.wait_for_timeout(14_000)
            page.screenshot(path=f"{OUT}/{nom}.png")
            print(nom, flush=True)
        nav.close()


if __name__ == "__main__":
    main()
