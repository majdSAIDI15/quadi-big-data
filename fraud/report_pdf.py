"""Fiche d'investigation PDF d'une alerte (export depuis le dashboard)."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
from fpdf import FPDF

FONT_DIR = "/usr/share/fonts/truetype/dejavu"
LEVEL_RGB = {"LOW": (137, 135, 129), "MEDIUM": (250, 178, 25), "HIGH": (236, 131, 90), "CRITICAL": (208, 59, 59)}
LEVEL_LABEL = {"LOW": "FAIBLE", "MEDIUM": "MOYEN", "HIGH": "ÉLEVÉ", "CRITICAL": "CRITIQUE"}


def _fmt(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ") + " MAD"


class _Pdf(FPDF):
    def __init__(self):
        super().__init__(format="A4")
        self.unicode = os.path.exists(os.path.join(FONT_DIR, "DejaVuSans.ttf"))
        if self.unicode:
            self.add_font("dv", "", os.path.join(FONT_DIR, "DejaVuSans.ttf"))
            self.add_font("dv", "B", os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf"))
        self.family = "dv" if self.unicode else "helvetica"
        self.set_auto_page_break(True, margin=15)

    def footer(self):
        self.set_y(-12)
        self.set_font(self.family, "", 8)
        self.set_text_color(137, 135, 129)
        self.cell(0, 6, f"Document confidentiel — usage interne LCB-FT — page {self.page_no()}", align="C")

    def heading(self, text: str, size: int = 12):
        self.set_font(self.family, "B", size)
        self.set_text_color(20, 20, 20)
        self.cell(0, 8, text, new_x="LMARGIN", new_y="NEXT")

    def para(self, text: str, size: int = 10):
        self.set_font(self.family, "", size)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 5.5, text if self.unicode else text.encode("latin-1", "replace").decode("latin-1"),
                        new_x="LMARGIN", new_y="NEXT")


def investigation_pdf(alert: dict, vertices: pd.DataFrame, edges: pd.DataFrame) -> bytes:
    pdf = _Pdf()
    pdf.add_page()
    level = alert.get("risk_level", "LOW")
    r, g, b = LEVEL_RGB.get(level, (137, 135, 129))

    pdf.set_fill_color(26, 26, 25)
    pdf.rect(0, 0, 210, 30, "F")
    pdf.set_xy(12, 8)
    pdf.set_font(pdf.family, "B", 16)
    pdf.set_text_color(240, 239, 236)
    pdf.cell(0, 8, "Fiche d'investigation — alerte de fraude")
    pdf.set_xy(12, 17)
    pdf.set_font(pdf.family, "", 10)
    pdf.cell(0, 6, f"{alert['alert_id']}   ·   générée le "
                   f"{datetime.now(timezone.utc).strftime('%d/%m/%Y %H:%M')} UTC")
    pdf.set_xy(160, 9)
    pdf.set_fill_color(r, g, b)
    pdf.set_text_color(11, 11, 11) if level == "MEDIUM" else pdf.set_text_color(255, 255, 255)
    pdf.set_font(pdf.family, "B", 11)
    pdf.cell(38, 12, f"{LEVEL_LABEL.get(level, level)} · {alert['risk_score']}", align="C", fill=True)
    pdf.set_y(36)

    pdf.heading("Synthèse")
    detected = pd.Timestamp(alert["detected_at"]).tz_convert("UTC")
    rows = [
        ("Typologie", alert["alert_type"]),
        ("Détectée le", detected.strftime("%d/%m/%Y %H:%M:%S UTC")),
        ("Période du schéma", f"{pd.Timestamp(alert['window_start']).strftime('%H:%M:%S')} → "
                              f"{pd.Timestamp(alert['window_end']).strftime('%H:%M:%S')} UTC"),
        ("Montant total", _fmt(alert["total_amount"])),
        ("Comptes impliqués", str(len(alert["accounts"]))),
        ("Latence de détection", f"{alert['latency_ms'] / 1000:.1f} s".replace(".", ",")
         if pd.notna(alert.get("latency_ms")) else "—"),
        ("Moteur", alert.get("detector") or "—"),
    ]
    pdf.set_font(pdf.family, "", 10)
    for k, v in rows:
        pdf.set_text_color(82, 81, 78)
        pdf.cell(50, 6, k)
        pdf.set_text_color(11, 11, 11)
        pdf.cell(0, 6, str(v), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    pdf.heading("Pourquoi cette alerte ?")
    pdf.para(alert.get("explanation") or "")
    pdf.ln(2)

    if vertices is not None and not vertices.empty:
        pdf.heading("Comptes et rôles")
        v = vertices.drop_duplicates("account_id")
        pdf.para(", ".join(f"{a} ({str(role).lower()})" for a, role in zip(v["account_id"], v["role"])))
        pdf.ln(2)

    if edges is not None and not edges.empty:
        pdf.heading("Transactions du schéma")
        pdf.set_font(pdf.family, "B", 8.5)
        pdf.set_fill_color(240, 239, 236)
        widths = [44, 24, 24, 34, 24, 36]
        for w, head in zip(widths, ["Transaction", "Émetteur", "Bénéficiaire", "Montant", "Heure UTC", "Opération"]):
            pdf.cell(w, 6, head, fill=True)
        pdf.ln()
        pdf.set_font(pdf.family, "", 8.5)
        for e in edges.sort_values("event_time").head(40).itertuples(index=False):
            vals = [e.transaction_id, e.src, e.dst, _fmt(e.montant),
                    pd.Timestamp(e.event_time).strftime("%H:%M:%S"), str(e.type_operation or "")[:20]]
            for w, val in zip(widths, vals):
                pdf.cell(w, 5.5, str(val))
            pdf.ln()
        pdf.ln(3)

    pdf.heading("Décision de l'analyste")
    box = "☐" if pdf.unicode else "[ ]"
    pdf.para(f"{box} Classement sans suite      {box} Surveillance renforcée      "
          f"{box} Escalade conformité      {box} Déclaration de soupçon")
    pdf.ln(2)
    pdf.para("Commentaire : ____________________________________________________________________")
    pdf.para("Analyste : ______________________     Date : ____ / ____ / ________     Visa : __________")
    return bytes(pdf.output())
