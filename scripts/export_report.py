"""Export du rapport Markdown en PDF (diagrammes Mermaid rendus, graphiques inclus).

    docker run --rm -v "<projet>/docs:/docs" -v "<projet>/scripts:/s" \
        mcr.microsoft.com/playwright/python:v1.49.0-noble \
        sh -c "pip install -q playwright==1.49.0 markdown==3.7 && python /s/export_report.py"
"""
from __future__ import annotations

import os
import re
import sys

import markdown
from playwright.sync_api import sync_playwright

DOCS = os.environ.get("DOCS_DIR", "/docs")
SRC = os.path.join(DOCS, "rapport.md")
HTML = os.path.join(DOCS, "rapport.html")
PDF = os.path.join(DOCS, "rapport.pdf")

CSS = """
@page { size: A4; margin: 18mm 16mm 20mm 16mm; }
:root { --ink:#0E1726; --body:#2F3A4C; --muted:#6B7280; --accent:#2563B8; --line:#D6D0C3; --soft:#F3F1EC; }
html { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
body { font-family: 'Inter', 'Segoe UI', Arial, sans-serif; color: var(--body); font-size: 10.5pt; line-height: 1.5; }
h1 { font-family: 'Space Grotesk', Arial, sans-serif; color: var(--ink); font-size: 24pt; line-height: 1.15; margin: 0 0 4mm; }
h2 { font-family: 'Space Grotesk', Arial, sans-serif; color: var(--ink); font-size: 15pt; margin: 9mm 0 3mm;
     padding-bottom: 1.5mm; border-bottom: 1.5px solid var(--line); break-after: avoid; }
h3 { color: var(--ink); font-size: 12pt; margin: 6mm 0 2mm; break-after: avoid; }
p, li { margin: 0 0 2.2mm; }
strong { color: var(--ink); }
a { color: var(--accent); text-decoration: none; }
hr { border: none; border-top: 1px solid var(--line); margin: 6mm 0; }
table { border-collapse: collapse; width: 100%; margin: 3mm 0 5mm; font-size: 9.5pt; }
tr { break-inside: avoid; }
th { text-align: left; color: var(--ink); background: var(--soft); font-weight: 600; }
th, td { padding: 1.6mm 2.4mm; border-bottom: 1px solid var(--line); vertical-align: top; }
code { font-family: 'JetBrains Mono', Consolas, monospace; font-size: 8.8pt; background: var(--soft); padding: 0.3mm 1mm; border-radius: 2px; }
pre { background: var(--soft); padding: 3mm 4mm; border-radius: 4px; overflow: hidden; white-space: pre-wrap; break-inside: avoid; }
pre code { background: none; padding: 0; }
blockquote { margin: 3mm 0; padding: 2mm 5mm; border-left: 3px solid #B9400E; background: #FBF7F2; color: var(--ink); }
img { max-width: 100%; break-inside: avoid; }
.mermaid { text-align: center; margin: 4mm 0; break-inside: avoid; }
.cover-meta { color: var(--muted); }
"""

TEMPLATE = """<!doctype html><html lang="fr"><head><meta charset="utf-8">
<title>Rapport technique — Graph Analytics & blanchiment</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=Space+Grotesk:wght@500;700&display=swap">
<style>{css}</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script>
</head><body>{body}
<script>mermaid.initialize({{startOnLoad:true, theme:'neutral', flowchart:{{useMaxWidth:true}}}});</script>
</body></html>"""


def main() -> int:
    with open(SRC, encoding="utf-8") as f:
        md = f.read()
    md = md.replace("<!-- RESULTATS -->", "")

    def mermaid(m: re.Match) -> str:
        return f'<div class="mermaid">{m.group(1)}</div>'

    md = re.sub(r"```mermaid\n(.*?)```", mermaid, md, flags=re.S)
    body = markdown.markdown(md, extensions=["tables", "fenced_code", "sane_lists"])
    with open(HTML, "w", encoding="utf-8") as f:
        f.write(TEMPLATE.format(css=CSS, body=body))
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(f"file://{HTML}", wait_until="networkidle")
        page.wait_for_timeout(2500)
        page.pdf(path=PDF, format="A4", print_background=True, display_header_footer=True,
                 header_template="<span></span>",
                 footer_template=('<div style="width:100%;font-size:8px;color:#6B7280;padding:0 16mm;'
                                  'display:flex;justify-content:space-between;"><span>Mini-projet 17 — '
                                  'Graph Analytics &amp; blanchiment d’argent</span><span>'
                                  '<span class="pageNumber"></span> / <span class="totalPages"></span></span></div>'),
                 margin={"top": "18mm", "bottom": "20mm", "left": "16mm", "right": "16mm"})
        browser.close()
    os.remove(HTML)
    print("PDF :", PDF)
    return 0


if __name__ == "__main__":
    sys.exit(main())
