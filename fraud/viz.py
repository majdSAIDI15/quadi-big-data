"""Rendu visuel du dashboard : graphes PyVis et graphiques Plotly.

Encodage (palette de référence, mode sombre) :
  - couleur  = niveau de risque (palette « status » réservée, toujours avec son libellé) ;
  - forme    = rôle du compte dans le schéma (origine, pivot, mule…) ;
  - épaisseur d'arête = montant (échelle log).
"""
from __future__ import annotations

import math

import pandas as pd
import plotly.graph_objects as go
from pyvis.network import Network

# Surfaces & encres (mode sombre)
SURFACE = "#1a1a19"
PAGE = "#0d0d0d"
INK = "#f0efec"
INK_2 = "#c3c2b7"
MUTED = "#898781"
GRID = "#2c2c2a"
AXIS = "#383835"

# Niveaux de risque -> palette status (LOW reste neutre : simple information)
LEVEL_COLORS = {"LOW": MUTED, "MEDIUM": "#fab219", "HIGH": "#ec835a", "CRITICAL": "#d03b3b"}
LEVEL_ORDER = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
LEVEL_RANK = {lvl: i for i, lvl in enumerate(LEVEL_ORDER)}
LEVEL_LABEL = {"LOW": "Faible", "MEDIUM": "Moyen", "HIGH": "Élevé", "CRITICAL": "Critique"}

# Catégories (ordre fixe de la palette catégorielle, mode sombre)
SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#0ca30c", "#9085e9", "#e66767"]
TYPE_COLORS = dict(zip(["CYCLE", "CYCLE_LONG", "MULE", "SCATTER_GATHER", "PIVOT", "CLUSTER"], SERIES))
TYPE_LABEL = {"CYCLE": "Carrousel (3)", "CYCLE_LONG": "Cycle long (4-5)", "MULE": "Compte mule",
              "SCATTER_GATHER": "Scatter-gather", "PIVOT": "Compte pivot", "CLUSTER": "Réseau (cluster)"}

ROLE_SHAPES = {"ORIGINE": "star", "PIVOT": "diamond", "MULE": "triangle", "SOURCE": "dot",
               "DESTINATION": "square", "RELAIS": "dot", "MEMBRE": "dot", "CONTEXTE": "dot", "FOCUS": "star"}


def fmt_mad(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ") + " MAD"


# --------------------------------------------------------------------------- PyVis
def network_html(edges: pd.DataFrame, vertices: pd.DataFrame | None = None, height: int = 520,
                 focus: str | None = None, physics: bool = True) -> str:
    """edges : src, dst, montant, event_time, type_operation, [risk_level]
    vertices : account_id, role, risk_level, [pagerank]"""
    net = Network(height=f"{height}px", width="100%", directed=True, bgcolor=SURFACE, font_color=INK,
                  cdn_resources="in_line")
    node_info: dict[str, dict] = {}
    if vertices is not None and not vertices.empty:
        for r in vertices.itertuples(index=False):
            cur = node_info.get(r.account_id)
            lvl = getattr(r, "risk_level", None) or "LOW"
            role = getattr(r, "role", None) or "MEMBRE"
            if cur is None or LEVEL_RANK.get(lvl, 0) > LEVEL_RANK.get(cur["level"], 0):
                node_info[r.account_id] = {"level": lvl, "role": role,
                                           "pagerank": getattr(r, "pagerank", None)}
            elif role in ("PIVOT", "MULE", "ORIGINE"):
                cur["role"] = role
    accounts = set(edges["src"]).union(edges["dst"]) if not edges.empty else set(node_info)
    for acc in sorted(accounts):
        info = node_info.get(acc)
        if acc == focus:
            role, color, level = "FOCUS", "#3987e5", None
        elif info:
            role, level = info["role"], info["level"]
            color = LEVEL_COLORS.get(level, MUTED)
        else:
            role, color, level = "CONTEXTE", "#52514e", None
        pr = (info or {}).get("pagerank")
        size = 14 + (18 * pr if isinstance(pr, (int, float)) and not math.isnan(pr) else 0)
        if role in ("PIVOT", "ORIGINE", "MULE", "FOCUS"):
            size += 6
        title = f"{acc}\nRôle : {role.lower()}"
        if level:
            title += f"\nRisque : {LEVEL_LABEL.get(level, level)}"
        if isinstance(pr, (int, float)) and not math.isnan(pr):
            title += f"\nPageRank : top {100 * (1 - pr):.1f} %"
        net.add_node(acc, label=acc, color={"background": color, "border": SURFACE,
                                            "highlight": {"background": color, "border": INK}},
                     shape=ROLE_SHAPES.get(role, "dot"), size=size, title=title,
                     font={"color": INK, "size": 13, "strokeWidth": 3, "strokeColor": SURFACE},
                     borderWidth=2)
    for r in edges.itertuples(index=False):
        amount = float(r.montant)
        lvl = getattr(r, "risk_level", None)
        color = LEVEL_COLORS.get(lvl, MUTED) if lvl and lvl != "LOW" else MUTED
        when = pd.Timestamp(r.event_time)
        when_s = when.tz_convert("UTC").strftime("%H:%M:%S") if when.tzinfo else when.strftime("%H:%M:%S")
        net.add_edge(r.src, r.dst, value=max(1.0, math.log10(max(amount, 10)) - 2),
                     title=f"{fmt_mad(amount)}\n{when_s} UTC\n{getattr(r, 'type_operation', '') or ''}",
                     color={"color": color, "highlight": INK, "opacity": 0.9}, arrows="to",
                     smooth={"type": "curvedCW", "roundness": 0.15})
    net.set_options(f"""{{
      "physics": {{"enabled": {str(physics).lower()}, "solver": "forceAtlas2Based",
                   "forceAtlas2Based": {{"gravitationalConstant": -60, "springLength": 120}},
                   "stabilization": {{"iterations": 150}}}},
      "edges": {{"scaling": {{"min": 1, "max": 6}}, "arrows": {{"to": {{"scaleFactor": 0.6}}}}}},
      "interaction": {{"hover": true, "tooltipDelay": 80, "navigationButtons": false}},
      "nodes": {{"shadow": false}}
    }}""")
    html = net.generate_html()
    html = html.replace("<body>", f"<body style='margin:0;background:{SURFACE}'>", 1)
    return html.replace("</head>", "<style>#mynetwork{border:none !important;}</style></head>", 1)


# --------------------------------------------------------------------------- Plotly
def _layout(fig: go.Figure, title: str | None = None, height: int = 300, y_title: str = "") -> go.Figure:
    fig.update_layout(
        title={"text": title, "font": {"size": 15, "color": INK}, "x": 0, "xanchor": "left"} if title else None,
        height=height, margin={"l": 8, "r": 16, "t": 48 if title else 12, "b": 8},
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=SURFACE,
        font={"family": "system-ui, -apple-system, Segoe UI, sans-serif", "color": INK_2, "size": 12},
        legend={"orientation": "h", "yanchor": "top", "y": -0.16, "xanchor": "left", "x": 0,
                "font": {"color": INK_2}},
        hoverlabel={"bgcolor": PAGE, "bordercolor": AXIS, "font": {"color": INK}},
        bargap=0.25,
    )
    fig.update_xaxes(gridcolor=GRID, linecolor=AXIS, tickfont={"color": MUTED}, showgrid=False, automargin=True)
    fig.update_yaxes(gridcolor=GRID, linecolor=AXIS, tickfont={"color": MUTED}, zeroline=False, automargin=True,
                     title={"text": y_title, "font": {"color": MUTED}})
    return fig


def stacked_bars(df: pd.DataFrame, x: str, stack: str, order: list[str], colors: dict[str, str],
                 labels: dict[str, str], title: str, y_title: str = "", height: int = 300) -> go.Figure:
    fig = go.Figure()
    for key in order:
        sub = df[df[stack] == key]
        if sub.empty:
            continue
        fig.add_bar(x=sub[x], y=sub["n"], name=labels.get(key, key), marker_color=colors[key],
                    marker_line={"width": 2, "color": SURFACE}, marker_cornerradius=4,
                    hovertemplate=f"{labels.get(key, key)} : %{{y}}<extra></extra>")
    fig.update_layout(barmode="stack", hovermode="x unified")
    return _layout(fig, title, height, y_title)


def line(df: pd.DataFrame, x: str, y: str, title: str, y_title: str = "", color: str = SERIES[0],
         height: int = 260, fmt: str = ",.0f", fill: bool = False) -> go.Figure:
    fig = go.Figure(go.Scatter(x=df[x], y=df[y], mode="lines", line={"width": 2, "color": color},
                               fill="tozeroy" if fill else None,
                               fillcolor="rgba(57,135,229,0.12)" if fill else None,
                               hovertemplate=f"%{{y:{fmt}}}<extra></extra>"))
    fig.update_layout(hovermode="x unified", showlegend=False)
    return _layout(fig, title, height, y_title)


def hbar(labels: list[str], values: list[float], title: str, colors: list[str] | None = None,
         height: int = 320, fmt: str = ",.0f", x_title: str = "") -> go.Figure:
    fig = go.Figure(go.Bar(x=values, y=labels, orientation="h",
                           marker_color=colors or SERIES[0], marker_cornerradius=4,
                           marker_line={"width": 2, "color": SURFACE},
                           hovertemplate=f"%{{y}} : %{{x:{fmt}}}<extra></extra>"))
    fig.update_layout(showlegend=False, yaxis={"autorange": "reversed"})
    fig = _layout(fig, title, height)
    fig.update_xaxes(title={"text": x_title, "font": {"color": MUTED}}, showgrid=True)
    return fig


def histogram(values: list[float], title: str, x_title: str, height: int = 260) -> go.Figure:
    fig = go.Figure(go.Histogram(x=values, nbinsx=30, marker_color=SERIES[0],
                                 marker_line={"width": 2, "color": SURFACE},
                                 hovertemplate="%{x} : %{y} alertes<extra></extra>"))
    fig.update_layout(showlegend=False)
    fig = _layout(fig, title, height, "alertes")
    fig.update_xaxes(title={"text": x_title, "font": {"color": MUTED}})
    return fig
