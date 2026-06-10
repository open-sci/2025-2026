"""
Generate all pre-computed Plotly JSON files needed by the website.

Run from: bloom/website/
    python generate_charts.py

Outputs:
    assets/data/sunburst_data.json        – 12 sunburst figures (6 inst × 2 dirs)
    assets/data/reciprocity_scatter.json  – reciprocity scatter plot
"""

import re
import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path

# ── Shared config ─────────────────────────────────────────────────────────────

BASE_PATH = Path("../map_of_italian_science/data/citation_counts")

INSTITUTIONS = ["UNIBO", "UNIMI", "UNIPD", "UNITO", "UPO", "SNS"]

INST_LABELS = {
    "UNIBO": "University of Bologna",
    "UNIMI": "University of Milan",
    "UNIPD": "University of Padua",
    "UNITO": "University of Turin",
    "UPO":   "University of Eastern Piedmont",
    "SNS":   "Scuola Normale Superiore",
}

OUT_DIR = Path("assets/data")

# ═══════════════════════════════════════════════════════════════════════════════
# SUNBURST
# ═══════════════════════════════════════════════════════════════════════════════

DIR_LABELS = {
    "incoming": "Incoming citations",
    "outgoing": "Outgoing citations",
}

DIR_COLORS = {
    "incoming": "#B7990D",   # gold
    "outgoing": "#320E3B",   # dark purple
}

TOP_COUNTRIES = 20
TOP_ORGS = 50


# ── Colour helpers ────────────────────────────────────────────────────────────

def _to_rgb(color):
    color = str(color).strip()
    if color.startswith("rgb"):
        nums = re.findall(r"[\d.]+", color)
        return int(float(nums[0])), int(float(nums[1])), int(float(nums[2]))
    h = color.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _tint(color, factor):
    r, g, b = _to_rgb(color)
    return "#{:02x}{:02x}{:02x}".format(
        int(r + (255 - r) * factor),
        int(g + (255 - g) * factor),
        int(b + (255 - b) * factor),
    )


def _wrap_label(name, max_chars=14):
    """Break a name into lines of at most max_chars using <br> at word boundaries."""
    words = name.split()
    lines, current, length = [], [], 0
    for word in words:
        if length + len(word) + (1 if current else 0) > max_chars and current:
            lines.append(" ".join(current))
            current, length = [word], len(word)
        else:
            current.append(word)
            length += len(word) + (1 if len(current) > 1 else 0)
    if current:
        lines.append(" ".join(current))
    return "<br>".join(lines)


def build_country_color_map(agg_master):
    all_countries = sorted(agg_master["country_name"].dropna().unique())
    palette = px.colors.qualitative.Pastel + px.colors.qualitative.Set3
    return {country: palette[i % len(palette)] for i, country in enumerate(all_countries)}


# ── Data loading ──────────────────────────────────────────────────────────────

def load_institution_sunburst(inst):
    base = BASE_PATH / inst
    inc = pd.read_csv(base / "citation_counts_organizations_incoming.csv")
    out = pd.read_csv(base / "citation_counts_organizations_outgoing.csv")
    inc["direction"] = "incoming"
    out["direction"] = "outgoing"
    inc["italian_institution"] = inst
    out["italian_institution"] = inst
    return inc, out


def load_all_sunburst():
    frames = []
    for inst in INSTITUTIONS:
        try:
            inc, out = load_institution_sunburst(inst)
            frames.extend([inc, out])
        except FileNotFoundError as e:
            print(f"  Warning: {e}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ── Sunburst builders ─────────────────────────────────────────────────────────

def make_sunburst_data(agg_master, inst, direction, country_color_map):
    df_dir = agg_master[
        (agg_master["italian_institution"] == inst) &
        (agg_master["direction"] == direction)
    ].copy()
    if df_dir.empty:
        return None

    wcol = None
    for c in ("counts", "count", "derived_counts"):
        if c in df_dir.columns:
            wcol = c
            break
    if wcol is None:
        df_dir["derived_counts"] = 1
        wcol = "derived_counts"

    label_inst = INST_LABELS.get(inst, inst)

    org_agg = (
        df_dir
        .groupby(["country_name", "legal_name"])[wcol]
        .sum()
        .reset_index(name="citations")
    )

    country_totals = (
        org_agg.groupby("country_name")["citations"]
        .sum()
        .reset_index(name="country_total")
        .sort_values("country_total", ascending=False)
        .head(TOP_COUNTRIES)
    )
    grand_total = country_totals["country_total"].sum()
    top_country_set = set(country_totals["country_name"])
    org_agg = org_agg[org_agg["country_name"].isin(top_country_set)]

    top_orgs_idx = (
        org_agg
        .sort_values("citations", ascending=False)
        .groupby("country_name")
        .head(TOP_ORGS)
        [["country_name", "legal_name"]]
    )
    org_agg = org_agg.merge(top_orgs_idx, on=["country_name", "legal_name"])

    ids, labels, parents, values, customdata, colors = [], [], [], [], [], []

    ids.append("root")
    labels.append(_wrap_label(label_inst, max_chars=14))
    parents.append("")
    values.append(0)
    customdata.append(f"<b>{label_inst}</b>")
    colors.append("rgba(0,0,0,0)")

    for _, crow in country_totals.iterrows():
        country = crow["country_name"]
        ctotal = int(crow["country_total"])
        country_pct = ctotal / grand_total * 100
        base_color = country_color_map.get(country, "#cccccc")
        org_color = _tint(base_color, 0.30)

        ids.append(country)
        labels.append(country)
        parents.append("root")
        values.append(0)
        customdata.append(
            f"<b>{country}</b><br>"
            f"Citations: {ctotal:,}<br>"
            f"Share of {label_inst}: {country_pct:.1f}%"
        )
        colors.append(base_color)

        country_orgs = (
            org_agg[org_agg["country_name"] == country]
            .sort_values("citations", ascending=False)
        )
        for _, orow in country_orgs.iterrows():
            org = orow["legal_name"]
            ocites = int(orow["citations"])
            pct_c = ocites / ctotal * 100
            pct_g = ocites / grand_total * 100

            ids.append(f"{country}__{org}")
            labels.append(org)
            parents.append(country)
            values.append(ocites)
            customdata.append(
                f"<b>{org}</b><br>"
                f"Country: {country}<br>"
                f"Citations: {ocites:,}<br>"
                f"% of country: {pct_c:.1f}%<br>"
                f"% of {label_inst}: {pct_g:.1f}%"
            )
            colors.append(org_color)

    return dict(ids=ids, labels=labels, parents=parents, values=values,
                customdata=customdata, colors=colors)


def build_sunburst_figure(agg_master, inst, direction, country_color_map):
    d = make_sunburst_data(agg_master, inst, direction, country_color_map)
    label_inst = INST_LABELS.get(inst, inst)
    dir_label = DIR_LABELS.get(direction, direction)
    dir_badge = "▼ Incoming" if direction == "incoming" else "▲ Outgoing"
    badge_color = DIR_COLORS.get(direction, "#555")

    fig = go.Figure()
    if d is None:
        return fig

    fig.add_trace(go.Sunburst(
        ids=d["ids"],
        labels=d["labels"],
        parents=d["parents"],
        values=d["values"],
        customdata=d["customdata"],
        texttemplate="%{label}",
        hovertemplate="%{customdata}<extra></extra>",
        branchvalues="remainder",
        rotation=-90,
        sort=True,
        insidetextorientation="horizontal",
        leaf=dict(opacity=0.88),
        marker=dict(
            colors=d["colors"],
            line=dict(width=0.3, color="white"),
        ),
    ))

    fig.update_layout(
        title=dict(
            text=(
                f"<b>Country → Organisation Citation Structure</b>  ·  {label_inst}"
                f"<br><sup>Top {TOP_COUNTRIES} countries · top {TOP_ORGS} orgs per country"
                f" · {dir_label} · click a country to reveal organisations</sup>"
            ),
            font=dict(size=14, family="Playfair Display, serif", color="#FFFFFF"),
            x=0.0,
        ),
        annotations=[dict(
            xref="paper", yref="paper", x=0.0, y=1.07,
            text=(
                f'<span style="background:{badge_color};color:white;'
                f'padding:2px 8px;border-radius:4px;font-size:12px">'
                f"{dir_badge}</span>"
            ),
            showarrow=False, xanchor="left",
        )],
        paper_bgcolor="#2A0C32",
        plot_bgcolor="#2A0C32",
        font=dict(family="Inter, sans-serif", color="#FFFFFF", size=12),
        margin=dict(l=10, r=10, t=90, b=20),
        height=650,
        showlegend=False,
    )
    return fig


def generate_sunburst():
    print("\n── Sunburst ─────────────────────────────────────────────────────────")
    print("Loading organisation citation data…")
    agg_master = load_all_sunburst()
    if agg_master.empty:
        print("ERROR: No data loaded — check BASE_PATH.")
        raise SystemExit(1)

    print(f"  Loaded {len(agg_master):,} rows across {agg_master['italian_institution'].nunique()} institutions.")
    print("Building country colour map…")
    country_color_map = build_country_color_map(agg_master)

    print(f"Pre-computing sunburst figures ({len(INSTITUTIONS)} institutions × 2 directions)…")
    output = {}
    for inst in INSTITUTIONS:
        for direction in ("incoming", "outgoing"):
            key = f"{inst}__{direction}"
            print(f"  {key}…")
            fig = build_sunburst_figure(agg_master, inst, direction, country_color_map)
            output[key] = json.loads(fig.to_json())
    print(f"  Done — {len(output)} figures.")

    out_path = OUT_DIR / "sunburst_data.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f)
    print(f"Saved to {out_path}  ({out_path.stat().st_size / 1024:.0f} KB)")


# ═══════════════════════════════════════════════════════════════════════════════
# RECIPROCITY SCATTER
# ═══════════════════════════════════════════════════════════════════════════════

RECIP_TOP = 500

COUNTRY_COLORS = {
    "United States":    "#1f77b4",
    "France":           "#B7990D",
    "United Kingdom":   "#d62728",
    "Germany":          "#2ca02c",
    "China":            "#ff7f0e",
    "Spain":            "#9467bd",
    "Japan":            "#e377c2",
    "Canada":           "#17becf",
    "Australia":        "#bcbd22",
    "The Netherlands":  "#8c564b",
    "Switzerland":      "#aec7e8",
    "Russia":           "#c5b0d5",
    "India":            "#ffbb78",
    "South Korea":      "#98df8a",
    "Brazil":           "#ff9896",
    "Poland":           "#f7b6d2",
    "Belgium":          "#c49c94",
    "Türkiye":          "#dbdb8d",
    "Sweden":           "#9edae5",
    "Finland":          "#393b79",
    "Denmark":          "#637939",
    "Taiwan":           "#8c6d31",
    "Greece":           "#843c39",
    "Portugal":         "#7b4173",
    "Austria":          "#5254a3",
    "Italy":            "#e6550d",
}
OTHER_COUNTRY_COLOR = "#cccccc"

LEGEND_COUNTRIES = [
    "France", "United States", "United Kingdom", "Germany", "Spain", "China",
    "Italy", "Canada", "Switzerland", "Japan", "Denmark", "Brazil", "Russia",
    "Australia", "Greece", "Poland", "The Netherlands", "Finland", "Sweden",
    "India", "South Korea", "Belgium",
]


def load_institution_scatter(inst):
    base = BASE_PATH / inst
    inb = pd.read_csv(base / "citation_counts_organizations_incoming.csv")
    out = pd.read_csv(base / "citation_counts_organizations_outgoing.csv")
    self_name = INST_LABELS[inst]
    inb = inb[inb["legal_name"] != self_name]
    out = out[out["legal_name"] != self_name]
    return inb, out


def compute_reciprocity(inb, out, recip_top=RECIP_TOP):
    inb_top = (inb.groupby(["legal_name", "country_name"])["count"].sum()
               .reset_index().nlargest(recip_top, "count")
               .rename(columns={"count": "incoming"}))
    out_top = (out.groupby(["legal_name", "country_name"])["count"].sum()
               .reset_index().nlargest(recip_top, "count")
               .rename(columns={"count": "outgoing"}))
    recip = pd.merge(inb_top[["legal_name", "country_name", "incoming"]],
                     out_top[["legal_name", "country_name", "outgoing"]],
                     on=["legal_name", "country_name"], how="inner")
    recip["total"] = recip["incoming"] + recip["outgoing"]
    recip["asymmetry"] = (recip["outgoing"] - recip["incoming"]) / recip["total"]
    return recip


def build_scatter_figure():
    fig = go.Figure()

    for i, inst in enumerate(INSTITUTIONS):
        inb, out = load_institution_scatter(inst)
        recip = compute_reciprocity(inb, out, RECIP_TOP)
        visible = (i == 0)

        t = recip["total"]
        sizes = 5 + 32 * (t - t.min()) / (t.max() - t.min() + 1)
        colors = [COUNTRY_COLORS.get(c, OTHER_COUNTRY_COLOR) for c in recip["country_name"]]

        fig.add_trace(go.Scatter(
            x=recip["incoming"].tolist(),
            y=recip["outgoing"].tolist(),
            mode="markers",
            name=INST_LABELS[inst],
            visible=visible,
            marker=dict(
                size=sizes.tolist(),
                color=colors,
                opacity=0.72,
                line=dict(width=0.5, color="white"),
            ),
            customdata=recip[["legal_name", "country_name", "incoming", "outgoing", "total"]].values.tolist(),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>%{customdata[1]}<br>"
                "Incoming: %{customdata[2]:,.0f}<br>"
                "Outgoing: %{customdata[3]:,.0f}<br>"
                "Total: %{customdata[4]:,.0f}<extra></extra>"
            ),
            showlegend=False,
        ))

        ax_min = float(min(recip[["incoming", "outgoing"]].min()))
        ax_max = float(max(recip[["incoming", "outgoing"]].max()))
        fig.add_trace(go.Scatter(
            x=[ax_min, ax_max],
            y=[ax_min, ax_max],
            mode="lines",
            line=dict(color="gray", dash="dash", width=1),
            visible=visible,
            showlegend=False,
            hoverinfo="skip",
        ))

    n_inst_traces = len(INSTITUTIONS) * 2
    n_country_traces = len(LEGEND_COUNTRIES)

    for country in LEGEND_COUNTRIES:
        fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode="markers",
            name=country,
            marker=dict(color=COUNTRY_COLORS.get(country, OTHER_COUNTRY_COLOR), size=9),
            showlegend=True,
            visible=True,
            legendgroup="countries",
        ))

    buttons = []
    for i, inst in enumerate(INSTITUTIONS):
        vis = [False] * n_inst_traces + [True] * n_country_traces
        vis[i * 2] = True
        vis[i * 2 + 1] = True
        buttons.append(dict(
            label=INST_LABELS[inst],
            method="update",
            args=[
                {"visible": vis},
                {"title": {"text": (
                    f"Reciprocity Scatter — Top-{RECIP_TOP} Bilateral Partners"
                    f"  ({INST_LABELS[inst]})<br>"
                    "<sup>Log scale · bubble size = total citations · colour = country</sup>"
                ), "x": 0.5}},
            ],
        ))

    fig.update_layout(
        template="plotly_white",
        height=640,
        title=dict(text=(
            f"Reciprocity Scatter — Top-{RECIP_TOP} Bilateral Partners"
            f"  ({INST_LABELS[INSTITUTIONS[0]]})<br>"
            "<sup>Log scale · bubble size = total citations · colour = country</sup>"
        ), x=0.5),
        xaxis=dict(type="log", title="Incoming citations"),
        yaxis=dict(type="log", title="Outgoing citations"),
        legend_title_text="Country",
        updatemenus=[dict(
            buttons=buttons,
            direction="down",
            showactive=False,
            x=0.0,
            xanchor="left",
            y=1.13,
            yanchor="top",
            bgcolor="#320E3B",
            bordercolor="rgba(255, 255, 255, 0.2)",
            font=dict(size=12, color="#FFFFFF", family="Inter, sans-serif"),
        )],
        margin=dict(t=120),
    )
    return fig


def generate_scatter():
    print("\n── Reciprocity Scatter ───────────────────────────────────────────────")
    print("Loading data and building figure…")
    fig = build_scatter_figure()
    out_path = OUT_DIR / "reciprocity_scatter.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig_json = fig.to_json()
    with open(out_path, "w") as f:
        f.write(fig_json)
    print(f"Saved to {out_path}")
    with open(out_path) as f:
        d = json.load(f)
    print(f"Traces: {len(d['data'])}, first trace type: {d['data'][0].get('type')}, "
          f"mode: {d['data'][0].get('mode')}")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    generate_sunburst()
    generate_scatter()
    print("\n✓ All charts generated.")
