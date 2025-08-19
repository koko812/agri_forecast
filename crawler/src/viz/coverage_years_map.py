#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
from pathlib import Path
import pandas as pd
import folium

CSV = Path("data/processed/pref_coverage_score.csv")
# どちらか存在する方を使う
GEO_CANDIDATES = [
    Path("src/assets/prefectures.fixed.geojson"),
    Path("src/assets/prefectures.geojson"),
    Path("src/assets/japan_pref.geojson"),
]
OUT_HTML = Path("data/processed/coverage_years_map.html")
OUT_CSV  = Path("data/processed/pref_coverage_years.csv")

def load_geojson():
    for p in GEO_CANDIDATES:
        if p.exists() and p.stat().st_size > 0:
            txt = p.read_text(encoding="utf-8", errors="ignore").lstrip()
            if not (txt.startswith("{") or txt.startswith("[")):
                continue
            g = json.loads(txt)
            if "features" in g:
                return g, p
    raise FileNotFoundError("GeoJSON not found. Put a valid GeoJSON under src/assets/. "
                            "Tried: " + ", ".join(str(x) for x in GEO_CANDIDATES))

def pick_name_field(g):
    props = g["features"][0].get("properties", {})
    for c in ["name","nam_ja","NAME_1","N03_001","pref","pref_name"]:
        if c in props: return c
    for k,v in props.items():
        if isinstance(v,str): return k
    raise ValueError("No suitable name field in geojson properties")

def main():
    # 1) read CSV and compute coverage_years
    df = pd.read_csv(CSV)
    # guard: None -> 0
    df["coverage_years"] = (
        (df["latest_year"].fillna(0).astype(int) - df["earliest_year"].fillna(0).astype(int) + 1)
        .where(df["earliest_year"].notna() & df["latest_year"].notna(), 0)
        .clip(lower=0)
    )
    # 保存（一覧も見たい時用）
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df[["prefecture","earliest_year","latest_year","coverage_years"]].to_csv(OUT_CSV, index=False)

    # 2) geojson
    g, used = load_geojson()
    name_field = pick_name_field(g)

    # 3) map
    m = folium.Map(location=[36.5,137.0], zoom_start=5, tiles="cartodbpositron")
    folium.Choropleth(
        geo_data=g,
        data=df,
        columns=["prefecture","coverage_years"],
        key_on=f"feature.properties.{name_field}",
        fill_color="YlOrRd",  # ★色を変更
        nan_fill_color="lightgray",
        fill_opacity=0.9,
        line_opacity=0.4,
        legend_name="Covered Years (count)"
    ).add_to(m)

    # hoverで県名と年数を出す
    tooltip = folium.GeoJsonTooltip(
        fields=[name_field],
        aliases=["都道府県"],
        sticky=False
    )
    folium.GeoJson(g, name="labels", tooltip=tooltip).add_to(m)

    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(OUT_HTML))
    print(f"geojson: {used}")
    print(f"saved: {OUT_CSV}")
    print(f"saved: {OUT_HTML}")

if __name__ == "__main__":
    main()

