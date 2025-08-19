#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import pandas as pd
import folium
from pathlib import Path

IN = Path("data/processed/pref_coverage_score.csv")
GEO = Path("src/assets/prefectures.fixed.geojson")
OUT = Path("data/processed/coverage_map.html")

# GeoJSONの都道府県名とCSVのprefecture列が一致する必要があります
# 例: "北海道","青森県",...,"沖縄県"
def main():
    df = pd.read_csv(IN)
    g = json.loads(GEO.read_text(encoding="utf-8"))

    # キーは都道府県名
    df = df[["prefecture","score10"]].copy()

    m = folium.Map(location=[36.5, 137.0], zoom_start=5, tiles="cartodbpositron")

    folium.Choropleth(
        geo_data=g,
        data=df,
        columns=["prefecture","score10"],
        key_on="feature.properties.name",  # GeoJSON側の都道府県名プロパティ
        fill_color="YlOrRd",
        nan_fill_color="lightgray",
        fill_opacity=0.8,
        line_opacity=0.4,
        legend_name="Coverage Score (0–10)"
    ).add_to(m)

    # ホバーで県名とスコア表示
    folium.features.GeoJson(
        g,
        name="labels",
        tooltip=folium.features.GeoJsonTooltip(
            fields=["name"],
            aliases=["都道府県"],
            sticky=False
        )
    ).add_to(m)

    # ポップアップにスコア
    for _, r in df.iterrows():
        # ラベルを地図に直接重ねたい場合はpref座標が必要。ここでは省略。
        pass

    OUT.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(OUT))
    print(f"saved: {OUT}")

if __name__ == "__main__":
    main()

