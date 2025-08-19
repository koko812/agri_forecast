# pip install h3 pydeck streamlit pandas
import pandas as pd, math, random
import h3, pydeck as pdk, streamlit as st

st.title("気温マップ（実データCSV→H3 最近傍）")

CSV_PATH = "data/stations_temp.csv"
RES = 7                      # H3解像度（~1km）
RING = 8                     # 何セル分広げるか（表示の広がり）

# ---- 1) 観測点CSVを読む ----
df = pd.read_csv(CSV_PATH)

# ---- 2) セル集合（観測点セルを起点にリングを広げる）----
seed_cells = [h3.latlng_to_cell(r.lat, r.lon, RES) for r in df.itertuples()]
cells = set(seed_cells)
for c in seed_cells:
    cells |= set(h3.grid_disk(c, RING))

# ---- 3) 最近傍ステーションを探す（簡易ハバースイン距離）----
stations = [(r.lat, r.lon, float(r.temp)) for r in df.itertuples()]
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2*R*math.asin(math.sqrt(a))

def nn_temp(lat, lon):
    best = None
    for (slat, slon, t) in stations:
        d = haversine(lat, lon, slat, slon)
        if (best is None) or (d < best[0]):
            best = (d, t)
    return best[1]

# ---- 4) H3セルごとに値を付与 ----
data = []
for c in cells:
    lat, lon = h3.cell_to_latlng(c)
    temp = nn_temp(lat, lon)           # 最近傍の観測値
    data.append({"h3": c, "value": round(temp, 1), "unit": "°C"})

# ---- 5) カラーマップ & レイヤ ----
def cmap(v, vmin=26, vmax=36):
    v = max(vmin, min(vmax, v)); t = (v - vmin) / (vmax - vmin + 1e-9)
    r = int(20 + t*(255-20)); g = int(120 + t*(60-120)); b = int(255 + t*(60-255))
    return [r, g, b]

for d in data:
    d["color"] = cmap(d["value"])

h3_layer = pdk.Layer(
    "H3HexagonLayer",
    data,
    get_hexagon="h3",
    get_fill_color="color",
    stroked=True,
    get_line_color=[50, 50, 50],
    lineWidthMinPixels=1.2,
    filled=True,
    opacity=0.4,   # あなたの好みに合わせた透過
    pickable=True,
)

# 観測点も点で重ねると説得力UP
scatter = pdk.Layer(
    "ScatterplotLayer",
    [{"lat": r.lat, "lon": r.lon, "temp": float(r.temp)} for r in df.itertuples()],
    get_position="[lon, lat]",
    get_radius=200,
    get_fill_color=[0,0,0],
    pickable=True,
)

st.pydeck_chart(pdk.Deck(
    initial_view_state=pdk.ViewState(latitude=df.lat.mean(), longitude=df.lon.mean(), zoom=9),
    layers=[h3_layer, scatter],
    map_style="light",
    tooltip={"text": "H3: {h3}\nTemp: {value}{unit}"}
))

