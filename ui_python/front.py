import pandas as pd, math
import h3, pydeck as pdk, streamlit as st

st.title("気温マップ（実データCSV→H3 最近傍）")

# ① 先に CSV を選ぶ（ここが一番上）
csv_choice = st.selectbox(
    "観測点CSVを選択",
    ["dummy_data/stations_temp.csv", "dummy_data/stations_temp_dense.csv", "dummy_data/stations_temp_extra.csv", "dummy_data/tokyo_dense.csv"]
)
df = pd.read_csv(csv_choice)

RES  = 7
RING = 8

# ② ここから下は「選んだ df」を使って再計算
seed_cells = [h3.latlng_to_cell(r.lat, r.lon, RES) for r in df.itertuples()]
cells = set(seed_cells)
for c in seed_cells:
    cells |= set(h3.grid_disk(c, RING))

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

data = []
for c in cells:
    lat, lon = h3.cell_to_latlng(c)
    temp = nn_temp(lat, lon)
    data.append({"h3": c, "value": round(temp, 1), "unit": "°C"})

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
    opacity=0.4,
    pickable=True,
)

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

