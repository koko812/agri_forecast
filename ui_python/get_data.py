# get_hourly_extras.py  ← 時刻, 気温, 降水量, 風速 を取得
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

BASE = "https://www.data.jma.go.jp/stats/etrn/view/hourly_s1.php"
PARAMS = dict(prec_no="44", block_no="47662", year="2025", month="1", day="1", view="p1")

def to_num(s: str, zero_for_dash=False):
    s = s.strip().replace("−","-")
    if s in ("--","",None):
        return 0.0 if zero_for_dash else None
    try:
        return float(s)
    except ValueError:
        return None

resp = requests.get(BASE, params=PARAMS, headers={"User-Agent":"Mozilla/5.0"}, timeout=20)
resp.raise_for_status()
soup = BeautifulSoup(resp.content.decode("cp932","ignore"), "html.parser")

rows = []
table = soup.select_one("#tablefix1")
for tr in table.select("tr"):
    tds = tr.find_all("td")
    if len(tds) < 10:  # データ行でない
        continue
    h_txt = tds[0].get_text(strip=True)
    if not h_txt.isdigit():
        continue
    hour   = int(h_txt)
    temp   = to_num(tds[4].get_text(strip=True))                 # 気温(℃)
    precip = to_num(tds[3].get_text(strip=True), zero_for_dash=True)  # 降水量(mm) '--'→0
    wind   = to_num(tds[8].get_text(strip=True))                 # 風速(m/s)

    y,m,d = map(int, (PARAMS["year"], PARAMS["month"], PARAMS["day"]))
    base = datetime(y,m,d)
    ts = (base + timedelta(days=1)).replace(hour=0) if hour==24 else base.replace(hour=hour)

    rows.append({"datetime": ts, "temp": temp, "precip": precip, "wind_speed": wind})

df = pd.DataFrame(rows).dropna(subset=["datetime"])
print(df.head(12))
# df.to_csv("data/hourly_tokyo_20250101.csv", index=False)

