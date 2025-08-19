# jma_fetch.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

BASE = "https://www.data.jma.go.jp/stats/etrn/view/hourly_s1.php"


def _to_num(s: str, zero_for_dash=False):
    if s is None:
        return None
    s = s.strip().replace("−", "-")
    if s in ("--", ""):
        return 0.0 if zero_for_dash else None
    try:
        return float(s)
    except ValueError:
        return None

# --- 英語キーへのマップ（列順はJMAの行データに対応） ---
EXPECTED_HEADERS_JA = [
    "時",                 # 0: 時刻（データではtd[0]）
    "現地",               # 1: 気圧(hPa) 現地
    "海面",               # 2: 気圧(hPa) 海面
    "降水量(mm)",         # 3
    "気温(℃)",           # 4
    "露点温度(℃)",       # 5
    "蒸気圧(hPa)",       # 6
    "湿度(％)",           # 7
    "風速(m/s)",          # 8
    "風向",               # 9
    "日照時間(h)",        # 10
    "全天日射量(MJ/㎡)", # 11
    "降雪(cm)",           # 12
    "積雪(cm)",           # 13
    "天気",               # 14
    "雲量",               # 15
    "視程(km)",           # 16
]

HEADER_MAP = {
    "現地": "pressure_local",
    "海面": "pressure_sea",
    "降水量(mm)": "precip",
    "気温(℃)": "temp",
    "露点温度(℃)": "dew_point",
    "蒸気圧(hPa)": "vapor_pressure",
    "湿度(％)": "humidity",
    "風速(m/s)": "wind_speed",
    "風向": "wind_dir",
    "日照時間(h)": "sunshine",
    "全天日射量(MJ/㎡)": "solar_radiation",
    "降雪(cm)": "snowfall",
    "積雪(cm)": "snow_depth",
    "天気": "weather",
    "雲量": "cloud_cover",
    "視程(km)": "visibility",
}

NUMERIC_KEYS = {
    "pressure_local","pressure_sea","precip","temp","dew_point",
    "vapor_pressure","humidity","wind_speed","sunshine",
    "solar_radiation","snowfall","snow_depth","visibility"
}

def _td_text(td):
    """テキストが空なら <img alt="…"> を拾う（天気記号用）"""
    txt = td.get_text(strip=True).replace("\xa0", "")
    if not txt:
        img = td.find("img")
        if img and img.get("alt"):
            txt = img["alt"]
    return txt

def fetch_hourly_data(prec_no: str, block_no: str, date: datetime) -> pd.DataFrame:
    params = dict(
        prec_no=str(int(prec_no)),
        block_no=str(block_no).zfill(5),
        year=str(date.year),
        month=str(date.month),
        day=str(date.day),
        view="p1",
    )
    resp = requests.get(BASE, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content.decode("cp932", "ignore"), "html.parser")

    table = soup.select_one("#tablefix1")
    if table is None:
        return pd.DataFrame()

    trs = table.select("tr")
    if len(trs) < 3:
        return pd.DataFrame()

    # データ行を読む（列順は上の EXPECTED_HEADERS_JA に一致）
    base = datetime(date.year, date.month, date.day)
    rows = []
    for tr in trs[2:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        htxt = _td_text(tds[0])
        if not htxt.isdigit():
            continue

        hour = int(htxt)
        ts = (base + timedelta(days=1)).replace(hour=0) if hour == 24 else base.replace(hour=hour)

        row = {"datetime": ts}
        # tds[1] 以降を EXPECTED_HEADERS_JA[1:] に対応付け
        for ja_name, td in zip(EXPECTED_HEADERS_JA[1:], tds[1:]):
            key = HEADER_MAP.get(ja_name, ja_name)  # 英語キー
            val = _td_text(td)
            if key in NUMERIC_KEYS:
                row[key] = _to_num(val, zero_for_dash=True)
            else:
                row[key] = val
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("datetime").reset_index(drop=True)
    return df

