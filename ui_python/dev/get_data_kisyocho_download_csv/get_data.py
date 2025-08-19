# jma_to_obs.py : JMAの時間データCSV → obs.csv に整形
# 例の列名: [観測所番号, 観測所名, 年月日, 時, 気温(℃), ...]
import pandas as pd

jma_csv = "jma_hourly_sample.csv"    # ダウンロードした元CSV
target_date = "2024-08-01"           # 取り出したい日
target_hour = 14                     # 取り出したい時
station_map_csv = "stations.csv"     # 観測所メタ（station_id,name,lat,lon）

raw = pd.read_csv(jma_csv)
# 列名が日本語でもOKなようにざっくりrename（適宜調整）
cols = {c:c for c in raw.columns}
for c in raw.columns:
    if "観測所番号" in c: cols[c] = "station_id"
    if "年月日"   in c: cols[c] = "date"
    if c == "時":      cols[c] = "hour"
    if "気温"     in c: cols[c] = "temp"
raw = raw.rename(columns=cols)

raw["date"] = pd.to_datetime(raw["date"])
one = raw[(raw["date"]==pd.to_datetime(target_date)) & (raw["hour"]==target_hour)].copy()
one = one[["station_id","temp"]].dropna()

# station_id が文字列/ゼロ埋めの場合があるので統一
one["station_id"] = one["station_id"].astype(str)

# 出力
one.to_csv("obs.csv", index=False)
print("wrote obs.csv")

# stations.csv は別途用意（station_id,name,lat,lon）
# JMAの観測所一覧CSVから作る or 手元のメタから作る

