import pandas as pd

path = "data/data.csv"  # ダウンロードしたCSV
# メタ行を飛ばす。環境でズレる場合は 5→6 や 7 に調整
df = pd.read_csv(path, encoding="cp932", skiprows=5)

dt_col   = df.columns[0]          # 'Unnamed: 0'（= 日時）
temp_col = df.columns[1]          # 'Unnamed: 1'（= 気温）

# 形式を揃える
out = df[[dt_col, temp_col]].rename(columns={dt_col: "datetime", temp_col: "temp"})
out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
out["temp"]     = pd.to_numeric(out["temp"], errors="coerce")
out = out.dropna(subset=["datetime", "temp"])

print(out.head(3))  # 確認用

# 例：特定時刻の値を obs.csv に書き出し（東京=47662）
target = pd.to_datetime("2025-04-20 14:00:00")
row = out.loc[out["datetime"] == target]
if not row.empty:
    row.iloc[[0]][["temp"]].assign(station_id="47662")[["station_id","temp"]] \
       .to_csv("data/obs.csv", index=False)
    print("wrote data/obs.csv")
else:
    print("指定時刻が見つかりませんでした。`out['datetime']` を確認して下さい。")

