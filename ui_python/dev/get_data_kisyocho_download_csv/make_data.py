# 使い捨て：東京BBOXにダミー観測点をN点生成してCSVに保存
# 既存プロジェクトのどこかで一度実行 → data/tokyo_dense.csv を作る
import random, csv

N = 120  # 欲しい密度に調整
BBOX = [139.55, 35.55, 139.95, 35.85]  # [lon_w, lat_s, lon_e, lat_n] ざっくり都心

with open("data/tokyo_dense.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["station_id","lat","lon","temp"])
    for i in range(N):
        lon = random.uniform(BBOX[0], BBOX[2])
        lat = random.uniform(BBOX[1], BBOX[3])
        # ざっくり海風・都心ヒートの勾配 + ノイズ（見栄え用）
        base = 30.0 + 0.8*(0.75 - abs(lon-139.76)) - 0.5*(lat-35.68)
        temp = round(base + random.uniform(-0.8, 0.8), 1)
        w.writerow([f"tky{i:03d}", lat, lon, temp])

