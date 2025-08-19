# read_jma_html_wide6.py
import pandas as pd

# 例: 東京都・日ごとの値（都内各地点が横に並ぶページ）
url = "https://www.data.jma.go.jp/stats/etrn/view/daily_s1.php?prec_no=44&block_no=&year=2025&month=4&day=&view="

METRIC = "平均気温(℃)"
N_COLS = 6

# 1) とりあえず素で読む（テーブル群）
tables = pd.read_html(url, header=None)   # headerは後で指定し直す
# 2) “年月日”が含まれるテーブルを探す
target = None
for t in tables:
    if (t.astype(str).apply(lambda s: s.str.contains("年月日").any())).any():
        target = t
        break
if target is None:
    raise SystemExit("年月日を含むテーブルが見つかりませんでした。URL/ページ種別を確認してください。")

# 3) 3行目=地点, 4行目=要素 という想定で MultiIndex 列に組み直す
#    （HTMLだとちょうどこの構造になっていることが多い）
header_names = target.iloc[2]   # 地点名の行
header_elems = target.iloc[3]   # 要素名の行
data = target.iloc[6:].reset_index(drop=True)  # 1,2,5,6行目は不要、7行目以降がデータ

data.columns = pd.MultiIndex.from_arrays([header_names.values, header_elems.values])

# 4) 日付列（level1 == '年月日'）を index に
date_col = [c for c in data.columns if isinstance(c, tuple) and c[1] == "年月日"][0]
data[date_col] = pd.to_datetime(data[date_col], errors="coerce")
wide = data.set_index(date_col).xs(METRIC, axis=1, level=1).dropna(axis=1, how="all")

# 5) 6列だけ表示（左から）
cols = list(wide.columns)[:N_COLS]
view = wide[cols]

pd.set_option("display.width", 2000)
pd.set_option("display.max_columns", N_COLS + 1)
print(f"\n=== ワイド表（要素: {METRIC}、列数: {len(cols)}）=== ")
print("列（地点）:", ", ".join(cols))
print()
print(view.to_string(index=True))

