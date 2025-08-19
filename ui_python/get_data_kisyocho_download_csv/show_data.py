# show_wide6.py
import pandas as pd
from pathlib import Path
from io import StringIO

SRC = Path("data/many_points_data_utf8.csv")
METRIC = "平均気温(℃)"  # 必要に応じて "最高気温(℃)", "最低気温(℃)" に変更
N_COLS = 6              # 表示する地点（列）数

def read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return p.read_text(encoding="cp932")

# --- ヘッダ整形：3行目=地点, 4行目=要素。1,2,5,6行目は破棄 ---
txt = read_text(SRC)
lines = txt.splitlines()
subtxt = "\n".join([lines[2], lines[3]] + lines[6:])  # 3&4行目をヘッダに、7行目以降がデータ

# --- 2段ヘッダで読み込み ---
df = pd.read_csv(StringIO(subtxt), header=[0, 1], engine="python")

# --- 日付列（level1='年月日'）を特定して index化 ---
date_cols = [c for c in df.columns if isinstance(c, tuple) and str(c[1]).strip() == "年月日"]
if not date_cols:
    raise SystemExit("年月日の列が見つかりません。前提の行番号(3,4)をご確認ください。")
date_col = date_cols[0]
df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
df = df.set_index(date_col)

# --- 欲しい要素だけをワイドで抽出（横=地点, 縦=日付）---
wide = df.xs(METRIC, axis=1, level=1)   # 列: 観測所名, 行: 日付
# すべて欠損の列は落とす
wide = wide.dropna(axis=1, how="all")

# --- 表示する列（地点）を6つに制限（左から順）---
cols = list(wide.columns)[:N_COLS]
view = wide[cols]

# --- 見やすく印字（折り返し防止）---
pd.set_option("display.width", 2000)
pd.set_option("display.max_columns", N_COLS + 1)
pd.set_option("display.max_rows", 200)  # 必要なら調整

print(f"\n=== ワイド表（要素: {METRIC}、列数: {len(cols)}）=== ")
print("列（地点）:", ", ".join(cols))
print()
print(view.to_string(index=True))

