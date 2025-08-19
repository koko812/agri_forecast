# show_data_from_fixed_header.py
import pandas as pd
from pathlib import Path
from io import StringIO

PATH = Path("data/many_points_data_utf8.csv")  # 対象CSV（UTF-8でもSJISでもOK）
TARGET_DATE = pd.to_datetime("2025-04-20")     # 取り出したい日
METRIC = "平均気温(℃)"                         # 例: "平均気温(℃)","最高気温(℃)","最低気温(℃)"

def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="cp932")

# --- 1) テキストとして読み、3&4行目をヘッダ化、5&6行目を捨てて再構成 ---
txt = read_text(PATH)
lines = txt.splitlines()

# 1-index基準: 3行目=地点, 4行目=要素, 1,2,5,6行目は不要
header_names = lines[2]      # 3行目
header_elems = lines[3]      # 4行目
data_lines   = lines[6:]     # 7行目以降=データ本体

subtxt = "\n".join([header_names, header_elems] + data_lines)

# --- 2) 2段ヘッダとして読み込み（大量列に備えて engine='python'） ---
df = pd.read_csv(StringIO(subtxt), header=[0, 1], engine="python")

# --- 3) 日付列（level=1 が '年月日' の列）を特定して index 化 ---
def is_date_col(col):
    return isinstance(col, tuple) and len(col) > 1 and str(col[1]).strip() == "年月日"

date_cols = [c for c in df.columns if is_date_col(c)]
if not date_cols:
    raise SystemExit("年月日の列が見つかりません。ヘッダ位置の仮定を再確認してください。")
date_col = date_cols[0]

df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
df = df.set_index(date_col)

# --- 4) 指定日の、指定METRICだけを抽出してプリント（観測所: 値） ---
print(f"=== {TARGET_DATE.date()} の {METRIC} ===")
row = df.loc[TARGET_DATE]   # 対象日1行ぶん（Wide形式）

# 観測所（MultiIndex level=0）の一覧
stations = sorted({c[0] for c in df.columns if isinstance(c, tuple)})

count = 0
for s in stations:
    key = (s, METRIC)
    if key in row.index and pd.notna(row[key]):
        print(f"{s}: {row[key]}")
        count += 1
print(f"\n観測所数: {count}")

# --- 5) ついでにロング化（後で使う tidy 形） ---
#   name,temp の2列だけのテーブルを作って、先頭だけ表示
wide = row.xs(METRIC, axis=0, level=1)   # 1段目=観測所名の Series
tidy = wide.reset_index()
tidy.columns = ["name", "temp"]
print("\n--- tidy preview ---")
print(tidy.head())

