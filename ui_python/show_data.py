# show_data.py  ← 値をprintするだけ（保存しない）
import pandas as pd
from pathlib import Path
from io import StringIO

SRC = Path("data/many_points_data_utf8.csv")  # UTF-8化したやつ
TARGET_DATE = pd.to_datetime("2025-04-20")    # 取り出したい日
METRIC = "平均気温(℃)"                        # 例: "平均気温(℃)","最高気温(℃)","最低気温(℃)"

def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="cp932")

txt = read_text(SRC)
lines = txt.splitlines()

# 1) 「年月日」を含む行番号を探す → その1行上が観測所名の行
idx_metrics = next(i for i, ln in enumerate(lines) if "年月日" in ln)
idx_names   = idx_metrics - 1

# 2) そこからを2段ヘッダとして読み込む
subtxt = "\n".join(lines[idx_names:])
df = pd.read_csv(StringIO(subtxt), header=[0, 1], engine="python")

# 3) 日付列（level1 が '年月日' の列）を特定
def is_date_col(col):
    if isinstance(col, tuple) and len(col) > 1:
        return str(col[1]).strip() == "年月日"
    return str(col).strip() == "年月日"

date_cols = [c for c in df.columns if is_date_col(c)]
if not date_cols:
    raise SystemExit("年月日の列が見つかりませんでした。ヘッダ行の検出がずれている可能性があります。")
date_col = date_cols[0]

# 4) 日付をindex化
df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
df = df.set_index(date_col)

# 5) 指定日の値を観測所ごとにprint（METRICだけ）
print(f"=== {TARGET_DATE.date()} の {METRIC} ===")
row = df.loc[TARGET_DATE]

# 観測所名の集合（MultiIndex level 0）
stations = sorted({c[0] for c in df.columns if isinstance(c, tuple)})
count = 0
for s in stations:
    key = (s, METRIC)
    if key in row.index:
        val = row[key]
        if pd.notna(val):
            print(f"{s}: {val}")
            count += 1
print(f"\n観測所数: {count}")

