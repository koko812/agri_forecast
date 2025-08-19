# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import date as date_cls
from jma_fetch import fetch_hourly_data  # 英語キーで返す版

from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent          # ui_python/
DATA_DIR = BASE_DIR / "data"                        # ui_python/data

# （念のため）同階層モジュールがimportできるように
import sys
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

st.set_page_config(page_title="JMA 時間別データビューア", layout="wide")

@st.cache_data
def load_prec() -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "prec_no_list.csv")   # ← ここを DATA_DIR に
    df["prec_no"] = df["prec_no"].astype(str).str.strip()
    df["area"] = df["area"].astype(str).str.strip()
    return df

@st.cache_data
def load_stations() -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "station_list.csv")   # ← ここを DATA_DIR に
    df["prec_no"] = df["prec_no"].astype(str).str.strip()
    df["block_no"] = df["block_no"].astype(str).str.zfill(5)
    df["name"] = df["name"].astype(str).str.replace('"', '').str.strip()
    return df

def _safe_line(df, col, title):
    if col in df.columns:
        st.caption(title)
        st.line_chart(df.set_index("datetime")[col])

def _safe_bar(df, col, title):
    if col in df.columns:
        st.caption(title)
        st.bar_chart(df.set_index("datetime")[col])

st.title("JMA 時間別データビューア")

# ---- 上：選択UI（フル幅） ----
with st.container():
    st.subheader("選択")
    try:
        prec_df = load_prec()
        stns_df = load_stations()
    except Exception as e:
        st.error(f"CSV 読み込みエラー: {e}")
        st.stop()

    c1, c2, c3 = st.columns([2, 3, 2])
    with c1:
        area_label = st.selectbox(
            "都府県・地方（prec_no）",
            options=(prec_df["area"] + " (prec_no=" + prec_df["prec_no"] + ")").tolist(),
            index=0 if len(prec_df) else None
        )
        sel_prec = area_label.split("prec_no=")[-1].strip(")") if area_label else None

    with c2:
        stns_sub = stns_df[stns_df["prec_no"] == sel_prec].copy() if sel_prec else stns_df.iloc[0:0]
        station_label = st.selectbox(
            "地点名（block_no）",
            options=(stns_sub["name"] + " (block_no=" + stns_sub["block_no"] + ")").tolist(),
            index=0 if not stns_sub.empty else None,
            disabled=stns_sub.empty
        )

    with c3:
        sel_date = st.date_input("取得日", value=date_cls(2025, 1, 1), min_value=date_cls(2010, 1, 1))

    run = st.button("取得・表示", type="primary", use_container_width=True)

# ---- 下：結果表示（テーブル→グラフ） ----
if run and sel_prec and station_label:
    sel_block = station_label.split("block_no=")[-1].strip(")")
    with st.spinner(f"取得中: prec_no={sel_prec}, block_no={sel_block}, date={sel_date}"):
        df = fetch_hourly_data(sel_prec, sel_block, pd.Timestamp(sel_date).to_pydatetime())

    st.subheader("テーブル")
    if df.empty:
        st.warning("データが見つかりませんでした。日付や地点を確認してください。")
    else:
        st.dataframe(df, use_container_width=True, height=520)

        # ダウンロード
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "CSV をダウンロード",
            data=csv_bytes,
            file_name=f"hourly_{sel_prec}_{sel_block}_{sel_date}.csv",
            mime="text/csv",
            use_container_width=True
        )

        st.subheader("簡易可視化")
        _safe_line(df, "temp", "Temperature (℃)")
        _safe_line(df, "wind_speed", "Wind Speed (m/s)")
        _safe_bar(df, "precip", "Precipitation (mm)")

