#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import textwrap

IN = Path("data/processed/pref_coverage_score.csv")
OUT_MD = Path("data/processed/coverage_report.md")
IMG_DIR = Path("data/processed")
IMG_BAR = IMG_DIR / "coverage_scores_bar.png"
IMG_HIST = IMG_DIR / "coverage_scores_hist.png"
IMG_SCAT = IMG_DIR / "coverage_scatter_htmlratio.png"

def save_bar(df):
    s = df.sort_values("score10", ascending=False)
    plt.figure(figsize=(10, 9))
    plt.barh(s["prefecture"], s["score10"])
    plt.gca().invert_yaxis()
    plt.xlabel("Score (0–10)")
    plt.title("Prefecture Coverage Scores")
    plt.tight_layout()
    plt.savefig(IMG_BAR, dpi=160)
    plt.close()

def save_hist(df):
    plt.figure(figsize=(6,4))
    plt.hist(df["score10"], bins=10)
    plt.xlabel("Score (0–10)")
    plt.ylabel("Count")
    plt.title("Distribution of Scores")
    plt.tight_layout()
    plt.savefig(IMG_HIST, dpi=160)
    plt.close()

def save_scatter(df):
    plt.figure(figsize=(6,4))
    plt.scatter(df["html_ratio"], df["score10"])
    plt.xlabel("HTML ratio")
    plt.ylabel("Score (0–10)")
    plt.title("Score vs. HTML ratio")
    plt.tight_layout()
    plt.savefig(IMG_SCAT, dpi=160)
    plt.close()

def main():
    df = pd.read_csv(IN)
    n = len(df)
    top = df.nlargest(8, "score10")[["prefecture","score10","latest_year","hits_3y","html_ratio","has_backnumber"]]
    worst = df.nsmallest(8, "score10")[["prefecture","score10","latest_year","hits_3y","html_ratio","has_backnumber"]]

    save_bar(df)
    save_hist(df)
    save_scatter(df)

    md = []
    md.append("# 予察情報 充実度レポート（自動生成）\n")
    md.append(f"- 評価対象: {n} 都道府県\n")
    md.append(f"- 画像: \n  - {IMG_BAR.name}\n  - {IMG_HIST.name}\n  - {IMG_SCAT.name}\n")
    md.append("\n## 上位（Top 8）\n")
    md.append(top.to_markdown(index=False))
    md.append("\n## 下位（Bottom 8）\n")
    md.append(worst.to_markdown(index=False))

    # 簡易メモ
    note = """
    ### 読み方メモ
    - score10: Freshness/Depth/Volume/Machine-readable を合算した簡易10点スコア
    - latest_year: ページ内の公開日/本文から推定した最新年（未来年は除外済み）
    - hits_3y: 直近3年に相当する年の痕跡ヒット数（浅いスキャンに基づく概算）
    - html_ratio: 解析対象リンクに対するHTML比率（XML/PDFは除外）
    - has_backnumber: “バックナンバー/年度”といったインデックス的痕跡がある=1
    """
    md.append("\n" + textwrap.dedent(note))

    OUT_MD.write_text("\n\n".join(md), encoding="utf-8")
    print(f"saved: {OUT_MD}")
    print(f"saved: {IMG_BAR}, {IMG_HIST}, {IMG_SCAT}")

if __name__ == "__main__":
    main()

