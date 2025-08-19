#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大阪府・病害虫発生予察情報の“年別サイトマップ”作成
- 起点ページからバックナンバー年ページURLを収集
- 各年ページ内のPDFリンクを、見出し区分付きでCSV化

使い方:
  uv add requests beautifulsoup4 lxml python-dateutil
  uv run python osaka_site_map.py \
    --base https://www.pref.osaka.lg.jp/o120090/nosei/byogaicyu/yohou/2024yohou.html \
    --outdir data/osaka_site_map

出力:
  data/osaka_site_map/backnumbers.csv
  data/osaka_site_map/entries.csv
"""
import os, re, csv, time, argparse, hashlib
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from datetime import date

# -------------------------
# 設定
# -------------------------
HEADERS = {"User-Agent": "Mozilla/5.0 (research; yosatsu-site-map)"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

WERA_OFFSET = {"令和": 2018, "平成": 1988, "昭和": 1925}  # 西暦=和暦年+オフセット

# アンカー文から抜くパターン（例）
RE_ISSUE_NO = re.compile(r"第\s*(\d+)\s*号")
RE_MONTH_FORECAST = re.compile(r"(\d{1,2})\s*月\s*予報")
RE_SIZE = re.compile(r"PDF[:：]?\s*（?(\d+)\s*KB）?|PDF[:：]\s*(\d+)\s*KB", re.IGNORECASE)

# “令和5年4月28日発表” などを拾う
DATE_PATS = [
    re.compile(r"(?P<y>\d{4})[./年](?P<m>\d{1,2})[./月](?P<d>\d{1,2})日?"),
    re.compile(r"(?P<g>令和|平成|昭和)\s*(?P<y>\d+|元)\s*年\s*(?P<m>\d{1,2})\s*月\s*(?P<d>\d{1,2})\s*日"),
]

# セクション名で拾いたいキーワード例（なくてもOK。見出し文字列そのものを使う）
SECTION_HINTS = ["予報", "注意報", "警報", "特殊報", "防除情報", "発生情報", "速報", "解説"]


# -------------------------
# 共通ユーティリティ
# -------------------------
def get(url: str, timeout: int = 20) -> requests.Response:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r

def normalize_date(text: str) -> str | None:
    if not text:
        return None
    for pat in DATE_PATS:
        m = pat.search(text)
        if not m:
            continue
        gd = m.groupdict()
        if "g" in gd and gd.get("g"):
            era = gd["g"]; y = gd["y"]
            y = 1 if y == "元" else int(y)
            yyyy = WERA_OFFSET.get(era, 0) + y
        else:
            yyyy = int(gd["y"])
        mm = int(gd["m"]); dd = int(gd["d"])
        try:
            return date(yyyy, mm, dd).isoformat()
        except ValueError:
            return None
    return None

def nearest_section_text(a_tag) -> str:
    """
    PDFリンクの直近上位にある h2/h3 の見出しテキストを探す。
    同階層で遡る → 親を辿る → ページ先頭へ、と段階的に探索。
    """
    # まず兄弟を遡って見出しを探す
    cur = a_tag
    for _ in range(30):
        prev = cur.find_previous_sibling()
        if prev is None:
            break
        if prev.name in ("h2", "h3"):
            return " ".join(prev.get_text(" ").split())
        cur = prev

    # 親を辿りつつ、その直前の見出しを探索
    parent = a_tag.parent
    hops = 0
    while parent and hops < 10:
        h = parent.find_previous(["h2", "h3"])
        if h:
            return " ".join(h.get_text(" ").split())
        parent = parent.parent
        hops += 1

    return ""  # 見つからなければ空


# -------------------------
# 起点ページ→バックナンバーURL取得
# -------------------------
def collect_backnumbers(base_url: str) -> list[tuple[str, str]]:
    """
    戻り値: [(year_label, url), ...]
    """
    res = get(base_url)
    soup = BeautifulSoup(res.text, "lxml")
    pairs: list[tuple[str, str]] = []

    for a in soup.select("a[href]"):
        text = " ".join(a.get_text(" ").split())
        if "病害虫発生予察情報" in text and ("令和" in text or "平成" in text):
            href = a.get("href")
            year_url = urljoin(base_url, href)
            pairs.append((text, year_url))

    # 自ページ（該当年度）も含める（年ラベルを付けにくければそのまま入れる）
    pairs.append(("この年ページ", base_url))

    # 重複除去
    uniq = {}
    for lab, u in pairs:
        uniq[u] = lab
    # URL降順（だいたい新しい年度が上に来ることが多い）
    return [(lab, u) for u, lab in sorted(uniq.items(), key=lambda x: x[0], reverse=True)]


# -------------------------
# 年ページ→PDFリンク抽出
# -------------------------
def parse_year_page(year_label: str, year_url: str) -> list[dict]:
    """
    各PDFリンクごとに辞書レコードを返す。
    """
    res = get(year_url)
    soup = BeautifulSoup(res.text, "lxml")

    rows = []
    for a in soup.select('a[href$=".pdf"], a[href*=".pdf?"]'):
        href = a.get("href") or ""
        pdf_url = urljoin(year_url, href)
        anchor = " ".join(a.get_text(" ").split())
        section = nearest_section_text(a)

        # 抽出
        issue_no = RE_ISSUE_NO.search(anchor)
        month_fc = RE_MONTH_FORECAST.search(anchor)
        size_m = RE_SIZE.search(anchor)

        issued_iso = normalize_date(anchor)

        # セクション名を少し整形（ヒント語だけ残す等）
        section_norm = section
        for hint in SECTION_HINTS:
            if hint in section_norm:
                section_norm = hint
                break

        rows.append({
            "pref": "大阪府",
            "year_label": year_label,
            "page_url": year_url,
            "section": section_norm,
            "item_title": anchor,
            "pdf_url": pdf_url,
            "filename": os.path.basename(urlparse(pdf_url).path),
            "issue_no": (issue_no.group(1) if issue_no else ""),
            "forecast_month": (month_fc.group(1) if month_fc else ""),
            "issued_date_iso": (issued_iso or ""),
            "pdf_size_kb_hint": (size_m.group(1) or size_m.group(2) if size_m else ""),
        })

    return rows


# -------------------------
# メイン
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="起点（最新年度）ページURL")
    ap.add_argument("--outdir", required=True, help="出力ディレクトリ")
    ap.add_argument("--sleep", type=float, default=0.2, help="礼儀的スリープ秒")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    back_csv = os.path.join(args.outdir, "backnumbers.csv")
    ent_csv = os.path.join(args.outdir, "entries.csv")

    backnumbers = collect_backnumbers(args.base)
    print(f"[backnumbers] {len(backnumbers)} pages")
    with open(back_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["year_label", "year_url"])
        for lab, u in backnumbers:
            w.writerow([lab, u])

    all_rows = []
    for lab, u in backnumbers:
        print(f"[parse] {lab} -> {u}")
        rows = parse_year_page(lab, u)
        print(f"  -> {len(rows)} PDFs")
        all_rows.extend(rows)
        time.sleep(args.sleep)

    if all_rows:
        fieldnames = [
            "pref","year_label","page_url","section",
            "item_title","pdf_url","filename",
            "issue_no","forecast_month","issued_date_iso","pdf_size_kb_hint",
        ]
        with open(ent_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(all_rows)

    print(f"[done] backnumbers -> {back_csv}")
    print(f"[done] entries     -> {ent_csv}")


if __name__ == "__main__":
    main()

