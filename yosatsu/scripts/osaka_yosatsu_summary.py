#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大阪府「病害虫発生予察情報」バックナンバー サマリー作成
- 起点（バックナンバー一覧）から年度ページを列挙
- 各年度ページのセクション（予報/注意報/警報/特殊報/その他の防除情報）直下のPDFリンクを収集
- 年度ごとのサマリーCSV + 全アイテムCSVを出力

使い方例:
  uv add requests beautifulsoup4 lxml python-dateutil
  uv run python osaka_yosatsu_summary.py \
    --index https://www.pref.osaka.lg.jp/o120090/nosei/byogaicyu/yohou/ \
    --outdir data/osaka_summary
"""
import os, re, csv, argparse
from datetime import date
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, UnicodeDammit

# ----------------------
# 設定
# ----------------------
HEADERS = {"User-Agent": "Mozilla/5.0 (yosatsu-summary)"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

WERA_OFFSET = {"令和": 2018, "平成": 1988, "昭和": 1925}  # 西暦 = 和暦年 + オフセット

# 年度ページ内のセクション見出しに含まれる代表語
SECTION_CANON = {
    "予報": ["予察情報", "予報"],
    "注意報": ["注意報"],
    "警報": ["警報"],
    "特殊報": ["特殊報"],
    "その他の防除情報": ["その他の防除情報", "防除情報"]
}

# 和暦/西暦日付の抽出（アンカー文から）
DATE_PATS = [
    re.compile(r"(?P<y>\d{4})[./年](?P<m>\d{1,2})[./月](?P<d>\d{1,2})日?"),
    re.compile(r"(?P<g>令和|平成|昭和)\s*(?P<y>\d+|元)\s*年\s*(?P<m>\d{1,2})\s*月\s*(?P<d>\d{1,2})\s*日"),
]

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

def fetch_soup(url: str, timeout: int = 20) -> BeautifulSoup:
    """バイト列→UnicodeDammit→cp932フォールバックでデコード安定化"""
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    raw = r.content
    dammit = UnicodeDammit(raw, is_html=True)
    html = dammit.unicode_markup
    if (not html) or ("\u0080" in html) or ("�" in html and "charset" not in (html[:400].lower())):
        try:
            html = raw.decode("cp932", errors="ignore")
        except Exception:
            r.encoding = r.apparent_encoding or "utf-8"
            html = r.text
    return BeautifulSoup(html, "lxml")

def pick_year_pages(index_url: str) -> list[tuple[str, str]]:
    """
    バックナンバー一覧ページから「令和x/平成x 年度へ」の年度ページURLを拾う。
    戻り値: [(year_label, year_url), ...]
    """
    soup = fetch_soup(index_url)
    pairs: list[tuple[str, str]] = []
    for a in soup.select("#tmp_contents a[href]"):
        t = " ".join(a.get_text(" ", strip=True).split())
        if ("年度へ" in t) and ("令和" in t or "平成" in t):
            pairs.append((t, urljoin(index_url, a.get("href"))))
    # 重複除去
    seen = {}
    for lab, u in pairs:
        seen[u] = lab
    return [(lab, u) for u, lab in sorted(seen.items(), key=lambda x: x[0], reverse=True)]

def canon_section(s: str) -> str:
    """見出しテキストから標準化ラベルを決める（最初にマッチしたもの）"""
    s = (s or "").strip()
    for canon, hints in SECTION_CANON.items():
        for h in hints:
            if h in s:
                return canon
    # どれにも当たらない場合は元の見出しを返す（保険）
    return s or ""

def parse_year_page(year_label: str, year_url: str) -> list[dict]:
    """
    年度ページからPDFリンク（1レコード=1PDF）を抽出。
    セクションは <h2>/<h3>/<h4> を上へ辿って最寄りの見出しを採用。
    """
    soup = fetch_soup(year_url)
    rows = []
    for a in soup.select('a[href$=".pdf"], a[href*=".pdf?"]'):
        href = a.get("href") or ""
        pdf_url = urljoin(year_url, href)
        anchor = " ".join(a.get_text(" ", strip=True).split())

        # 近傍の見出しを探索
        sec = ""
        cur = a
        for _ in range(24):
            prev = cur.find_previous_sibling()
            if not prev:
                break
            if prev.name in ("h2", "h3", "h4"):
                sec = " ".join(prev.get_text(" ", strip=True).split())
                break
            cur = prev
        if not sec:
            parent = a.parent
            hop = 0
            while parent and hop < 8 and not sec:
                h = parent.find_previous(["h2", "h3", "h4"])
                if h:
                    sec = " ".join(h.get_text(" ", strip=True).split())
                    break
                parent = parent.parent
                hop += 1

        sec_canon = canon_section(sec)
        issued_iso = normalize_date(anchor)

        rows.append({
            "pref": "大阪府",
            "year_label": year_label,
            "page_url": year_url,
            "section_raw": sec,
            "section": sec_canon,
            "item_title": anchor,
            "pdf_url": pdf_url,
            "filename": os.path.basename(urlparse(pdf_url).path),
            "issued_date_iso": issued_iso or ""
        })
    return rows

def make_summary_per_year(items: list[dict]) -> dict:
    """年度内のPDFアイテムから件数と日付レンジのサマリーを作る"""
    total = len(items)
    by_sec = {}
    dates = []
    for r in items:
        by_sec[r["section"]] = by_sec.get(r["section"], 0) + 1
        if r["issued_date_iso"]:
            dates.append(r["issued_date_iso"])
    first_date = min(dates) if dates else ""
    last_date  = max(dates) if dates else ""
    # 代表セクションを固定列で
    out = {
        "pref": items[0]["pref"] if items else "大阪府",
        "year_label": items[0]["year_label"] if items else "",
        "page_url": items[0]["page_url"] if items else "",
        "total_pdfs": total,
        "予報_count": by_sec.get("予報", 0),
        "注意報_count": by_sec.get("注意報", 0),
        "警報_count": by_sec.get("警報", 0),
        "特殊報_count": by_sec.get("特殊報", 0),
        "その他の防除情報_count": by_sec.get("その他の防除情報", 0),
        "first_issued_date": first_date,
        "last_issued_date": last_date,
    }
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--index", required=True, help="バックナンバー一覧ページのURL（/yohou/ 直下）")
    ap.add_argument("--outdir", required=True, help="出力ディレクトリ")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    items_csv = os.path.join(args.outdir, "items_all.csv")
    summary_csv = os.path.join(args.outdir, "summary_by_year.csv")

    year_pages = pick_year_pages(args.index)
    print(f"[year pages] {len(year_pages)}")

    all_items = []
    for ylab, yurl in year_pages:
        print(f"  - {ylab}: {yurl}")
        rows = parse_year_page(ylab, yurl)
        all_items.extend(rows)

    # 書き出し（全アイテム）
    if all_items:
        fieldnames = ["pref","year_label","page_url","section_raw","section",
                      "item_title","pdf_url","filename","issued_date_iso"]
        with open(items_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(all_items)
        print(f"[write] {items_csv} ({len(all_items)} rows)")

    # 年度サマリー
    by_year = {}
    for r in all_items:
        by_year.setdefault(r["year_label"], []).append(r)
    summaries = [make_summary_per_year(v) for _, v in sorted(by_year.items(), reverse=True)]

    if summaries:
        fieldnames = ["pref","year_label","page_url","total_pdfs","予報_count","注意報_count",
                      "警報_count","特殊報_count","その他の防除情報_count",
                      "first_issued_date","last_issued_date"]
        with open(summary_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(summaries)
        print(f"[write] {summary_csv} ({len(summaries)} rows)")

if __name__ == "__main__":
    main()

