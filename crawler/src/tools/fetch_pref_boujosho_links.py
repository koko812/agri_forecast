import csv
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

INDEX_URL = "https://www.maff.go.jp/j/syouan/syokubo/gaicyu/yosatu/index.html"
UA = "agri-forecast/0.1"

def fetch_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    return r.text

def extract_pref_links(html: str, base: str):
    soup = BeautifulSoup(html, "lxml")

    p_tag = soup.find("p", string=lambda t: t and "地域ごとの詳細な情報" in t)
    if not p_tag:
        raise RuntimeError("指定の<p>タグが見つかりませんでした")

    table = p_tag.find_next("table")
    if not table:
        raise RuntimeError("指定の<p>タグの次に<table>が見つかりませんでした")

    rows = []
    for a in table.select("a[href]"):
        pref_name = a.get_text(strip=True)
        href = urljoin(base, a["href"])
        rows.append({"prefecture": pref_name, "url": href})

    return rows

def main(out_csv: str = "data/processed/pref_boujosho_links.csv"):
    # 出力ディレクトリを作成
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)

    html = fetch_html(INDEX_URL)
    rows = extract_pref_links(html, INDEX_URL)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["prefecture", "url"])
        w.writeheader()
        w.writerows(rows)
    print(f"saved: {out_csv} ({len(rows)} links)")

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "data/processed/pref_boujosho_links.csv"
    main(out)

