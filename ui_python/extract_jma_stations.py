#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import csv
import re
from pathlib import Path

URL = "http://k-ichikawa.blog.enjoy.jp/etc/HP/htm/jmaP0.html"

def parse_prec_no(text: str):
    """都府県・地方区分一覧のテキストをCSV行にパース"""
    rows = []
    for line in text.strip().splitlines():
        # 例: '  1, "宗谷地方",  11'
        m = re.match(r'\s*(\d+),\s*"?(.*?)"?\s*,\s*(\d+)', line)
        if m:
            no, area, prec = m.groups()
            rows.append([int(no), area.strip(), prec])
    return rows

def parse_station_list(text: str):
    """全観測点一覧のテキストをCSV行にパース"""
    rows = []
    for line in text.strip().splitlines():
        # 例: '     1,   1, "稚内"　　, 11, 47401, 45,24.9,141,40.7,2.8'
        # 全角空白やカンマ区切りを正規化
        line = line.replace("　", " ")
        parts = [p.strip() for p in line.split(",") if p.strip()]
        if len(parts) < 9:  # 不完全な行はスキップ
            continue
        try:
            no = int(parts[0])
            idx = int(parts[1])
            name = parts[2].strip('"')
            prec_no = parts[3]
            block_no = parts[4]
            lat_deg, lat_min = parts[5].split() if " " in parts[5] else (parts[5], None)
            lon_deg, lon_min = parts[6].split() if " " in parts[6] else (parts[6], None)
            alt_m = parts[7]
            # 経度緯度が (45,24.9,141,40.7,2.8) のように続いてるパターンもあるので調整
            if len(parts) >= 9:
                lat_deg, lat_min = parts[5], parts[6]
                lon_deg, lon_min = parts[7], parts[8]
                alt_m = parts[9] if len(parts) > 9 else None
            rows.append([no, idx, name, prec_no, block_no, lat_deg, lat_min, lon_deg, lon_min, alt_m])
        except Exception:
            continue
    return rows

def main():
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "html.parser")

    pre_tags = soup.find_all("pre")
    if len(pre_tags) < 2:
        raise RuntimeError("pre タグが想定より少ないです")

    prec_no_text = pre_tags[0].get_text()
    station_text = pre_tags[1].get_text()

    prec_rows = parse_prec_no(prec_no_text)
    station_rows = parse_station_list(station_text)

    Path("data").mkdir(exist_ok=True)

    with open("data/prec_no_list.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["no", "area", "prec_no"])
        writer.writerows(prec_rows)

    with open("data/station_list.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["no", "i", "name", "prec_no", "block_no", "lat_deg", "lat_min", "lon_deg", "lon_min", "alt_m"])
        writer.writerows(station_rows)

    print("Saved data/prec_no_list.csv and data/station_list.csv")

if __name__ == "__main__":
    main()

