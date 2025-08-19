#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re, os, sys, csv, time, hashlib
from dataclasses import dataclass, asdict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE = "https://www.pref.osaka.lg.jp/o120090/nosei/byogaicyu/yohou/2024yohou.html"
OUT_DIR = "data/osaka"
RAW_DIR = os.path.join(OUT_DIR, "raw")
TEXT_DIR = os.path.join(OUT_DIR, "text")
CATALOG_CSV = os.path.join(OUT_DIR, "osaka_yosatsu_catalog.csv")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(TEXT_DIR, exist_ok=True)

SESS = requests.Session()
SESS.headers.update({"User-Agent":"Mozilla/5.0 (+research; yosatsu-crawler)"})

WERA = {"令和":2018,"平成":1988,"昭和":1925}
ISSUE_TYPE_PAT = r"(発生予察|予察|注意報|警報|特殊報|発生情報|速報|解説)"
LEVEL_PAT = r"(警報|注意報|注意|平年並み|やや多い|多発|少ない)"

CROP_PAT = r"(イネ|稲|水稲|陸稲|コムギ|ダイズ|トマト|ナス|ピーマン|キュウリ|イチゴ|ネギ|キャベツ|タマネギ)"
PEST_PAT = r"(いもち|稲熱|斑点米カメムシ|コブノメイガ|シロイチモジヨトウ|ハスモンヨトウ|トビイロウンカ|ニカメイガ|トマトキバガ|白さび|べと病)"

DATE_PATS = [
    r"(?P<y>\d{4})[./年](?P<m>\d{1,2})[./月](?P<d>\d{1,2})日?",
    r"(?P<g>令和|平成|昭和)\s?(?P<y>\d+|元)年\s?(?P<m>\d{1,2})月\s?(?P<d>\d{1,2})日",
]

@dataclass
class Record:
    pref: str
    year_label: str
    page_url: str
    anchor_text: str
    pdf_url: str
    filename: str
    issued_date_hint: str
    issue_type_hint: str
    level_hint: str
    crop_hint: str
    pest_hint: str
    saved_path: str = ""
    text_path: str = ""
    issued_date_from_text: str = ""
    issue_type_from_text: str = ""
    level_from_text: str = ""
    crop_from_text: str = ""
    pest_from_text: str = ""
    is_text_pdf: bool = False
    text_hash: str = ""

def get(url):
    r = SESS.get(url, timeout=20)
    r.raise_for_status()
    return r

def normalize_date(text):
    if not text: return None
    for pat in DATE_PATS:
        m = re.search(pat, text)
        if not m: continue
        gd = m.groupdict()
        if "g" in gd and gd.get("g"):
            era = gd["g"]; y = gd["y"]; y = 1 if y=="元" else int(y)
            y = WERA[era] + y
        else:
            y = int(gd["y"])
        m_ = int(gd["m"]); d_ = int(gd["d"])
        return f"{y:04d}-{m_:02d}-{d_:02d}"
    return None

def parse_year_page(url):
    """その年のページからPDFリンク群を抽出"""
    res = get(url)
    soup = BeautifulSoup(res.text, "lxml")
    # 本文内のPDFアンカーをすべて拾う
    recs = []
    for a in soup.select('a[href$=".pdf"], a[href*=".pdf?"]'):
        href = a.get("href") or ""
        atxt = " ".join(a.get_text(" ").split())
        pdf_url = urljoin(url, href)
        fn = os.path.basename(urlparse(pdf_url).path)
        issued_hint = normalize_date(atxt)
        issue_type = re.search(ISSUE_TYPE_PAT, atxt)
        level = re.search(LEVEL_PAT, atxt)
        crop = re.search(CROP_PAT, atxt)
        pest = re.search(PEST_PAT, atxt)
        recs.append((atxt, pdf_url, fn, issued_hint,
                     issue_type.group(1) if issue_type else "",
                     level.group(1) if level else "",
                     crop.group(1) if crop else "",
                     pest.group(1) if pest else ""))
    return recs

def find_backnumbers(start_url):
    """起点ページからバックナンバー年ページのURLを取得"""
    res = get(start_url)
    soup = BeautifulSoup(res.text, "lxml")
    years = []
    # 下部に「病害虫発生予察情報(令和X年)」などのリンクが集約されている
    for a in soup.select('a'):
        t = a.get_text()
        if "病害虫発生予察情報" in t and ("令和" in t or "平成" in t):
            years.append((t.strip(), urljoin(start_url, a.get("href"))))
    # 今の年ページ自身も含める
    years.append(("この年ページ", start_url))
    # 重複除去
    uniq = {}
    for label, u in years:
        uniq[u] = label
    # 見つけやすい順（新→旧）で返すため軽く並べ替え
    return [(lab, u) for u, lab in sorted(uniq.items(), key=lambda x:x[0], reverse=True)]

def save_binary(url, out_path):
    if os.path.exists(out_path): return
    r = get(url)
    with open(out_path, "wb") as f:
        f.write(r.content)

def extract_pdf_text(path, maxpages=None):
    try:
        from pdfminer.high_level import extract_text
        txt = extract_text(path, maxpages=maxpages)
        return txt or ""
    except Exception:
        return ""

def text_density(txt):
    import math
    nonspace = len(re.findall(r"\S", txt))
    return nonspace / max(1, len(txt))

def enrich_from_text(txt):
    # 本文から再抽出（アンカーで拾えない情報の上書き）
    issued = normalize_date(txt)
    issue_type = re.search(ISSUE_TYPE_PAT, txt)
    level = re.search(LEVEL_PAT, txt)
    crop = re.search(CROP_PAT, txt)
    pest = re.search(PEST_PAT, txt)
    return {
        "issued_date_from_text": issued or "",
        "issue_type_from_text": issue_type.group(1) if issue_type else "",
        "level_from_text": level.group(1) if level else "",
        "crop_from_text": crop.group(1) if crop else "",
        "pest_from_text": pest.group(1) if pest else "",
    }

def main():
    years = find_backnumbers(BASE)
    print(f"[years] {len(years)} pages")
    rows = []
    for year_label, page_url in years:
        print(f"[scan] {year_label} -> {page_url}")
        for (atxt, pdf_url, fn, d_hint, t_hint, lvl_hint, crop_hint, pest_hint) in parse_year_page(page_url):
            rec = Record(
                pref="大阪府",
                year_label=year_label,
                page_url=page_url,
                anchor_text=atxt,
                pdf_url=pdf_url,
                filename=fn,
                issued_date_hint=d_hint or "",
                issue_type_hint=t_hint,
                level_hint=lvl_hint,
                crop_hint=crop_hint,
                pest_hint=pest_hint,
            )
            # 保存
            out_pdf = os.path.join(RAW_DIR, fn)
            save_binary(pdf_url, out_pdf)
            rec.saved_path = out_pdf

            # PDFテキスト抽出（全ページ→失敗/重いなら1P）
            txt = extract_pdf_text(out_pdf, maxpages=None)
            if not txt:
                txt = extract_pdf_text(out_pdf, maxpages=1)
            if txt:
                h = hashlib.sha256(txt.encode("utf-8", "ignore")).hexdigest()[:16]
                rec.text_hash = h
                dens = text_density(txt)
                rec.is_text_pdf = dens >= 0.02
                tp = os.path.join(TEXT_DIR, fn.replace(".pdf",".txt"))
                with open(tp, "w", encoding="utf-8") as f:
                    f.write(txt)
                rec.text_path = tp
                upd = enrich_from_text(txt)
                for k,v in upd.items():
                    setattr(rec, k, v)

            rows.append(asdict(rec))
            time.sleep(0.2)  # 礼儀的スリープ

    # カタログ出力
    fieldnames = list(rows[0].keys()) if rows else [f.name for f in Record.__dataclass_fields__.values()]
    with open(CATALOG_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

if __name__ == "__main__":
    main()

