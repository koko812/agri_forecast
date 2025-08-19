#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, csv, json, queue, re, time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from urllib import robotparser
import logging
from datetime import datetime

# --- ここだけ置き換え（冒頭の logging 設定）---
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# コンソール出力のみ（県ごとのファイル出力は map_pref 内で設定）
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(console_handler)
# --- 置き換えここまで ---


# ==== 入出力（ルートから実行前提） ====
SEEDS_CSV = Path("data/processed/pref_boujosho_links.csv")
OUT_DIR   = Path("data/processed/site_map")
LOG_DIR   = Path("logs/site_map")

UA = "agri-forecast-sitemap/0.1"
TIMEOUT = 20

# 年候補（本文は読まない。アンカー/タイトル/URLからだけ拾う）
YEAR_PAT  = re.compile(r"(令和\s?\d+|平成\s?\d+|20\d{2})")
FUTURE_GUARD_WORDS = ("計画","予定","案","募集","予算","方針","公募")

def setup_dirs():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

def safe_name(s: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", s)

# ---- 県別ログ ----
def log_txt(pref: str, msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with (LOG_DIR / f"{safe_name(pref)}.log").open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def log_event(pref: str, ev: dict):
    ev = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), **ev}
    with (LOG_DIR / f"{safe_name(pref)}.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(ev, ensure_ascii=False) + "\n")

# ---- robots.txt ----
class Robots:
    def __init__(self): self.cache={}
    def allowed(self, url:str)->bool:
        base = urlparse(url).scheme + "://" + urlparse(url).netloc
        if base not in self.cache:
            rp = robotparser.RobotFileParser()
            rp.set_url(urljoin(base, "/robots.txt"))
            try: rp.read()
            except Exception: pass
            self.cache[base] = rp
        return self.cache[base].can_fetch(UA, url)
ROBOTS = Robots()

# ---- HTTP ----
def fetch(url:str):
    if not ROBOTS.allowed(url):
        return None, None, None, "robots_disallow"
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT, allow_redirects=True)
        ct = (r.headers.get("Content-Type") or "").lower()
        return r.status_code, ct, r.content, None
    except Exception as e:
        return None, None, None, str(e)

def is_same_domain(a:str, b:str)->bool:
    try:
        A = urlparse(a).netloc.split(":")[0]
        B = urlparse(b).netloc.split(":")[0]
        return (A == B) or (A.endswith(".go.jp") and B.endswith(".go.jp"))
    except: return False

def classify(ct:str, url:str)->str:
    u = (url or "").lower()
    if (ct and "pdf" in ct) or u.endswith(".pdf"): return "pdf"
    if (ct and ("html" in ct or "xhtml" in ct)) or u.endswith((".html",".htm","/")): return "html"
    if (ct and "xml" in ct) or u.endswith(".xml"): return "xml"
    if u.endswith(".csv"): return "csv"
    if u.endswith((".xlsx",".xls",".xlsm")): return "xls"
    if u.endswith(".json"): return "json"
    return "other"

def extract_years_from_text(s:str):
    if not s: return []
    s = s.replace("　"," ")
    years=[]
    for m in YEAR_PAT.findall(s):
        token=m.strip()
        y=None
        if token.startswith("令和"):
            try: y = 2018 + int(re.sub(r"\D","",token))  # 令和1=2019
            except: pass
        elif token.startswith("平成"):
            try: y = 1988 + int(re.sub(r"\D","",token))  # 平成1=1989
            except: pass
        else:
            try: y=int(re.search(r"20\d{2}",token).group(0))
            except: pass
        if not y: continue
        # 未来年は除外
        if y > time.localtime().tm_year: continue
        years.append(y)
    # ユニーク化・降順
    return sorted(set(years), reverse=True)

def parse_html_for_links(base_url:str, html_bytes:bytes):
    # HTMLのタイトル・h1・リンク群（PDF含む）だけ抽出
    try: html = html_bytes.decode("utf-8", errors="ignore")
    except: html = html_bytes.decode("cp932", errors="ignore")
    soup = BeautifulSoup(html, "lxml")
    title = (soup.title.get_text(strip=True) if soup.title else "")
    h1 = (soup.select_one("h1").get_text(strip=True) if soup.select_one("h1") else "")
    anchors=[]
    for a in soup.select("a[href]"):
        href = a.get("href",""); text = a.get_text(" ", strip=True)
        full = urljoin(base_url, href)
        anchors.append((full, text))
    return title, h1, anchors

def section_key(url:str, levels:int=2):
    # ドメイン以降のパス先頭n階層で粗く分類
    p = urlparse(url).path.strip("/")
    parts = [x for x in p.split("/") if x]
    return "/".join(parts[:levels]) if parts else ""

def filename_of(url:str):
    p = urlparse(url).path
    return p.split("/")[-1] if p else ""

def map_pref(pref:str, start_url:str, max_pages=80, max_depth=2, sleep=0.3):
    # 県ごとのファイルロガー
    pref_logger = logging.getLogger(f"crawl.{pref}")
    pref_logger.setLevel(logging.INFO)
    # 二重にハンドラが増えないようにチェック
    if not any(isinstance(h, logging.FileHandler) for h in pref_logger.handlers):
        log_path = LOG_DIR / f"{safe_name(pref)}.crawl.{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        pref_logger.addHandler(fh)
        pref_logger.info(f"Log file: {log_path}")

    print(f"[MAP] {pref} … {start_url}", flush=True)
    log_txt(pref, f"[START] {start_url}")
    log_event(pref, {"type":"start","pref":pref,"url":start_url})
    pref_logger.info(f"START {pref} url={start_url} depth<= {max_depth} pages<= {max_pages}")

    visited=set()
    q=queue.Queue()
    q.put((0, start_url, None))  # (depth, url, parent)

    pages=[]
    pdf_rows=[]
    sections_count={}

    while not q.empty() and len(visited) < max_pages:
        depth, url, parent = q.get()
        if url in visited:
            continue
        visited.add(url)

        progress = f"{len(visited)}/{len(visited) + q.qsize()}"
        pref_logger.info(f"[{progress}] GET depth={depth} url={url}")

        time.sleep(sleep)
        status, ct, body, err = fetch(url)
        size = (len(body) if body else 0)
        kind = classify(ct or "", url)
        pref_logger.info(f" -> status={status} ct={ct} kind={kind} bytes={size} err={err or ''}")
        log_event(pref, {"type":"fetch","url":url,"status":status,"ct":ct,"bytes":size,"err":err})

        title = h1 = ""
        n_links = n_pdf = 0

        if status == 200 and body and kind == "html":
            title, h1, anchors = parse_html_for_links(url, body)
            pref_logger.info(f" parse html: title='{(title or '')[:60]}' links={len(anchors)}")

            # 内部リンクを走査
            shown = 0
            for full, text in anchors:
                if not full.startswith("http"):
                    continue
                if not is_same_domain(start_url, full):
                    continue

                if full.lower().endswith(".pdf"):
                    n_pdf += 1
                    years = extract_years_from_text(f"{text} {title} {full}")
                    pdf_rows.append({
                        "prefecture": pref,
                        "source_page": url,
                        "depth": depth,
                        "pdf_url": full,
                        "anchor_text": text,
                        "filename": filename_of(full),
                        "years": "|".join(map(str, years)) if years else ""
                    })
                    if shown < 10:
                        pref_logger.info(f"   [pdf] {full}  anchor='{(text or '')[:50]}' years={years or []}")
                        shown += 1
                    continue  # PDFは辿らない

                n_links += 1
                if depth < max_depth:
                    q.put((depth+1, full, url))
                    if shown < 10:
                        pref_logger.info(f"   [link] -> depth={depth+1} {full}")
                        shown += 1
            if len(anchors) > shown:
                pref_logger.info(f"   ... more {len(anchors)-shown} links omitted ...")

        pages.append({
            "prefecture": pref,
            "url": url,
            "depth": depth,
            "parent": parent or "",
            "status": status if status is not None else "",
            "ctype": ct or "",
            "kind": kind,
            "title": title,
            "h1": h1,
            "section": section_key(url, levels=2),
            "n_out_links": n_links,
            "n_pdf_links": n_pdf
        })

        sk = pages[-1]["section"]
        sections_count[sk] = sections_count.get(sk, 0) + 1

    # 出力
    out_pages   = OUT_DIR / f"{safe_name(pref)}.pages.csv"
    out_pdfs    = OUT_DIR / f"{safe_name(pref)}.pdfs.csv"
    out_sections= OUT_DIR / f"{safe_name(pref)}.sections.csv"

    with out_pages.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["prefecture","url","depth","parent","status","ctype","kind","title","h1","section","n_out_links","n_pdf_links"])
        w.writeheader(); w.writerows(pages)

    with out_pdfs.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["prefecture","source_page","depth","pdf_url","anchor_text","filename","years"])
        w.writeheader(); w.writerows(pdf_rows)

    with out_sections.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["prefecture","section","pages"])
        w.writeheader()
        for k,v in sorted(sections_count.items(), key=lambda x:(x[0] or "zzz")):
            w.writerow({"prefecture":pref,"section":k,"pages":v})

    log_event(pref, {"type":"end","pref":pref,"pages":len(pages),"pdfs":len(pdf_rows)})
    log_txt(pref, f"[END] pages={len(pages)} pdfs={len(pdf_rows)}")
    pref_logger.info(f"DONE pages={len(pages)} pdfs={len(pdf_rows)} visited={len(visited)} queued={q.qsize()}")
    pref_logger.info(f"Wrote: {out_pages.name}, {out_pdfs.name}, {out_sections.name}")
    print(f"saved: {out_pages}")
    print(f"saved: {out_pdfs}")
    print(f"saved: {out_sections}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pref", type=str, default=None, help="対象都道府県名（未指定なら全件）")
    parser.add_argument("--max-pages", type=int, default=80)
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--sleep", type=float, default=0.3)
    args = parser.parse_args()

    setup_dirs()
    if not SEEDS_CSV.exists():
        raise FileNotFoundError(f"not found: {SEEDS_CSV}")

    seeds = list(csv.DictReader(SEEDS_CSV.open(encoding="utf-8")))
    if args.pref:
        seeds = [r for r in seeds if r["prefecture"] == args.pref]
        if not seeds: raise SystemExit(f"pref not found: {args.pref}")

    for r in seeds:
        pref = r["prefecture"].strip()
        url  = r["url"].strip()
        print(f"[START] {pref} … {url}")
        map_pref(pref, url, max_pages=args.max_pages, max_depth=args.max_depth, sleep=args.sleep)

if __name__ == "__main__":
    main()

