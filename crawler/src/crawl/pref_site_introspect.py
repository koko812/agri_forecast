#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import csv
import io
import json
import queue
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from pdfminer.high_level import extract_text as pdf_extract_text
from urllib import robotparser

# ========= パス（プロジェクトルートから実行前提） =========
INPUT_CSV = Path("data/processed/pref_boujosho_links.csv")
OUT_DIR   = Path("data/processed/pref_introspect")
OUT_SUM   = Path("data/processed/pref_introspect_summary.csv")
LOG_DIR   = Path("logs/by_pref_introspect")

UA = "agri-forecast-introspect/0.1"
TIMEOUT = 20

KEYWORDS  = ["予察","発生予察","発生情報","注意報","警報","バックナンバー","年度","病害虫","害虫","病害"]
URL_HINTS = ["yosatsu","yosatu","yohou","byogaichu","gaicyu","gaichu","byogai","yosan"]
YEAR_PAT  = re.compile(r"(令和\s?\d+年度?|平成\s?\d+年度?|20\d{2}(?:年度?)?)")
FUTURE_GUARD_WORDS = ("計画","予定","案","募集","予算","方針","公募")

# ========= 県別ログ =========
def setup_dirs():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_SUM.parent.mkdir(parents=True, exist_ok=True)

def safe_name(s: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", s)

def pref_log_txt(pref: str, msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    p = LOG_DIR / f"{safe_name(pref)}.log"
    with p.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def pref_log_event(pref: str, event: dict):
    event = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), **event}
    p = LOG_DIR / f"{safe_name(pref)}.jsonl"
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")

# ========= robots.txt =========
class RobotsCache:
    def __init__(self):
        self.cache = {}
    def allowed(self, url: str) -> bool:
        host = urlparse(url).scheme + "://" + urlparse(url).netloc
        if host not in self.cache:
            rp = robotparser.RobotFileParser()
            rp.set_url(urljoin(host, "/robots.txt"))
            try:
                rp.read()
            except Exception:
                pass
            self.cache[host] = rp
        return self.cache[host].can_fetch(UA, url)

ROBOTS = RobotsCache()

# ========= HTTP =========
def fetch(url: str, method="GET"):
    if not ROBOTS.allowed(url):
        return None, None, None, "robots_disallow"
    headers = {"User-Agent": UA}
    try:
        if method == "HEAD":
            r = requests.head(url, headers=headers, timeout=TIMEOUT, allow_redirects=True)
            return r.status_code, r.headers.get("Content-Type","").lower(), None, None
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        ct = r.headers.get("Content-Type","").lower()
        return r.status_code, ct, r.content, None
    except Exception as e:
        return None, None, None, str(e)

# ========= 共通解析 =========
def is_same_domain(base: str, link: str) -> bool:
    try:
        b = urlparse(base).netloc.split(":")[0]
        l = urlparse(link).netloc.split(":")[0]
        return (b == l) or (b.endswith(".go.jp") and l.endswith(".go.jp"))
    except Exception:
        return False

def extract_year_candidates(text: str):
    text = text.replace("　"," ")
    current_year = time.localtime().tm_year
    cand = []
    for m in YEAR_PAT.finditer(text):
        token = m.group(0).strip()
        y = None
        if token.startswith("令和"):
            n = int(re.sub(r"\D","", token)); y = 2018 + n  # 令和1=2019
        elif token.startswith("平成"):
            n = int(re.sub(r"\D","", token)); y = 1988 + n  # 平成1=1989
        else:
            num = re.search(r"20\d{2}", token)
            if num: y = int(num.group(0))
        if not y: continue
        if y > current_year:  # 未来は除外
            continue
        start = max(0, m.start()-20); end = min(len(text), m.end()+20)
        near = text[start:end]
        if any(w in near for w in FUTURE_GUARD_WORDS):
            continue
        if 1990 <= y <= current_year:
            cand.append(y)
    return cand

def parse_html(content_bytes: bytes):
    try:
        html = content_bytes.decode("utf-8", errors="ignore")
    except Exception:
        html = content_bytes.decode("cp932", errors="ignore")
    soup = BeautifulSoup(html, "lxml")
    # 公開日メタ
    meta_time = None
    for sel, attr in [
        ('meta[property="article:published_time"]', "content"),
        ('meta[property="og:updated_time"]', "content"),
        ('time[datetime]', "datetime"),
    ]:
        tag = soup.select_one(sel)
        if tag and tag.get(attr):
            try:
                meta_time = dateparser.parse(tag.get(attr), fuzzy=True); break
            except Exception:
                pass
    title = (soup.title.get_text(strip=True) if soup.title else "")[:200]
    body  = " ".join(x.get_text(" ", strip=True) for x in soup.select("h1,h2,h3,p,li"))[:8000]
    years = [meta_time.year] if meta_time else extract_year_candidates(title + " " + body)
    # リンク抽出（優先度計算用）
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href",""); text = a.get_text(" ", strip=True)
        links.append((href, text))
    return {
        "doc_type":"html",
        "title": title,
        "text_len": len(body),
        "years": years,
        "links": links
    }

def parse_xml(content_bytes: bytes):
    try:
        xml = content_bytes.decode("utf-8", errors="ignore")
    except Exception:
        xml = content_bytes.decode("cp932", errors="ignore")
    soup = BeautifulSoup(xml, "lxml-xml")
    title = (soup.title.get_text(strip=True) if soup.title else "")[:200]
    body  = soup.get_text(" ", strip=True)[:8000]
    years = extract_year_candidates(title + " " + body)
    # XMLはリンク抽出を行わない（フィードなどのときはURLが本文にないことが多い）
    return {"doc_type":"xml","title":title,"text_len":len(body),"years":years,"links":[]}

def parse_pdf_quick(url: str, ct: str, content: bytes, size_limit=2_000_000):
    # Content-Length 大きすぎるPDFは本文抽出せずメタのみ
    text_len = 0
    years = []
    if content and len(content) <= size_limit:
        try:
            txt = pdf_extract_text(io.BytesIO(content), maxpages=1) or ""
            text_len = len(txt)
            years = extract_year_candidates(txt[:4000])
        except Exception:
            pass
    return {"doc_type":"pdf","title":"","text_len":text_len,"years":years,"links":[]}

def classify_ext(url: str, ct: str):
    u = url.lower()
    if (ct and "pdf" in ct) or u.endswith(".pdf"): return "pdf"
    if (ct and "xml" in ct) or u.endswith(".xml"): return "xml"
    if u.endswith(".csv"): return "csv"
    if u.endswith(".json"): return "json"
    if any(u.endswith(x) for x in (".xlsx",".xls",".xlsm")): return "xls"
    if (ct and ("html" in ct or "xhtml" in ct)) or any(u.endswith(x) for x in (".html",".htm","/")): return "html"
    return "other"

def priority_score(text: str, url: str):
    score = 0
    for k in KEYWORDS:
        if k in (text or ""): score += 3
    for h in URL_HINTS:
        if h in (url or "").lower(): score += 2
    # 年度・西暦がURLに含まれる場合は加点
    if re.search(r"20\d{2}", url or ""): score += 1
    if "back" in (url or "").lower() or "バックナンバー" in (text or ""): score += 2
    return score

# ========= クロール =========
def introspect_pref(pref: str, start_url: str, max_pages=60, max_depth=2, sleep_sec=0.4):
    print(f"[INTROSPECT] {pref} … {start_url}", flush=True)
    pref_log_txt(pref, f"[START] {pref} {start_url}")
    pref_log_event(pref, {"type":"start","pref":pref,"url":start_url})

    visited = set()
    q = queue.PriorityQueue()
    # (負の優先度, depth, url)
    q.put((-10, 0, start_url, "root"))

    rows_detail = []  # 明細
    years_all = []

    # ドメイン制限
    base_netloc = urlparse(start_url).netloc

    while not q.empty() and len(visited) < max_pages:
        prio, depth, url, src = q.get()
        if url in visited: continue
        visited.add(url)
        time.sleep(sleep_sec)

        status, ct, body, err = fetch(url, method="GET")
        pref_log_event(pref, {"type":"fetch","url":url,"status":status,"ct":ct,"bytes":len(body) if body else 0,"err":err})
        if status != 200 or err:
            rows_detail.append({"url":url,"depth":depth,"from":src,"status":status,"ctype":ct,"kind":"error","title":"","years":"","text_len":0})
            continue

        kind = classify_ext(url, ct)
        meta = {"doc_type":kind,"title":"","text_len":0,"years":[],"links":[]}
        try:
            if kind == "html":
                meta = parse_html(body)
            elif kind == "xml":
                meta = parse_xml(body)
            elif kind == "pdf":
                meta = parse_pdf_quick(url, ct, body)
            else:
                # CSV/JSON/XLS等は本文解析しない
                pass
        except Exception as e:
            pref_log_event(pref, {"type":"parse_error","url":url,"msg":str(e)})

        years_all += meta.get("years", []) or []
        rows_detail.append({
            "url": url,
            "depth": depth,
            "from": src,
            "status": status,
            "ctype": ct or "",
            "kind": kind,
            "title": meta.get("title",""),
            "years": "|".join(str(x) for x in meta.get("years",[]) or []),
            "text_len": meta.get("text_len",0),
        })

        # 次のリンクをキューへ（HTMLだけ）
        if depth < max_depth and kind in ("html","xml"):
            links = meta.get("links", [])
            for href, text in links:
                full = urljoin(url, href)
                if not full.startswith("http"): continue
                if not is_same_domain(start_url, full): continue
                if full in visited: continue
                p = priority_score(text, full)
                q.put((-(p), depth+1, full, url))

    # 集計
    counts = defaultdict(int)
    for r in rows_detail:
        counts[r["kind"]] += 1
    latest = max(years_all) if years_all else None
    earliest = min(years_all) if years_all else None
    coverage_years = (latest - earliest + 1) if (latest and earliest) else 0

    # “機械可読”のヒット
    machine_hits = counts["csv"] + counts["json"] + counts["xml"] + counts["xls"]

    # PDFの“テキストPDF”っぽさの比率（text_len>50 を閾値）
    pdf_texty = sum(1 for r in rows_detail if r["kind"]=="pdf" and r["text_len"]>50)
    pdf_total = counts["pdf"]
    pdf_text_ratio = round(pdf_texty / pdf_total, 2) if pdf_total else 0.0

    pref_log_event(pref, {"type":"end","pref":pref,"pages":len(rows_detail),
                          "earliest_year":earliest,"latest_year":latest,
                          "coverage_years":coverage_years,
                          "pdf_text_ratio":pdf_text_ratio,
                          "machine_files":machine_hits})
    pref_log_txt(pref, f"[END] pages={len(rows_detail)} years={earliest}-{latest} cov={coverage_years} pdf_text={pdf_texty}/{pdf_total}")

    return rows_detail, {
        "prefecture": pref,
        "pages_scanned": len(rows_detail),
        "earliest_year": earliest,
        "latest_year": latest,
        "coverage_years": coverage_years,
        "count_html": counts["html"],
        "count_xml": counts["xml"],
        "count_pdf": counts["pdf"],
        "count_csv": counts["csv"],
        "count_json": counts["json"],
        "count_xls": counts["xls"],
        "count_other": counts["other"],
        "pdf_text_ratio": pdf_text_ratio,
        "machine_file_hits": machine_hits
    }

# ========= メイン =========
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pref", type=str, default=None, help="対象都道府県名（未指定なら全件）")
    parser.add_argument("--max-pages", type=int, default=60, help="1県あたり最大取得ページ数")
    parser.add_argument("--max-depth", type=int, default=2, help="リンク深さの最大値")
    parser.add_argument("--workers", type=int, default=4, help="並列数")
    parser.add_argument("--sleep", type=float, default=0.4, help="取得間隔(秒)")
    args = parser.parse_args()

    setup_dirs()
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"not found: {INPUT_CSV}")

    seeds = list(csv.DictReader(INPUT_CSV.open(encoding="utf-8")))
    if args.pref:
        seeds = [r for r in seeds if r["prefecture"] == args.pref]
        if not seeds:
            raise SystemExit(f"pref not found in CSV: {args.pref}")

    summaries = []
    OUT_SUM.write_text("", encoding="utf-8")  # 初回は空ファイルで作る

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = []
        for r in seeds:
            pref = r["prefecture"].strip()
            url  = r["url"].strip()
            futs.append(ex.submit(introspect_pref, pref, url, args.max_pages, args.max_depth, args.sleep))

        for fut in as_completed(futs):
            try:
                rows_detail, summary = fut.result()
                # 明細出力
                inv_path = OUT_DIR / f"{safe_name(summary['prefecture'])}.inventory.csv"
                with inv_path.open("w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=["url","depth","from","status","ctype","kind","title","years","text_len"])
                    w.writeheader(); w.writerows(rows_detail)
                # サマリ蓄積
                summaries.append(summary)
            except Exception as e:
                print("worker exception:", e, flush=True)

    # サマリ出力（県名ソート）
    summaries.sort(key=lambda x: x["prefecture"])
    with OUT_SUM.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "prefecture","pages_scanned","earliest_year","latest_year","coverage_years",
            "count_html","count_xml","count_pdf","count_csv","count_json","count_xls","count_other",
            "pdf_text_ratio","machine_file_hits"
        ])
        w.writeheader(); w.writerows(summaries)

    print(f"saved: {OUT_SUM}")
    print(f"saved inventories under: {OUT_DIR}")

if __name__ == "__main__":
    main()

