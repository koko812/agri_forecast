#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pref_coverage_probe.py
- 入力: data/processed/pref_boujosho_links.csv （列: prefecture,url）
- 出力:
    data/processed/pref_coverage_score.csv
    logs/by_pref/<県名>.log
    logs/by_pref/<県名>.jsonl
- 概要:
  各県の起点URLから1階層だけ軽量スキャンし、年の痕跡やHTML比率などから
  「予察情報の充実度」を10点満点で概算スコア化。
  未来年は除外、公開日メタがあればそれを優先。
"""

import argparse
import csv
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# ================== 設定（ルートから実行前提） ==================
INPUT_CSV = Path("data/processed/pref_boujosho_links.csv")
OUT_CSV   = Path("data/processed/pref_coverage_score.csv")
BY_PREF   = Path("logs/by_pref")

UA = "agri-forecast-probe/0.2"
TIMEOUT = 20

# 予察関連のヒット指標
KEYWORDS  = ["予察", "発生予察", "発生情報", "注意報", "警報", "バックナンバー", "年度"]
URL_HINTS = ["yosatsu", "yosatu", "yohou", "byogaichu", "gaicyu", "gaichu"]

# 年・年度の抽出用（和暦/西暦）
YEAR_PAT = re.compile(r"(令和\s?\d+年度?|平成\s?\d+年度?|20\d{2}(?:年度?)?)")
# 将来系語の近傍に出る年は除外（例: 来年度計画）
FUTURE_GUARD_WORDS = ("計画", "予定", "案", "募集", "予算", "方針")

# ================== ユーティリティ ==================
def setup_dirs():
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    BY_PREF.mkdir(parents=True, exist_ok=True)

def safe_name(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", name)

def log_pref_text(pref: str, msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    p = BY_PREF / f"{safe_name(pref)}.log"
    with p.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

def log_pref_event(pref: str, event: dict):
    event = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), **event}
    p = BY_PREF / f"{safe_name(pref)}.jsonl"
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")

def fetch(url: str):
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        ct = r.headers.get("Content-Type", "").lower()
        return r.status_code, ct, r.content
    except Exception:
        return None, None, None

def is_same_domain(base: str, link: str) -> bool:
    try:
        b = urlparse(base).netloc.split(":")[0]
        l = urlparse(link).netloc.split(":")[0]
        return (b == l) or (b.endswith(".go.jp") and l.endswith(".go.jp"))
    except Exception:
        return False

# ================== 抽出・解析 ==================
def extract_links(base_url: str, html: str):
    """トップページから1階層目の予察系リンクを抽出"""
    soup = BeautifulSoup(html, "lxml")
    hits = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        full = urljoin(base_url, href)
        if not is_same_domain(base_url, full):
            continue
        if any(k in text for k in KEYWORDS) or any(h in href for h in URL_HINTS):
            hits.append({"title": text, "url": full})
    # 重複除去
    seen, out = set(), []
    for h in hits:
        if h["url"] not in seen:
            seen.add(h["url"])
            out.append(h)
    return out

def extract_year_candidates(text: str):
    """本文・タイトルから年/年度候補を抽出（今年より未来は除外。将来系近傍は除外）"""
    text = text.replace("　", " ")
    current_year = time.localtime().tm_year
    cand = []

    for m in YEAR_PAT.finditer(text):
        token = m.group(0).strip()
        y = None
        if token.startswith("令和"):
            n = int(re.sub(r"\D", "", token))
            y = 2018 + n  # 令和1=2019
        elif token.startswith("平成"):
            n = int(re.sub(r"\D", "", token))
            y = 1988 + n  # 平成1=1989
        else:
            num = re.search(r"20\d{2}", token)
            if num:
                y = int(num.group(0))

        if not y:
            continue
        if y > current_year:
            continue  # 未来年は除外

        start = max(0, m.start() - 20)
        end = min(len(text), m.end() + 20)
        near = text[start:end]
        if any(w in near for w in FUTURE_GUARD_WORDS):
            continue  # 将来系文脈の年は除外

        if 1990 <= y <= current_year:
            cand.append(y)

    return cand

def parse_html(content_bytes: bytes):
    """HTML: 公開日メタ優先で年を1つ採用。無ければ本文から候補抽出"""
    try:
        html = content_bytes.decode("utf-8", errors="ignore")
    except Exception:
        html = content_bytes.decode("cp932", errors="ignore")
    soup = BeautifulSoup(html, "lxml")

    meta_time = None
    for sel, attr in [
        ('meta[property="article:published_time"]', "content"),
        ('meta[property="og:updated_time"]', "content"),
        ('time[datetime]', "datetime"),
    ]:
        tag = soup.select_one(sel)
        if tag and tag.get(attr):
            try:
                meta_time = dateparser.parse(tag.get(attr), fuzzy=True)
                break
            except Exception:
                pass

    title = (soup.title.get_text(strip=True) if soup.title else "")[:200]
    body = " ".join(x.get_text(" ", strip=True) for x in soup.select("h1,h2,h3,p,li"))[:8000]

    if meta_time:
        years = [meta_time.year]
    else:
        years = extract_year_candidates(title + " " + body)

    return {"doc_type": "html", "title": title, "text_len": len(body), "years": years}

def parse_xml(content_bytes: bytes):
    """XML: XMLモードでパースしてテキストを素朴抽出→年候補"""
    try:
        xml = content_bytes.decode("utf-8", errors="ignore")
    except Exception:
        xml = content_bytes.decode("cp932", errors="ignore")
    soup = BeautifulSoup(xml, "lxml-xml")  # XMLモード
    title = (soup.title.get_text(strip=True) if soup.title else "")[:200]
    # XMLはタグ名様々なので、テキスト全体から軽く拾う
    body = soup.get_text(" ", strip=True)[:8000]
    years = extract_year_candidates(title + " " + body)
    return {"doc_type": "xml", "title": title, "text_len": len(body), "years": years}

def parse_pdf_quick(_content_bytes: bytes):
    """軽量版: PDF本文は読まない（必要になれば pdfminer を追加）"""
    return {"doc_type": "pdf", "title": "", "text_len": 0, "years": []}

# ================== スコアリング ==================
def score_pref(records: list):
    years = []
    html_cnt = xml_cnt = pdf_cnt = 0
    for r in records:
        years += r.get("years", [])
        t = r.get("doc_type", "")
        if t == "html":
            html_cnt += 1
        elif t == "xml":
            xml_cnt += 1
        elif t == "pdf":
            pdf_cnt += 1

    latest = max(years) if years else None
    earliest = min(years) if years else None

    current_year = time.localtime().tm_year
    hits_3y = sum(
        1 for r in records for y in r.get("years", []) if current_year - 2 <= y <= current_year
    )

    has_backnumber = any(
        ("バックナンバー" in (r.get("title", "") or "")) or ("年度" in (r.get("title", "") or ""))
        for r in records
    )

    total = max(1, len(records))
    html_ratio = html_cnt / total  # XMLはHTMLと別扱い

    # 10点満点の粗いスコア
    fresh = 2 if (latest and latest >= current_year - 1) else (1 if (latest and latest == current_year - 2) else 0)
    depth = 3 if has_backnumber else (2 if len(years) >= 10 else (1 if len(years) >= 3 else 0))
    volume = 3 if hits_3y >= 30 else (2 if hits_3y >= 10 else (1 if hits_3y >= 3 else 0))
    machine = 2 if html_ratio >= 0.6 else (1 if html_ratio >= 0.3 else 0)
    score10 = round(fresh + depth + volume + machine, 1)

    return {
        "latest_year": latest,
        "earliest_year": earliest,
        "hits_3y": hits_3y,
        "html_ratio": round(html_ratio, 2),
        "has_backnumber": int(has_backnumber),
        "score10": score10,
    }

# ================== 県ごとの処理 ==================
def process_pref(pref: str, url: str, max_follow: int):
    print(f"[SCAN] {pref} … {url}", flush=True)  # 進捗表示
    log_pref_text(pref, f"[START] {pref} {url}")
    log_pref_event(pref, {"type": "pref_start", "pref": pref, "url": url})

    code, ct, content = fetch(url)
    if code != 200 or content is None:
        log_pref_text(pref, f"[ERROR] root fetch failed status={code}")
        log_pref_event(pref, {"type": "error", "stage": "root_fetch", "status": code})
        return pref, {"latest_year": None, "earliest_year": None, "hits_3y": 0,
                      "html_ratio": 0.0, "has_backnumber": 0, "score10": 0.0}, ""

    root_html = ""
    if ct and ("text/html" in ct or "application/xhtml" in ct):
        try:
            root_html = content.decode("utf-8", errors="ignore")
        except Exception:
            root_html = content.decode("cp932", errors="ignore")

    links = extract_links(url, root_html) if root_html else []
    links = links[:max_follow]

    records = []
    for lk in links:
        time.sleep(0.4)  # レート制御
        u = lk["url"]
        title_hint = lk["title"]
        code2, ct2, content2 = fetch(u)
        log_pref_event(pref, {"type": "fetch", "url": u, "status": code2, "ct": ct2,
                              "bytes": len(content2) if content2 else 0})
        if code2 != 200 or content2 is None:
            continue

        # コンテンツタイプと拡張子でルーティング
        is_pdf = (ct2 and "pdf" in ct2) or u.lower().endswith(".pdf")
        is_xml = (ct2 and "xml" in ct2) or u.lower().endswith(".xml")

        if is_pdf:
            meta = parse_pdf_quick(content2)
        elif is_xml:
            meta = parse_xml(content2)
        else:
            meta = parse_html(content2)

        if not meta.get("title"):
            meta["title"] = title_hint
        meta["url"] = u
        records.append(meta)

    score = score_pref(records)
    samples = ";".join([r.get("url", "") for r in records[:3]])
    log_pref_event(pref, {"type": "pref_end", "pref": pref, **score, "samples": samples})
    log_pref_text(pref, f"[END] score={score['score10']} hits_3y={score['hits_3y']} latest={score['latest_year']}")
    return pref, score, samples

# ================== エントリポイント ==================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-per-pref", type=int, default=20, help="各県フォロー数上限")
    parser.add_argument("--workers", type=int, default=4, help="並列数")
    args = parser.parse_args()

    setup_dirs()
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"not found: {INPUT_CSV}")

    rows = list(csv.DictReader(INPUT_CSV.open(encoding="utf-8")))
    results = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(process_pref, r["prefecture"].strip(), r["url"].strip(), args.max_per_pref)
                   for r in rows]
        for fut in as_completed(futures):
            try:
                pref, score, samples = fut.result()
                results.append({
                    "prefecture": pref,
                    **score,
                    "sample_links": samples
                })
            except Exception as e:
                # 各県内の例外は県別JSONLに出ているはずなのでここでは表示のみ
                print("worker exception:", e, flush=True)

    results.sort(key=lambda x: x["prefecture"])
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "prefecture", "latest_year", "earliest_year", "hits_3y",
            "html_ratio", "has_backnumber", "score10", "sample_links"
        ])
        w.writeheader()
        w.writerows(results)

    print(f"saved: {OUT_CSV} ({len(results)} prefs)")

if __name__ == "__main__":
    main()

