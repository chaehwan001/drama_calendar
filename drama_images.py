# -*- coding: utf-8 -*-
"""
Namuwiki drama image fetcher (requests-only, user-selectors + fallback)

규칙:
  1) /w/{제목} 기본 페이지 열기
     - cue 탐지(방송시간/방송기간/제작사)가 성공하면 저장
     - cue 실패 → (드라마) 변형 시도
  2) (드라마) 변형 시도 (공백 포함/미포함)
     - 문서 있으면 저장
  3) (드라마) 변형도 실패하면 검색(/Search?q=...) 시도
  4) 모든 게 실패하면 → 마지막으로 기본 페이지에서 og:image만 시도
"""

import os
import re
import csv
from pathlib import Path
from urllib.parse import quote, urljoin, urlparse, unquote

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ===== 고정 경로 =====
CSV_PATH = Path(r"C:\Users\PC\Desktop\workspace\kdrama_2025.csv")
OUT_DIR  = Path(r"C:\Users\PC\Desktop\workspace\namu_images")
LOG_PATH = Path(r"C:\Users\PC\Desktop\workspace\namu_image_results.csv")

# ===== 설정 =====
BASE = "https://namu.wiki"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0.0 Safari/537.36"),
    "Accept-Language": "ko,ko-KR;q=0.9,en;q=0.8",
}
TIMEOUT = 4
ALLOWED_PREFIXES = ("/", "/w/", "/Search", "/img/", "/i/", "/js/", "/css/", "/_nuxt/")
SEARCH_CAND_LIMIT = 12
PRIMARY_FIRST_CELL = "article table tbody tr:nth-of-type(1) td strong a:nth-of-type(2)"
NS_BLOCK = {"틀:", "분류:", "파일:", "나무뉴스:", "포털:", "나무위키:"}

# ===== 유틸 =====
def allowed(url: str) -> bool:
    p = urlparse(url)
    return any(p.path.startswith(pref) for pref in ALLOWED_PREFIXES)

def nurl(src: str, base: str = BASE) -> str:
    if not src: return src
    s = src.strip()
    if s.startswith("//"): return "https:" + s
    if s.startswith("/"):  return urljoin(base, s)
    if s.lower().startswith(("http://", "https://")): return s
    return urljoin(base, s)

def sanitize(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", str(name)).strip() or "untitled"

def norm_title(s: str) -> str:
    if not s: return ""
    t = str(s).strip()
    t = re.sub(r"\s*\(드라마\)\s*$", "", t)   # 입력에 (드라마) 붙어있으면 제거
    t = re.sub(r"[《》〈〉“”‘’\"'`]+", "", t)
    t = re.sub(r"\s+", " ", t).strip(" .")
    return t

def get(url: str):
    if not allowed(url): return None, "disallowed_path"
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200: return None, f"http_{r.status_code}"
        return r.text, ""
    except requests.RequestException as e:
        return None, type(e).__name__

def extract_og_image(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("meta", attrs={"property": "og:image"})
    if not tag: return None
    val = (tag.get("content") or "").strip()
    if not val: return None
    if val.startswith("data:"): return None
    if re.search(r"\.(svg|ico)(?:$|\?)", val, re.I): return None
    if re.search(r"(logo|favicon|sprite|icon)", val, re.I): return None
    return nurl(val)

def page_title_from_href(href: str) -> str:
    try:
        path = urlparse(href).path
        seg = path.split("/w/", 1)[1]
        return unquote(seg)
    except Exception:
        return ""

# ===== cue 탐지 =====
USER_CUE_SELECTORS = [
    "#app > div._33tYGuFD.j70YQcVX > div._2YoJ2ekz.gjUlfO4c.nJWG3DK4 > div > "
    "div._2TXHt7\\+b > div.JQ25nrVt.xziyWtO1 > div.XnsdLk84.V1CsAF42 > "
    "div.a2-QXwj\\+.uAm4KzJH > div.LNdKUH95.mwhzH2ge > table > tbody > "
    "tr:nth-child(5) > td._59c09bccb6640fbd0e897b67a6e26041 > div > strong",

    "#app > div._33tYGuFD.j70YQcVX > div._2YoJ2ekz.gjUlfO4c.nJWG3DK4 > div > "
    "div._2TXHt7\\+b > div.JQ25nrVt.xziyWtO1 > div.XnsdLk84.V1CsAF42 > "
    "div.a2-QXwj\\+.uAm4KzJH > div.LNdKUH95.mwhzH2ge > table > tbody > "
    "tr:nth-child(6) > td._59c09bccb6640fbd0e897b67a6e26041 > div > strong",

    "#app > div._33tYGuFD.j70YQcVX > div._2YoJ2ekz.gjUlfO4c.nJWG3DK4 > div > "
    "div._2TXHt7\\+b > article > div.XnsdLk84.V1CsAF42 > "
    "div.a2-QXwj\\+.uAm4KzJH > div.LNdKUH95.mwhzH2ge > table > tbody > "
    "tr:nth-child(6) > td._60767ea1adaefa1c429fee5d0f53cfcf > div > strong",
]
CUE_KEYWORDS = {"방송시간", "방송 시간", "방송기간", "방송 기간", "제작사"}

def has_user_cue_or_fallback(html: str) -> tuple[bool, str]:
    soup = BeautifulSoup(html, "html.parser")

    for sel in USER_CUE_SELECTORS:
        el = soup.select_one(sel)
        if not el: continue
        txt = (el.get_text() or "").strip()
        if txt in ("방송시간", "방송기간"):
            return True, "user_sel"

    article = soup.select_one("article")
    if not article:
        return False, "none"

    tables = article.select("table")[:6] or []
    for tbl in tables:
        for el in tbl.select("strong, th, td, b, span"):
            raw = (el.get_text() or "").strip()
            if not raw: continue
            norm = re.sub(r"\s+", " ", raw).strip(" :\t\r\n")
            if norm in CUE_KEYWORDS:
                return True, "fallback"
    return False, "none"

# ===== 검색 후보 고르기 =====
def pick_first_result_href_from_search(html: str, prefer_exact: str | None = None) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    a = soup.select_one(PRIMARY_FIRST_CELL)
    if a and a.get("href"):
        href = nurl(a.get("href"))
        if allowed(href):
            if not prefer_exact or page_title_from_href(href) == prefer_exact:
                return href
    cands = []
    for a in soup.select("article a[href^='/w/']"):
        href = a.get("href") or ""
        full = nurl(href)
        if not allowed(full): continue
        title_text = page_title_from_href(href)
        if any(title_text.startswith(ns) for ns in NS_BLOCK):
            continue
        cands.append((full, title_text))
        if len(cands) >= SEARCH_CAND_LIMIT * 3:
            break
    if not cands:
        return None
    if prefer_exact:
        for full, title_text in cands:
            if title_text == prefer_exact:
                return full
    return cands[0][0]

# ===== 열기 보조 =====
def open_w_exact(title_text: str):
    url = f"{BASE}/w/{quote(title_text, safe='')}"
    html, err = get(url)
    if html is None:
        return None, url, f"open_detail_failed:{err}"
    return html, url, "OK"

def search_and_open_first(query: str, prefer_exact: str | None = None):
    url = f"{BASE}/Search?q={quote(query)}"
    html, err = get(url)
    if html is None:
        return None, "", f"open_search_failed:{err}"
    href = pick_first_result_href_from_search(html, prefer_exact=prefer_exact)
    if not href:
        return None, "", "no_first_result"
    doc, err2 = get(href)
    if doc is None:
        return None, href, f"open_detail_failed:{err2}"
    return doc, href, "OK"

def download_image(url: str, out_path: Path, referer: str) -> bool:
    if not allowed(url): return False
    headers = dict(HEADERS); headers["Referer"] = referer
    try:
        with requests.get(url, headers=headers, stream=True, timeout=TIMEOUT) as r:
            if r.status_code != 200: return False
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(16384):
                    if chunk: f.write(chunk)
        return True
    except requests.RequestException:
        return False

def save_og_as(title_disp: str, page_url: str, html: str, note_tag: str):
    img = extract_og_image(html)
    if not img:
        return {"title": title_disp, "status": "FAIL", "note": f"{note_tag}:no_og_image",
                "page_url": page_url, "image_url": "", "saved_path": ""}
    safe = sanitize(title_disp)
    ext = os.path.splitext(img.split("?")[0].split("#")[0])[-1] or ".jpg"
    outp = OUT_DIR / f"{safe}{ext}"
    if not download_image(img, outp, referer=page_url):
        return {"title": title_disp, "status": "FAIL", "note": f"{note_tag}:download_failed",
                "page_url": page_url, "image_url": img, "saved_path": ""}
    return {"title": title_disp, "status": "OK", "note": note_tag,
            "page_url": page_url, "image_url": img, "saved_path": str(outp)}

# ===== 메인 플로우 =====
def process_title(title: str):
    base = norm_title(title)

    # 0) 기본 문서
    html0, href0, note0 = open_w_exact(base)
    if html0 is not None:
        ok, how = has_user_cue_or_fallback(html0)
        if ok:
            return save_og_as(title, href0, html0, f"exact_base:{how}")

    # 1) (드라마) 변형
    drama_variants = [f"{base} (드라마)", f"{base}(드라마)"]
    for dv in drama_variants:
        html1, href1, note1 = open_w_exact(dv)
        if html1 is not None:
            return save_og_as(title, href1, html1, "drama:exact")

    # 2) 검색 보조
    html2, href2, note2 = search_and_open_first(f"{base} 드라마", prefer_exact=drama_variants[0])
    if html2 is not None:
        return save_og_as(title, href2, html2, "drama:search_exact")

    # 3) '(드라마)' 자체 검색
    for dv in drama_variants:
        html3, href3, note3 = search_and_open_first(dv, prefer_exact=dv)
        if html3 is not None:
            return save_og_as(title, href3, html3, "drama:search_exact2")

    # 4) 모든 게 실패 → 기본 문서 og:image만 시도
    if html0 is not None:
        res_try = save_og_as(title, href0, html0, "fallback:og_only")
        if res_try["status"] == "OK":
            return res_try

    return {
        "title": title, "status": "FAIL",
        "note": f"no_match:{note0}",
        "page_url": href0, "image_url": "", "saved_path": ""
    }

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(CSV_PATH, encoding="utf-8")
    title_col = next((c for c in ("title", "제목") if c in df.columns), None)
    if not title_col:
        raise SystemExit("CSV에 'title' 또는 '제목' 컬럼이 없습니다.")
    titles = [str(x).strip() for x in df[title_col].fillna("") if str(x).strip()]

    rows = []
    for i, t in enumerate(titles, 1):
        print(f"[{i}/{len(titles)}] {t} ...")
        res = process_title(t)
        rows.append(res)
        print(f"  -> {res['status']} | note={res['note']} | page={res.get('page_url','')} | img={res.get('image_url','')}")

    keys = ["title", "page_url", "image_url", "saved_path", "status", "note"]
    with open(LOG_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)

    ok = sum(1 for r in rows if r["status"] == "OK")
    print(f"\nDone. OK={ok}, FAIL={len(rows)-ok}, out_dir={OUT_DIR}, log={LOG_PATH}")

if __name__ == "__main__":
    main()
