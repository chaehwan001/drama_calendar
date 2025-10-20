# -*- coding: utf-8 -*-
"""
Namuwiki drama image fetcher (심플 플로우 버전)

요구사항대로 단순화:
  1) /w/{제목} 기본 페이지 열어 html0, href0 확보
  2) (드라마) 변형 2종(공백/무공백) 중 '존재하면' 그 문서의 og:image 저장 → 끝
     - (드라마) 문서가 있지만 이미지가 없거나 다운로드 실패면 기본으로 폴백
  3) (드라마) 문서가 없으면 기본 페이지의 og:image 저장
  4) 위가 모두 실패면 FAIL

검색, cue 탐지 전부 제거함.
"""

import os
import re
import csv
from pathlib import Path
from urllib.parse import quote, urljoin, urlparse, unquote

import requests
import pandas as pd
from bs4 import BeautifulSoup

CSV_PATH = Path("kdrama_2025.csv")
OUT_DIR  = Path("namu_images")
LOG_PATH = Path("namu_image_results.csv")

# ===== 설정 =====
BASE = "https://namu.wiki"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0.0 Safari/537.36"),
    "Accept-Language": "ko,ko-KR;q=0.9,en;q=0.8",
}
TIMEOUT = 4
ALLOWED_PREFIXES = ("/", "/w/", "/img/", "/i/", "/js/", "/css/", "/_nuxt/")

# ===== 유틸 =====
def allowed(url: str) -> bool:
    p = urlparse(url)
    return any(p.path.startswith(pref) for pref in ALLOWED_PREFIXES)

def nurl(src: str, base: str = BASE) -> str:
    if not src:
        return src
    s = src.strip()
    if s.startswith("//"):
        return "https:" + s
    if s.startswith("/"):
        return urljoin(base, s)
    if s.lower().startswith(("http://", "https://")):
        return s
    return urljoin(base, s)

def sanitize(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", str(name)).strip() or "untitled"

def norm_title(s: str) -> str:
    if not s:
        return ""
    t = str(s).strip()
    t = re.sub(r"\s*\(드라마\)\s*$", "", t)   # 입력에 (드라마) 붙어있으면 제거
    t = re.sub(r"[《》〈〉“”‘’\"'`]+", "", t)
    t = re.sub(r"\s+", " ", t).strip(" .")
    return t

def get(url: str):
    if not allowed(url):
        return None, "disallowed_path"
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return None, f"http_{r.status_code}"
        return r.text, ""
    except requests.RequestException as e:
        return None, type(e).__name__

def extract_og_image(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("meta", attrs={"property": "og:image"})
    if not tag:
        return None
    val = (tag.get("content") or "").strip()
    if not val:
        return None
    if val.startswith("data:"):
        return None
    if re.search(r"\.(svg|ico)(?:$|\?)", val, re.I):
        return None
    if re.search(r"(logo|favicon|sprite|icon)", val, re.I):
        return None
    return nurl(val)

def open_w_exact(title_text: str):
    """정확히 /w/{title} 문서를 연다."""
    url = f"{BASE}/w/{quote(title_text, safe='')}"
    html, err = get(url)
    if html is None:
        return None, url, f"open_detail_failed:{err}"
    return html, url, "OK"

def download_image(url: str, out_path: Path, referer: str) -> bool:
    if not allowed(url):
        return False
    headers = dict(HEADERS)
    headers["Referer"] = referer
    try:
        with requests.get(url, headers=headers, stream=True, timeout=TIMEOUT) as r:
            if r.status_code != 200:
                return False
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(16384):
                    if chunk:
                        f.write(chunk)
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

# ===== 메인 플로우 (단순화) =====
def process_title(title: str):
    """
    0) 기본 /w/{제목} 열기 (html0, href0)
    1) (드라마) 변형 2종 중 하나라도 '문서가 있으면' 그 문서 이미지 저장
       - 이미지가 없거나 다운로드 실패면 기본으로 폴백
    2) (드라마) 문서가 없으면 기본 페이지 이미지 저장
    """
    base = norm_title(title)

    # 0) 기본 문서
    html0, href0, note0 = open_w_exact(base)

    # 1) (드라마) 변형 (공백/무공백)
    for dv in (f"{base} (드라마)", f"{base}(드라마)"):
        html1, href1, note1 = open_w_exact(dv)
        if html1 is not None:
            res = save_og_as(title, href1, html1, "drama:exact")
            if res["status"] == "OK":
                return res
            # (드라마) 문서는 있으나 이미지 문제 → 기본으로 폴백 시도 (루프 탈출)
            break

    # 2) 기본 문서로 폴백
    if html0 is not None:
        res0 = save_og_as(title, href0, html0, "base_page")
        if res0["status"] == "OK":
            return res0
        return {"title": title, "status": "FAIL", "note": "no_image_on_base",
                "page_url": href0, "image_url": "", "saved_path": ""}

    # 기본 문서 자체가 열리지 않은 경우
    return {"title": title, "status": "FAIL", "note": f"open_base_failed:{note0}",
            "page_url": href0, "image_url": "", "saved_path": ""}

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
