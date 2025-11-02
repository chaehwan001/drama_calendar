# -*- coding: utf-8 -*-
"""
actor_images.py — NamuWiki actor image fetcher

입력:  person.csv (컬럼: name 또는 이름)
동작:  각 이름에 대해 '이름 (배우)' → '이름(배우)' → '이름' 순으로 나무위키 페이지 열기
       og:image(대표 이미지) 추출 후 파일 저장 및 CSV 기록
출력:  person_image.csv (컬럼: type, url, sort_no)  ※ type='image', sort_no=1 고정
      이미지 파일은 namu_person_images/ 폴더에 저장
"""

import os, re
from pathlib import Path
from urllib.parse import quote, urljoin, urlparse
import requests, pandas as pd
from bs4 import BeautifulSoup

CSV_PATH  = Path("person.csv")
OUT_DIR   = Path("namu_person_images")
FINAL_CSV = Path("person_image.csv")

BASE = "https://namu.wiki"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0.0 Safari/537.36"),
    "Accept-Language": "ko,ko-KR;q=0.9,en;q=0.8",
}
TIMEOUT = 4
ALLOWED_PREFIXES = ("/", "/w/", "/img/", "/i/", "/js/", "/css/", "/_nuxt/")

def allowed(url: str) -> bool:
    p = urlparse(url)
    return any(p.path.startswith(pref) for pref in ALLOWED_PREFIXES)

def nurl(src: str, base: str = BASE) -> str:
    if not src: return src
    s = src.strip()
    if s.startswith("//"): return "https:" + s
    if s.startswith("/"):  return urljoin(base, s)
    if s.lower().startswith(("http://","https://")): return s
    return urljoin(base, s)

def sanitize(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", str(name)).strip() or "untitled"

def norm_name(s: str) -> str:
    if not s: return ""
    t = str(s).strip()
    # 뒤에 이미 (배우)가 붙어 있으면 정리
    t = re.sub(r"\s*\(배우\)\s*$", "", t)
    t = re.sub(r"[《》〈〉“”‘’\"'`]+", "", t)
    t = re.sub(r"\s+", " ", t).strip(" .")
    return t

def get_html(url: str):
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
    if not val or val.startswith("data:"): return None
    if re.search(r"\.(svg|ico)(?:$|\?)", val, re.I): return None
    if re.search(r"(logo|favicon|sprite|icon)", val, re.I): return None
    return nurl(val)

def open_w_exact(title_text: str):
    url = f"{BASE}/w/{quote(title_text, safe='')}"
    html, err = get_html(url)
    if html is None: return None, url, f"open_failed:{err}"
    return html, url, "OK"

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

def find_and_download(name_display: str):
    """배우용: (배우) → (배우)무공백 → 기본 순서로 og:image 찾고 다운로드."""
    base = norm_name(name_display)
    for cand in (f"{base} (배우)", f"{base}(배우)", base):
        html, page_url, _ = open_w_exact(cand)
        if html is None: 
            continue
        img_url = extract_og_image(html)
        if not img_url:
            continue
        ext = os.path.splitext(img_url.split("?")[0].split("#")[0])[-1] or ".jpg"
        outp = OUT_DIR / f"{sanitize(name_display)}{ext}"
        if download_image(img_url, outp, referer=page_url):
            return img_url
    return None  # 실패 시 None

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 인코딩 자동 감지
    try:
        df = pd.read_csv(CSV_PATH, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(CSV_PATH, encoding="cp949")
    
    # name 컬럼 찾기
    name_col = next((c for c in ("name", "이름") if c in df.columns), None)
    if not name_col:
        raise SystemExit("CSV에 'name' 또는 '이름' 컬럼이 없습니다.")
    
    # 이름 목록 정리
    names = [str(x).strip() for x in df[name_col].fillna("") if str(x).strip()]

    out_rows = []
    for i, n in enumerate(names, 1):
        print(f"[{i}/{len(names)}] {n} ...", end="")
        img_url = find_and_download(n)
        if img_url:
            print(" OK")
            url_value = img_url
        else:
            print(" (no image)")
            url_value = ""  # 못가져오면 비워둠

        # 최종 CSV 스키마 (요청대로 name/title 없음)
        out_rows.append({
            "type": "image",
            "url": url_value,
            "sort_no": 1
        })

    pd.DataFrame(out_rows, columns=["type","url","sort_no"]) \
      .to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")

    ok_cnt = sum(1 for r in out_rows if r["url"])
    print(f"\n저장 완료: {FINAL_CSV} (총 {len(out_rows)}개, 성공 {ok_cnt}개, 실패 {len(out_rows)-ok_cnt}개)")
    print(f"이미지 폴더: {OUT_DIR}")

if __name__ == "__main__":
    main()
