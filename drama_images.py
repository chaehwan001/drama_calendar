# -*- coding: utf-8 -*-
import os, re
from pathlib import Path
from urllib.parse import quote, urljoin, urlparse
import requests, pandas as pd
from bs4 import BeautifulSoup

CSV_PATH  = Path("kdrama_2025.csv")
OUT_DIR   = Path("namu_images")
FINAL_CSV = Path("drama_image.csv")

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
    p = urlparse(url); return any(p.path.startswith(pref) for pref in ALLOWED_PREFIXES)

def nurl(src: str, base: str = BASE) -> str:
    if not src: return src
    s = src.strip()
    if s.startswith("//"): return "https:" + s
    if s.startswith("/"):  return urljoin(base, s)
    if s.lower().startswith(("http://","https://")): return s
    return urljoin(base, s)

def sanitize(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", str(name)).strip() or "untitled"

def norm_title(s: str) -> str:
    if not s: return ""
    t = str(s).strip()
    t = re.sub(r"\s*\(드라마\)\s*$", "", t)
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

def find_and_download(title_display: str):
    """(드라마) → (드라마)무공백 → 기본 순서로 og:image 찾고 다운로드."""
    base = norm_title(title_display)
    for cand in (f"{base} (드라마)", f"{base}(드라마)", base):
        html, page_url, _ = open_w_exact(cand)
        if html is None: continue
        img_url = extract_og_image(html)
        if not img_url: continue
        ext = os.path.splitext(img_url.split("?")[0].split("#")[0])[-1] or ".jpg"
        outp = OUT_DIR / f"{sanitize(title_display)}{ext}"
        if download_image(img_url, outp, referer=page_url):
            return img_url
    return None  # 실패 시 None

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(CSV_PATH, encoding="utf-8")
    title_col = next((c for c in ("title","제목") if c in df.columns), None)
    if not title_col: raise SystemExit("CSV에 'title' 또는 '제목' 컬럼이 없습니다.")
    titles = [str(x).strip() for x in df[title_col].fillna("") if str(x).strip()]

    out_rows = []
    for i, t in enumerate(titles, 1):
        print(f"[{i}/{len(titles)}] {t} ...", end="")
        img_url = find_and_download(t)
        if img_url:
            print(" OK")
            url_value = img_url
        else:
            print(" (no image)")
            url_value = ""  # 못가져오면 비워둠

        out_rows.append({
            "title": t,
            "type": "drama_image",
            "url": url_value,
            "sort_no": 1
        })

    pd.DataFrame(out_rows, columns=["title","type","url","sort_no"]) \
      .to_csv(FINAL_CSV, index=False, encoding="utf-8-sig")

    ok_cnt = sum(1 for r in out_rows if r["url"])
    print(f"\n저장 완료: {FINAL_CSV} (총 {len(out_rows)}개, 성공 {ok_cnt}개, 실패 {len(out_rows)-ok_cnt}개)")
    print(f"이미지 폴더: {OUT_DIR}")

if __name__ == "__main__":
    main()
