# -*- coding: utf-8 -*-
"""
description.py — NamuWiki drama table scraper (section-locked + fallback)

요구사항
- 공통 셀렉터: 섹션 5~10 범위의 테이블만 1차 캡처
  (직계 table + div.kZb-CLkK._1BEih8Vh > table 둘 다 포함)
- 1차에서 못 찾으면 article 전역 테이블 폴백
- table의 모든 th/td 텍스트를 탭/줄바꿈 없이 '한 줄'로 직렬화
- (드라마)/(드라마) 변형 문서도 동일 로직
- CSV 저장 시 index 제거, UTF-8-SIG
"""

import re
import time
from pathlib import Path
from urllib.parse import quote

import requests
import pandas as pd
from bs4 import BeautifulSoup, Tag

IN_CSV  = Path("kdrama_2025_fin.csv")
OUT_CSV = Path("description.csv")

BASE = "https://namu.wiki"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0.0 Safari/537.36"),
    "Accept-Language": "ko,ko-KR;q=0.9,en;q=0.8",
}
TIMEOUT = 8
SLEEP   = 0.35

COMMON_SELECTOR_PRIMARY = (
    "div.BpaiDiJp.M4Ezwymi > div:nth-child(5) div.kZb-CLkK._1BEih8Vh > table, "
    "div.BpaiDiJp.M4Ezwymi > div:nth-child(6) div.kZb-CLkK._1BEih8Vh > table, "
    "div.BpaiDiJp.M4Ezwymi > div:nth-child(7) div.kZb-CLkK._1BEih8Vh > table, "
    "div.BpaiDiJp.M4Ezwymi > div:nth-child(8) div.kZb-CLkK._1BEih8Vh > table"
)


# 폴백: article 전역 테이블
COMMON_SELECTOR_FALLBACK = "article table"

def norm_title(s: str) -> str:
    if not s:
        return ""
    t = str(s).strip()
    t = re.sub(r"\s*\(드라마\)\s*$", "", t)
    t = re.sub(r"\s+", " ", t).strip(" .")
    return t

def get_html(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            print(f"[http] {r.status_code}: {url}")
            return None
        return r.text
    except requests.RequestException as e:
        print(f"[error] request_exception: {url} ({e})")
        return None

def open_w(title_text: str) -> str | None:
    url = f"{BASE}/w/{quote(title_text, safe='')}"
    html = get_html(url)
    print(f"[open] {title_text} -> {'OK' if html else 'FAIL'} {url}")
    return html

def table_to_text_one_line(tbl: Tag) -> str:
    cells = tbl.select("th, td")
    if not cells:
        return ""
    text = " ".join(c.get_text(" ", strip=True) for c in cells)
    # 각주 제거
    text = re.sub(r"\[[^\]]*\]", "", text)
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text

def pick_best_table(tables: list[Tag]) -> Tag | None:
    if not tables:
        return None
    return max(tables, key=lambda t: len(t.select("th, td")))

def extract_by_common_selector(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # 1차: 섹션 5~10 범위 내 테이블 (직계 + 래퍼 내부)
    primary = soup.select(COMMON_SELECTOR_PRIMARY)
    primary = [t for t in primary if len(t.select("th, td")) > 0]

    candidates = primary

    # 1차에서 못 찾으면 폴백(article 전역)
    if not candidates:
        fallback = soup.select(COMMON_SELECTOR_FALLBACK)
        fallback = [t for t in fallback if len(t.select("th, td")) > 0]
        candidates = fallback

    if not candidates:
        return ""

    best = pick_best_table(candidates)
    return table_to_text_one_line(best) if best else ""

def process_one_title(title: str) -> str:
    base = norm_title(title)

    for dv in (f"{base} (드라마)", f"{base}(드라마)"):
        html = open_w(dv)
        if html:
            txt = extract_by_common_selector(html)
            if txt:
                print(f"[OK] matched on drama-variant: {dv}")
                return txt
            else:
                print(f"[miss] no match on drama-variant: {dv}")

    html = open_w(base)
    if html:
        txt = extract_by_common_selector(html)
        if txt:
            print("[OK] matched on base page")
            return txt
        else:
            print("[miss] no match on base page")

    print("[FAIL] no table matched")
    return ""

def main():
    if not IN_CSV.exists():
        raise SystemExit("kdrama_2025.csv 파일이 없습니다.")
    df = pd.read_csv(IN_CSV, encoding="utf-8")
    title_col = next((c for c in ("title", "제목") if c in df.columns), None)
    if not title_col:
        raise SystemExit("CSV에 'title' 또는 '제목' 컬럼이 없습니다.")

    rows = []
    titles = [str(x).strip() for x in df[title_col].fillna("") if str(x).strip()]
    for i, t in enumerate(titles, 1):
        print(f"\n[{i}/{len(titles)}] {t}")
        desc = process_one_title(t)
        rows.append({"title": t, "description": desc})
        time.sleep(SLEEP)

    out_df = pd.DataFrame(rows, columns=["title", "description"])
    out_df = out_df.drop_duplicates(subset=["title"], keep="last")
    out_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    filled = (out_df["description"].astype(str).str.len() > 0).sum()
    print(f"\n[✓] 저장 완료: {OUT_CSV} (총 {len(out_df)}행, 채움 {filled}개, 비어있음 {len(out_df)-filled}개)")

if __name__ == "__main__":
    main()
