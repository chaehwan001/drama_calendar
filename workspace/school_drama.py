# -*- coding: utf-8 -*-
"""
카테고리 '대한민국의 학교 드라마'에서 모든 컬럼(ㄱ/ㄴ/ㄷ …) + 페이지네이션까지 훑어서
각 항목 상세 페이지의 '제목 / 장르 / 방송사'만 추출하여 CSV 저장.

출력: school_dramas_all.csv (UTF-8 with BOM), 컬럼: title, genre, broadcaster
"""

import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://ko.wikipedia.org"
CATEGORY_URL = "https://ko.wikipedia.org/wiki/%EB%B6%84%EB%A5%98:%EB%8C%80%ED%95%9C%EB%AF%BC%EA%B5%AD%EC%9D%98_%ED%95%99%EA%B5%90_%EB%93%9C%EB%9D%BC%EB%A7%88"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.5",
}

SLEEP = 0.7  # 요청 간 매너 타임(초)

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\[[^\]]*\]", "", s)  # [주 1], [1] 등 제거
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def iter_all_category_links(first_url: str):
    """
    카테고리 페이지에서:
      - 컬럼 인덱스 1부터 증가시키며 존재할 때까지(없으면 종료)
      - 각 컬럼의 모든 <li><a>를 수집
      - '다음 페이지'가 있으면 따라가며 반복
    """
    url = first_url
    seen_urls = set()

    while url and url not in seen_urls:
        seen_urls.add(url)
        soup = get_soup(url)

        col_idx = 1
        found_any = False
        while True:
            col_sel = f"#mw-pages > div > div > div:nth-child({col_idx})"
            col = soup.select_one(col_sel)
            if not col:
                break

            for a in col.select("ul > li > a"):
                href = a.get("href")
                if not href:
                    continue
                yield urljoin(BASE, href)
                found_any = True

            col_idx += 1

        if not found_any:
            break

        # '다음 페이지' 따라가기
        next_link = None
        for a in soup.select("#mw-pages a"):
            if clean_text(a.get_text()) == "다음 페이지":
                next_link = urljoin(BASE, a.get("href"))
                break

        url = next_link
        time.sleep(SLEEP)

def scrape_detail(detail_url: str) -> dict:
    """
    상세 페이지에서 '제목 / 장르 / 방송사' 추출.
    - 제목: h1#firstHeading
    - 장르/방송사: infobox의 th 라벨 확인
    """
    soup = get_soup(detail_url)

    # 제목
    title_el = soup.select_one("#firstHeading")
    title = clean_text(title_el.get_text()) if title_el else ""

    # infobox
    infobox = soup.select_one("#mw-content-text table.infobox")
    genre = ""
    broadcaster = ""

    if infobox:
        for tr in infobox.select("tr"):
            th = tr.find("th")
            td = tr.find("td")
            if not td:
                continue
            label = clean_text(th.get_text()) if th else ""
            value_text = clean_text(td.get_text())

            # 장르
            if label and ("장르" in label) and not genre:
                genre = value_text

            # 방송사/채널/방송국
            if label and re.search(r"(방송\s*사|방송\s*채널|채널|방송국)", label) and not broadcaster:
                links = [clean_text(a.get_text()) for a in td.select("a") if clean_text(a.get_text())]
                broadcaster = "; ".join(links) if links else value_text

    return {"title": title, "genre": genre, "broadcaster": broadcaster}

def main():
    print("[*] 크롤링 시작:", CATEGORY_URL)
    rows = []
    seen = set()

    for idx, url in enumerate(iter_all_category_links(CATEGORY_URL), 1):
        if url in seen:
            continue
        seen.add(url)

        print(f"  ({idx}) {url}")
        try:
            row = scrape_detail(url)
            rows.append(row)
        except Exception as e:
            print(f"     - FAIL: {e}")
        time.sleep(SLEEP)

    if not rows:
        print("[-] 결과가 없습니다.")
        return

    # DataFrame 구성 및 정리
    df = pd.DataFrame(rows, columns=["title", "genre_name", "channel_name"])
    for c in ["title", "genre_name", "channel_name"]:
        df[c] = df[c].astype(str).map(clean_text)

    # 제목 기준 중복 제거
    df = df.drop_duplicates(subset=["title"])

    out = "school_dramas_all.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: {out} (행 수: {len(df)})")

if __name__ == "__main__":
    main()
