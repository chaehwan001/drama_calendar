# -*- coding: utf-8 -*-
"""
카테고리 '대한민국의 공포 드라마'에서
제목 / 장르 / 방송사 크롤링 후 장르 자동 보정까지 한 번에 저장.

출력: horror_dramas_all.csv (UTF-8 with BOM)
보정 규칙:
  1) genre_name이 비었거나 None/Nan이면 → "공포"
  2) genre_name에 "공포"가 없으면 → "공포, {기존장르}"
"""

import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://ko.wikipedia.org"
CATEGORY_URL = "https://ko.wikipedia.org/wiki/%EB%B6%84%EB%A5%98:%EB%8C%80%ED%95%9C%EB%AF%BC%EA%B5%AD%EC%9D%98_%EA%B3%B5%ED%8F%AC_%EB%93%9C%EB%9D%BC%EB%A7%88"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.5",
}

SLEEP = 0.7


def clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\[[^\]]*\]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def iter_all_category_links(first_url: str):
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

        next_link = None
        for a in soup.select("#mw-pages a"):
            if clean_text(a.get_text()) == "다음 페이지":
                next_link = urljoin(BASE, a.get("href"))
                break

        url = next_link
        time.sleep(SLEEP)


def scrape_detail(detail_url: str) -> dict:
    soup = get_soup(detail_url)
    title_el = soup.select_one("#firstHeading")
    title = clean_text(title_el.get_text()) if title_el else ""

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

            if label and "장르" in label and not genre:
                genre = value_text

            if label and re.search(r"(방송\s*사|방송\s*채널|채널|방송국)", label) and not broadcaster:
                links = [clean_text(a.get_text()) for a in td.select("a") if clean_text(a.get_text())]
                broadcaster = "; ".join(links) if links else value_text

    return {
        "title": title,
        "genre_name": genre,
        "channel_name": broadcaster.replace(";", ", ").strip(", ").strip()
    }


def fix_genre_value(s: object) -> str:
    if pd.isna(s):
        return "공포"
    s = str(s).strip()
    if not s or s.lower() in {"nan", "none"}:
        return "공포"
    s = s.strip(",; ")
    if "공포" in s:
        return s
    fixed = f"공포, {s}"
    fixed = re.sub(r"[;,]\s*[;,]+", ", ", fixed)
    fixed = re.sub(r"\s*,\s*", ", ", fixed)
    return fixed.strip(",; ").strip() or "공포"


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

    df = pd.DataFrame(rows, columns=["title", "genre_name", "channel_name"])
    for c in ["title", "genre_name", "channel_name"]:
        df[c] = df[c].astype(str).map(clean_text)

    df = df.drop_duplicates(subset=["title"])

    # 🔹 장르 자동 보정
    df["genre_name"] = df["genre_name"].apply(fix_genre_value)

    out = "horror_dramas_all.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✓] 장르 보정 포함 저장 완료: {out} (행 수: {len(df)})")


if __name__ == "__main__":
    main()
