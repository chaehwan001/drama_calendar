# -*- coding: utf-8 -*-
"""
Genre_Image.csv의 title에서 (드라마) 제거 → TMDB 검색 → TMDB 포스터 URL로 새로운 url 생성
기존 url은 무조건 무시하고 새로 만든 url로 덮어씀
"""

import argparse
import os
import time
import pandas as pd
import requests
from typing import Optional

TMDB_SEARCH_TV_URL = "https://api.themoviedb.org/3/search/tv"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p"  # /w500 + path


def read_csv_smart(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp949")


def clean_title(title: str) -> str:
    if not isinstance(title, str):
        title = str(title)
    t = title.strip()
    t = t.replace("(드라마)", "").strip()
    return t


def search_tmdb_tv(api_key: str, title: str) -> Optional[dict]:
    params = {
        "api_key": api_key,
        "query": title,
        "language": "ko-KR",
        "include_adult": "false",
    }
    try:
        res = requests.get(TMDB_SEARCH_TV_URL, params=params, timeout=5)
        res.raise_for_status()
        data = res.json()
    except Exception:
        return None

    results = data.get("results") or []
    if not results:
        return None
    return results[0]


def build_img_url(path: Optional[str], size: str = "w500") -> Optional[str]:
    if not path:
        return None
    return f"{TMDB_IMG_BASE}/{size}{path}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_csv", default="Genre_Image.csv")
    parser.add_argument("--out", dest="out_csv", default="Genre_Image_tmdb.csv")
    parser.add_argument("--api-key", dest="api_key")
    parser.add_argument("--sleep", dest="sleep_sec", type=float, default=0.25)
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("TMDB_API_KEY")
    if not api_key:
        raise SystemExit("TMDB_API_KEY 환경변수를 설정해야 합니다.")

    df = read_csv_smart(args.in_csv)

    if "title" not in df.columns:
        raise SystemExit("입력 CSV에 'title' 컬럼이 없습니다.")

    # title 정제 (드라마 제거)
    df["title"] = df["title"].astype(str).apply(clean_title)

    urls = []

    for idx, row in df.iterrows():
        title = row["title"]
        print(f"[{idx+1}/{len(df)}] TMDB 검색: {title} ...", end=" ")

        best = search_tmdb_tv(api_key, title)
        if best:
            poster_url = build_img_url(best.get("poster_path"))
            print(f"OK (poster={bool(poster_url)})")
        else:
            poster_url = None
            print("결과 없음")

        urls.append(poster_url)
        time.sleep(args.sleep_sec)

    # 기존 url 컬럼 삭제 후 새 url 컬럼 생성
    df["url"] = urls

    df.to_csv(args.out_csv, index=False, encoding="utf-8-sig")
    print(f"[완료] TMDB 포스터 URL로 갱신 완료: {args.out_csv}")


if __name__ == "__main__":
    main()
