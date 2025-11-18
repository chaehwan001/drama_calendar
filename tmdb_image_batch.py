# -*- coding: utf-8 -*-
"""
TMDB에서 드라마 포스터/백드롭 이미지 URL을 미리 받아와 CSV로 저장하는 스크립트 (방식 A)

[기능]
- 입력 CSV에서 드라마 제목(title/drama_title/제목)을 읽어서
- TMDB TV 검색 API로 조회
- 가장 상단 결과에서 poster_path, backdrop_path를 가져와
- 실제 이미지 URL(poster_url, backdrop_url)로 변환
- drama_tmdb_image.csv로 저장

[사용 예시]
    python tmdb_image_batch.py --in drama.csv --api-key TMDB_API_KEY

API 키를 코드 안에 직접 넣기 싫으면:
    set TMDB_API_KEY=내키
    python tmdb_image_batch.py --in drama.csv

Windows PowerShell 기준:
    $env:TMDB_API_KEY="내키"
    python tmdb_image_batch.py --in drama.csv
"""

import argparse
import os
import sys
import time
from typing import Optional

import pandas as pd
import requests

TMDB_SEARCH_TV_URL = "https://api.themoviedb.org/3/search/tv"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p"  # 뒤에 /w500, /original + path 붙임


def detect_title_column(df: pd.DataFrame) -> str:
    """CSV 안에서 제목으로 쓸 컬럼명을 자동 탐색."""
    candidates = ["title", "drama_title", "제목", "name"]
    for c in candidates:
        if c in df.columns:
            return c
    raise SystemExit(f"제목 컬럼을 찾을 수 없습니다. (지원: {candidates})")


def search_tmdb_tv(api_key: str, title: str) -> Optional[dict]:
    """TMDB TV 검색 API 호출 -> 최상단 결과 반환 (없으면 None)"""
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
    except Exception as e:
        print(f"[에러] TMDB 요청 실패: {title} -> {e}")
        return None

    results = data.get("results") or []
    if not results:
        return None

    # 일단 가장 첫 번째 결과 사용
    best = results[0]
    return best


def build_img_url(path: Optional[str], size: str = "w500") -> Optional[str]:
    """poster_path 같은 TMDB path를 실제 이미지 URL로 변환."""
    if not path:
        return None
    return f"{TMDB_IMG_BASE}/{size}{path}"


def main():
    parser = argparse.ArgumentParser(description="TMDB 드라마 이미지 배치 수집(방식 A)")
    parser.add_argument("--in", dest="in_csv", required=True, help="입력 CSV 경로 (드라마 제목 목록)")
    parser.add_argument("--out", dest="out_csv", default="drama_tmdb_image.csv", help="출력 CSV 경로")
    parser.add_argument("--api-key", dest="api_key", help="TMDB API 키 (없으면 TMDB_API_KEY 환경변수 사용)")
    parser.add_argument("--sleep", dest="sleep_sec", type=float, default=0.25,
                        help="API 호출 사이 딜레이(초) 기본=0.25")
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("TMDB_API_KEY")
    if not api_key:
        raise SystemExit("TMDB API 키가 없습니다. --api-key 또는 TMDB_API_KEY 환경변수로 설정해 주세요.")

    in_path = args.in_csv
    if not os.path.exists(in_path):
        raise SystemExit(f"입력 CSV를 찾을 수 없습니다: {in_path}")

    # CSV 읽기 (utf-8 → cp949 순서로 시도)
    try:
        df = pd.read_csv(in_path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(in_path, encoding="cp949")

    title_col = detect_title_column(df)
    print(f"[정보] 제목 컬럼: {title_col}")

    rows = []
    total = len(df)

    for idx, val in enumerate(df[title_col], start=1):
        title = str(val).strip()
        if not title or title.lower() == "nan":
            continue

        print(f"[{idx}/{total}] TMDB 검색 중: {title!r} ...", end=" ")

        best = search_tmdb_tv(api_key, title)
        if best is None:
            print("결과 없음")
            rows.append({
                "drama_title": title,
                "tmdb_id": None,
                "poster_url": None,
                "backdrop_url": None,
                "source": "none",
            })
        else:
            tmdb_id = best.get("id")
            poster_path = best.get("poster_path")
            backdrop_path = best.get("backdrop_path")

            poster_url = build_img_url(poster_path, size="w500")
            backdrop_url = build_img_url(backdrop_path, size="w780")

            print(f"OK (id={tmdb_id}, poster={bool(poster_url)}, backdrop={bool(backdrop_url)})")

            rows.append({
                "drama_title": title,
                "tmdb_id": tmdb_id,
                "poster_url": poster_url,
                "backdrop_url": backdrop_url,
                "source": "tmdb",
            })

        time.sleep(args.sleep_sec)

    out_df = pd.DataFrame(rows,
                          columns=["drama_title", "tmdb_id", "poster_url", "backdrop_url", "source"])
    out_df.to_csv(args.out_csv, index=False, encoding="utf-8-sig")
    print(f"[완료] 저장: {args.out_csv} (총 {len(out_df)}행)")


if __name__ == "__main__":
    main()
