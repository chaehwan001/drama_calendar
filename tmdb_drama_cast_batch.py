# -*- coding: utf-8 -*-
"""
드라마 CSV를 기반으로 TMDB에서 출연진(배우) 리스트를 가져와서 하나의 CSV로 저장하는 스크립트.

입력:
    --in drama.csv (기본값)
    - 최소한 title 또는 drama_title 컬럼이 있어야 함.

출력:
    --out drama_cast_tmdb.csv (기본값)
    컬럼:
        drama_title   : 드라마 제목 (입력 CSV의 제목 그대로)
        person_name   : 배우 이름 (TMDB cast.name)
        role_type     : "actor" 고정
        character_name: 배역 이름 (TMDB cast.character, ko-KR 기준)
        order_no      : 1 고정 (요청사항대로)
"""

import argparse
import os
import time
from typing import Optional, List, Dict

import pandas as pd
import requests

TMDB_SEARCH_TV_URL = "https://api.themoviedb.org/3/search/tv"
TMDB_TV_CREDITS_URL = "https://api.themoviedb.org/3/tv/{tv_id}/credits"


def read_csv_smart(path: str) -> pd.DataFrame:
    """utf-8 -> cp949 순서로 시도해서 읽기"""
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp949")


def detect_title_column(df: pd.DataFrame) -> str:
    """제목 컬럼 자동 탐색"""
    candidates = ["title", "drama_title", "제목", "name"]
    for c in candidates:
        if c in df.columns:
            return c
    raise SystemExit(f"제목 컬럼을 찾을 수 없습니다. (지원 후보: {candidates})")


def clean_for_search(title: str) -> str:
    """TMDB 검색용으로 약간 정리: (드라마) 제거 + 양쪽 공백 제거"""
    if not isinstance(title, str):
        title = str(title)
    t = title.strip()
    t = t.replace("(드라마)", "").strip()
    return t


def search_tmdb_tv(api_key: str, title: str) -> Optional[int]:
    """제목으로 TMDB TV 검색 → tv_id 반환 (없으면 None)"""
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
        print(f"[에러] TMDB 검색 실패: {title} -> {e}")
        return None

    results = data.get("results") or []
    if not results:
        return None

    best = results[0]
    return best.get("id")


def fetch_tv_credits(api_key: str, tv_id: int) -> List[Dict]:
    """TV id로 출연진 목록 가져오기 (cast 리스트)"""
    url = TMDB_TV_CREDITS_URL.format(tv_id=tv_id)
    params = {
        "api_key": api_key,
        "language": "ko-KR",  # 가능하면 한글 캐릭터명/배우명
    }
    try:
        res = requests.get(url, params=params, timeout=5)
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        print(f"[에러] credits 요청 실패: tv_id={tv_id} -> {e}")
        return []

    cast = data.get("cast") or []
    return cast


def main():
    parser = argparse.ArgumentParser(description="TMDB 드라마 출연진 배치 수집")
    parser.add_argument("--in", dest="in_csv", default="drama.csv",
                        help="입력 드라마 CSV 경로 (기본: drama.csv)")
    parser.add_argument("--out", dest="out_csv", default="drama_cast_tmdb.csv",
                        help="출력 CSV 경로 (기본: drama_cast_tmdb.csv)")
    parser.add_argument("--api-key", dest="api_key",
                        help="TMDB API 키 (없으면 TMDB_API_KEY 환경변수 사용)")
    parser.add_argument("--sleep", dest="sleep_sec", type=float, default=0.25,
                        help="각 TV 처리 후 대기 시간(초) 기본 0.25")
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("TMDB_API_KEY")
    if not api_key:
        raise SystemExit("TMDB API 키가 없습니다. --api-key 또는 TMDB_API_KEY 환경변수로 설정해 주세요.")

    in_path = args.in_csv
    if not os.path.exists(in_path):
        raise SystemExit(f"입력 CSV를 찾을 수 없습니다: {in_path}")

    df = read_csv_smart(in_path)
    title_col = detect_title_column(df)
    print(f"[정보] 제목 컬럼: {title_col}")

    rows = []
    total = len(df)

    for idx, raw_title in enumerate(df[title_col], start=1):
        drama_title = str(raw_title).strip()
        if not drama_title or drama_title.lower() == "nan":
            continue

        search_title = clean_for_search(drama_title)
        print(f"\n[{idx}/{total}] '{drama_title}' (검색용: '{search_title}') ... ", end="")

        tv_id = search_tmdb_tv(api_key, search_title)
        if tv_id is None:
            print("TMDB 검색 결과 없음")
            continue

        print(f"tv_id={tv_id} → 출연진 조회 중...")
        cast_list = fetch_tv_credits(api_key, tv_id)

        if not cast_list:
            print("  → cast 없음 또는 요청 실패")
            continue

        added = 0
        for c in cast_list:
            person_name = c.get("name") or ""
            character_name = c.get("character") or ""

            # 요청사항대로 role_type, order_no 고정
            rows.append({
                "drama_title": drama_title,    # 원본 제목 그대로
                "person_name": person_name,
                "role_type": "actor",
                "character_name": character_name,
                "order_no": 1,
            })
            added += 1

        print(f"  → {added}명 추가")
        time.sleep(args.sleep_sec)

    if not rows:
        print("\n[경고] 어떤 출연진도 수집되지 않았습니다.")
    else:
        out_df = pd.DataFrame(
            rows,
            columns=["drama_title", "person_name", "role_type", "character_name", "order_no"]
        )
        out_df.to_csv(args.out_csv, index=False, encoding="utf-8-sig")
        print(f"\n[완료] 저장: {args.out_csv} (총 {len(out_df)}행)")


if __name__ == "__main__":
    main()
