# -*- coding: utf-8 -*-
"""
TMDB Person 검색으로 배우 프로필 이미지 URL 수집 스크립트

[입력]
- allperson.csv (name 컬럼 필수)

[출력]
- person_tmdb_image.csv
    name            : 배우 이름 (입력 그대로)
    tmdb_person_id  : TMDB person id
    profile_url     : 프로필 이미지 URL (없으면 None)
    source          : "tmdb" 또는 "none"
"""

import argparse
import os
import time
from typing import Optional

import pandas as pd
import requests

TMDB_SEARCH_PERSON_URL = "https://api.themoviedb.org/3/search/person"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p"  # /w500 + path


def detect_name_column(df: pd.DataFrame) -> str:
    """배우 이름 컬럼 자동 탐색"""
    candidates = ["name", "이름", "actor_name"]
    for c in candidates:
        if c in df.columns:
            return c
    raise SystemExit(f"이름 컬럼을 찾을 수 없습니다. (지원: {candidates})")


def search_person(api_key: str, name: str) -> Optional[dict]:
    """TMDB Person 검색 → 가장 적절한 결과 하나 반환"""
    params = {
        "api_key": api_key,
        "query": name,
        "language": "ko-KR",
        "include_adult": "false",
    }
    try:
        res = requests.get(TMDB_SEARCH_PERSON_URL, params=params, timeout=5)
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        print(f"[에러] TMDB 요청 실패: {name} -> {e}")
        return None

    results = data.get("results") or []
    if not results:
        return None

    # 우선 Acting(연기자)인 사람을 우선 선택
    acting = [p for p in results if p.get("known_for_department") == "Acting"]
    if acting:
        return acting[0]
    return results[0]


def build_profile_url(path: Optional[str], size: str = "w500") -> Optional[str]:
    if not path:
        return None
    return f"{TMDB_IMG_BASE}/{size}{path}"


def main():
    parser = argparse.ArgumentParser(description="TMDB 배우 프로필 이미지 배치 수집")
    parser.add_argument("--in", dest="in_csv", default="allperson.csv",
                        help="입력 CSV 경로 (기본: allperson.csv)")
    parser.add_argument("--out", dest="out_csv", default="person_tmdb_image.csv",
                        help="출력 CSV 경로 (기본: person_tmdb_image.csv)")
    parser.add_argument("--api-key", dest="api_key",
                        help="TMDB API 키 (없으면 TMDB_API_KEY 환경변수 사용)")
    parser.add_argument("--sleep", dest="sleep_sec", type=float, default=0.25,
                        help="API 호출 사이 딜레이(초) 기본=0.25")
    args = parser.parse_args()

    api_key = args.api_key or os.getenv("TMDB_API_KEY")
    if not api_key:
        raise SystemExit("TMDB API 키가 없습니다. --api-key 또는 TMDB_API_KEY 환경변수로 설정해 주세요.")

    in_path = args.in_csv
    if not os.path.exists(in_path):
        raise SystemExit(f"입력 CSV를 찾을 수 없습니다: {in_path}")

    # CSV 읽기 (utf-8 -> cp949 순서로 시도)
    try:
        df = pd.read_csv(in_path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(in_path, encoding="cp949")

    name_col = detect_name_column(df)
    print(f"[정보] 이름 컬럼: {name_col}")

    rows = []
    total = len(df)

    for idx, val in enumerate(df[name_col], start=1):
        name = str(val).strip()
        if not name or name.lower() == "nan":
            continue

        print(f"[{idx}/{total}] TMDB 배우 검색: {name!r} ...", end=" ")

        best = search_person(api_key, name)
        if best is None:
            print("결과 없음")
            rows.append({
                "name": name,
                "tmdb_person_id": None,
                "profile_url": None,
                "source": "none",
            })
        else:
            tmdb_person_id = best.get("id")
            profile_path = best.get("profile_path")
            profile_url = build_profile_url(profile_path, size="w500")

            print(f"OK (id={tmdb_person_id}, profile={bool(profile_url)})")

            rows.append({
                "name": name,
                "tmdb_person_id": tmdb_person_id,
                "profile_url": profile_url,
                "source": "tmdb",
            })

        time.sleep(args.sleep_sec)

    out_df = pd.DataFrame(rows, columns=["name", "tmdb_person_id", "profile_url", "source"])
    out_df.to_csv(args.out_csv, index=False, encoding="utf-8-sig")
    print(f"[완료] 저장: {args.out_csv} (총 {len(out_df)}행)")


if __name__ == "__main__":
    main()
