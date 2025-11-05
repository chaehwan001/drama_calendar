# C:\Users\PC\Desktop\workspace\merge_runtime.py
import os
import re
import pandas as pd
import unicodedata as ud

BASE = r"C:\Users\PC\Desktop\workspace"
WEEKLY = os.path.join(BASE, "drama_weekly.csv")   # 1번 파일
EPISODE = os.path.join(BASE, "episode_bild.csv")  # 2번 파일
OUTPATH = os.path.join(BASE, "episode.csv")       # 최종 결과

def read_csv_any(path):
    # utf-8 실패 시 cp949(ANSI)로 재시도
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp949")

def nfc_strip(s: str) -> str:
    # 유니코드 정규화 + 앞뒤 공백 제거 + 내부 연속 공백 1개로
    s = ud.normalize("NFC", str(s))
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s

def normalize_title(s: str) -> str:
    # 비교용 타이틀 정규화(대소문자 유지, 공백 정리만)
    return nfc_strip(s)

def normalize_runtime_to_minutes_label(x) -> str:
    """
    입력 예:
      - 70, '70', '70분', '70 분', '70m', '70 min', '70분(예정)' 등
    출력: '70분'
    숫자를 찾지 못하면 원문에 '분'만 보정해서 반환
    """
    if pd.isna(x):
        return ""
    s = str(x)
    # 숫자만 추출 (정수 우선)
    m = re.search(r"(\d+)", s)
    if m:
        return f"{int(m.group(1))}분"
    # 숫자 없으면 공백 제거 후 '분' 정리
    s = nfc_strip(s)
    s = re.sub(r"\s*분\s*$", "", s)  # 끝의 '분'류 제거
    return (s + "분") if s else ""

def main():
    # 1) 읽기
    df1 = read_csv_any(WEEKLY)
    df2 = read_csv_any(EPISODE)

    # 2) 필수 컬럼 확인
    needed1 = {"title", "runtime"}
    needed2 = {"drama_title", "runtime_min"}
    missing1 = needed1 - set(map(str, df1.columns))
    missing2 = needed2 - set(map(str, df2.columns))
    if missing1:
        raise SystemExit(f"drama_weekly.csv에 필요한 컬럼이 없습니다: {missing1}")
    if missing2:
        raise SystemExit(f"episode_bild.csv에 필요한 컬럼이 없습니다: {missing2}")

    # 3) 정규화 키 생성
    df1["_key"] = df1["title"].map(normalize_title)
    df2["_key"] = df2["drama_title"].map(normalize_title)

    # 4) 1번에서 타이틀별 runtime 사전 생성(중복 시 첫 값 우선)
    runtime_map = (
        df1.dropna(subset=["_key"])
           .drop_duplicates(subset=["_key"], keep="first")
           .set_index("_key")["runtime"]
           .to_dict()
    )

    # 5) 매칭 여부 파악 및 채우기
    before_filled = df2["runtime_min"].notna().sum()
    match_mask = df2["_key"].isin(runtime_map.keys())
    matched_count = int(match_mask.sum())

    # 채우기: 매칭된 행은 1번 runtime으로 덮어씀
    df2.loc[match_mask, "runtime_min"] = (
        df2.loc[match_mask, "_key"].map(runtime_map).map(normalize_runtime_to_minutes_label)
    )

    after_filled = df2["runtime_min"].notna().sum()

    # 6) 결과 저장
    # 2번의 기존 컬럼 순서를 유지하되, _key는 제거
    cols = [c for c in df2.columns if c != "_key"]
    df2[cols].to_csv(OUTPATH, index=False, encoding="utf-8-sig")

    # 7) 로그 출력
    print("=== 매칭/병합 요약 ===")
    print(f"- 입력: drama_weekly={len(df1)}행, episode_bild={len(df2)}행")
    print(f"- 매칭된 제목 수: {matched_count}")
    print(f"- 덮어쓰기 전 runtime_min(비결측) 개수: {before_filled}")
    print(f"- 덮어쓰기 후 runtime_min(비결측) 개수: {after_filled}")
    print(f"- 저장 완료: {OUTPATH}")

if __name__ == "__main__":
    main()
