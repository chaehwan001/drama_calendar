# merge_by_title_exact.py
import pandas as pd
from pathlib import Path

KDRAMA = "kdrama_2025_fin.csv"   # 드라마 CSV
DESC   = "description.csv"       # 줄거리 CSV
OUT    = "drama.csv"

# 두 파일의 제목/줄거리 컬럼명 지정 (다르면 여기만 바꿔줘)
K_TITLE = "title"        # kdrama_2025_fin.csv의 제목 컬럼명
D_TITLE = "title"        # description.csv의 제목 컬럼명
D_DESC  = "description"  # description.csv의 줄거리 컬럼명

def read_csv(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path, engine="python")

kdf = read_csv(KDRAMA)
ddf = read_csv(DESC)

# description 쪽에서 제목, 줄거리만 사용해 lookup 만들기
lookup = ddf[[D_TITLE, D_DESC]].drop_duplicates(subset=[D_TITLE], keep="first")
lookup = lookup.rename(columns={D_TITLE: "__title_key__", D_DESC: "description"})

# kdrama 쪽과 완전 일치로 left-merge
merged = kdf.merge(lookup, left_on=K_TITLE, right_on="__title_key__", how="left")
merged = merged.drop(columns=["__title_key__"])  # 보조 키 제거

# description 컬럼을 맨 뒤로 이동
cols = [c for c in merged.columns if c != "description"] + ["description"]
merged = merged[cols]

merged.to_csv(OUT, index=False, encoding="utf-8-sig")
print(f"완료: {OUT}  (행 {len(merged)}, 매칭 {merged['description'].notna().sum()}개)")
