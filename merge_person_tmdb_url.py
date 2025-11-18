import pandas as pd

# 입력 파일
orig = "allperson.csv"            # 기존 배우 정보 CSV
tmdb = "person_tmdb_image.csv"    # TMDB에서 가져온 (name, profile_url)

# 출력 파일
out_file = "allperson_with_tmdb.csv"

# CSV 읽기 (UTF-8 → CP949 순서로 시도)
def read_csv_smart(path):
    try:
        return pd.read_csv(path, encoding="utf-8")
    except:
        return pd.read_csv(path, encoding="cp949")

df_orig = read_csv_smart(orig)
df_tmdb = read_csv_smart(tmdb)

# name 컬럼 둘 다 문자열 형태로 통일
df_orig["name"] = df_orig["name"].astype(str).str.strip()
df_tmdb["name"] = df_tmdb["name"].astype(str).str.strip()

# TMDB에서 가져온 url 컬럼 이름 통일(profile_url → url)
df_tmdb = df_tmdb.rename(columns={"profile_url": "url"})

# name 기준 left join (기존 데이터 유지)
merged = pd.merge(
    df_orig,
    df_tmdb[["name", "url"]],   # 필요한 컬럼만 join
    on="name",
    how="left",
    suffixes=("", "_tmdb")
)

# 기존 url 컬럼이 있다면 → tmdb url이 있으면 덮어쓰기
if "url" in df_orig.columns:
    merged["url"] = merged["url_tmdb"].combine_first(merged["url"])
    merged = merged.drop(columns=["url_tmdb"])
else:
    merged = merged.rename(columns={"url_tmdb": "url"})

# 저장
merged.to_csv(out_file, index=False, encoding="utf-8-sig")

print(f"[완료] 저장됨: {out_file}")
