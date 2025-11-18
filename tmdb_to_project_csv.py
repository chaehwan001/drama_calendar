import pandas as pd

src = "drama_tmdb_image.csv"   # TMDB 크롤링 결과
dst = "drama_image_tmdb.csv"   # 우리 프로젝트에서 사용할 최종 CSV

df = pd.read_csv(src, encoding="utf-8")

out = pd.DataFrame({
    "title": df["drama_title"],
    "type": "drama_image",
    "url": df["poster_url"],
    "sort_no": 1,
})

out.to_csv(dst, index=False, encoding="utf-8-sig")
print("저장 완료:", dst)
