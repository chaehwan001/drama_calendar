# -*- coding: utf-8 -*-
"""
2025년 대한민국 텔레비전 드라마 목록 → 각 작품 상세 페이지 크롤링 (+목록 폴백, 범례표 제외)

- 목록 페이지: https://ko.wikipedia.org/wiki/2025년_대한민국의_텔레비전_드라마_목록
- 상세 페이지: 인포박스에서 제목/장르/방송 채널/방송 기간/방송 시간/연출/극본 파싱
- 시청률: 본문 내 '시청률' 표(들)에서 % 값 전체 평균(소수점 첫째 자리)
- 폴백: 상세페이지(정상 링크)가 없거나 실패하면 목록 표의 같은 행 텍스트로 채움
- 범례/설명 표(면색/노란색/회색/시청 등급 등)는 목록 수집에서 제외
- 출력: kdrama_2025.csv (UTF-8 with BOM)
"""

import re
import time
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date

BASE = "https://ko.wikipedia.org"
LIST_URL = "https://ko.wikipedia.org/wiki/2025년_대한민국의_텔레비전_드라마_목록"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.5",
}
SLEEP = 0.6

TARGET_COLUMNS = ["제목", "장르", "방송 채널", "방송 기간", "방송 시간", "연출", "극본", "시청률"]

# ---------------- utils ----------------
def clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\[[^\]]*\]", "", s)   # [1], [주 1] 각주 제거
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def normalize_kr_date(s):
    """
    '2025년 1월 3일', '2025.1.3', '2025-01-03' 같은 문자열을 date로 변환.
    일(day)이 없으면 1일로 가정. 파싱 실패 시 None.
    """
    if s is None or pd.isna(s):
        return None
    t = str(s).strip()
    if not t:
        return None

    # 구분자/형식 정리
    t = t.replace("년", "-").replace("월", "-").replace("일", "")
    t = re.sub(r"[.\u00A0\s]+", "-", t)     # 점/공백/nbsp → -
    t = re.sub(r"[^0-9\-]", "", t)          # 숫자/대시 외 제거
    t = re.sub(r"-+", "-", t).strip("-")    # 중복 대시 정리

    parts = t.split("-")
    if not parts or not parts[0].isdigit():
        return None

    try:
        y = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
        d = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 1
        return date(y, m, d)
    except Exception:
        return None

# ---------------- list page helpers ----------------
def is_legend_table(table: BeautifulSoup) -> bool:
    """범례/설명 표(면색/노란색/회색/시청 등급 등)를 감지해서 제외."""
    cap = table.find("caption")
    cap_txt = clean_text(cap.get_text()).replace(" ", "") if cap else ""
    if any(k in cap_txt for k in ("범례", "설명")):
        return True
    txt = clean_text(table.get_text()).replace(" ", "")
    bad_keys = ("면색", "노란색", "회색", "분홍색", "극본및연출", "방송횟수", "시청등급", "웹을기반")
    return any(k in txt for k in bad_keys)

def header_map(table: BeautifulSoup) -> Dict[str, int]:
    """테이블 헤더에서 관심 컬럼의 1-base 인덱스를 찾는다. 없으면 합리적 디폴트."""
    idx = {"제목": 1, "극본": 2, "연출": 3, "방송 기간": 4, "방송사": 5, "방송 시간": 6}
    thead = table.find("thead")
    header_row = thead.find("tr") if thead else None
    if not header_row:
        first_tr = table.find("tr")
        if first_tr and first_tr.find_all("th"):
            header_row = first_tr
    if not header_row:
        return idx

    def norm(x: str) -> str:
        return clean_text(x).replace(" ", "")

    ths = header_row.find_all("th")
    for i, th in enumerate(ths, start=1):
        t = norm(th.get_text())
        if any(k in t for k in ("제목", "작품명", "프로그램명")):
            idx["제목"] = i
        if "극본" in t or "각본" in t:
            idx["극본"] = i
        if "연출" in t or "감독" in t:
            idx["연출"] = i
        if "방송기간" in t:
            idx["방송 기간"] = i
        if any(k in t for k in ("방송사", "방송채널", "채널", "방송국")):
            idx["방송사"] = i
        if "방송시간" in t or "편성시간" in t:
            idx["방송 시간"] = i
    return idx

def extract_list_items(list_url: str) -> List[Dict]:
    """
    목록 페이지의 wikitable을 훑어 행 단위 아이템 생성.
    - 캡션에 '범례/설명'이 있는 표만 제외
    - 헤더에 '제목/작품명/프로그램명' 칼럼이 없는 표는 건너뜀
    - 제목 칸에 링크가 없어도 행 텍스트로 폴백 저장
    """
    soup = get_soup(list_url)
    items: List[Dict] = []

    tables = soup.select("#mw-content-text table.wikitable, #content table.wikitable")
    print(f"   - 테이블 발견: {len(tables)}개")

    used_tables = 0
    skipped_caption = 0
    skipped_no_title = 0

    for table in tables:
        # 1) 캡션이 '범례/설명'인 경우만 스킵 (본문 키워드 스캔은 제거)
        cap = table.find("caption")
        cap_txt = clean_text(cap.get_text()).replace(" ", "") if cap else ""
        if any(k in cap_txt for k in ("범례", "설명")):
            skipped_caption += 1
            continue

        # 2) 헤더에서 '제목' 칼럼 위치 찾기 (없으면 이 표는 스킵)
        idx = {"제목": None, "극본": None, "연출": None, "방송 기간": None, "방송사": None, "방송 시간": None}
        thead = table.find("thead")
        header_row = thead.find("tr") if thead else None
        if not header_row:
            first_tr = table.find("tr")
            if first_tr and first_tr.find_all("th"):
                header_row = first_tr

        def norm(x: str) -> str:
            return clean_text(x).replace(" ", "")

        if header_row:
            ths = header_row.find_all("th")
            for i, th in enumerate(ths, start=1):
                t = norm(th.get_text())
                if any(k in t for k in ("제목", "작품명", "프로그램명")):
                    idx["제목"] = i
                if "극본" in t or "각본" in t:
                    idx["극본"] = i
                if "연출" in t or "감독" in t:
                    idx["연출"] = i
                if "방송기간" in t:
                    idx["방송 기간"] = i
                if any(k in t for k in ("방송사", "방송채널", "채널", "방송국")):
                    idx["방송사"] = i
                if "방송시간" in t or "편성시간" in t:
                    idx["방송 시간"] = i

        if not idx["제목"]:
            skipped_no_title += 1
            continue

        used_tables += 1

        # 3) 데이터 행 순회
        for tr in table.select("tr"):
            if tr.find_all("th") and not tr.find("td"):
                continue
            tds = tr.find_all("td")
            if not tds or len(tds) < (idx["제목"] or 1):
                continue

            def get_td(pos: Optional[int]) -> Optional[BeautifulSoup]:
                if not pos or len(tds) < pos:
                    return None
                return tds[pos - 1]

            td_title = get_td(idx["제목"])
            if not td_title:
                continue

            title_text_cell = clean_text(td_title.get_text())

            # 제목 칼럼의 첫 링크(정상 문서)만 detail_url로 사용
            a = td_title.find("a", href=True)
            detail_url = None
            if a:
                href = a["href"]
                is_red = ("new" in (a.get("class") or [])) or ("redlink=1" in href)
                if href.startswith("/wiki/") and ":" not in href and not is_red:
                    detail_url = urljoin(BASE, href)

            def td_text(key: str) -> str:
                pos = idx.get(key)
                td = get_td(pos)
                return clean_text(td.get_text()) if td else ""

            items.append({
                "detail_url": detail_url,
                "fallback": {
                    "제목": title_text_cell,
                    "극본": td_text("극본"),
                    "연출": td_text("연출"),
                    "방송 기간": td_text("방송 기간"),
                    "방송 채널": td_text("방송사"),
                    "방송 시간": td_text("방송 시간"),
                }
            })

    print(f"   - 사용한 표: {used_tables}개, 캡션으로 스킵: {skipped_caption}개, '제목' 헤더없어 스킵: {skipped_no_title}개")

    # 중복 제거(동일 detail_url/제목 조합)
    seen = set()
    uniq: List[Dict] = []
    for it in items:
        key = (it["detail_url"], it["fallback"]["제목"])
        if key not in seen:
            seen.add(key)
            uniq.append(it)
    return uniq

# ---------------- detail page ----------------
def extract_title_from_infobox(soup: BeautifulSoup) -> str:
    # 사용자가 제시한 매우 구체적 셀렉터
    sel = ("#mw-content-text > div.mw-content-ltr.mw-parser-output > "
           "table.infobox > tbody > tr:nth-child(1) > td > div > span > big:nth-child(1) > big > b")
    el = soup.select_one(sel)
    if el and el.get_text(strip=True):
        return clean_text(el.get_text())
    # 폴백 1: 인포박스 첫 행의 굵은 텍스트
    el = soup.select_one("#mw-content-text table.infobox tr:first-child b, "
                         "#mw-content-text table.infobox tr:first-child strong")
    if el and el.get_text(strip=True):
        return clean_text(el.get_text())
    # 폴백 2: 문서 헤딩
    h1 = soup.select_one("#firstHeading")
    if h1 and h1.get_text(strip=True):
        return clean_text(h1.get_text())
    return ""

def parse_infobox_fields(soup: BeautifulSoup) -> dict:
    info = {"장르": "", "방송 채널": "", "방송 기간": "", "방송 시간": "", "연출": "", "극본": ""}
    box = soup.select_one("#mw-content-text table.infobox")
    if not box:
        return info

    def norm_label(s: str) -> str:
        return clean_text(s).replace(" ", "")

    for tr in box.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not td:
            continue
        label = norm_label(th.get_text()) if th else ""
        val = clean_text(td.get_text())

        if not info["장르"] and "장르" in label:
            info["장르"] = val
        if not info["방송 채널"] and re.search(r"(방송채널|방송사|채널|방송국)", label):
            links = [clean_text(a.get_text()) for a in td.select("a") if clean_text(a.get_text())]
            info["방송 채널"] = "; ".join(links) if links else val
        if not info["방송 기간"] and "방송기간" in label:
            info["방송 기간"] = val
        if not info["방송 시간"] and "방송시간" in label:
            info["방송 시간"] = val
        if not info["연출"] and re.search(r"(연출|감독)", label):
            links = [clean_text(a.get_text()) for a in td.select("a") if clean_text(a.get_text())]
            info["연출"] = "; ".join(links) if links else val
        if not info["극본"] and re.search(r"(극본|각본)", label):
            links = [clean_text(a.get_text()) for a in td.select("a") if clean_text(a.get_text())]
            info["극본"] = "; ".join(links) if links else val
    return info

def extract_ratings_avg(soup: BeautifulSoup) -> Optional[float]:
    content = soup.select_one("#mw-content-text .mw-parser-output")
    if not content:
        return None
    vals = []
    for table in content.select("table"):
        cap = table.find("caption")
        cap_txt = clean_text(cap.get_text()) if cap else ""
        tbl_txt = clean_text(table.get_text())
        if ("시청률" not in cap_txt) and ("시청률" not in tbl_txt):
            continue
        for td in table.select("td"):
            txt = clean_text(td.get_text())
            for m in re.finditer(r"(\d+(?:\.\d+)?)\s*%", txt):
                try:
                    vals.append(float(m.group(1)))
                except ValueError:
                    pass
    if not vals:
        return None
    avg = sum(vals) / len(vals)
    return float(f"{avg:.1f}")

def scrape_detail_or_fallback(item: Dict) -> Dict:
    """
    detail_url이 있으면 상세 파싱, 없거나 실패하면 목록 폴백으로 행 생성.
    """
    fb = item["fallback"]
    row = {k: "" for k in TARGET_COLUMNS}

    if item["detail_url"]:
        try:
            soup = get_soup(item["detail_url"])
            title = extract_title_from_infobox(soup)
            fields = parse_infobox_fields(soup)
            rating = extract_ratings_avg(soup)
            row.update({
                "제목": title or fb.get("제목", ""),
                "장르": fields["장르"],
                "방송 채널": fields["방송 채널"] or fb.get("방송 채널", ""),
                "방송 기간": fields["방송 기간"] or fb.get("방송 기간", ""),
                "방송 시간": fields["방송 시간"] or fb.get("방송 시간", ""),
                "연출": fields["연출"] or fb.get("연출", ""),
                "극본": fields["극본"] or fb.get("극본", ""),
                "시청률": "" if rating is None else rating,
            })
            return row
        except Exception:
            pass  # 상세 실패 → 폴백으로 넘어감

    # 상세 페이지가 없거나 실패한 경우: 목록 행 텍스트로 구성
    row.update({
        "제목": fb.get("제목", ""),
        "장르": "",
        "방송 채널": fb.get("방송 채널", ""),
        "방송 기간": fb.get("방송 기간", ""),
        "방송 시간": fb.get("방송 시간", ""),
        "연출": fb.get("연출", ""),
        "극본": fb.get("극본", ""),
        "시청률": "",
    })
    return row

# ---------------- main ----------------
def main():
    print("[*] 목록 페이지:", LIST_URL)
    items = extract_list_items(LIST_URL)
    print(f" - 행 수집: {len(items)}개")

    rows = []
    for i, it in enumerate(items, 1):
        print(f"  ({i}/{len(items)}) detail={'-' if not it['detail_url'] else it['detail_url']}")
        rows.append(scrape_detail_or_fallback(it))
        time.sleep(SLEEP)

    if not rows:
        print("[-] 수집 결과 없음"); return

    df = pd.DataFrame(rows, columns=TARGET_COLUMNS)
    for c in ["제목", "장르", "방송 채널", "방송 기간", "방송 시간", "연출", "극본"]:
        df[c] = df[c].astype(str).map(clean_text)

    # 방송 기간 분리 (구분자: ~, −, —, - 등)
    period = df["방송 기간"].fillna("").astype(str)
    period = (period.str.replace("–", "~")
                      .str.replace("—", "~")
                      .str.replace("-", "~"))
    df["first_day"] = period.str.split("~").str[0].str.strip()
    df["end_day"]   = period.str.split("~").str[1].str.strip()
    df["end_day"]   = df["end_day"].fillna("")

    # 상태(status) 계산: 시작/종료일 모두 반영 (예정/방영중/종영)
    today = date.today()

    def decide_status(row):
        start_raw = row.get("first_air_date", "")
        end_raw   = row.get("end_air_date", "")

        # '예정' 키워드가 명시된 경우 바로 방송 예정
        raw_join = f"{start_raw} {end_raw}"
        if "예정" in str(raw_join):
            # 시작일이 없고 '예정'만 있으면 예정으로 분류
            start = normalize_kr_date(start_raw)
            if (not start) or (today < start):
                return "방송 예정"

        start = normalize_kr_date(start_raw)
        end   = normalize_kr_date(end_raw)

        # 방송 예정: 오늘 < 시작일
        if start and today < start:
            return "방송 예정"

        # 방영중
        if start and today >= start:
            if end:
                return "방영중" if today <= end else "종영"
            else:
                return "방영중"

        # 시작일이 없을 때 → 종료일만 판단
        if end:
            return "종영" if end < today else "방송 예정"

        return "방영중"

    df["status"] = df.apply(decide_status, axis=1)

    # 시청률에 % 붙이기
    def fmt_rating(x):
        if pd.isna(x) or str(x).strip() in ("", "nan", "None"):
            return ""
        return f"{x}%"
    df["시청률"] = df["시청률"].apply(fmt_rating)

    # 연출/극본: 다중 값 구분자 ';' → ', ' 로 통일
    for col in ["연출", "극본"]:
        df[col] = (df[col].astype(str)
                        .str.replace(r"\s*;\s*", ", ", regex=True)
                        .str.strip(", ").str.strip())

    # 컬럼명 매핑
    rename_map = {
        "제목": "title",
        "장르": "genre_name",
        "방송 채널": "channel_name",
        "연출": "directir",   # 요청한 오타 그대로 유지
        "극본": "writer",
        "시청률": "avg_rating",
    }
    df = df.rename(columns=rename_map)

    # 최종 컬럼 순서
    df = df[[
        "title", "genre_name", "channel_name",
        "first_day", "end_day", "status",
        "directir", "writer", "avg_rating"
    ]]

    # 제목 기준 중복 제거
    before = len(df)
    df = df.drop_duplicates(subset=["title"])
    after = len(df)
    print(f" - 중복 제거: {before} → {after}")

    out = "kdrama_2025.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: {out} (행 수: {len(df)})")

if __name__ == "__main__":
    main()
