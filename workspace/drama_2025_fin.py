# -*- coding: utf-8 -*-
"""
2025년 대한민국 텔레비전 드라마 목록 크롤러
- 방송기간은 '목록 페이지 표' 4열(또는 '방송 기간' 헤더 열)에서만 수집
- 방송기간 셀은 <br> 줄바꿈을 보존해 받아 라인별로 'YYYY.M.D' 패턴만 추출 후 'YYYY.MM.DD'로 포맷
- status 규칙(최신):
  * first_day만 있고 end_day가 비어 있으면 → today < first_day: '방송 예정', else: '방영중'
  * first_day와 end_day 둘 다 있으면 → 일반 로직
  * 그 외(예: start 없음) → 빈 문자열
- 상세 페이지에서는 장르/채널/방송시간/연출/극본/시청률/에피소드만 보조 수집
- 최종 CSV: title, first_day, end_day, status, avg_rating, episode_count
"""

import re
import time
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin
from datetime import date

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://ko.wikipedia.org"
LIST_URL = "https://ko.wikipedia.org/wiki/2025년_대한민국의_텔레비전_드라마_목록"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.5",
}
SLEEP = 0.6

TARGET_COLUMNS = ["제목", "장르", "방송 채널", "방송 기간", "방송 시간", "연출", "극본", "시청률", "episode_count"]

# ---------------- utils ----------------
def clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\[[^\]]*\]", "", s)   # [주] 각주 제거
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def normalize_kr_date(s):
    """ '2025년 1월 3일', '2025.1.3', '2025-01-03' 등을 date로. 일 없으면 1일 가정. 실패 시 None. """
    if s is None or pd.isna(s):
        return None
    t = str(s).strip()
    if not t:
        return None
    t = t.replace("년", "-").replace("월", "-").replace("일", "")
    t = re.sub(r"[.\u00A0\s]+", "-", t)
    t = re.sub(r"[^0-9\-]", "", t)
    t = re.sub(r"-+", "-", t).strip("-")
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

CHANNEL_TOKENS = r"(KBS\s*Joy|KBS\s*2|KBS|MBC|SBS|tvN|ENA|JTBC|TV조선|MBN|채널A|쿠팡플레이|웨이브|디즈니\+|넷플릭스|조이)"
STATUS_TOKENS = r"(현재|예정|진행중|방영중|종영)"

def clean_title_brackets(t: str) -> str:
    if not isinstance(t, str):
        return ""
    t = re.sub(r"[《》〈〉«»「」『']|[<>]", "", t)
    t = re.sub(r"<<|>>", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()

def fmt_date_yyyy_mm_dd(y: int, m: int, d: int) -> str:
    try:
        return f"{int(y):04d}.{int(m):02d}.{int(d):02d}"
    except Exception:
        return ""

DATE_RE = re.compile(r"(?P<y>\d{4})\.(?P<m>\d{1,2})\.(?P<d>\d{1,2})")

def extract_first_date(text: str) -> str:
    """문자열에서 'YYYY.M.D' 첫 매치를 찾아 'YYYY.MM.DD'로 반환. 없으면 빈 문자열."""
    if not isinstance(text, str):
        return ""
    m = DATE_RE.search(text)
    if not m:
        return ""
    return fmt_date_yyyy_mm_dd(m.group("y"), m.group("m"), m.group("d"))

def normalize_episode_count(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    if not s:
        return ""
    m = re.search(r"(\d+)", s)
    if not m:
        return ""
    return f"{int(m.group(1))}부작"

# ------------- 목록 페이지 -------------
def extract_list_items(list_url: str) -> List[Dict]:
    """
    목록 페이지의 wikitable을 훑어 행 단위 아이템 생성.
    - '범례/설명' 표 제외
    - 헤더에서 '제목' / '방송 기간' 위치 탐색, 없으면 방송기간은 4열 폴백
    - 방송기간 셀은 줄바꿈 보존(get_text(separator="\\n"))
    """
    soup = get_soup(list_url)
    items: List[Dict] = []

    tables = soup.select("#mw-content-text table.wikitable, #content table.wikitable")
    print(f"   - 테이블 발견: {len(tables)}개")

    for table in tables:
        cap = table.find("caption")
        cap_txt = clean_text(cap.get_text()).replace(" ", "") if cap else ""
        if any(k in cap_txt for k in ("범례", "설명")):
            continue

        idx = {"제목": None, "방송 기간": None}
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
                if "방송기간" in t:
                    idx["방송 기간"] = i

        default_period_col = 4 if idx["방송 기간"] is None else idx["방송 기간"]
        if not idx["제목"]:
            continue

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

            a = td_title.find("a", href=True)
            detail_url = None
            if a:
                href = a["href"]
                is_red = ("new" in (a.get("class") or [])) or ("redlink=1" in href)
                if href.startswith("/wiki/") and ":" not in href and not is_red:
                    detail_url = urljoin(BASE, href)

            td_period = get_td(default_period_col)
            # ✅ 줄바꿈 보존(여기서는 절대 clean_text를 먼저 쓰지 말 것!)
            period_text = td_period.get_text(separator="\n").strip() if td_period else ""

            items.append({
                "detail_url": detail_url,
                "fallback": {"제목": title_text_cell, "방송 기간": period_text}
            })

    # 중복 제거
    seen = set()
    uniq: List[Dict] = []
    for it in items:
        key = (it["detail_url"], it["fallback"]["제목"], it["fallback"]["방송 기간"])
        if key not in seen:
            seen.add(key)
            uniq.append(it)
    return uniq

# ------------- 상세 페이지 -------------
def extract_title_from_infobox(soup: BeautifulSoup) -> str:
    sel = ("#mw-content-text > div.mw-content-ltr.mw-parser-output > "
           "table.infobox > tbody > tr:nth-child(1) > td > div > span > big:nth-child(1) > big > b")
    el = soup.select_one(sel)
    if el and el.get_text(strip=True):
        return clean_text(el.get_text())
    el = soup.select_one("#mw-content-text table.infobox tr:first-child b, "
                         "#mw-content-text table.infobox tr:first-child strong")
    if el and el.get_text(strip=True):
        return clean_text(el.get_text())
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
        val = clean_text(td.get_text(separator=" "))

        if not info["장르"] and "장르" in label:
            info["장르"] = val
        if not info["방송 채널"] and re.search(r"(방송채널|방송사|채널|방송국)", label):
            links = [clean_text(a.get_text()) for a in td.select("a") if clean_text(a.get_text())]
            info["방송 채널"] = "; ".join(links) if links else val
        # 방송 기간(인포박스)은 사용하지 않음
        if not info["방송 시간"] and "방송시간" in label:
            info["방송 시간"] = val
        if not info["연출"] and re.search(r"(연출|감독)", label):
            links = [clean_text(a.get_text()) for a in td.select("a") if clean_text(a.get_text())]
            info["연출"] = "; ".join(links) if links else val
        if not info["극본"] and re.search(r"(극본|각본)", label):
            links = [clean_text(a.get_text()) for a in td.select("a") if clean_text(a.get_text())]
            info["극본"] = "; ".join(links) if links else val
    return info

def extract_episode_count(soup: BeautifulSoup) -> str:
    box = soup.select_one("#mw-content-text table.infobox")
    if not box:
        return ""
    keys = ("방송횟수", "에피소드", "편수", "부작", "회수")
    for tr in box.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not td:
            continue
        label = clean_text(th.get_text()).replace(" ", "") if th else ""
        if any(k in label for k in keys):
            return clean_text(td.get_text())
    alt = soup.select_one("#mw-content-text table.infobox > tbody > tr:nth-child(7) > td")
    if alt:
        return clean_text(alt.get_text())
    return ""

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
    fb = item["fallback"]
    row = {k: "" for k in TARGET_COLUMNS}
    period_from_list = fb.get("방송 기간", "")

    if item["detail_url"]:
        try:
            soup = get_soup(item["detail_url"])
            title = extract_title_from_infobox(soup)
            fields = parse_infobox_fields(soup)
            rating = extract_ratings_avg(soup)
            epi_count = extract_episode_count(soup)

            row.update({
                "제목": title or fb.get("제목", ""),
                "장르": fields["장르"],
                "방송 채널": fields["방송 채널"],
                "방송 기간": period_from_list,  # 목록 전용
                "방송 시간": fields["방송 시간"],
                "연출": fields["연출"],
                "극본": fields["극본"],
                "시청률": "" if rating is None else rating,
                "episode_count": epi_count,
            })
            return row
        except Exception:
            pass

    row.update({
        "제목": fb.get("제목", ""),
        "장르": "",
        "방송 채널": "",
        "방송 기간": period_from_list,
        "방송 시간": "",
        "연출": "",
        "극본": "",
        "시청률": "",
        "episode_count": "",
    })
    return row

# -------- 방송기간 → first/end (줄바꿈+패턴만 추출) --------
def split_period_to_dates(raw: str) -> Tuple[str, str]:
    """
    방송기간 셀(raw)을 first_day/end_day로 분리.
      A) 줄바꿈 2줄 -> 각 줄에서 'YYYY.M.D' 첫 매치만 추출
      B) 한 줄에 ~ / – / — / - 포함 -> 좌/우에서 각각 첫 매치 추출
      C) 한 줄에 날짜 하나만 있으면 시작만 채움
    """
    if not isinstance(raw, str):
        return "", ""
    s = raw.strip()
    if not s:
        return "", ""

    # 1) 줄바꿈 우선: 라인별 첫 날짜만 채택
    lines = [x.strip() for x in s.splitlines() if x.strip()]
    if len(lines) >= 2:
        start = extract_first_date(lines[0])
        end   = extract_first_date(lines[1])
        return start, end

    # 2) 한 줄에 구분자 포함
    s2 = (s.replace("–", "~")
           .replace("—", "~")
           .replace("-", "~"))
    if "~" in s2:
        left, right = [t.strip() for t in s2.split("~", 1)]
        return extract_first_date(left), extract_first_date(right)

    # 3) 한 줄에서 단일 날짜
    return extract_first_date(s), ""

# -------- status 계산 --------
def decide_status(row):
    start_raw = str(row.get("first_day", "")).strip()
    end_raw   = str(row.get("end_day", "")).strip()

    # 둘 다 전혀 없으면 공백
    if not start_raw and not end_raw:
        return ""

    st = normalize_kr_date(start_raw) if start_raw else None
    ed = normalize_kr_date(end_raw) if end_raw else None
    today = date.today()

    # ✅ first만 있고 end 없음 → 요구사항대로 처리
    if st and not ed:
        return "방송 예정" if today < st else "방영중"

    # 둘 다 있으면 일반 로직
    if st and ed:
        if today < st:
            return "방송 예정"
        if st <= today <= ed:
            return "방영중"
        if today > ed:
            return "종영"

    # 그 외(예: start 없음) → 공백
    return ""

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

    # 방송기간 분리(먼저!)
    raw_periods = df["방송 기간"].fillna("").astype(str).tolist()
    first_days, end_days = [], []
    for raw in raw_periods:
        a, b = split_period_to_dates(raw)
        first_days.append(a)
        end_days.append(b)
    df["first_day"] = first_days
    df["end_day"]   = end_days

    # 나머지 텍스트 정리
    for c in ["제목", "장르", "방송 채널", "방송 시간", "연출", "극본", "episode_count"]:
        df[c] = df[c].astype(str).map(clean_text)

    # ✅ status 계산
    df["status"] = df.apply(decide_status, axis=1)

    # 시청률 포맷
    def fmt_rating(x):
        if pd.isna(x) or str(x).strip() in ("", "nan", "None"):
            return ""
        return f"{x}%"
    df["시청률"] = df["시청률"].apply(fmt_rating)

    # 연출/극본 구분자 통일
    for col in ["연출", "극본"]:
        df[col] = (df[col].astype(str)
                        .str.replace(r"\s*;\s*", ", ", regex=True)
                        .str.strip(", ")
                        .str.strip())

    # 컬럼명 매핑(원본 호환)
    rename_map = {
        "제목": "title",
        "장르": "genre_name",
        "방송 채널": "channel_name",
        "연출": "directir",   # (원본 오타 유지)
        "극본": "writer",
        "시청률": "avg_rating",
    }
    df = df.rename(columns=rename_map)

    # 제목 중복 제거
    before = len(df)
    df = df.drop_duplicates(subset=["title"])
    after = len(df)
    print(f" - 중복 제거: {before} → {after}")

    # 제목/부작 정리
    df["title"] = df["title"].map(clean_title_brackets)
    df["episode_count"] = df["episode_count"].map(normalize_episode_count)

    # '스페셜' 제외
    df = df[~df["title"].str.contains("스페셜", case=False, na=False)]

    final_df = pd.DataFrame({
        "title": df["title"],
        "first_day": df["first_day"],
        "end_day": df["end_day"],
        "status": df["status"],
        "avg_rating": df["avg_rating"],
        "episode_count": df["episode_count"],
    })

    out = "kdrama_2025_fin.csv"
    final_df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: {out} (행 수: {len(final_df)})")

if __name__ == "__main__":
    main()
