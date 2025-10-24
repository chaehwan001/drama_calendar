# -*- coding: utf-8 -*-
"""
drama_weekly.py — 위키 드라마 인포박스에서 방송시간/런타임 추출
변경사항(최소 패치):
  1) 제목 격자/괄호 제거: 《 》, 〈 〉, << >>, 「 」, 『 』, « », < >
  2) 런타임 자동 추정(후처리): runtime 비었고 start_time이 범위면 40~120분에서만 계산
     ※ 라벨 기반 런타임 있으면 절대 덮어쓰지 않음 → '철벽 구분' 유지
"""

import re
import time
import argparse
from typing import List, Dict, Optional
from urllib.parse import urljoin

import requests
import pandas as pd
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

# ---------------- utils ----------------
def clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\[[^\]]*\]", "", s)   # 각주 제거
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def strip_brackets(title: str) -> str:
    """제목의 《 》, 〈 〉, << >>, 「 」, 『 』, « », < > 제거"""
    if not title:
        return ""
    # 격자/괄호 문자 제거
    title = re.sub(r"[《》〈〉「」『』«»<>]", "", title)
    title = re.sub(r"<<|>>", "", title)
    # 공백 정리
    title = re.sub(r"\s+", " ", title).strip()
    return title

def get_soup(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ---------------- 목록에서 제목/링크 ----------------
def extract_list_items(list_url: str) -> List[Dict]:
    soup = get_soup(list_url)
    items: List[Dict] = []
    tables = soup.select("#mw-content-text table.wikitable, #content table.wikitable")

    def norm(x: str) -> str:
        return clean_text(x).replace(" ", "")

    for table in tables:
        cap = table.find("caption")
        cap_txt = clean_text(cap.get_text()).replace(" ", "") if cap else ""
        if any(k in cap_txt for k in ("범례", "설명")):
            continue

        idx_title = None
        thead = table.find("thead")
        header_row = thead.find("tr") if thead else None
        if not header_row:
            first_tr = table.find("tr")
            if first_tr and first_tr.find_all("th"):
                header_row = first_tr

        if header_row:
            ths = header_row.find_all("th")
            for i, th in enumerate(ths, start=1):
                t = norm(th.get_text())
                if any(k in t for k in ("제목", "작품명", "프로그램명")):
                    idx_title = i

        if not idx_title:
            continue

        for tr in table.select("tr"):
            if tr.find_all("th") and not tr.find("td"):
                continue
            tds = tr.find_all("td")
            if not tds or len(tds) < idx_title:
                continue

            td_title = tds[idx_title - 1]
            # ⬇ 제목 격자/괄호 제거(기존 clean_text 대신 strip_brackets만 적용)
            title_text = strip_brackets(td_title.get_text())

            a = td_title.find("a", href=True)
            detail_url = None
            if a:
                href = a["href"]
                is_red = ("new" in (a.get("class") or [])) or ("redlink=1" in href)
                if href.startswith("/wiki/") and ":" not in href and not is_red:
                    detail_url = urljoin(BASE, href)

            if title_text:
                items.append({"title": title_text, "detail_url": detail_url})

    # 중복 제거
    seen = set()
    uniq: List[Dict] = []
    for it in items:
        key = (it["title"], it["detail_url"])
        if key not in seen:
            seen.add(key)
            uniq.append(it)
    return uniq

# ---------------- 방송시간/런타임 파싱 ----------------
DAY_PATTERN = r"(월|화|수|목|금|토|일)요일"
DAY_CONNECTOR = r"[·,/\s]*(?:및)?[·,/\s]*"

def extract_days(text: str) -> List[str]:
    t = text
    # '수·목' → '수요일 목요일', '수/목', '수,목', '수 및 목' → 동일 처리
    t_expanded = re.sub(
        r"(월|화|수|목|금|토|일)\s*·\s*(월|화|수|목|금|토|일)",
        lambda m: f"{m.group(1)}요일 {m.group(2)}요일",
        t
    )
    t_expanded = re.sub(
        rf"(월|화|수|목|금|토|일){DAY_CONNECTOR}(월|화|수|목|금|토|일)",
        lambda m: f"{m.group(1)}요일 {m.group(2)}요일",
        t_expanded
    )
    days = re.findall(DAY_PATTERN, t_expanded)
    out = []
    for d in days:
        s = f"{d}요일"
        if s not in out:
            out.append(s)
    return out

def normalize_special_words(s: str) -> str:
    """자정/정오/밤 12시 등을 오전/오후 표기로 정규화."""
    t = s
    t = re.sub(r"밤\s*12\s*시", "오전 12시", t)
    t = re.sub(r"자정\s*12\s*시", "오전 12시", t)
    t = re.sub(r"\b자정\b", "오전 12시", t)
    t = re.sub(r"정오\s*12\s*시", "오후 12시", t)
    t = re.sub(r"\b정오\b", "오후 12시", t)
    return t

def detect_ampm(word: Optional[str]) -> Optional[str]:
    if not word:
        return None
    w = word.strip()
    if w in ("오전", "AM", "am", "새벽"):
        return "AM"
    if w in ("오후", "PM", "pm", "저녁", "밤", "늦은밤"):
        return "PM"
    if w in ("낮",):
        return None
    return None

def to_24h_hour(h: int, ampm: Optional[str]) -> int:
    h = max(0, min(12, h))
    if ampm == "AM":
        return 0 if h == 12 else h
    if ampm == "PM":
        return 12 if h == 12 else h + 12
    return h  # 컨텍스트 없으면 그대로

def extract_time_range(text: str) -> str:
    """
    방송 시간 td에서 24시간제 'HH:MM' 또는 'HH:MM~HH:MM' 반환.
    - 좌/우 각각에 표기된 '오전/오후/밤/새벽/저녁'을 개별 적용
    - 한쪽만 있으면 서로에게 전파(컨텍스트 상속)
    - 특수어 보정(자정/정오/밤 12시)
    - 구분자 '~', '-', '–', '—' 허용
    """
    raw = clean_text(text)
    t = normalize_special_words(raw)

    # 0) AM/PM + HH:MM ~ AM/PM + HH:MM (양쪽 또는 한쪽 컨텍스트)
    m = re.search(
        r"(오전|오후|밤|새벽|저녁|낮)?\s*(\d{1,2}):(\d{2}).*?[~\-–—]\s*(오전|오후|밤|새벽|저녁|낮)?\s*(\d{1,2}):(\d{2})",
        t
    )
    if m:
        am1, h1, m1, am2, h2, m2 = m.groups()
        # 컨텍스트 상속
        ctx_left  = detect_ampm(am1)
        ctx_right = detect_ampm(am2) or ctx_left
        if ctx_left is None and ctx_right is not None:
            ctx_left = ctx_right
        H1 = to_24h_hour(int(h1), ctx_left)
        H2 = to_24h_hour(int(h2), ctx_right)
        return f"{H1:02d}:{int(m1):02d}~{H2:02d}:{int(m2):02d}"

    # 1) HH:MM ~ HH:MM (문장 내 공통 컨텍스트)
    context = None
    if re.search(r"(오전|AM|am|새벽)\b", t):
        context = "AM"
    elif re.search(r"(오후|PM|pm|저녁|밤|늦은밤)\b", t):
        context = "PM"

    colon_times = re.findall(r"(\d{1,2}):(\d{2})", t)
    if colon_times:
        def conv(hm, ampm=context):
            h, m_ = int(hm[0]), int(hm[1])
            H = to_24h_hour(h, ampm)
            return f"{H:02d}:{m_:02d}"
        if len(colon_times) >= 2:
            return f"{conv(colon_times[0])}~{conv(colon_times[1])}"
        return conv(colon_times[0])

    # 2) 한글 시각 범위: (오전/오후) H시 M분 ~ (오전/오후) H시 M분
    m = re.search(
        r"(오전|오후|밤|새벽|저녁|낮)?\s*(\d{1,2})\s*시(?:\s*(\d{1,2})\s*분)?\s*[~\-–—]\s*"
        r"(오전|오후|밤|새벽|저녁|낮)?\s*(\d{1,2})\s*시(?:\s*(\d{1,2})\s*분)?",
        t
    )
    if m:
        am1, h1, mm1, am2, h2, mm2 = m.groups()
        mm1 = int(mm1) if mm1 else 0
        mm2 = int(mm2) if mm2 else 0
        ctx_left  = detect_ampm(am1)
        ctx_right = detect_ampm(am2) or ctx_left
        if ctx_left is None and ctx_right is not None:
            ctx_left = ctx_right
        H1 = to_24h_hour(int(h1), ctx_left)
        H2 = to_24h_hour(int(h2), ctx_right)
        return f"{H1:02d}:{mm1:02d}~{H2:02d}:{mm2:02d}"

    # 3) 한글 시각 단일
    m = re.search(r"(오전|오후|밤|새벽|저녁|낮)?\s*(\d{1,2})\s*시(?:\s*(\d{1,2})\s*분)?", t)
    if m:
        am, h, mm = m.groups()
        H = to_24h_hour(int(h), detect_ampm(am))
        mm = int(mm) if mm else 0
        return f"{H:02d}:{mm:02d}"

    return ""

# --- 런타임 파싱(철벽 구분 + 가드): '시간/분' 토큰 필수 + 편성 특징 있으면 배제
def parse_runtime_minutes_strict(text: str) -> Optional[int]:
    t = clean_text(text)

    # 편성 특징(요일/범위/콜론) 포함 시 런타임 아님
    if re.search(DAY_PATTERN, t) or "~" in t or ":" in t:
        return None

    if not re.search(r"(시간|분)", t):
        return None

    m = re.search(r"(\d+)\s*시간\s*(\d+)\s*분", t)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.search(r"(\d+)\s*시간", t)
    if m:
        return int(m.group(1)) * 60
    m = re.search(r"(\d+)\s*분", t)
    if m:
        return int(m.group(1))
    return None

# ---------------- 인포박스 추출 (라벨 우선, 상호 배제) ----------------
TIME_LABEL_RE = re.compile(r"(방송시간|방영시간)")
RUNTIME_LABEL_RE = re.compile(r"(상영시간|방송분량|러닝타임|분량)")

def extract_broadcast_fields_from_infobox(soup: BeautifulSoup) -> Dict[str, str]:
    out = {"dow": "", "start_time": "", "runtime": ""}

    box = soup.select_one("#mw-content-text > div.mw-content-ltr.mw-parser-output > table.infobox")
    if not box:
        return out

    time_td = None
    runtime_td = None

    # 1) 라벨(th) 기반 안전 탐색 (상호 배제)
    for tr in box.select("tr"):
        th = tr.find("th"); td = tr.find("td")
        if not td:
            continue
        label = clean_text(th.get_text()).replace(" ", "") if th else ""
        if not time_td and TIME_LABEL_RE.search(label):
            time_td = td
        elif not runtime_td and RUNTIME_LABEL_RE.search(label):
            runtime_td = td
        if time_td and runtime_td:
            break

    # 방송시간 → dow, start_time
    if time_td:
        raw = time_td.get_text(separator=" ")
        t = clean_text(raw)
        # 여러 표현이 섞여도 첫 문장 위주(재방/특집 등 자연히 뒤로 밀림)
        t_first = t.split(" / ")[0].split(" ; ")[0]
        days = extract_days(t_first)
        if days:
            out["dow"] = ", ".join(days)
        trange = extract_time_range(t_first)
        if trange:
            out["start_time"] = trange

    # 런타임 → 라벨 기반에서만 엄격 파싱 (nth-child 폴백 금지, 편성 특징 배제)
    if runtime_td:
        rt = clean_text(runtime_td.get_text(separator=" ").strip())
        minutes = parse_runtime_minutes_strict(rt)
        if minutes is not None:
            out["runtime"] = f"{minutes}분"

    return out

# ---------------- 후처리: 런타임 추정(항상 ON, '철벽 구분' 영향 없음) ----------------
def maybe_infer_runtime(start_time: str,
                        runtime: str,
                        min_ok: int = 30,
                        max_ok: int = 120) -> str:
    """
    라벨 기반 runtime이 비어 있고, start_time이 'HH:MM~HH:MM'일 때만
    종료-시작 차이를 분으로 계산해 '숫자분' 반환.
    가드: 결과가 [min_ok, max_ok] 범위를 벗어나면 추정하지 않고 원본 runtime('') 유지.
    """
    if runtime:
        return runtime  # 라벨 기반 값 존중
    if not start_time or "~" not in start_time:
        return runtime

    try:
        left, right = start_time.split("~", 1)
        h1, m1 = map(int, left.split(":"))
        h2, m2 = map(int, right.split(":"))
        start_minutes = h1 * 60 + m1
        end_minutes = h2 * 60 + m2
        diff = end_minutes - start_minutes
        if diff < 0:  # 자정 넘김
            diff += 24 * 60
        if min_ok <= diff <= max_ok:
            return f"{diff}분"
        else:
            return runtime
    except Exception:
        return runtime

# ---------------- 메인 ----------------
def main():
    parser = argparse.ArgumentParser()
    # NOTE: 옵션은 남겨두되, 실제 동작은 항상 추정 ON (원하면 끄기 위해 플래그 사용 가능)
    parser.add_argument("--guard-min", type=int, default=40, help="추정 허용 최소 분(기본 40)")
    parser.add_argument("--guard-max", type=int, default=120, help="추정 허용 최대 분(기본 120)")
    args = parser.parse_args()

    print("[*] 목록 페이지:", LIST_URL)
    items = extract_list_items(LIST_URL)
    print(f" - 대상 작품 수: {len(items)}")

    rows = []
    for i, it in enumerate(items, 1):
        title = it["title"]; url = it["detail_url"]
        print(f"  ({i}/{len(items)}) {title} — detail={'-' if not url else url}")
        fields = {"dow": "", "start_time": "", "runtime": ""}

        if url:
            try:
                soup = get_soup(url)
                fields = extract_broadcast_fields_from_infobox(soup)
            except Exception:
                pass
            time.sleep(SLEEP)

        # ★ 런타임 자동 추정(후처리: 항상 ON, 라벨 값 있으면 그대로 유지)
        fields["runtime"] = maybe_infer_runtime(
            fields["start_time"], fields["runtime"],
            min_ok=args.guard_min, max_ok=args.guard_max
        )

        rows.append({
            "title": title,
            "dow": fields["dow"],
            "start_time": fields["start_time"],  # 예: '22:30~00:00' 또는 '21:30'
            "runtime": fields["runtime"],        # 예: '70분' (없으면 빈 문자열)
        })

    df = (pd.DataFrame(rows, columns=["title", "dow", "start_time", "runtime"])
            .drop_duplicates(subset=["title"], keep="first"))

    out = "drama_weekly.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: {out} (행 수: {len(df)})")

if __name__ == "__main__":
    main()
