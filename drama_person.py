# -*- coding: utf-8 -*-
"""
drama_person.py — 위키 목록 → 상세(기본) 또는 단일 상세(--url)
허용 섹션의 인물 리스트에서 '배우명 : 배역명(…)? 역 - 설명' 포맷만 추출.

출력: drama_person.csv (title, role_type, character_name, order_no=1)

사용:
  python drama_person.py
  python drama_person.py --url "https://ko.wikipedia.org/wiki/작품제목"
"""

import re
import time
import argparse
from typing import List, Dict, Optional
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://ko.wikipedia.org"
LIST_URL = "https://ko.wikipedia.org/wiki/2025년_대한민국의_텔레비전_드라마_목록"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.5",
    "Connection": "keep-alive",
}
SLEEP = 0.12
WORKERS = 8

# ---- 섹션 키워드 ----
ALLOW_SECTIONS = [
    "출연", "출연진", "출연자",
    "등장인물", "인물", "배역", "캐스팅",
    "주요 인물", "조연", "특별 출연", "카메오"
]
BLOCK_SECTIONS = [
    "외부 링크", "같이 보기", "각주", "주석", "참고", "참조",
    "제작", "제작진", "기획", "방송", "방영", "편성",
    "시청률", "에피소드", "회차", "OST", "음악",
    "수상", "평가", "연표", "관련", "기타", "비고", "목차",
    "개요", "줄거리", "작품 소개", "기획 의도", "시놉시스", "방송 시간"
]

# ---- 정규식/정리 ----
RE_PARENS = re.compile(r"\([^)]*\)")
RE_FOOT   = re.compile(r"\[[^\]]*\]")
COLON_SPLIT = re.compile(r"\s*[:：]\s*")
SPLIT_DASH  = re.compile(r"\s*[–—-]\s*", re.UNICODE)  # -, –, — 허용

# 노이즈(크루·방송사·OST·스페셜 등) 키워드
CREW_WORDS = (
    "연출", "감독", "각본", "극본", "프로듀서", "제작", "기획",
    "촬영", "음악", "편성", "방송", "재방송", "예고"
)
CHANNEL_WORDS = (
    "KBS", "MBC", "SBS", "EBS", "JTBC", "TV조선", "채널A", "MBN", "ENA", "OCN", "Mnet", "tvN",
    "KBS1", "KBS2", "SBS플러스", "JTBC4", "SBS FiL", "E Channel",
    "넷플릭스", "Netflix", "티빙", "Tving", "웨이브", "Wavve",
    "디즈니", "Disney+", "쿠팡플레이", "Coupang Play", "U+모바일tv", "왓챠", "Watcha", "Prime Video"
)
OST_WORDS = ("OST", "Special Track", "스페셜", "드라마 스페셜")

def clean_text(s: str) -> str:
    if not s: return ""
    s = RE_FOOT.sub("", s)           # 각주 제거
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def is_noise_line(t: str) -> bool:
    """크루/OST/방송사/스페셜 등 노이즈 문장 거르기"""
    # 작품명 괄호 + 크루/OST 키워드 동시 등장 → 노이즈로 간주
    has_angle = ("《" in t and "》" in t)
    if any(w in t for w in OST_WORDS):
        return True
    if any(w in t for w in CREW_WORDS):
        if has_angle:
            return True
    if has_angle and any(w in t for w in CHANNEL_WORDS):
        return True
    # 채널명이 잔뜩 섞여 있는 작업 이력 문장
    if any(w in t for w in CHANNEL_WORDS) and any(w in t for w in CREW_WORDS):
        return True
    return False

# ---- 세션/요청 ----
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429,500,502,503,504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=64, pool_maxsize=64)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    r = session.get(url, timeout=20)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ---- 제목 ----
def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("#firstHeading")
    return clean_text(h1.get_text()) if h1 else ""

# ---- 섹션/스코프 판정 ----
def _skip_block(tag: Tag) -> bool:
    if not isinstance(tag, Tag): return True
    cls = " ".join(tag.get("class", []))
    if any(x in cls for x in ["navbox", "vertical-navbox", "sidebar", "sistersitebox", "metadata"]):
        return True
    if tag.name == "table" and "infobox" in cls:
        return True
    if tag.has_attr("id") and tag["id"] in ("references", "toc"):
        return True
    return False

def _nearest_prev_heading_text(root: Tag, node: Tag) -> str:
    cur = node
    while cur is not None and cur is not root:
        prev = cur.previous_sibling
        while prev:
            if isinstance(prev, Tag) and prev.name in ("h2", "h3", "h4"):
                return clean_text(prev.get_text())
            prev = prev.previous_sibling
        cur = cur.parent
    return ""

def _in_allowed_section(root: Tag, node: Tag) -> bool:
    sec = _nearest_prev_heading_text(root, node)
    if not sec:
        return False
    s = sec.split("[", 1)[0].split(":", 1)[0]
    if any(b in s for b in BLOCK_SECTIONS):
        return False
    return any(a in s for a in ALLOW_SECTIONS)

# ---- 파서 (엄격 버전) ----
def parse_role_line_strict(text: str) -> Optional[Dict[str, str]]:
    """
    '배우명 : 배역명 … 역 - 설명' 포맷만 허용
    - 콜론 필수, 대시(-/–/—) 필수, 왼쪽에 '역' 포함 필수
    - 배역명에서 괄호 제거(아역 등), '역'은 유지
    """
    t = clean_text(text)
    if not t:
        return None
    if is_noise_line(t):
        return None

    # 1) '배우명 : tail' 필수
    parts = COLON_SPLIT.split(t, maxsplit=1)
    if len(parts) != 2:
        return None
    tail = parts[1]

    # 2) 대시로 좌/우 분리 필수
    parts2 = SPLIT_DASH.split(tail, maxsplit=1)
    if len(parts2) != 2:
        return None
    left, right = parts2[0], parts2[1]

    # 3) 왼쪽에 반드시 '역' 포함
    if "역" not in left:
        return None

    # 4) 배역명: 괄호 제거(아역 등), '역' 유지
    character_name = RE_PARENS.sub("", left).strip()
    role_type = right.strip()

    if not character_name or not role_type:
        return None
    return {"character_name": character_name, "role_type": role_type}

# ---- 상세 수집기 ----
def _scan_blocks(root: Tag, scoped_only: bool, tag_names: List[str]) -> List[Tag]:
    out = []
    for tg in root.find_all(tag_names):
        anc, skip = tg, False
        while anc and anc is not root:
            if _skip_block(anc):
                skip = True; break
            anc = anc.parent
        if skip:
            continue
        if scoped_only and not _in_allowed_section(root, tg):
            continue
        out.append(tg)
    return out

def scrape_detail(session: requests.Session, url: str, title_fallback: str = "") -> List[Dict[str, str]]:
    soup = get_soup(session, url)
    title = extract_title(soup) or title_fallback
    root = soup.select_one("#mw-content-text > div.mw-parser-output")
    if not root:
        return []

    rows: List[Dict[str, str]] = []

    # 1) 허용 섹션 내부 우선
    for ul in _scan_blocks(root, True, ["ul"]):
        for li in ul.find_all("li", recursive=False):
            d = parse_role_line_strict(li.get_text())
            if d:
                rows.append({"title": title, "role_type": d["role_type"], "character_name": d["character_name"], "order_no": 1})

    # 2) 허용 섹션 내 dl/table도 시도
    if not rows:
        for dl in _scan_blocks(root, True, ["dl"]):
            for node in dl.find_all(["dt", "dd"], recursive=False):
                d = parse_role_line_strict(node.get_text())
                if d:
                    rows.append({"title": title, "role_type": d["role_type"], "character_name": d["character_name"], "order_no": 1})

    if not rows:
        for tb in _scan_blocks(root, True, ["table"]):
            for cell in tb.find_all(["td", "th"]):
                d = parse_role_line_strict(cell.get_text())
                if d:
                    rows.append({"title": title, "role_type": d["role_type"], "character_name": d["character_name"], "order_no": 1})

    # 3) 폴백: 전체 영역
    if not rows:
        for ul in _scan_blocks(root, False, ["ul"]):
            for li in ul.find_all("li", recursive=False):
                d = parse_role_line_strict(li.get_text())
                if d:
                    rows.append({"title": title, "role_type": d["role_type"], "character_name": d["character_name"], "order_no": 1})

    if not rows:
        for dl in _scan_blocks(root, False, ["dl"]):
            for node in dl.find_all(["dt", "dd"], recursive=False):
                d = parse_role_line_strict(node.get_text())
                if d:
                    rows.append({"title": title, "role_type": d["role_type"], "character_name": d["character_name"], "order_no": 1})

    if not rows:
        for tb in _scan_blocks(root, False, ["table"]):
            for cell in tb.find_all(["td", "th"]):
                d = parse_role_line_strict(cell.get_text())
                if d:
                    rows.append({"title": title, "role_type": d["role_type"], "character_name": d["character_name"], "order_no": 1})

    print(f"[detail] {title} -> rows:{len(rows)}")
    return rows

# ---- 목록 페이지 ----
def extract_list_items(session: requests.Session, list_url: str) -> List[Dict]:
    soup = get_soup(session, list_url)
    items: List[Dict] = []
    tables = soup.select("#mw-content-text table.wikitable, #content table.wikitable")

    def norm(x: str) -> str:
        return clean_text(x).replace(" ", "")

    for table in tables:
        cap = table.find("caption")
        if cap and any(k in norm(cap.get_text()) for k in ("범례","설명")):
            continue

        idx_title = None
        header = table.find("thead")
        row = header.find("tr") if header else None
        if not row:
            for tr in table.select("tr"):
                if tr.find_all("th"):
                    row = tr; break
        if row:
            for i, th in enumerate(row.find_all("th"), start=1):
                if any(k in norm(th.get_text()) for k in ("제목","작품명","프로그램명")):
                    idx_title = i
        if not idx_title:
            continue

        for tr in table.select("tr"):
            if tr.find_all("th") and not tr.find("td"):
                continue
            tds = tr.find_all("td")
            if not tds or len(tds) < idx_title:
                continue
            td = tds[idx_title - 1]
            title_text = clean_text(td.get_text())
            if not title_text:
                continue

            detail_url = None
            a = td.find("a", href=True)
            if a:
                href = a["href"]
                is_red = ("new" in (a.get("class") or [])) or ("redlink=1" in href)
                if href.startswith("/wiki/") and ":" not in href and not is_red:
                    detail_url = urljoin(BASE, href)

            items.append({"title_fallback": title_text, "detail_url": detail_url})

    # dedupe
    seen, uniq = set(), []
    for it in items:
        key = (it["detail_url"], it["title_fallback"])
        if key not in seen:
            seen.add(key); uniq.append(it)
    return uniq

# ---- 메인 ----
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=False, help="단일 상세 페이지 URL (없으면 목록→상세 전체 수집)")
    args = ap.parse_args()

    session = make_session()
    rows: List[Dict[str, str]] = []

    if args.url:
        rows.extend(scrape_detail(session, args.url))
    else:
        print("[*] 목록 페이지:", LIST_URL)
        items = extract_list_items(session, LIST_URL)
        print(f" - 작품 수집: {len(items)}개")
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [
                ex.submit(scrape_detail, session, it["detail_url"], it["title_fallback"])
                for it in items if it["detail_url"]
            ]
            for fut in as_completed(futs):
                try:
                    rows.extend(fut.result())
                finally:
                    time.sleep(SLEEP)

    if not rows:
        print("[-] 추출 결과 없음"); return

    df = pd.DataFrame(rows, columns=["title", "role_type", "character_name", "order_no"])
    df = df.drop_duplicates()
    df = df.sort_values(by=["title", "character_name"], kind="stable")
    df.to_csv("drama_person.csv", index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: drama_person.csv (행 수: {len(df)})")

if __name__ == "__main__":
    main()
