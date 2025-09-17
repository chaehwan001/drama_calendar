# -*- coding: utf-8 -*-
"""
2025년 대한민국 드라마 목록 → 상세 페이지 →
.mw-parser-output 내부에서 '출연/등장인물/배역/캐스팅' 섹션 범위 안의 배우만 수집.

수집 규칙:
  - 허용 섹션(ALLOW_SECTIONS)에만 포함
  - 차단 섹션(BLOCK_SECTIONS), navbox/infobox/각주/목차/사이드바 등은 제외
  - li/td/th/dt/dd 안에서 '배우 같은 링크(is_actor_link)'가 **첫 번째 a**여야 함
  - 배우 같은 링크의 개수가 정확히 1개 → OK
  - 방송사/플랫폼/OTT/프로그램 이름, 인물 아닌 힌트어 포함 시 제외
  - 그래도 없으면 상단부 제한 폴백

출력: kdrama_2025_cast_filtered.csv (제목, 배우)
"""

import re
import time
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag, NavigableString
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================= 기본 설정 =================
BASE = "https://ko.wikipedia.org"
LIST_URL = "https://ko.wikipedia.org/wiki/2025년_대한민국의_텔레비전_드라마_목록"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.5",
    "Connection": "keep-alive",
}

SLEEP = 0.15
WORKERS = 8

# ======= 섹션 키워드 =======
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

# ======= 이름/링크 필터 =======
KOREAN_NAME_RE = re.compile(r"^[가-힣]{2,4}(?:\s[가-힣]{2,4})?$")
NON_PERSON_HINTS = (
    "연출", "각본", "작가", "감독", "제작", "기획", "촬영", "음악",
    "회사", "방송", "채널"
)
NON_PERSON_BRANDS = (
    "KBS", "MBC", "SBS", "EBS", "JTBC", "TV조선", "채널A", "MBN", "ENA", "OCN", "Mnet", "tvN",
    "KBS1", "KBS2", "SBS플러스", "JTBC4", "SBS FiL", "E Channel",
    "넷플릭스", "Netflix", "티빙", "Tving", "웨이브", "Wavve",
    "디즈니", "Disney+", "쿠팡플레이", "Coupang Play",
    "U+모바일tv", "왓챠", "Watcha", "Prime Video", "아마존 프라임",
    "예고", "재방송", "편성표", "프로그램", "시리즈"
)

# ================= 유틸 =================
def clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\[[^\]]*\]", "", s)  # 각주 제거
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def looks_like_person_name(txt: str) -> bool:
    if KOREAN_NAME_RE.match(txt):
        return True
    if 2 <= len(txt) <= 5 and re.fullmatch(r"[가-힣·]+", txt):
        return True
    return False

def is_actor_link(a: Tag) -> bool:
    if not a or not a.has_attr("href"):
        return False
    href = a["href"]
    if not href.startswith("/wiki/"):
        return False
    tail = href.split("/wiki/")[-1]
    if ":" in tail:
        return False

    txt: str = clean_text(a.get_text())
    if not txt:
        return False
    if any(b in txt for b in NON_PERSON_BRANDS):
        return False
    if any(b in tail for b in NON_PERSON_BRANDS):
        return False
    if any(h in txt for h in NON_PERSON_HINTS):
        return False

    base = txt.split("(")[0].strip()
    return looks_like_person_name(base)

def pick_actor_anchor(container: Tag) -> Optional[Tag]:
    """
    - 첫 번째 a가 배우 같은 링크여야 함
    - 배우 a 총 개수는 정확히 1개
    - 첫 a 이전에 공백 외 노드 있으면 제외
    """
    if not isinstance(container, Tag):
        return None
    first_a = None
    for ch in container.children:
        if isinstance(ch, NavigableString):
            if ch.strip():
                return None
            continue
        if isinstance(ch, Tag):
            if ch.name == "a":
                first_a = ch
                break
            else:
                return None
    if first_a is None or not is_actor_link(first_a):
        return None
    actor_count = sum(1 for a in container.find_all("a", recursive=False) if is_actor_link(a))
    return first_a if actor_count == 1 else None

# ================= 세션/요청 =================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=64, pool_maxsize=64)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    r = session.get(url, timeout=20)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ================= 목록 페이지 =================
def extract_list_items(session: requests.Session, list_url: str) -> List[Dict]:
    soup = get_soup(session, list_url)
    items: List[Dict] = []
    tables = soup.select("#mw-content-text table.wikitable, #content table.wikitable")
    def norm(x: str) -> str: return clean_text(x).replace(" ", "")
    for table in tables:
        cap = table.find("caption")
        cap_txt = clean_text(cap.get_text()).replace(" ", "") if cap else ""
        if any(k in cap_txt for k in ("범례", "설명")):
            continue
        idx_title = None
        header_row = (table.find("thead").find("tr")
                      if table.find("thead") else
                      (table.find("tr") if table.find("tr") and table.find("tr").find_all("th") else None))
        if header_row:
            for i, th in enumerate(header_row.find_all("th"), start=1):
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
            title_text = clean_text(td_title.get_text())
            if not title_text:
                continue
            detail_url: Optional[str] = None
            a = td_title.find("a", href=True)
            if a:
                href = a["href"]
                is_red = ("new" in (a.get("class") or [])) or ("redlink=1" in href)
                if href.startswith("/wiki/") and ":" not in href and not is_red:
                    detail_url = urljoin(BASE, href)
            items.append({"title_fallback": title_text, "detail_url": detail_url})
    seen, uniq = set(), []
    for it in items:
        key = (it["detail_url"], it["title_fallback"])
        if key not in seen:
            seen.add(key); uniq.append(it)
    for order, it in enumerate(uniq):
        it["order"] = order
    return uniq

# ================= 섹션/스킵 판정 =================
def _skip_block(tag: Tag) -> bool:
    if not isinstance(tag, Tag):
        return True
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
    sec_title = _nearest_prev_heading_text(root, node)
    if not sec_title:
        return False
    s = sec_title.split("[", 1)[0].split(":", 1)[0]
    if any(b in s for b in BLOCK_SECTIONS):
        return False
    return any(a in s for a in ALLOW_SECTIONS)

# ================= 수집 =================
def collect_from_uls(root: Tag, page_title: str) -> List[str]:
    out: List[str] = []
    for ul in root.find_all("ul"):
        anc, skip = ul, False
        while anc and anc is not root:
            if _skip_block(anc):
                skip = True; break
            anc = anc.parent
        if skip or not _in_allowed_section(root, ul):
            continue
        for li in ul.find_all("li", recursive=False):
            a = pick_actor_anchor(li)
            if not a:
                continue
            txt = clean_text(a.get_text()).split("(")[0].strip()
            if txt and txt != page_title:
                out.append(txt)
    return out

def collect_from_tables(root: Tag, page_title: str) -> List[str]:
    out: List[str] = []
    for table in root.find_all(["table"]):
        anc, skip = table, False
        while anc and anc is not root:
            if _skip_block(anc):
                skip = True; break
            anc = anc.parent
        if skip or not _in_allowed_section(root, table):
            continue
        for cell in table.find_all(["td", "th"]):
            a = pick_actor_anchor(cell)
            if not a:
                continue
            txt = clean_text(a.get_text()).split("(")[0].strip()
            if txt and txt != page_title:
                out.append(txt)
    return out

def collect_from_definition_lists(root: Tag, page_title: str) -> List[str]:
    out: List[str] = []
    for dl in root.find_all("dl"):
        anc, skip = dl, False
        while anc and anc is not root:
            if _skip_block(anc):
                skip = True; break
            anc = anc.parent
        if skip or not _in_allowed_section(root, dl):
            continue
        for node in dl.find_all(["dt", "dd"], recursive=False):
            a = pick_actor_anchor(node)
            if not a:
                continue
            txt = clean_text(a.get_text()).split("(")[0].strip()
            if txt and txt != page_title:
                out.append(txt)
    return out

def extract_cast_scoped(soup: BeautifulSoup) -> Tuple[List[str], Dict[str, int]]:
    stats = {"ul": 0, "table": 0, "dl": 0, "fallback": 0}
    title_node = soup.select_one("#firstHeading")
    page_title = clean_text(title_node.get_text()) if title_node else ""
    root = soup.select_one("#mw-content-text > div.mw-content-ltr.mw-parser-output")
    if not root:
        return [], stats
    names: List[str] = []
    ul_names = collect_from_uls(root, page_title);            stats["ul"] = len(ul_names)
    tb_names = collect_from_tables(root, page_title);          stats["table"] = len(tb_names)
    dl_names = collect_from_definition_lists(root, page_title); stats["dl"] = len(dl_names)
    names.extend(ul_names + tb_names + dl_names)
    uniq, seen = [], set()
    for n in names:
        if n not in seen:
            seen.add(n); uniq.append(n)
    if uniq:
        return uniq, stats
    # 폴백
    fallback: List[str] = []
    for child in root.children:
        if not isinstance(child, Tag):
            continue
        if child.name in ("h2", "h3", "h4"):
            title = clean_text(child.get_text())
            if any(b in title for b in BLOCK_SECTIONS):
                break
        if child.name == "ul":
            for li in child.find_all("li", recursive=False):
                a = pick_actor_anchor(li)
                if not a:
                    continue
                t = clean_text(a.get_text()).split("(")[0].strip()
                if t and t != page_title:
                    fallback.append(t)
        if len(fallback) >= 30:
            break
    uniq_fb, seen_fb = [], set()
    for n in fallback:
        if n not in seen_fb:
            seen_fb.add(n); uniq_fb.append(n)
    stats["fallback"] = len(uniq_fb)
    return uniq_fb, stats

# ================= 제목/상세 =================
def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("#firstHeading")
    if h1 and h1.get_text(strip=True):
        return clean_text(h1.get_text())
    el = soup.select_one("#mw-content-text table.infobox tr:first-child b, "
                         "#mw-content-text table.infobox tr:first-child strong")
    if el and el.get_text(strip=True):
        return clean_text(el.get_text())
    return ""

def scrape_detail(session: requests.Session, it: Dict) -> List[Dict[str, str]]:
    url = it["detail_url"]; title_fallback = it["title_fallback"]
    if not url:
        return []
    try:
        soup = get_soup(session, url)
        title = extract_title(soup) or title_fallback
        names, stats = extract_cast_scoped(soup)
        print(f"[detail] {title}  UL:{stats['ul']}  TABLE:{stats['table']}  DL:{stats['dl']}  FB:{stats['fallback']}")
        # ✅ 컬럼 이름을 title, person_name으로 반환
        return [{"title": title, "person_name": n, "_order": it["order"]} for n in names]
    except Exception as e:
        print(f"[ERR] {url} -> {e}")
        return []

# ================= 실행 =================
def main():
    session = make_session()
    print("[*] 목록 페이지:", LIST_URL)
    items = extract_list_items(session, LIST_URL)
    print(f" - 작품 수집: {len(items)}개")
    rows: List[Dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(scrape_detail, session, it) for it in items]
        for fut in as_completed(futs):
            rows.extend(fut.result())
            time.sleep(SLEEP)
    if not rows:
        print("[-] 결과 없음"); return
    # ✅ 컬럼명 변경
    df = pd.DataFrame(rows, columns=["title", "person_name", "_order"])
    df = df.drop_duplicates(subset=["title", "person_name"])
    df = df.sort_values(by=["_order", "title", "person_name"], kind="stable").drop(columns=["_order"])
    out = "actor.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: {out} (행 수: {len(df)})")

if __name__ == "__main__":
    main()
