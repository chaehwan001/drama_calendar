# -*- coding: utf-8 -*-
"""
2025년 대한민국 드라마 목록 → 상세 페이지 → 배우 링크 수집 → 배우 개인 문서에서
name, birth_date, gender 추출.

출력: person_bild_people.csv (name, birth_date, gender)
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
    "예고", "재방송", "편성표", "프로그램", "시리즈",
    # 사용자 지정 필터(회사/스튜디오/브랜드명 → 배우 아님)
    "스튜디오 드래곤", "박스미디어", "아센디오", "키이스트",
    "아누팜 트리파티", "덱스터 스튜디오", "미스터 로맨스"
)

DATE_RE = re.compile(r"(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)")  # 1982년 11월 5일

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
    if ":" in tail:  # 파일/분류/틀 등 제외
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

# ================= 수집: 배우 링크(이름+URL) =================
def collect_actor_links_from_uls(root: Tag, page_title: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
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
                out.append((txt, urljoin(BASE, a["href"])))
    return out

def collect_actor_links_from_tables(root: Tag, page_title: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
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
                out.append((txt, urljoin(BASE, a["href"])))
    return out

def collect_actor_links_from_definition_lists(root: Tag, page_title: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
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
                out.append((txt, urljoin(BASE, a["href"])))
    return out

def extract_actor_links_scoped(soup: BeautifulSoup) -> Tuple[List[Tuple[str, str]], Dict[str, int]]:
    stats = {"ul": 0, "table": 0, "dl": 0, "fallback": 0}
    title_node = soup.select_one("#firstHeading")
    page_title = clean_text(title_node.get_text()) if title_node else ""
    root = soup.select_one("#mw-content-text > div.mw-content-ltr.mw-parser-output")
    if not root:
        return [], stats
    pairs: List[Tuple[str, str]] = []
    ul_pairs = collect_actor_links_from_uls(root, page_title);              stats["ul"] = len(ul_pairs)
    tb_pairs = collect_actor_links_from_tables(root, page_title);            stats["table"] = len(tb_pairs)
    dl_pairs = collect_actor_links_from_definition_lists(root, page_title);  stats["dl"] = len(dl_pairs)
    pairs.extend(ul_pairs + tb_pairs + dl_pairs)

    # uniq by (name, url)
    seen, uniq = set(), []
    for nm, href in pairs:
        key = (nm, href)
        if key not in seen:
            seen.add(key); uniq.append((nm, href))

    if uniq:
        return uniq, stats

    # 폴백(상단부 제한)
    fallback: List[Tuple[str, str]] = []
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
                    fallback.append((t, urljoin(BASE, a["href"])))
        if len(fallback) >= 30:
            break
    # uniq fallback
    seen_fb, uniq_fb = set(), []
    for nm, href in fallback:
        key = (nm, href)
        if key not in seen_fb:
            seen_fb.add(key); uniq_fb.append((nm, href))
    stats["fallback"] = len(uniq_fb)
    return uniq_fb, stats

# ================= 배우 개인 문서 파서 =================
def _infobox(soup: BeautifulSoup) -> Optional[Tag]:
    return soup.select_one("#mw-content-text table.infobox")

def _infobox_value_by_header(soup: BeautifulSoup, header_keywords: List[str]) -> Optional[str]:
    box = _infobox(soup)
    if not box:
        return None
    for tr in box.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        h = clean_text(th.get_text())
        if any(k in h for k in header_keywords):
            return clean_text(td.get_text(" ").strip())
    return None

def extract_birth_date_and_gender(session: requests.Session, url: str) -> Tuple[Optional[str], Optional[str]]:
    """배우 문서에서 birth_date(YYYY년 M월 D일)와 gender('남성'/'여성' 등) 추출"""
    try:
        soup = get_soup(session, url)
    except Exception:
        return None, None

    # 1) 생년월일
    raw_birth = _infobox_value_by_header(soup, ["출생", "생년월일"])
    birth_date = None
    if raw_birth:
        # 예) "1982년 11월 5일(42세) 대한민국 서울특별시 ..."
        m = DATE_RE.search(raw_birth)
        if m:
            birth_date = m.group(1)

    # 2) 성별 우선: 인포박스 '성별' 항목
    gender = _infobox_value_by_header(soup, ["성별"])
    if gender:
        gender = clean_text(gender)
    else:
        # 3) 보조: 카테고리(남자 배우 / 여자 배우 포함 여부)
        cats = [clean_text(a.get_text()) for a in soup.select("#catlinks a")]
        # 좀 더 보수적으로: '남자' & '배우' / '여자' & '배우'
        if any(("남자" in c and "배우" in c) for c in cats):
            gender = "남성"
        elif any(("여자" in c and "배우" in c) for c in cats):
            gender = "여성"
        # 그래도 없으면 None 그대로 둠

    return birth_date, gender

# ================= 상세 페이지 → 배우 인물 정보 스크랩 =================
def scrape_detail_for_people(session: requests.Session, it: Dict) -> List[Dict[str, str]]:
    url = it["detail_url"]; title_fallback = it["title_fallback"]
    if not url:
        return []
    try:
        soup = get_soup(session, url)
        # 배우 (이름, URL) 목록
        pairs, stats = extract_actor_links_scoped(soup)
        print(f"[detail] {title_fallback}  UL:{stats['ul']}  TABLE:{stats['table']}  DL:{stats['dl']}  FB:{stats['fallback']}")
        out_rows: List[Dict[str, str]] = []
        for name, person_url in pairs:
            bday, gender = extract_birth_date_and_gender(session, person_url)
            out_rows.append({"name": name, "birth_date": bday or "", "gender": gender or ""})
        return out_rows
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
        futs = [ex.submit(scrape_detail_for_people, session, it) for it in items]
        for fut in as_completed(futs):
            rows.extend(fut.result())
            time.sleep(SLEEP)

    if not rows:
        print("[-] 결과 없음"); return

    df = pd.DataFrame(rows, columns=["name", "birth_date", "gender"])
    # 배우 단위 중복 제거 (동명이인은 나중에 필요하면 URL 기준으로 확장)
    df = df.drop_duplicates(subset=["name", "birth_date", "gender"])
    out = "person.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[✓] 저장 완료: {out} (행 수: {len(df)})")

if __name__ == "__main__":
    main()
