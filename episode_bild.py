# -*- coding: utf-8 -*-
"""
episode_bild.py — CSV의 제목 목록을 읽어 나무위키 회차 정보 수집 (안전 저장/정리 포함)

규칙(속도 최적화):
  A) 기본 문서 존재 확인: "{제목} (드라마)" → "{제목}(드라마)" → "{제목}" 중 최초 200 하나만 확정
  B) 확정된 제목에 대해서만 하위문서 "/방영 목록" 단일 시도
  C) 실패 시 기본 문서의 '방영 목록/방송 목록/회차 목록/에피소드/회차 정보/줄거리' 섹션 표 파싱

표 파서:
  - 가로 헤더형(wikitable) / 세로 라벨형 / 인덱스 백업형 모두 지원
  - 회차 표가 여러 행이면 여러 개 수집

안전장치:
  - 텍스트 내부 제어문자/널바이트 제거
  - 원자적 저장(임시파일→교체), UTF-8-SIG, \n 개행 고정

입력:  c:/Users/최은영/Desktop/workspace/drama.csv   (컬럼: title / 제목 / drama_title 중 하나)
출력:  c:/Users/최은영/Desktop/workspace/episode_bild.csv
스키마: drama_title, episode_no, title, broadcast_at, description
"""

import os
import re
import time
import csv
import shutil
import tempfile
from pathlib import Path
from urllib.parse import quote, urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup, Tag

# ---------------- 기본 설정 ----------------
BASE = "https://namu.wiki"
WORKDIR = Path("c:/Users/최은영/Desktop/workspace")
IN_CSV = WORKDIR / "drama.csv"
OUT_CSV = WORKDIR / "episode_bild.csv"

TIMEOUT = 8
SLEEP = 0.35  # 조금 더 빠르게
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0.0 Safari/537.36"),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.6",
}
ALLOWED_PREFIXES = ("/", "/w/", "/img/", "/i/", "/js/", "/css/", "/_nuxt/")
SUBPAGE_NAME = "방영 목록"
SECTION_TITLES = ["방영 목록", "방송 목록", "회차 목록", "에피소드", "회차 정보", "줄거리"]


# ---------------- 공통 유틸 ----------------
_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

def strip_ctrl(s: str) -> str:
    if not s:
        return ""
    return _CTRL_RE.sub("", s)

def clean_text(s: str) -> str:
    if not s:
        return ""
    t = re.sub(r"\[[^\]]*\]", "", s)  # 각주 제거
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"\s+", " ", t)
    return strip_ctrl(t).strip()

def norm_title(s: str) -> str:
    if not s:
        return ""
    t = str(s).strip()
    t = re.sub(r"[《》〈〉「」『』“”‘’\"'`]+", "", t)
    t = re.sub(r"\s*\(드라마\)\s*$", "", t)
    t = re.sub(r"\s+", " ", t).strip(" .")
    return t

def allowed(url: str) -> bool:
    p = urlparse(url)
    return any((p.path or "").startswith(pref) for pref in ALLOWED_PREFIXES)

def get_html_with_status(session: requests.Session, url: str):
    if not allowed(url):
        return None, None
    try:
        r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
        return (r.text if r.status_code == 200 else None), r.status_code
    except requests.RequestException:
        return None, None

def atomic_write_csv(df: pd.DataFrame, out_path: Path):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # 임시 파일에 먼저 저장
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8-sig", newline="") as tmp:
        tmp_name = tmp.name
        df.to_csv(tmp, index=False, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    # 교체
    if out_path.exists():
        os.remove(out_path)
    shutil.move(tmp_name, out_path)


# ---------------- 섹션 탐색 ----------------
def iter_section_tables(soup: BeautifulSoup, section_titles=SECTION_TITLES):
    """
    주어진 섹션명 중 하나가 보이면 그 섹션부터 다음 같은 레벨 섹션 전까지의 table만 yield.
    섹션을 못 찾으면 문서 전체 표를 폴백으로 반환.
    """
    heading = None
    for level in ("h2", "h3"):
        for h in soup.select(level):
            txt = clean_text(h.get_text())
            if any(k in txt for k in section_titles):
                heading = h
                break
        if heading:
            break

    if not heading:
        for tbl in soup.find_all("table"):
            yield tbl
        return

    level_name = heading.name
    cur = heading.next_sibling
    while cur:
        if isinstance(cur, Tag):
            if cur.name == level_name:  # 같은 레벨 다음 섹션 도달 → 종료
                break
            if cur.name == "table":
                yield cur
        cur = cur.next_sibling


# ---------------- 표 파싱 ----------------
def _text(elem: Tag | None) -> str:
    return clean_text(elem.get_text(separator=" ")) if elem else ""

def normalize_episode_no(s: str) -> str:
    t = clean_text(s)
    m = re.search(r"(\d+)", t)
    return f"{int(m.group(1))}화" if m else t

def parse_table_horizontal(table: Tag) -> list[dict]:
    """
    가로 헤더형: 첫 행에서 헤더(th/td) 열 이름 매핑 후, 모든 데이터 행 파싱.
    """
    rows = table.find_all("tr")
    if not rows:
        return []
    header_ths = rows[0].find_all(["th", "td"])
    if not header_ths:
        return []

    def keyname(x: str) -> str:
        x = clean_text(x).replace(" ", "")
        if "회차" in x or x == "회":
            return "episode_no"
        if "방영일" in x or "방송일" in x or "방영" in x:
            return "broadcast_at"
        if "제목" in x or "부제" in x or "소제목" in x:
            return "title"
        if "줄거리" in x or "개요" in x or "내용" in x:
            return "description"
        return ""

    idx_map: dict[int, str] = {}
    for idx, th in enumerate(header_ths):
        k = keyname(_text(th))
        if k:
            idx_map[idx] = k

    has_core = ("episode_no" in idx_map.values()) and (("title" in idx_map.values()) or ("broadcast_at" in idx_map.values()))
    if not has_core:
        return []

    out: list[dict] = []
    for tr in rows[1:]:
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue
        item = {"episode_no": "", "broadcast_at": "", "title": "", "description": ""}
        for i, td in enumerate(tds):
            if i not in idx_map:
                continue
            key = idx_map[i]
            val = _text(td)
            if key == "episode_no":
                val = normalize_episode_no(val)
            if key == "title":
                strong = td.find("strong")
                if strong:
                    val = _text(strong)
            item[key] = val
        if item["episode_no"] or item["title"]:
            out.append(item)
    return out

def parse_table_vertical(table: Tag) -> list[dict]:
    """
    세로 라벨형: tr>th가 '회차/방영일/제목/줄거리'일 때 1건을 dict로 반환.
    """
    rows = table.find_all("tr")
    if not rows:
        return []
    label_map = {}
    for tr in rows:
        th = tr.find("th")
        tds = tr.find_all("td")
        if th and tds:
            key = _text(th).replace(" ", "")
            val = _text(tds[-1])
            if key:
                label_map[key] = val
    hit = sum(1 for k in label_map if any(x in k for x in ("회차", "방영일", "제목", "줄거리")))
    if hit < 2:
        return []
    ep = label_map.get("회차", "")
    if ep:
        ep = normalize_episode_no(ep)
    return [{
        "episode_no": ep,
        "broadcast_at": label_map.get("방영일", ""),
        "title": label_map.get("제목", ""),
        "description": label_map.get("줄거리", "")
    }]

def parse_table_backup_indexed(table: Tag) -> list[dict]:
    """
    네가 준 tr 인덱스 기반 백업형. 표 하나 → 1건.
    """
    rows = table.find_all("tr")
    if not rows:
        return []
    try:
        td0 = rows[0].find("td")
        ep_text = _text(td0.find("strong") or td0) if td0 else ""
        if not re.search(r"(\d+)", ep_text):
            return []
        broadcast_at = title = description = ""
        if len(rows) >= 3:
            tds = rows[2].find_all("td")
            if len(tds) >= 2:
                broadcast_at = _text(tds[1])
        if len(rows) >= 4:
            tds = rows[3].find_all("td")
            if len(tds) >= 2:
                title = _text(tds[1].find("strong") or tds[1])
        if len(rows) >= 5:
            tds = rows[4].find_all("td")
            if len(tds) >= 2:
                description = _text(tds[1])
        return [{
            "episode_no": normalize_episode_no(ep_text),
            "broadcast_at": broadcast_at,
            "title": title,
            "description": description
        }]
    except Exception:
        return []

def parse_episode_table(table: Tag) -> list[dict]:
    """
    표 → 0..N건(가로헤더/세로라벨/백업형 모두 지원)
    """
    out = parse_table_horizontal(table)
    if out:
        return out
    out = parse_table_vertical(table)
    if out:
        return out
    return parse_table_backup_indexed(table)

def parse_document_for_episodes(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for tbl in iter_section_tables(soup, SECTION_TITLES):
        parsed = parse_episode_table(tbl)
        if parsed:
            rows.extend(parsed)
    # 회차 숫자 기준 정렬
    def ep_key(d):
        m = re.search(r"(\d+)", d.get("episode_no", "") or "")
        return int(m.group(1)) if m else 10**9
    rows.sort(key=ep_key)
    return rows


# ---------------- 드라마별 처리 (기본 문서 확정 → 방영 목록만) ----------------
def fetch_episodes_for_title(session: requests.Session, title_display: str) -> tuple[list[dict], list[str]]:
    tried: list[str] = []
    base = norm_title(title_display)
    title_cands = (f"{base} (드라마)", f"{base}(드라마)", base)

    # A) 기본 문서 존재 확인 — 최초 200 하나만 확정
    base_ok = None
    base_url = None
    for cand in title_cands:
        url = f"{BASE}/w/{quote(cand, safe='')}"
        tried.append(url)
        html, status = get_html_with_status(session, url)
        print(f"    - base check: {url} [{status}]")
        if html:
            base_ok = html
            base_url = url
            break
        time.sleep(SLEEP)

    if not base_ok:
        return [], tried

    # B) 하위문서: 방영 목록만 1회 시도
    sub_url = f"{base_url}/" + quote(SUBPAGE_NAME, safe="")
    tried.append(sub_url)
    html, status = get_html_with_status(session, sub_url)
    print(f"    - subpage:    {sub_url} [{status}]")
    if html:
        rows = parse_document_for_episodes(html)
        if rows:
            return rows, tried

    # C) 하위문서 실패 → 기본 문서 섹션 파싱 1회
    rows = parse_document_for_episodes(base_ok)
    if rows:
        return rows, tried

    return [], tried


# ---------------- 메인 ----------------
def main():
    # CSV 읽기
    try:
        df = pd.read_csv(IN_CSV, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(IN_CSV, encoding="cp949")

    title_col = next((c for c in ("title", "제목", "drama_title") if c in df.columns), None)
    if not title_col:
        print("CSV에 'title' 또는 '제목' 또는 'drama_title' 컬럼이 없습니다.")
        return

    titles = [str(x).strip() for x in df[title_col].fillna("") if str(x).strip()]
    out_rows = []

    with requests.Session() as session:
        for i, t in enumerate(titles, 1):
            print(f"[{i}/{len(titles)}] {t}")
            eps, tried = fetch_episodes_for_title(session, t)
            if not eps:
                print("  - (no episode list)")
                for u in tried[-4:]:
                    print(f"      • {u}")
                continue
            for ep in eps:
                out_rows.append({
                    "drama_title": t,
                    "episode_no": ep.get("episode_no", ""),
                    "title": ep.get("title", ""),
                    "broadcast_at": ep.get("broadcast_at", ""),
                    "description": ep.get("description", ""),
                })
            time.sleep(SLEEP)

    # 결과 정리 + 저장
    cols = ["drama_title", "episode_no", "title", "broadcast_at", "description"]
    if out_rows:
        # 필드값 클린업(제어문자/널 등 제거)
        for r in out_rows:
            for k in cols:
                r[k] = strip_ctrl(str(r.get(k, "")))
        df_out = pd.DataFrame(out_rows, columns=cols)
    else:
        df_out = pd.DataFrame(columns=cols)

    atomic_write_csv(df_out, OUT_CSV)
    print(f"[✓] 저장 완료: {OUT_CSV} (총 {len(df_out)}행)")

if __name__ == "__main__":
    main()
