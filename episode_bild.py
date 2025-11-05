# -*- coding: utf-8 -*-
"""
episode_bild.py — 나무위키 '{제목}(드라마) → /방영 목록'만 크롤링해 회차 CSV 생성
- 스키마: drama_title, episode_no, title, broadcast_at, runtime_min, description
- runtime_min은 비워둠
- 같은 회차 중복 병합 + '제목 비고 줄거리만 있을 때 30자 규칙' 반영
"""

import os, re, csv, time, shutil, tempfile
from pathlib import Path
from urllib.parse import quote, urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup, Tag

BASE = "https://namu.wiki"
WORKDIR = Path(r"C:/Users/PC/Desktop/workspace")
IN_CSV = WORKDIR / "drama.csv"
OUT_CSV = WORKDIR / "episode_bild.csv"

TIMEOUT = 8
SLEEP = 0.35
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/127.0.0.0 Safari/537.36"),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.6",
}
ALLOWED_PREFIXES = ("/", "/w/", "/img/", "/i/", "/js/", "/css/", "/_nuxt/")
SUBPAGE_NAME = "방영 목록"

# --- text helpers ---
_CTRL_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
ZWJ_RE   = re.compile(r"[\u200D\uFE0E\uFE0F]")
EMOJI_RE = re.compile(r"[\U00010000-\U0010FFFF]")

def strip_ctrl_emoji(s: str) -> str:
    if not s: return ""
    s = _CTRL_RE.sub("", str(s))
    s = ZWJ_RE.sub("", s)
    s = EMOJI_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_text(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\[[^\]]*\]", "", str(s))
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\s+", " ", s).strip()
    return strip_ctrl_emoji(s)

def norm_title(s: str) -> str:
    t = clean_text(s)
    t = re.sub(r"[《》〈〉「」『』“”‘’\"'`]+", "", t)
    t = re.sub(r"\s*\(드라마\)\s*$", "", t)
    return t.strip(" .")

def allowed(url: str) -> bool:
    from urllib.parse import urlparse
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
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8-sig", newline="") as tmp:
        tmp_name = tmp.name
        df.to_csv(tmp, index=False, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    if out_path.exists():
        os.remove(out_path)
    shutil.move(tmp_name, out_path)

# --- table parsing ---
def _text(elem: Tag | None) -> str:
    return clean_text(elem.get_text(separator=" ")) if elem else ""

def normalize_episode_no(s: str) -> str:
    m = re.search(r"(\d+)", clean_text(s))
    return f"{int(m.group(1))}화" if m else clean_text(s)

def parse_table_horizontal(table: Tag) -> list[dict]:
    rows = table.find_all("tr")
    if not rows: return []
    header = rows[0].find_all(["th", "td"])
    if not header: return []

    def keyname(x: str) -> str:
        x = clean_text(x).replace(" ", "")
        if "회차" in x or x == "회": return "episode_no"
        if "방영일" in x or "방송일" in x or "방영" in x: return "broadcast_at"
        if "제목" in x or "부제" in x or "소제목" in x: return "title"
        if "줄거리" in x or "개요" in x or "내용" in x: return "description"
        return ""

    idx_map: dict[int, str] = {}
    for i, th in enumerate(header):
        k = keyname(_text(th))
        if k: idx_map[i] = k

    keys = set(idx_map.values())
    if "episode_no" not in keys or not (("title" in keys) or ("broadcast_at" in keys)):
        return []

    out: list[dict] = []
    for tr in rows[1:]:
        cells = tr.find_all(["td", "th"])
        if not cells: continue
        item = {"episode_no":"", "title":"", "broadcast_at":"", "runtime_min":"", "description":""}
        for i, td in enumerate(cells):
            if i not in idx_map: continue
            key = idx_map[i]
            val = _text(td)
            if key == "episode_no":
                val = normalize_episode_no(val)
            if key == "title":
                strong = td.find("strong")
                if strong: val = _text(strong)
            item[key] = val
        # 숫자 회차만
        if not re.search(r"\d+", item["episode_no"] or ""):
            continue
        out.append(item)
    return out

def parse_table_vertical(table: Tag) -> list[dict]:
    rows = table.find_all("tr")
    if not rows: return []
    label = {}
    for tr in rows:
        th = tr.find("th")
        tds = tr.find_all("td")
        if th and tds:
            label[_text(th).replace(" ","")] = _text(tds[-1])
    hit = sum(1 for k in label if any(x in k for x in ("회차","방영일","제목","줄거리")))
    if hit < 2: return []
    ep = normalize_episode_no(label.get("회차",""))
    if not re.search(r"\d+", ep or ""): return []
    return [{
        "episode_no": ep,
        "title": label.get("제목",""),
        "broadcast_at": label.get("방영일",""),
        "runtime_min": "",
        "description": label.get("줄거리",""),
    }]

def parse_table_backup_indexed(table: Tag) -> list[dict]:
    rows = table.find_all("tr")
    if not rows: return []
    try:
        td0 = rows[0].find("td")
        ep_text = _text(td0.find("strong") or td0) if td0 else ""
        ep = normalize_episode_no(ep_text)
        if not re.search(r"\d+", ep or ""): return []
        broadcast_at = title = description = ""
        if len(rows) >= 3:
            tds = rows[2].find_all("td")
            if len(tds) >= 2: broadcast_at = _text(tds[1])
        if len(rows) >= 4:
            tds = rows[3].find_all("td")
            if len(tds) >= 2: title = _text(tds[1].find("strong") or tds[1])
        if len(rows) >= 5:
            tds = rows[4].find_all("td")
            if len(tds) >= 2: description = _text(tds[1])
        return [{
            "episode_no": ep,
            "title": title,
            "broadcast_at": broadcast_at,
            "runtime_min": "",
            "description": description,
        }]
    except Exception:
        return []

def parse_episode_table(tbl: Tag) -> list[dict]:
    out = parse_table_horizontal(tbl)
    if out: return out
    out = parse_table_vertical(tbl)
    if out: return out
    return parse_table_backup_indexed(tbl)

def parse_document(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []
    for tbl in soup.find_all("table"):
        rows.extend(parse_episode_table(tbl))
    # 정렬
    def ep_key(d):
        m = re.search(r"(\d+)", d.get("episode_no","") or "")
        return int(m.group(1)) if m else 10**9
    rows.sort(key=ep_key)
    return rows

# --- crawler ---
def fetch_episodes_for_title(session: requests.Session, title_display: str) -> list[dict]:
    base = norm_title(title_display)
    title_cands = (f"{base} (드라마)", f"{base}(드라마)", base)

    base_url = None
    for cand in title_cands:
        url = f"{BASE}/w/{quote(cand, safe='')}"
        html, status = get_html_with_status(session, url)
        print(f"    - base check: {url} [{status}]")
        if html:
            base_url = url
            break
        time.sleep(SLEEP)
    if not base_url:
        return []

    sub_url = f"{base_url}/" + quote(SUBPAGE_NAME, safe="")
    html, status = get_html_with_status(session, sub_url)
    print(f"    - subpage:    {sub_url} [{status}]")
    if not html:
        return []
    return parse_document(html)

# --- merge & 30-char rule ---
def choose_better(a: str, b: str) -> str:
    """비지 않은 쪽, 길이가 더 긴 쪽 우선"""
    a = a or ""
    b = b or ""
    if not a: return b
    if not b: return a
    return a if len(a) >= len(b) else b

def collapse_episodes(eps: list[dict]) -> list[dict]:
    """같은 episode_no 병합 + 제목 비고 줄거리만 있을 때 30자 규칙 적용"""
    merged: dict[str, dict] = {}
    for e in eps:
        key = e.get("episode_no","")
        if key not in merged:
            merged[key] = e.copy()
        else:
            cur = merged[key]
            cur["title"]        = choose_better(cur.get("title",""),        e.get("title",""))
            cur["broadcast_at"] = choose_better(cur.get("broadcast_at",""), e.get("broadcast_at",""))
            # runtime_min은 빈칸 유지
            cur["description"]  = choose_better(cur.get("description",""),  e.get("description",""))
            merged[key] = cur

    # 30자 규칙 적용
       # 30자 규칙 적용 (title/description 위치 정리)
    out = []
    for ep_no, row in merged.items():
        title = (row.get("title","") or "").strip()
        desc  = (row.get("description","") or "").strip()

        # 1) 제목이 없고 줄거리만 있을 때: 30자 이하면 제목으로 승격, 초과면 줄거리 유지
        if not title and desc:
            if len(desc) <= 30:
                title, desc = desc, ""
        # 2) 제목이 있는데 너무 길면(>30자) 사실상 줄거리로 판단 → 아래로 내림
        elif title and len(title) > 30 and not desc:
            desc, title = title, ""

        out.append({
            "episode_no": ep_no,
            "title": title,
            "broadcast_at": row.get("broadcast_at",""),
            "runtime_min": "",  # 항상 빈칸
            "description": desc,
        })

    # 회차 순서 정렬
    def ep_key(d):
        m = re.search(r"(\d+)", d.get("episode_no","") or "")
        return int(m.group(1)) if m else 10**9
    out.sort(key=ep_key)
    return out

# --- main ---
def main():
    try:
        df = pd.read_csv(IN_CSV, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(IN_CSV, encoding="cp949")

    title_col = next((c for c in ("title","제목","drama_title") if c in df.columns), None)
    if not title_col:
        print("CSV에 'title' 또는 '제목' 또는 'drama_title' 컬럼이 없습니다.")
        return

    titles = [str(x).strip() for x in df[title_col].fillna("") if str(x).strip()]
    out_rows = []

    with requests.Session() as session:
        for i, t in enumerate(titles, 1):
            print(f"[{i}/{len(titles)}] {t}")
            eps = fetch_episodes_for_title(session, t)
            if not eps:
                print("  - (no episode list)")
                continue
            eps = collapse_episodes(eps)  # 중복 병합 + 30자 규칙
            for ep in eps:
                out_rows.append({
                    "drama_title": t,
                    **ep,  # episode_no, title, broadcast_at, runtime_min, description
                })
            time.sleep(SLEEP)

    cols = ["drama_title","episode_no","title","broadcast_at","runtime_min","description"]
    df_out = pd.DataFrame(out_rows, columns=cols) if out_rows else pd.DataFrame(columns=cols)
    atomic_write_csv(df_out, OUT_CSV)
    print(f"[✓] 저장 완료: {OUT_CSV} (총 {len(df_out)}행)")

if __name__ == "__main__":
    main()
