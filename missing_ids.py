#!/usr/bin/env python3
"""
snowstorm_fetch.py
──────────────────
ids.txt 에서 읽은 SNOMED CT ID를 Snowstorm로 조회해 concepts.json 파일로 저장
- 존재하지 않는 ID는 not_found.txt 에 별도 기록
- 표준 라이브러리만 사용 (requests 설치 불필요)
"""

import json, pathlib, time, urllib.parse, urllib.request

# ▶ 필요한 부분만 환경에 맞게 바꾸세요
BASE_URL = "http://localhost:8080"   # Snowstorm 기본 경로
BRANCH   = "MAIN"                            # 조회할 브랜치
INPUT    = "missing_ids.txt"                         # ID 목록 파일
OUTPUT   = "concepts.json"
NOTFOUND = "not_found.txt"
CHUNK    = 50                                # 한번에 보낼 최대 ID 수
HEADERS  = {"Accept": "application/json",    # 한국어 FSN이 필요하면 ko 로 변경
            "Accept-Language": "en"}

# ──────────────────────────────────────────────────────────────────────────
ids = [i.strip() for i in pathlib.Path(INPUT).read_text().splitlines() if i.strip()]
good, missing = [], []

def request_slice(id_slice: list[str]) -> list[dict]:
    """id_slice(<=CHUNK) → items 배열 반환, 잘못된 ID 섞여 있으면 400"""
    params = [("conceptIds", cid) for cid in id_slice] + [
        ("form", "inferred"),                 # stated 등으로 교체 가능
        ("includeLeafFlag", "false")
        # 필요하면 ("expand", "all()") 추가 → 설명·관계까지 풀 리턴
    ]
    url = f"{BASE_URL}/{BRANCH}/concepts?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)["items"]       # 페이지 dict → items 만

def fetch_recursive(id_slice: list[str]) -> list[dict]:
    if not id_slice:
        return []
    try:
        return request_slice(id_slice)
    except urllib.error.HTTPError as e:
        if e.code == 400:                     # 한 덩어리에 문제 ID 섞임
            if len(id_slice) == 1:            # 더 쪼갤 수 없으면 불량 ID
                missing.append(id_slice[0])
                return []
            mid = len(id_slice) // 2          # 반으로 쪼개 재시도
            return fetch_recursive(id_slice[:mid]) + fetch_recursive(id_slice[mid:])
        raise

# 메인 루프 ────────────────────────────────────────────────────────────────
for i in range(0, len(ids), CHUNK):
    good.extend(fetch_recursive(ids[i:i+CHUNK]))
    time.sleep(0.05)                          # 서버 과부하 방지

# 결과 저장
with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(good, f, ensure_ascii=False, indent=2)

print(f"✅ {len(good)} concepts saved to {OUTPUT}")
if missing:
    pathlib.Path(NOTFOUND).write_text("\n".join(missing))
    print(f"⚠️  {len(missing)} IDs not found → {NOTFOUND}")
