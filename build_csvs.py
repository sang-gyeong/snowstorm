import json, csv, re, pathlib

print('123')

# ---------------- CONFIG ----------------
BASE_DIR = pathlib.Path(r"C:/Users/taesh/snowstorm")  # 수정 필요 없으면 그대로
FILES = {
    "BodyStructure": BASE_DIR / "bs.json",
    "ClinicalFinding": BASE_DIR / "cf.json",
    "Procedure": BASE_DIR / "pr.json",
}
RELS_FILE = BASE_DIR / "rels_core.txt"
OUT_CONCEPTS = BASE_DIR / "concepts.csv"
OUT_RELS     = BASE_DIR / "relationships.csv"

# Relationship typeId -> rel label (Neo4j)
RELTYPE_MAP = {
    "116680003": "IS_A",                 # Is a
    "363698007": "FINDING_SITE",         # Finding site
    "405813007": "PROC_SITE_DIR",        # Procedure site - Direct
    "363589002": "ASSOCIATED_PROCEDURE", # Associated procedure (확인)
    "116676008": "ASSOCIATED_MORPHOLOGY",
    "424226004": "USING_DEVICE",
    "105904001": "OCCURRENCE",
}
# Characteristic typeId readable map (optional)
CHAR_MAP = {
    "900000000000011006": "INFERRED",
    "900000000000010007": "STATED",
    "900000000000227009": "ADDITIONAL",
}

SEM_TAG_RE = re.compile(r"\(([^)]+)\)\s*$")

def semantic_tag_from_fsn(fsn_term: str):
    if not fsn_term:
        return None
    m = SEM_TAG_RE.search(fsn_term)
    return m.group(1).strip() if m else None

def load_concepts(path: pathlib.Path, domain_label: str):
    """
    Load Snowstorm JSON file that may be:
      - {\"items\": [ {...}, ... ] , ...}
      - [ {...}, ... ]    (pure array)
    Return list of concept dicts with normalized fields.
    """
    txt = path.read_text(encoding="utf-8").strip()
    if not txt:
        return []

    data = json.loads(txt)
    if isinstance(data, dict) and "items" in data:
        arr = data["items"]
    elif isinstance(data, list):
        arr = data

    out = []
    for obj in arr:
        cid = str(obj.get("conceptId") or obj.get("id"))
        fsn_term = obj.get("fsn", {}).get("term")
        pt_term  = obj.get("pt", {}).get("term")
        semtag = semantic_tag_from_fsn(fsn_term) or domain_label
        out.append({
            "conceptId": cid,
            "active": "true" if obj.get("active", True) else "false",
            "definitionStatus": obj.get("definitionStatus"),
            "moduleId": obj.get("moduleId"),
            "effectiveTime": obj.get("effectiveTime"),
            "fsn": fsn_term,
            "pt": pt_term,
            "semanticTag": semtag,
            "domainLabel": domain_label
        })
    return out

def build_concepts():
    merged = {}
    for dom, path in FILES.items():
        if not path.exists():
            print(f"[WARN] Missing file: {path}")
            continue
        rows = load_concepts(path, dom)
        for c in rows:
            cid = c["conceptId"]
            if cid in merged:
                # merge domain labels
                labs = set(merged[cid]["domainLabel"].split("|"))
                labs.add(dom)
                merged[cid]["domainLabel"] = "|".join(sorted(labs))
            else:
                merged[cid] = c
    return list(merged.values())

def write_concepts_csv(rows, out_path: pathlib.Path):
    hdr = [
        "conceptId","active","definitionStatus","moduleId","effectiveTime",
        "fsn","pt","semanticTag","domainLabel"
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=hdr)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[OK] wrote {out_path} ({len(rows)} rows)")

def write_relationships_csv(in_path: pathlib.Path, out_path: pathlib.Path):
    hdr = [
        "relId","sourceId","destId","typeId","relType",
        "groupId","charType","moduleId","effectiveTime","modifierId"
    ]
    with in_path.open(encoding="utf-8") as fin, \
         out_path.open("w", newline="", encoding="utf-8") as fout:
        w = csv.DictWriter(fout, fieldnames=hdr)
        w.writeheader()
        count = 0
        for line in fin:
            line = line.strip()
            if not line or line.startswith("id\t"):  # skip header line if present
                continue
            parts = line.split("\t")
            if len(parts) < 10:
                continue
            relId, eff, active, module, source, dest, group, typeId, charTypeId, modifierId = parts[:10]
            if active != "1":  # safety; rels_core.txt should be filtered already
                continue
            relType = RELTYPE_MAP.get(typeId, "ATTRIBUTE")
            charType = CHAR_MAP.get(charTypeId, charTypeId)
            w.writerow({
                "relId": relId,
                "sourceId": source,
                "destId": dest,
                "typeId": typeId,
                "relType": relType,
                "groupId": group,
                "charType": charType,
                "moduleId": module,
                "effectiveTime": eff,
                "modifierId": modifierId
            })
            count += 1
    print(f"[OK] wrote {out_path} ({count} rows)")

def main():
    concepts = build_concepts()
    write_concepts_csv(concepts, OUT_CONCEPTS)
    write_relationships_csv(RELS_FILE, OUT_RELS)

if __name__ == "__main__":
    main()
