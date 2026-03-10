import difflib
import re
import unicodedata
from typing import Dict, List

NOISE_WORDS = {"REUNION", "SYT", "PROJET", "AFFAIRE", "MISSION"}


def normalize_project_name(name: str) -> str:
    text = (name or "").upper().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Z0-9\s-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = [t for t in text.replace("-", " ").split(" ") if t and t not in NOISE_WORDS]
    return " ".join(tokens)


def match_project_name(app_project_name: str, boond_project_name: str) -> int:
    a = normalize_project_name(app_project_name)
    b = normalize_project_name(boond_project_name)
    if not a or not b:
        return 0
    if a == b:
        return 100
    if a in b or b in a:
        return 85

    ta = set(a.split())
    tb = set(b.split())
    inter = ta & tb
    if inter:
        token_score = int((len(inter) / max(1, len(ta | tb))) * 100)
    else:
        token_score = 0

    sim = int(difflib.SequenceMatcher(None, a, b).ratio() * 100)
    return max(token_score, sim)


def resolve_best_project_match(app_project_name: str, candidate_names: List[str]) -> Dict[str, object]:
    scored = []
    for name in candidate_names:
        score = match_project_name(app_project_name, name)
        if score > 0:
            scored.append((score, name))

    if not scored:
        return {
            "input_project": app_project_name,
            "matched_project_name": "",
            "confidence": 0,
            "warning": True,
            "warning_message": "Aucun projet BOOND correspondant.",
        }

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_name = scored[0]
    ambiguous = len(scored) > 1 and abs(scored[0][0] - scored[1][0]) <= 5
    low_conf = best_score < 70

    warning = ambiguous or low_conf
    warning_message = ""
    if ambiguous:
        warning_message = "Matching projet ambigu : plusieurs correspondances proches."
    elif low_conf:
        warning_message = "Matching projet avec confiance faible."

    return {
        "input_project": app_project_name,
        "matched_project_name": best_name,
        "confidence": best_score,
        "warning": warning,
        "warning_message": warning_message,
    }
