# =========================================================
# TEMPO • METRONOME • COMPTE-RENDU SYNTHESE (HTML / PRINT-FIRST) — V3.2+
# =========================================================
# ✅ Bleu = sujets traités dans la réunion sélectionnée (Meeting/ID)
# ✅ Rappels = tâches non clôturées ET en retard à la DATE DE SEANCE (pas "aujourd’hui")
# ✅ À suivre = tâches non clôturées NON en retard à la date de séance (inclut réunions précédentes)
# ✅ Rappels + À suivre classés PAR ZONE
# ✅ KPI "Rappels par entreprise" (logo + compteur)
# ✅ Bandeau projet via Projects.csv (image + infos)
# ✅ Images dans TÂCHES/MEMOS/RAPPELS/ÀSUIVRE (détection automatique colonne + parsing robuste)
# ✅ Commentaires tâches si dispo
# ✅ Ajout de mémos épinglés par zone (modal) — dispo aussi en "version imprimable"
# ✅ Plus de "badges" instables : colonne dédiée (UI) + colonne "Type" (PDF)
#
# INSTALL
#   python -m pip install fastapi uvicorn pandas openpyxl
#
# RUN
#   python -m uvicorn app:app --host 0.0.0.0 --port 8090 --reload
# =========================================================

from __future__ import annotations

import base64
import json
import logging
import os
import re
import urllib.parse
import urllib.request
import smtplib
from html import escape
from datetime import date, timedelta
from email.message import EmailMessage
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="TEMPO • CR Synthèse (METRONOME)")
logger = logging.getLogger("tempo_app")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# -------------------------
# PATHS (UNC)
# -------------------------
ENTRIES_PATH = os.getenv(
    "METRONOME_ENTRIES",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Entries (Tasks & Memos).csv",
)
MEETINGS_PATH = os.getenv(
    "METRONOME_MEETINGS",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Meetings.csv",
)
COMPANIES_PATH = os.getenv(
    "METRONOME_COMPANIES",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Companies.csv",
)
PROJECTS_PATH = os.getenv(
    "METRONOME_PROJECTS",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Projects.csv",
)
LOGO_TEMPO_PATH = os.getenv(
    "METRONOME_LOGO",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Content\Logo TEMPO.png",
)
LOGO_RYTHME_PATH = os.getenv(
    "METRONOME_LOGO_RYTHME",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Content\Rythme.png",
)
LOGO_T_MARK_PATH = os.getenv(
    "METRONOME_LOGO_TMARK",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Content\T logo.png",
)
LOGO_QR_PATH = os.getenv(
    "METRONOME_QR",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Content\QR CODE.png",
)
DOCUMENTS_PATH = os.getenv(
    "METRONOME_DOCUMENTS",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Documents.csv",
)
COMMENTS_PATH = os.getenv(
    "METRONOME_COMMENTS",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Comments.csv",
)
USERS_PATH = os.getenv(
    "METRONOME_USERS",
    r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Users.csv",
)

FINANCE_WORKBOOK_PATH = os.getenv(
    "METRONOME_FINANCE_WORKBOOK",
    r"\\192.168.10.100\03 - finances\10 - TABLEAU DE BORD\01 - TABLEAU ACTIVITEE\TABLEAU ACTIVITEE.xlsx",
)
FINANCE_SHEET_NAME = "AFFAIRES 2026"

FINANCE_COLUMN_MAPPING = {
    "A": "client",
    "B": "affaire",
    "C": "tag",
    "D": "numero",
    "E": "delai_reglement_jours",
    "F": "commande_ht",
    "G": "facturation_cumulee_2017",
    "H": "facturation_cumulee_2018",
    "I": "facturation_cumulee_2021",
    "J": "facturation_cumulee_2022",
    "K": "facturation_cumulee_2023",
    "L": "facturation_cumulee_2024",
    "M": "facturation_cumulee_2025",
    "N": "facturation_cumulee_2026",
    "O": "reste_a_facturer",
    "P": "janvier_previsionnel",
    "Q": "janvier_facture",
    "R": "fevrier_previsionnel",
    "S": "fevrier_facture",
    "T": "mars_previsionnel",
    "U": "mars_facture",
    "V": "avril_previsionnel",
    "W": "avril_facture",
    "X": "mai_previsionnel",
    "Y": "mai_facture",
    "Z": "juin_previsionnel",
    "AA": "juin_facture",
    "AB": "juillet_previsionnel",
    "AC": "juillet_facture",
    "AD": "aout_previsionnel",
    "AE": "aout_facture",
    "AF": "septembre_previsionnel",
    "AG": "septembre_facture",
    "AH": "octobre_previsionnel",
    "AI": "octobre_facture",
    "AJ": "novembre_previsionnel",
    "AK": "novembre_facture",
    "AL": "decembre_previsionnel",
    "AM": "decembre_facture",
    "AN": "total_previsionnel",
    "AO": "total_facture",
}
FINANCE_DATA_START_ROW = 14
FINANCE_HEADERS_ROW = 11
FINANCE_SUBHEADERS_ROW = 12

# -------------------------
# COLUMN NAMES (METRONOME EXPORTS)
# -------------------------
# Entries
E_COL_ID = "🔒 Row ID"
E_COL_TITLE = "Title"
E_COL_PROJECT_TITLE = "Project/Title"
E_COL_MEETING_ID = "Meeting/ID"
E_COL_IS_TASK = "Category/Task"
E_COL_CATEGORY = "Category/Name to display"
E_COL_AREAS = "Areas/Names"
E_COL_PACKAGES = "Packages/Names"
E_COL_COMPANY_TASK = "Company/Name for Tasks"
E_COL_OWNER = "Owner for Tasks/Full Name"
E_COL_CREATED = "Declaration Date/Editable"
E_COL_DEADLINE = "Deadline & Status for Tasks/Deadline"
E_COL_STATUS = "Deadline & Status for Tasks/Status Emoji + Text"
E_COL_COMPLETED = "Completed/true/false"
E_COL_COMPLETED_END = "Completed/Declared End"
E_COL_IMAGES_URLS = "Images/Autom input as text (dev)"  # nominal (may vary in exports)

E_COL_TASK_COMMENT_TEXT = "Comment for Tasks/Text"
E_COL_TASK_COMMENT_FULL = "Comment for Tasks/Full text to display if existing (dev)"
E_COL_TASK_COMMENT_AUTHOR = "Comment for Tasks/Editor Name (dev)"
E_COL_TASK_COMMENT_DATE = "Comment for Tasks/Date"

# Meetings
M_COL_ID = "🔒 Row ID"
M_COL_DATE = "Date/Editable"
M_COL_DATE_DISPLAY = "Date/To display (dev)"
M_COL_PROJECT_TITLE = "Project/Title (dev)"
M_COL_ATT_IDS = "Companies/Attending IDs"
M_COL_MISS_IDS = "Companies/Missing IDs"
M_COL_MISS_CALC_IDS = "Companies/Missing Calculated IDs (dev)"
M_COL_TASKS_COUNT = "Entries/Tasks Count"
M_COL_MEMOS_COUNT = "Entries/Memos Count"

# Companies
C_COL_ID = "🔒 Row ID"
C_COL_NAME = "Name"
C_COL_LOGO = "Logo"

# Projects
P_COL_TITLE = "Title"
P_COL_DESC = "Description"
P_COL_IMAGE = "Image"
P_COL_START_SENT = "Timeline/Start Sentence"
P_COL_END_SENT = "Timeline/End Sentence"
P_COL_ARCHIVED = "Archived/Text"

# -------------------------
# CACHE
# -------------------------
_cache = {
    "entries": (None, None),
    "meetings": (None, None),
    "companies": (None, None),
    "projects": (None, None),
    "documents": (None, None),
    "users": (None, None),
    "finance": (None, None),
}


def _mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except Exception:
        return -1.0


class MissingDataError(RuntimeError):
    def __init__(self, label: str, path: str, env_var: str):
        super().__init__(f"Fichier manquant pour {label}: {path} (env: {env_var})")
        self.label = label
        self.path = path
        self.env_var = env_var


def _load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig")


def _require_csv(path: str, label: str, env_var: str) -> None:
    if not os.path.exists(path):
        raise MissingDataError(label=label, path=path, env_var=env_var)


def get_entries() -> pd.DataFrame:
    m = _mtime(ENTRIES_PATH)
    old_m, df = _cache["entries"]
    if df is None or m != old_m:
        _require_csv(ENTRIES_PATH, "Entries", "METRONOME_ENTRIES")
        df = _load_csv(ENTRIES_PATH)
        _cache["entries"] = (m, df)
    return df


def get_meetings() -> pd.DataFrame:
    m = _mtime(MEETINGS_PATH)
    old_m, df = _cache["meetings"]
    if df is None or m != old_m:
        _require_csv(MEETINGS_PATH, "Meetings", "METRONOME_MEETINGS")
        df = _load_csv(MEETINGS_PATH)
        _cache["meetings"] = (m, df)
    return df


def get_companies() -> pd.DataFrame:
    m = _mtime(COMPANIES_PATH)
    old_m, df = _cache["companies"]
    if df is None or m != old_m:
        _require_csv(COMPANIES_PATH, "Companies", "METRONOME_COMPANIES")
        df = _load_csv(COMPANIES_PATH)
        _cache["companies"] = (m, df)
    return df


def get_projects() -> pd.DataFrame:
    m = _mtime(PROJECTS_PATH)
    old_m, df = _cache["projects"]
    if df is None or m != old_m:
        _require_csv(PROJECTS_PATH, "Projects", "METRONOME_PROJECTS")
        df = _load_csv(PROJECTS_PATH)
        _cache["projects"] = (m, df)
    return df


def get_documents() -> pd.DataFrame:
    m = _mtime(DOCUMENTS_PATH)
    old_m, df = _cache["documents"]
    if df is None or m != old_m:
        _require_csv(DOCUMENTS_PATH, "Documents", "METRONOME_DOCUMENTS")
        df = _load_csv(DOCUMENTS_PATH)
        _cache["documents"] = (m, df)
    return df


def get_users() -> pd.DataFrame:
    m = _mtime(USERS_PATH)
    old_m, df = _cache["users"]
    if df is None or m != old_m:
        if not os.path.exists(USERS_PATH):
            df = pd.DataFrame()
            _cache["users"] = (m, df)
            return df
        df = _load_csv(USERS_PATH)
        _cache["users"] = (m, df)
    return df


def _normalize_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_number(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return value
    raw = str(value).strip().replace(" ", "")
    if not raw:
        return None
    raw = raw.replace(",", ".")
    try:
        number = float(raw)
        return int(number) if number.is_integer() else number
    except ValueError:
        return _normalize_text(value)


def _row_to_finance_payload(row: dict, is_sub_line: bool, row_kind: str) -> dict:
    payload = {
        "is_sub_line": is_sub_line,
        "row_kind": row_kind,
    }
    for key, value in row.items():
        if key in {"client", "affaire", "tag", "numero"}:
            payload[key] = _normalize_text(value)
        else:
            payload[key] = _normalize_number(value)
    return payload


def _is_total_line(row: dict) -> bool:
    joined = " ".join(
        _normalize_text(row.get(field)).lower()
        for field in ["client", "affaire", "tag", "numero"]
    )
    return bool(joined) and any(token in joined for token in ["total", "totaux", "sous-total"])


def _is_detail_line(row: dict) -> bool:
    if _is_total_line(row):
        return True
    affaire = _normalize_text(row.get("affaire"))
    numero = _normalize_text(row.get("numero"))
    metrics = [
        row.get("commande_ht"),
        row.get("reste_a_facturer"),
        row.get("total_previsionnel"),
        row.get("total_facture"),
    ]
    has_metrics = any(_normalize_number(m) is not None for m in metrics)
    return bool(affaire or numero or has_metrics)


def _parse_finance_workbook() -> dict:
    _require_csv(FINANCE_WORKBOOK_PATH, "Finance workbook", "METRONOME_FINANCE_WORKBOOK")
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise RuntimeError("Le module openpyxl est requis pour lire le fichier FINANCE (.xlsx).") from exc
    wb = load_workbook(FINANCE_WORKBOOK_PATH, data_only=True, read_only=True)
    if FINANCE_SHEET_NAME not in wb.sheetnames:
        raise HTTPException(
            status_code=500,
            detail=f"Onglet introuvable: {FINANCE_SHEET_NAME}",
        )

    ws = wb[FINANCE_SHEET_NAME]
    main_headers = {
        col: _normalize_text(ws[f"{col}{FINANCE_HEADERS_ROW}"].value)
        for col in FINANCE_COLUMN_MAPPING
    }
    sub_headers = {
        col: _normalize_text(ws[f"{col}{FINANCE_SUBHEADERS_ROW}"].value)
        for col in FINANCE_COLUMN_MAPPING
    }

    logger.info(
        "[FINANCE] Parsing %s!%s from row %s",
        FINANCE_WORKBOOK_PATH,
        FINANCE_SHEET_NAME,
        FINANCE_DATA_START_ROW,
    )
    logger.debug("[FINANCE] Main headers row %s: %s", FINANCE_HEADERS_ROW, main_headers)
    logger.debug("[FINANCE] Sub headers row %s: %s", FINANCE_SUBHEADERS_ROW, sub_headers)

    affaires: dict[str, dict] = {}
    current_affaire_key = None
    parsed_rows = 0
    skipped_rows = 0

    for row_idx in range(FINANCE_DATA_START_ROW, ws.max_row + 1):
        raw = {
            field: ws[f"{col}{row_idx}"].value
            for col, field in FINANCE_COLUMN_MAPPING.items()
        }
        if all(v is None or (isinstance(v, str) and not v.strip()) for v in raw.values()):
            continue

        if not _is_detail_line(raw):
            skipped_rows += 1
            logger.debug("[FINANCE] Skip row=%s raw=%s", row_idx, raw)
            continue

        is_sub_line = not bool(_normalize_text(raw.get("affaire"))) and current_affaire_key is not None
        row_kind = "sub_line" if is_sub_line else "parent"
        if _is_total_line(raw):
            row_kind = "total"

        normalized = _row_to_finance_payload(raw, is_sub_line=is_sub_line, row_kind=row_kind)
        if not is_sub_line:
            affaire_key = _normalize_text(raw.get("numero")) or _normalize_text(raw.get("affaire"))
            if not affaire_key:
                skipped_rows += 1
                logger.debug("[FINANCE] Parent row without key row=%s raw=%s", row_idx, raw)
                continue
            current_affaire_key = affaire_key
            if affaire_key not in affaires:
                affaires[affaire_key] = {
                    "affaire_id": affaire_key,
                    "client": normalized.get("client", ""),
                    "affaire": normalized.get("affaire", ""),
                    "tag": normalized.get("tag", ""),
                    "numero": normalized.get("numero", ""),
                    "parent": normalized,
                    "totals": [],
                    "missions": [],
                }
            else:
                affaires[affaire_key]["parent"] = normalized
            parsed_rows += 1
            logger.debug("[FINANCE] Parent row=%s affaire=%s", row_idx, affaire_key)
            continue

        if current_affaire_key is None:
            skipped_rows += 1
            logger.debug("[FINANCE] Sub-line without parent row=%s raw=%s", row_idx, raw)
            continue

        if row_kind == "total":
            affaires[current_affaire_key]["totals"].append(normalized)
        else:
            affaires[current_affaire_key]["missions"].append(normalized)
        parsed_rows += 1
        logger.debug("[FINANCE] %s row=%s affaire=%s", row_kind, row_idx, current_affaire_key)

    affair_list = []
    for key, item in affaires.items():
        affair_list.append(
            {
                "affaire_id": key,
                "label": f"{item.get('numero') or key} - {item.get('affaire') or ''}".strip(" -"),
                "client": item.get("client", ""),
                "affaire": item.get("affaire", ""),
                "tag": item.get("tag", ""),
                "numero": item.get("numero", ""),
            }
        )

    affair_list.sort(key=lambda x: (x.get("client", ""), x.get("label", "")))
    logger.info(
        "[FINANCE] Parsing done: %s affaires, %s rows parsed, %s rows skipped",
        len(affaires),
        parsed_rows,
        skipped_rows,
    )
    wb.close()

    return {
        "metadata": {
            "source": FINANCE_WORKBOOK_PATH,
            "sheet": FINANCE_SHEET_NAME,
            "headers_row": FINANCE_HEADERS_ROW,
            "subheaders_row": FINANCE_SUBHEADERS_ROW,
            "data_start_row": FINANCE_DATA_START_ROW,
            "column_mapping": FINANCE_COLUMN_MAPPING,
            "main_headers": main_headers,
            "sub_headers": sub_headers,
            "parsed_rows": parsed_rows,
            "skipped_rows": skipped_rows,
        },
        "affaires": affaires,
        "affaire_list": affair_list,
    }


def get_finance_data() -> dict:
    m = _mtime(FINANCE_WORKBOOK_PATH)
    old_m, data = _cache["finance"]
    if data is None or m != old_m:
        data = _parse_finance_workbook()
        _cache["finance"] = (m, data)
    return data


# -------------------------
# UTILITIES
# -------------------------
def _escape(s) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#039;")
    )


def _series(df: pd.DataFrame, col: str, default) -> pd.Series:
    if col in df.columns:
        data = df[col]
        if isinstance(data, pd.DataFrame):
            return data.iloc[:, 0]
        return data
    return pd.Series([default] * len(df), index=df.index)


def _filter_entries_by_created_range(
    df: pd.DataFrame, start_date: Optional[date], end_date: Optional[date]
) -> pd.DataFrame:
    if df.empty or (start_date is None and end_date is None):
        return df
    created = _series(df, E_COL_CREATED, None).apply(_parse_date_any)
    mask = pd.Series(True, index=df.index)
    if start_date is not None:
        mask &= created.notna() & (created >= start_date)
    if end_date is not None:
        mask &= created.notna() & (created <= end_date)
    return df.loc[mask].copy()


def _safe_int(v) -> int:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _parse_ids(v) -> List[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return []
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def _split_multi_labels(v) -> List[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return []
    raw = str(v).strip()
    if not raw or raw.lower() == "nan":
        return []
    return [p.strip() for p in re.split(r"[,;/]+", raw) if p and p.strip()]


def _bool_true(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return False
    s = str(v).strip().lower()
    return s in {"true", "1", "yes", "y", "vrai"}


def _parse_date_any(v) -> Optional[date]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    m = re.match(r"^(\d{2})/(\d{2})/(\d{2,4})", s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


def _fmt_date(d: Optional[date]) -> str:
    return d.strftime("%d/%m/%y") if d else ""


def _norm_name(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def _trigram(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    raw = re.sub(r"[^A-Za-z0-9]", "", str(s).strip())
    if not raw:
        return ""
    return raw[:3].upper()


def _lot_abbrev(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    text = str(s).strip()
    if not text:
        return ""
    rules = [
        ("Électricité", "ELE"),
        ("Courants forts", "CFO"),
        ("Courants faibles", "CFA"),
        ("Plomberie", "PLB"),
        ("CVC", "CVC"),
        ("Structure", "STR"),
        ("Gros Oeuvre", "GOE"),
        ("Synthèse", "SYN"),
        ("Entreprise Générale", "EG"),
        ("Sprinklage", "SPK"),
    ]
    text_lower = text.lower()
    for label, abbrev in rules:
        if label.lower() in text_lower:
            return abbrev
    return _trigram(text)


def _lot_abbrev_list(value: str) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    parts = [p.strip() for p in re.split(r"[,;/]+", raw) if p.strip()]
    if not parts:
        return ""
    mapped = [_lot_abbrev(p) for p in parts]
    mapped = [m for m in mapped if m]
    if len(mapped) > 1:
        return "PL"
    return " / ".join(mapped)


def _concerne_trigram(value: str) -> str:
    trigram = _trigram(value)
    return trigram or "PE"


def _has_multiple_companies(value: str) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    raw = str(value).strip()
    if not raw:
        return False
    parts = [p.strip() for p in re.split(r"[,;/]+", raw) if p.strip()]
    return len(parts) > 1


def _split_words(value: str) -> set[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return set()
    raw = str(value).strip()
    if not raw:
        return set()
    return {part for part in re.split(r"[^\w]+", raw) if part}


def _logo_data_url(path: str) -> str:
    if not path:
        return ""
    normalized = os.path.normpath(path)
    if not os.path.exists(normalized):
        return ""
    try:
        with open(normalized, "rb") as f:
            data = base64.b64encode(f.read()).decode("utf-8")
        ext = os.path.splitext(normalized)[1].lower()
        if ext in {".jpg", ".jpeg"}:
            mime = "image/jpeg"
        elif ext == ".svg":
            mime = "image/svg+xml"
        else:
            mime = "image/png"
        return f"data:{mime};base64,{data}"
    except Exception:
        return ""


def _meeting_sequence_for_project(
    meetings_df: pd.DataFrame, meeting_id: str
) -> Tuple[int, int]:
    if meetings_df.empty:
        return 1, 1
    df = meetings_df.copy()
    df["__mid__"] = _series(df, M_COL_ID, "").fillna("").astype(str).str.strip()
    df["__mdate__"] = _series(df, M_COL_DATE, None).apply(_parse_date_any)
    df = df.loc[df["__mid__"] != ""].copy()
    if df.empty:
        return 1, 1
    df = df.sort_values(by=["__mdate__", "__mid__"], ascending=[True, True])
    ids = df["__mid__"].tolist()
    total = len(ids)
    if str(meeting_id) in ids:
        index = ids.index(str(meeting_id)) + 1
    else:
        index = total
    index = max(1, index)
    total = max(1, total)
    return index, total


# -------------------------
# IMAGES (robust)
# -------------------------
def detect_images_column(df: pd.DataFrame) -> Optional[str]:
    """Return likely image URL column name."""
    if df is None or df.empty:
        return None
    if E_COL_IMAGES_URLS in df.columns:
        return E_COL_IMAGES_URLS
    candidates = [c for c in df.columns if "images" in str(c).lower()]
    if not candidates:
        return None
    candidates.sort(key=lambda c: (0 if "autom" in str(c).lower() else 1, len(str(c))))
    return candidates[0]


def detect_memo_images_column(df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty:
        return None
    memo_candidates = [
        c
        for c in df.columns
        if "image" in str(c).lower() and "memo" in str(c).lower()
    ]
    if memo_candidates:
        memo_candidates.sort(key=lambda c: len(str(c)))
        return memo_candidates[0]
    return detect_images_column(df)


def parse_image_urls_any(v) -> List[str]:
    """Parse robust URLs (http/https) from a cell."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return []
    s = str(v)
    if not s.strip() or s.strip().lower() == "nan":
        return []
    urls = re.findall(r"https?://[^\s,\]\)\"\'<>]+", s)
    out, seen = [], set()
    for u in urls:
        u = u.strip()
        if u and u not in seen:
            out.append(u)
            seen.add(u)
    return out


def _format_entry_text_html(v) -> str:
    """Normalize text for tasks/memos and preserve bullet-like line breaks in HTML."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v)
    if not s.strip() or s.strip().lower() == "nan":
        return ""
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n[ \t]+", "\n", s)
    s = re.sub(r"(?<!\n)\s*(•|●|◦|▪|‣|\*)\s+", r"\n\1 ", s)
    s = re.sub(r"(?<!\n)(?<!\w)-\s+(?=\S)", r"\n- ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return _escape(s.strip()).replace("\n", "<br>")


def render_images_gallery(urls: List[str], print_mode: bool) -> str:
    if not urls:
        return ""
    max_imgs = 3 if print_mode else 10
    thumbs = []
    for u in urls[:max_imgs]:
        uu = _escape(u)
        thumbs.append(
            f"""
          <a class="imgThumb" href="{uu}" target="_blank" rel="noopener">
            <img src="{uu}" loading="lazy" alt="" referrerpolicy="no-referrer" />
            <span class="imgGrip" title="Redimensionner"></span>
          </a>
        """
        )
    return f"""<div class="imgRow">{''.join(thumbs)}</div>"""


# -------------------------
# COMMENTS (TASKS)
# -------------------------
def render_task_comment(r) -> str:
    txt = r.get(E_COL_TASK_COMMENT_FULL)
    if txt is None or (isinstance(txt, float) and pd.isna(txt)) or str(txt).strip() == "":
        txt = r.get(E_COL_TASK_COMMENT_TEXT)
    if txt is None or (isinstance(txt, float) and pd.isna(txt)) or str(txt).strip() == "":
        return ""
    author = _escape(r.get(E_COL_TASK_COMMENT_AUTHOR, ""))
    d = _fmt_date(_parse_date_any(r.get(E_COL_TASK_COMMENT_DATE)))
    body = _format_entry_text_html(txt)
    meta = " • ".join([x for x in [author, d] if x])
    return f"""
      <div class="topicComment">
        <div class="metaLabel">Commentaire</div>
        <div class="metaVal">{meta or "—"}</div>
        <div style="margin-top:6px">{body}</div>
      </div>
    """


def render_entry_comment(r) -> str:
    txt = r.get(E_COL_TASK_COMMENT_FULL)
    if txt is None or (isinstance(txt, float) and pd.isna(txt)) or str(txt).strip() == "":
        txt = r.get(E_COL_TASK_COMMENT_TEXT)
    if txt is None or (isinstance(txt, float) and pd.isna(txt)) or str(txt).strip() == "":
        return ""
    author = _escape(r.get(E_COL_TASK_COMMENT_AUTHOR, ""))
    d = _fmt_date(_parse_date_any(r.get(E_COL_TASK_COMMENT_DATE)))
    company = _escape(r.get(E_COL_COMPANY_TASK, ""))
    body = _format_entry_text_html(txt)
    meta = " • ".join([x for x in [author, company, d] if x])
    return f"""
      <div class="entryComment">
        <div class="metaVal">{meta or "—"}</div>
        <div style="margin-top:6px">{body}</div>
      </div>
    """


# -------------------------
# COMPANIES
# -------------------------
def companies_map_by_id() -> Dict[str, Dict[str, str]]:
    c = get_companies()
    mp = {}
    for _, r in c.iterrows():
        cid = str(r.get(C_COL_ID, "")).strip()
        if not cid:
            continue
        mp[cid] = {
            "name": str(r.get(C_COL_NAME, "")).strip(),
            "logo": str(r.get(C_COL_LOGO, "")).strip(),
        }
    return mp


def companies_logo_by_name() -> Dict[str, str]:
    c = get_companies()
    out = {}
    for _, r in c.iterrows():
        name = str(r.get(C_COL_NAME, "")).strip()
        logo = str(r.get(C_COL_LOGO, "")).strip()
        if name:
            out[_norm_name(name)] = logo
    return out


def companies_email_by_name() -> Dict[str, List[str]]:
    c = get_companies()
    email_cols = [col for col in c.columns if "mail" in str(col).lower() or "email" in str(col).lower()]
    out: Dict[str, List[str]] = {}
    for _, r in c.iterrows():
        name = str(r.get(C_COL_NAME, "")).strip()
        if not name:
            continue
        emails: List[str] = []
        for col in email_cols:
            raw = str(r.get(col, "") or "").strip()
            if not raw or raw.lower() == "nan":
                continue
            for part in re.split(r"[;,\s]+", raw):
                v = part.strip()
                if v and "@" in v:
                    emails.append(v)
        uniq = sorted({e for e in emails})
        out[_norm_name(name)] = uniq
    return out


def _extract_emails(raw: str) -> List[str]:
    if not raw:
        return []
    found = re.findall(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", str(raw), flags=re.I)
    return sorted({m.strip() for m in found if m and "@" in m})


def _contacts_by_company_name() -> Dict[str, List[Dict[str, str]]]:
    c = get_companies()
    out: Dict[str, List[Dict[str, str]]] = {}
    for _, r in c.iterrows():
        name = str(r.get(C_COL_NAME, "") or "").strip()
        if not name:
            continue
        contacts: List[Dict[str, str]] = []
        for col in c.columns:
            raw = str(r.get(col, "") or "").strip()
            if not raw or raw.lower() == "nan":
                continue
            # cas standard: "Nom <mail@x>"
            for m in re.finditer(r"([^<>{}\n\r]+?)\s*<\s*([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})\s*>", raw, flags=re.I):
                nm = re.sub(r"\s+", " ", str(m.group(1) or "").strip(" ;,:-\t"))
                em = str(m.group(2) or "").strip().lower()
                if em:
                    contacts.append({"name": nm, "email": em})
            # cas alternatifs: "Nom : mail@x" / "Nom - mail@x"
            for m in re.finditer(r"([^;\n\r,]+?)\s*[:\-]\s*([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", raw, flags=re.I):
                nm = re.sub(r"\s+", " ", str(m.group(1) or "").strip(" ;,:-\t"))
                em = str(m.group(2) or "").strip().lower()
                if em:
                    contacts.append({"name": nm, "email": em})
            # fallback: email seul
            for em in _extract_emails(raw):
                contacts.append({"name": "", "email": em.lower()})

        dedup: Dict[str, Dict[str, str]] = {}
        for ct in contacts:
            em = str(ct.get("email", "") or "").strip().lower()
            if not em:
                continue
            nm = str(ct.get("name", "") or "").strip()
            old = dedup.get(em, {"name": "", "email": em})
            if nm and not old.get("name"):
                old["name"] = nm
            dedup[em] = old
        out[_norm_name(name)] = sorted(dedup.values(), key=lambda x: (str(x.get("name", "")).lower(), str(x.get("email", "")).lower()))
    return out


def _project_deployed_people_by_company(project_title: str) -> Dict[str, List[str]]:
    e = get_entries().copy()
    if project_title:
        e = e.loc[_series(e, E_COL_PROJECT_TITLE, "").fillna("").astype(str).str.strip() == project_title].copy()
    if e.empty:
        return {}

    e["__company__"] = _series(e, E_COL_COMPANY_TASK, "").fillna("").astype(str).str.strip()
    e["__owner__"] = _series(e, E_COL_OWNER, "").fillna("").astype(str).str.strip()
    out: Dict[str, List[str]] = {}
    for comp, gg in e.groupby("__company__", dropna=False):
        comp_name = str(comp or "").strip()
        if not comp_name:
            continue
        owners: List[str] = []
        for raw in gg["__owner__"].tolist():
            for part in re.split(r"[;,/\|]+", str(raw or "")):
                val = re.sub(r"\s+", " ", str(part or "").strip())
                if val and val.lower() != "nan":
                    owners.append(val)
        uniq = sorted({x for x in owners if x})
        if uniq:
            out[_norm_name(comp_name)] = uniq
    return out

def _users_deployed_contacts_by_company(project_title: str) -> Dict[str, List[Dict[str, str]]]:
    u = get_users().copy()
    if u.empty:
        return {}

    cols = list(u.columns)
    email_cols = [c for c in cols if ("mail" in str(c).lower() or "email" in str(c).lower())]
    name_cols = [c for c in cols if ("name" in str(c).lower() or "nom" in str(c).lower())]
    company_cols = [c for c in cols if ("company" in str(c).lower() or "entreprise" in str(c).lower())]
    project_cols = [c for c in cols if ("project" in str(c).lower() or "projet" in str(c).lower())]

    if project_title and project_cols:
        pnorm = _norm_name(project_title)
        def _on_project(row) -> bool:
            for col in project_cols:
                raw = str(row.get(col, "") or "").strip()
                for part in _split_multi_labels(raw):
                    if _norm_name(part) == pnorm:
                        return True
                if raw and pnorm and pnorm in _norm_name(raw):
                    return True
            return False
        u = u.loc[u.apply(_on_project, axis=1)].copy()

    out: Dict[str, List[Dict[str, str]]] = {}
    if u.empty:
        return out

    for _, r in u.iterrows():
        company_vals: List[str] = []
        for col in company_cols:
            raw = str(r.get(col, "") or "").strip()
            if not raw or raw.lower() == "nan":
                continue
            parts = _split_multi_labels(raw) or [raw]
            company_vals.extend([x for x in parts if str(x).strip()])
        if not company_vals:
            continue

        person_name = ""
        for col in name_cols:
            raw = str(r.get(col, "") or "").strip()
            if raw and raw.lower() != "nan":
                person_name = raw
                break

        emails: List[str] = []
        for col in email_cols:
            emails.extend(_extract_emails(str(r.get(col, "") or "")))
        emails = sorted({e.lower() for e in emails if e})
        if not emails:
            continue

        for company in company_vals:
            key = _norm_name(str(company))
            out.setdefault(key, [])
            for em in emails:
                out[key].append({"name": person_name, "email": em})

    for k, vals in list(out.items()):
        dedup: Dict[str, Dict[str, str]] = {}
        for v in vals:
            em = str(v.get("email", "") or "").strip().lower()
            if not em:
                continue
            nm = str(v.get("name", "") or "").strip()
            old = dedup.get(em, {"name": "", "email": em})
            if nm and not old.get("name"):
                old["name"] = nm
            dedup[em] = old
        out[k] = sorted(dedup.values(), key=lambda x: (str(x.get("name", "")).lower(), str(x.get("email", "")).lower()))
    return out



def _short_text(val: str, limit: int = 160) -> str:
    txt = re.sub(r"\s+", " ", str(val or "").strip())
    if len(txt) <= limit:
        return txt
    return txt[: max(0, limit - 1)].rstrip() + "…"


def _fmt_mail_date(d: Optional[date]) -> str:
    return d.strftime("%d/%m/%Y") if isinstance(d, date) else "/"


def _split_companies_value(raw: str) -> List[str]:
    vals = _split_multi_labels(str(raw or ""))
    if vals:
        return [v.strip() for v in vals if str(v).strip()]
    out: List[str] = []
    for part in re.split(r"[;,/|]+", str(raw or "")):
        v = str(part or "").strip()
        if v:
            out.append(v)
    return out


def _companies_concerned_for_row(r: pd.Series, concern_cols: List[str]) -> List[str]:
    out: List[str] = []
    for col in concern_cols:
        raw = str(r.get(col, "") or "").strip()
        if not raw or raw.lower() == "nan":
            continue
        out.extend(_split_companies_value(raw))
    uniq = []
    seen = set()
    for c in out:
        n = _norm_name(c)
        if n and n not in seen:
            seen.add(n)
            uniq.append(c)
    return uniq


def _clean_area_name(area: str) -> str:
    txt = re.sub(r"\s+", " ", str(area or "").strip())
    if not txt:
        return "Généralité"
    return txt or "Généralité"


def _target_companies_phrase(selected_count: int) -> str:
    return "l'entreprise suivante" if int(selected_count or 0) == 1 else "les entreprises suivantes"


def build_company_email_html(
    meeting: dict,
    company: dict,
    items: list[dict],
    meeting_date: date,
    app_url: str = "https://app.atelier-tempo.fr"
) -> tuple[str, str]:
    """
    returns (subject, html)
    subject exemple: "CR Synthèse - {PROJECT_NAME} - Réunion du {DATE}"
    """
    project_name = str(meeting.get("project") or "").strip() or "Projet"
    company_name = str(company.get("name") or "").strip() or "Entreprise"
    meeting_txt = _fmt_mail_date(meeting_date)
    ref_txt = _fmt_mail_date(date.today())
    subject = f"CR Synthèse - {project_name} - Réunion du {meeting_txt}"

    def _rank(it: dict) -> int:
        tp = str(it.get("type") or "").lower()
        if tp == "reminder":
            return 0
        if tp == "open":
            return 1
        if tp == "memo":
            return 2
        if tp == "done":
            return 3
        return 9

    def _safe_text(value: str) -> str:
        return str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    def _cell_text(value: str) -> str:
        return escape(_safe_text(value)).replace("\n", "<br/>")
    def td(val: str, center: bool = False, raw: bool = False, nowrap: bool = False) -> str:
        align = "center" if center else "left"
        body = str(val or "") if raw else _cell_text(val)
        if center and not str(body).strip():
            body = "&nbsp;"
        extra = "white-space:nowrap;" if nowrap else ""
        return f'<td style="border:1px solid #999;padding:6px;vertical-align:top;text-align:{align};{extra}">{body}</td>'

    def tr(cells: list[str], raw_indices: Optional[set[int]] = None) -> str:
        raw_indices = raw_indices or set()
        parts = []
        for idx, c in enumerate(cells):
            # nowrap uniquement sur colonnes date/fait-le, pas sur Concerne
            use_nowrap = idx in {1, 2, 3}
            parts.append(td(c, center=(idx > 0), raw=(idx in raw_indices), nowrap=use_nowrap))
        return "<tr>" + "".join(parts) + "</tr>"

    area_map: Dict[str, List[dict]] = {}
    reminders_by_company: Dict[str, int] = {}
    for it in items:
        area = _clean_area_name(str(it.get("area") or "Généralité"))
        area_map.setdefault(area, []).append(it)
        if str(it.get("type") or "") == "reminder":
            for co in (it.get("target_companies") or it.get("concerne") or []):
                name = str(co or "").strip()
                if not name:
                    continue
                reminders_by_company[name] = int(reminders_by_company.get(name, 0)) + 1

    html_parts: List[str] = []
    html_parts.append('<html><body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#111;line-height:1.4;">')
    html_parts.append('<p>Bonjour,</p>')
    selected_company_count = int(meeting.get("selected_company_count") or 0)
    target_phrase = _target_companies_phrase(selected_company_count)
    html_parts.append(f"<p>Veuillez trouver ci-après la liste des sujets ouverts déployés à la date du {escape(ref_txt)} sur l'application METRONOME pour {target_phrase}:</p>")
    rappels_lines = [
        f"<b>{_cell_text(co)}</b> : {_cell_text(str(cnt))} {'rappel' if int(cnt) == 1 else 'rappels'} en cours"
        for co, cnt in sorted(reminders_by_company.items(), key=lambda kv: _norm_name(kv[0]))
        if int(cnt) >= 1
    ]
    if not rappels_lines:
        rappels_lines = ["Aucun rappel en cours"]
    html_parts.append('<p>' + '<br/>'.join(rappels_lines) + '</p>')

    for area in sorted(area_map.keys(), key=lambda x: _norm_name(x)):
        rows = sorted(
            area_map[area],
            key=lambda it: (
                _rank(it),
                _fmt_mail_date(it.get("due_date")),
                _fmt_mail_date(it.get("created_date")),
                _norm_name(str(it.get("subject") or "")),
            ),
        )
        if not rows:
            continue

        html_parts.append(f'<p><b><u>{escape(str(area))}</u></b></p>')

        header = (
            '<thead><tr style="background:#efefef;">'
            '<th style="border:1px solid #999;padding:6px;text-align:left;">Sujet</th>'
            '<th style="border:1px solid #999;padding:6px;text-align:center;">Écrit le</th>'
            '<th style="border:1px solid #999;padding:6px;text-align:center;">Pour le</th>'
            '<th style="border:1px solid #999;padding:6px;text-align:center;">Fait le</th>'
            '<th style="border:1px solid #999;padding:6px;text-align:center;">Concerne</th>'
            '</tr></thead>'
        )

        rows_html: List[str] = []
        for it in rows:
            sujet = str(it.get("subject") or "(sans titre)")
            ecrit_le = _fmt_mail_date(it.get("created_date"))
            pour_le = "PM" if str(it.get("type") or "") == "memo" else _fmt_mail_date(it.get("due_date"))
            is_reminder = str(it.get("type") or "") == "reminder"
            if is_reminder:
                rl = int(it.get("reminder_level") or 1)
                fait_le = f'<span style="color:#b91c1c;font-weight:700">RAPPEL {rl}</span>'
                raw_cols = {3}
            else:
                fait_le = str(it.get("done_label") or "").strip() or ("/" if str(it.get("type") or "") == "memo" else "")
                raw_cols = set()
            concerne = ", ".join([str(x).strip() for x in (it.get("concerne") or []) if str(x).strip()]) or company_name
            row_html = tr([sujet, ecrit_le, pour_le, fait_le, concerne], raw_indices=raw_cols)
            rows_html.append(row_html)

        colgroup = (
            '<col style="width:64%;" />'
            '<col style="width:9%;" />'
            '<col style="width:9%;" />'
            '<col style="width:9%;" />'
            '<col style="width:9%;" />'
        )
        table_html = '<table style="width:100%;table-layout:fixed;border-collapse:collapse;border:1px solid #999;">' + colgroup + header + '<tbody>' + ''.join(rows_html) + '</tbody></table>'
        html_parts.append(table_html)
        html_parts.append('<p>&nbsp;</p>')

    footer_url = str(app_url or "https://app.atelier-tempo.fr").strip()
    footer_label = footer_url.replace("https://", "").replace("http://", "")
    html_parts.append(
        "<p><b>Téléchargez gratuitement l'application de gestion de projet METRONOME.</b><br/>"
        "Celle-ci vous permettra de retrouver l'intégralité des réunions de synthèse, comptes rendu, planning et suivi des tâches depuis votre smartphone ou votre ordinateur.<br/>"
        f'Pour un suivi optimal, nous vous invitons à mettre à jour ou commenter vos tâches ouvertes/mémos sur <a href="{escape(footer_url)}" target="_blank" rel="noopener">{escape(footer_label)}</a></p>'
    )
    html_parts.append('<p>Vous en souhaitant bonne réception,</p>')
    html_parts.append('</body></html>')

    html = ''.join(html_parts)
    return subject, html


# -------------------------
# PROJECT INFO
# -------------------------
def project_info_by_title(project_title: str) -> Dict[str, str]:
    p = get_projects().copy()
    p[P_COL_TITLE] = p[P_COL_TITLE].fillna("").astype(str).str.strip()
    row = p.loc[p[P_COL_TITLE] == project_title]
    if row.empty:
        return {"title": project_title, "desc": "", "image": "", "start": "", "end": "", "status": ""}
    r = row.iloc[0]
    return {
        "title": str(r.get(P_COL_TITLE, "")).strip() or project_title,
        "desc": str(r.get(P_COL_DESC, "")).strip(),
        "image": str(r.get(P_COL_IMAGE, "")).strip(),
        "start": str(r.get(P_COL_START_SENT, "")).strip(),
        "end": str(r.get(P_COL_END_SENT, "")).strip(),
        "status": str(r.get(P_COL_ARCHIVED, "")).strip(),
    }


# -------------------------
# MEETING + ENTRIES
# -------------------------
def meeting_row(meeting_id: str) -> pd.Series:
    m = get_meetings()
    row = m.loc[m[M_COL_ID].astype(str) == str(meeting_id)]
    if row.empty:
        raise HTTPException(status_code=404, detail="Meeting not found")
    return row.iloc[0]


def entries_for_meeting(meeting_id: str) -> pd.DataFrame:
    e = get_entries()
    return e.loc[e[E_COL_MEETING_ID].astype(str) == str(meeting_id)].copy()


def compute_presence_lists(mrow: pd.Series) -> Tuple[List[Dict], List[Dict]]:
    mp = companies_map_by_id()
    attending_ids = _parse_ids(mrow.get(M_COL_ATT_IDS))
    missing_ids = _parse_ids(mrow.get(M_COL_MISS_IDS))
    if not missing_ids:
        missing_ids = _parse_ids(mrow.get(M_COL_MISS_CALC_IDS))

    def _to_items(ids: List[str]) -> List[Dict]:
        items = []
        for cid in ids:
            info = mp.get(cid, {"name": f"ID:{cid}", "logo": ""})
            items.append({"id": cid, "name": info.get("name", ""), "logo": info.get("logo", "")})
        items.sort(key=lambda x: (x["name"] or "").lower())
        return items

    return _to_items(attending_ids), _to_items(missing_ids)


# -------------------------
# KPI
# -------------------------
def kpis(mrow: pd.Series, edf: pd.DataFrame, ref_date: date) -> Dict[str, int]:
    tasks_count = _safe_int(mrow.get(M_COL_TASKS_COUNT))
    memos_count = _safe_int(mrow.get(M_COL_MEMOS_COUNT))
    total = len(edf)

    is_task = _series(edf, E_COL_IS_TASK, False).apply(_bool_true)
    tasks = edf[is_task].copy()
    completed = _series(tasks, E_COL_COMPLETED, False).apply(_bool_true)
    open_tasks = tasks[~completed]
    closed_tasks = tasks[completed]

    deadlines = _series(open_tasks, E_COL_DEADLINE, None).apply(_parse_date_any)
    late = (deadlines.notna()) & (deadlines < ref_date)
    late_count = int(late.sum())

    return {
        "total_entries": int(total),
        "tasks_meeting": int(tasks_count),
        "memos_meeting": int(memos_count),
        "open_tasks": int(len(open_tasks)),
        "closed_tasks": int(len(closed_tasks)),
        "late_tasks": int(late_count),
    }


# -------------------------
# REMINDERS / FOLLOW UPS (PROJECT-WIDE) — based on ref_date (date de séance)
# -------------------------
def reminder_level(deadline: Optional[date], completed: bool, ref_date: date) -> Optional[int]:
    """Rappel = tâche non clôturée et en retard à la date de séance (ref_date)."""
    if completed or not deadline:
        return None
    days_late = (ref_date - deadline).days
    if days_late <= 0:
        return None
    return ((days_late - 1) // 7) + 1


def reminder_level_at_done(deadline: Optional[date], done_date: Optional[date]) -> Optional[int]:
    """Rappel historique à la clôture: retard constaté à la date de fin."""
    if not deadline or not done_date:
        return None
    days_late = (done_date - deadline).days
    if days_late <= 0:
        return None
    return ((days_late - 1) // 7) + 1


def _explode_areas(df: pd.DataFrame) -> pd.DataFrame:
    if E_COL_AREAS in df.columns:
        df["__area__"] = df[E_COL_AREAS].fillna("").astype(str).str.strip()
        df.loc[df["__area__"] == "", "__area__"] = "Général"
    else:
        df["__area__"] = "Général"
    df["__area_list__"] = df["__area__"].apply(_split_multi_labels)
    df["__area_list__"] = df["__area_list__"].apply(lambda vals: vals if vals else ["Général"])
    df = df.explode("__area_list__")
    df["__area_list__"] = df["__area_list__"].fillna("Général").astype(str).str.strip()
    df.loc[df["__area_list__"] == "", "__area_list__"] = "Général"
    return df


def _explode_packages(df: pd.DataFrame) -> pd.DataFrame:
    if E_COL_PACKAGES in df.columns:
        df["__package__"] = df[E_COL_PACKAGES].fillna("").astype(str).str.strip()
    else:
        df["__package__"] = ""
    df["__package_list__"] = df["__package__"].apply(_split_multi_labels)
    df["__package_list__"] = df["__package_list__"].apply(lambda vals: vals if vals else ["Sans lot"])
    df = df.explode("__package_list__")
    df["__package_list__"] = df["__package_list__"].fillna("Sans lot").astype(str).str.strip()
    df.loc[df["__package_list__"] == "", "__package_list__"] = "Sans lot"
    return df


def reminders_for_project(
    project_title: str,
    ref_date: date,
    max_level: int = 8,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    e = get_entries().copy()
    e = e.loc[e[E_COL_PROJECT_TITLE].fillna("").astype(str).str.strip() == project_title].copy()
    e = _filter_entries_by_created_range(e, start_date, end_date)

    e["__is_task__"] = _series(e, E_COL_IS_TASK, False).apply(_bool_true)
    e = e.loc[e["__is_task__"] == True].copy()

    e["__completed__"] = _series(e, E_COL_COMPLETED, False).apply(_bool_true)
    e["__done__"] = _series(e, E_COL_COMPLETED_END, None).apply(_parse_date_any)
    e.loc[e["__done__"].notna(), "__completed__"] = True
    e["__deadline__"] = _series(e, E_COL_DEADLINE, None).apply(_parse_date_any)
    e["__reminder__"] = e.apply(lambda r: reminder_level(r["__deadline__"], r["__completed__"], ref_date), axis=1)

    e = e.loc[e["__reminder__"].notna()].copy()
    e["__reminder__"] = e["__reminder__"].astype(int)
    e = e.loc[e["__reminder__"] <= max_level].copy()

    e = _explode_areas(e)

    e["__company__"] = _series(e, E_COL_COMPANY_TASK, "").fillna("").astype(str).str.strip()
    e.loc[e["__company__"] == "", "__company__"] = "Non renseigné"

    e = e.sort_values(["__reminder__", "__deadline__"], ascending=[False, True])
    return e


def followups_for_project(
    project_title: str,
    ref_date: date,
    exclude_entry_ids: set[str],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """À suivre = tâches non clôturées NON en retard à ref_date (deadline >= ref_date ou deadline vide)."""
    e = get_entries().copy()
    e = e.loc[e[E_COL_PROJECT_TITLE].fillna("").astype(str).str.strip() == project_title].copy()
    e = _filter_entries_by_created_range(e, start_date, end_date)

    e["__id__"] = _series(e, E_COL_ID, "").fillna("").astype(str).str.strip()
    if exclude_entry_ids:
        e = e.loc[~e["__id__"].isin(exclude_entry_ids)].copy()

    e["__is_task__"] = _series(e, E_COL_IS_TASK, False).apply(_bool_true)
    e = e.loc[e["__is_task__"] == True].copy()

    e["__completed__"] = _series(e, E_COL_COMPLETED, False).apply(_bool_true)
    e["__done__"] = _series(e, E_COL_COMPLETED_END, None).apply(_parse_date_any)
    e.loc[e["__done__"].notna(), "__completed__"] = True
    e = e.loc[e["__completed__"] == False].copy()

    e["__deadline__"] = _series(e, E_COL_DEADLINE, None).apply(_parse_date_any)
    e = e.loc[e["__deadline__"].isna() | (e["__deadline__"] >= ref_date)].copy()

    e = _explode_areas(e)

    e["__company__"] = _series(e, E_COL_COMPANY_TASK, "").fillna("").astype(str).str.strip()
    e.loc[e["__company__"] == "", "__company__"] = "Non renseigné"

    e["__deadline_sort__"] = e["__deadline__"].apply(lambda d: date(2999, 12, 31) if d is None else d)
    e = e.sort_values(["__deadline_sort__", "__company__"], ascending=[True, True])
    return e


def reminders_by_company(rem_df: pd.DataFrame) -> List[Dict]:
    if rem_df.empty:
        return []
    logo_map = companies_logo_by_name()
    g = rem_df.groupby("__company__", dropna=False).size().reset_index(name="count")
    g["__norm__"] = g["__company__"].astype(str).apply(_norm_name)
    g["logo"] = g["__norm__"].apply(lambda k: logo_map.get(k, ""))
    g = g.sort_values("count", ascending=False)
    out = []
    for _, r in g.iterrows():
        out.append({"name": str(r["__company__"]), "logo": str(r["logo"] or "").strip(), "count": int(r["count"])})
    return out


# -------------------------
# ZONES (for meeting entries)
# -------------------------
def group_meeting_by_area(edf: pd.DataFrame) -> List[Tuple[str, pd.DataFrame]]:
    df = edf.copy()
    df = _explode_areas(df)
    areas: List[Tuple[str, pd.DataFrame]] = []
    for area, g in df.groupby("__area_list__", sort=True):
        areas.append((str(area), g.copy()))
    areas.sort(key=lambda x: (0 if x[0].lower() == "général" else 1, x[0].lower()))
    return areas


# -------------------------
# MEMO MODAL (UI)
# -------------------------
EDITOR_MEMO_MODAL_CSS = r"""
.btnAddMemo{margin-left:auto; font-size:12px; padding:6px 10px; border:1px solid #ddd; border-radius:10px; background:#fff; cursor:pointer}
.btnAddMemo:hover{background:#f7f7f7}
.memoModal{position:fixed; inset:0; padding:16px 16px 16px 290px; background:rgba(0,0,0,.35); display:none; align-items:flex-start; justify-content:center; overflow:auto; z-index:9999}
.memoModal .panel{background:#fff; width:min(720px, calc(100vw - 330px)); max-height:calc(100vh - 32px); overflow:auto; border-radius:14px; box-shadow:0 20px 60px rgba(0,0,0,.25)}
@media (max-width:1200px){.memoModal{padding:16px}.memoModal .panel{width:min(720px,94vw)}}
.memoModal .head{display:flex; gap:12px; align-items:center; padding:14px 16px; border-bottom:1px solid #eee}
.memoModal .list{padding:10px 16px}
.memoModal .item{display:block; padding:10px 10px; border:1px solid #eee; border-radius:12px; margin:8px 0}
.memoModal .item:hover{background:#fafafa}
.memoModal .actions{display:flex; gap:10px; justify-content:flex-end; padding:12px 16px; border-top:1px solid #eee}
.memoBtn{padding:8px 12px; border:1px solid #ddd; background:#fff; border-radius:10px; cursor:pointer}
.memoBtnPrimary{border-color:#111; background:#111; color:#fff}
"""

EDITOR_MEMO_MODAL_HTML = r"""
<div class="memoModal" id="memoModal">
  <div class="panel">
    <div class="head">
      <h3 id="memoModalTitle" style="margin:0">Ajouter des mémos</h3>
      <span class="muted" id="memoModalSub"></span>
      <div style="margin-left:auto"></div>
      <button class="memoBtn" id="memoModalClose" type="button">Fermer</button>
    </div>
    <div class="list" id="memoModalList"></div>
    <div class="actions">
      <button class="memoBtn" id="memoModalCancel" type="button">Annuler</button>
      <button class="memoBtn memoBtnPrimary" id="memoModalAdd" type="button">Ajouter</button>
    </div>
  </div>
</div>
"""

EDITOR_MEMO_MODAL_JS = r"""
(function(){
  const qs = (k) => new URLSearchParams(window.location.search).get(k) || "";
  const modal = document.getElementById('memoModal');
  if(!modal) return;
  const listEl = document.getElementById('memoModalList');
  const subEl = document.getElementById('memoModalSub');
  let currentArea = "";

  function open(area){
    currentArea = area;
    subEl.textContent = "Zone : " + area;
    listEl.innerHTML = "<div class='muted'>Chargement…</div>";
    modal.style.display = "flex";
    const project = qs("project") || "";
    fetch(`/api/memos?project=${encodeURIComponent(project)}&area=${encodeURIComponent(area)}`)
      .then(r => r.json())
      .then(data => {
        const pinned = (qs("pinned_memos")||"").split(",").map(s=>s.trim()).filter(Boolean);
        if(!data || !data.items || data.items.length===0){
          listEl.innerHTML = "<div class='muted'>Aucun mémo disponible pour cette zone.</div>";
          return;
        }
        listEl.innerHTML = data.items.map(it => {
          const checked = pinned.includes(it.id) ? "checked" : "";
          const meta = [it.created||"", it.company||"", it.owner||""].filter(Boolean).join(" • ");
          return `<label class="item">
            <div style="display:flex; gap:10px; align-items:flex-start">
              <input type="checkbox" data-id="${it.id}" ${checked} style="margin-top:3px"/>
              <div>
                <div style="font-weight:800">${it.title||"(Sans titre)"}</div>
                <div class="muted" style="margin-top:2px">${meta}</div>
              </div>
            </div>
          </label>`;
        }).join("");
      })
      .catch(()=>{ listEl.innerHTML = "<div class='muted'>Erreur de chargement.</div>"; });
  }

  function close(){ modal.style.display = "none"; currentArea = ""; }
  document.getElementById('memoModalClose').onclick = close;
  document.getElementById('memoModalCancel').onclick = close;
  modal.addEventListener('click', (e)=>{ if(e.target===modal) close(); });

  document.getElementById('memoModalAdd').onclick = function(){
    const ids = Array.from(listEl.querySelectorAll("input[type=checkbox][data-id]"))
      .filter(x=>x.checked).map(x=>x.getAttribute("data-id")).filter(Boolean);
    const u = new URL(window.location.href);
    const existing = (u.searchParams.get("pinned_memos")||"").split(",").map(s=>s.trim()).filter(Boolean);
    const merged = Array.from(new Set(existing.concat(ids))).join(",");
    if(merged) u.searchParams.set("pinned_memos", merged);
    else u.searchParams.delete("pinned_memos");
    window.location.href = u.toString();
  };

  document.addEventListener("click", (e) => {
    const btn = e.target.closest(".btnAddMemo");
    if(!btn) return;
    open(btn.getAttribute("data-area")||"");
  });
})();
"""

QUALITY_MODAL_CSS = r"""
.qualityModal{position:fixed; inset:0; padding:16px 16px 16px 290px; background:rgba(0,0,0,.35); display:none; align-items:flex-start; justify-content:center; overflow:auto; z-index:9998}
.qualityModal .panel{background:#fff; width:min(980px, calc(100vw - 330px)); max-height:calc(100vh - 32px); overflow:auto; border-radius:16px; box-shadow:0 20px 60px rgba(0,0,0,.25)}
@media (max-width:1200px){.qualityModal{padding:16px}.qualityModal .panel{width:min(980px,94vw)}}
.qualityModal .head{display:flex; gap:12px; align-items:center; padding:16px 18px; border-bottom:1px solid #eee}
.qualityModal .list{padding:14px 18px}
.qualityModal .item{border:1px solid #e2e8f0; border-radius:14px; padding:12px; margin:10px 0; background:#fff}
.qualityModal .meta{color:#475569; font-weight:700; font-size:12px}
.qualityScore{font-size:28px; font-weight:1000}
.qualityBadge{display:inline-flex; align-items:center; gap:8px; padding:6px 10px; border-radius:999px; background:#fff1f2; border:1px solid #fecdd3; font-weight:900; color:#b91c1c}
.qualityGrid{display:grid; grid-template-columns:repeat(3,1fr); gap:10px; margin-top:10px}
.qualityCard{border:1px solid #e2e8f0; border-radius:12px; padding:10px; background:#f8fafc}
.qualityHighlight{background:#fee2e2; padding:0 4px; border-radius:4px; font-weight:900; color:#b91c1c; position:relative; cursor:help}
.qualityHighlight:hover::after{content:attr(data-suggestion); position:absolute; left:0; top:100%; margin-top:6px; background:#111827; color:#fff; padding:6px 8px; border-radius:6px; font-size:11px; white-space:pre-wrap; z-index:20; min-width:140px; max-width:240px}
.qualityHighlight:hover::before{content:""; position:absolute; left:10px; top:100%; border:6px solid transparent; border-bottom-color:#111827}
.qualityFullText{margin-top:6px; line-height:1.4}
.qualityTips{border-left:4px solid #b91c1c; padding:10px 12px; background:#fff1f2; border-radius:10px; margin-top:12px}
.qualityItemTitle{color:#b91c1c; font-weight:900}
"""

QUALITY_MODAL_HTML = r"""
<div class="qualityModal" id="qualityModal">
  <div class="panel">
    <div class="head">
      <h3 style="margin:0">Qualité orthographique &amp; grammaticale</h3>
      <div style="margin-left:auto"></div>
      <button class="memoBtn" id="qualityModalClose" type="button">Fermer</button>
    </div>
    <div class="list" id="qualityModalList"></div>
  </div>
</div>
"""

QUALITY_MODAL_JS = r"""
(function(){
  const modal = document.getElementById('qualityModal');
  const listEl = document.getElementById('qualityModalList');
  if(!modal || !listEl) return;

  function open(){
    listEl.innerHTML = "<div class='muted'>Analyse en cours…</div>";
    modal.style.display = "flex";
    const qs = new URLSearchParams(window.location.search);
    const meetingId = qs.get("meeting_id") || "";
    const project = qs.get("project") || "";
    fetch(`/api/quality?meeting_id=${encodeURIComponent(meetingId)}&project=${encodeURIComponent(project)}`)
      .then(r => r.json())
      .then(data => {
        if(data.error){
          listEl.innerHTML = `<div class='muted'>${data.error}</div>`;
          return;
        }
        const score = data.score ?? 0;
        const total = data.total ?? 0;
        const issuesByArea = data.issues_by_area || {};
        const issueAreas = Object.keys(issuesByArea);
        const strengths = [
          score >= 95 ? "Très bonne qualité générale." : "Qualité perfectible, corrections recommandées.",
          total === 0 ? "Aucune faute détectée." : "Des corrections sont nécessaires.",
          "Objectif : un texte clair et professionnel."
        ];
        const summary = `
          <div class="qualityBadge">Score: <span class="qualityScore">${score}</span>/100</div>
          <div class="qualityGrid">
            <div class="qualityCard"><div class="meta">Erreurs détectées</div><div style="font-weight:900;font-size:18px">${total}</div></div>
            <div class="qualityCard"><div class="meta">Impact</div><div style="font-weight:900;font-size:18px">${score >= 90 ? "Faible" : score >= 75 ? "Moyen" : "Fort"}</div></div>
            <div class="qualityCard"><div class="meta">Relecture</div><div style="font-weight:900;font-size:18px">${score >= 90 ? "OK" : "Recommandée"}</div></div>
          </div>
          <div class="qualityTips">
            <div style="font-weight:900">Conseils pédagogiques</div>
            <ul style="margin:6px 0 0 16px">
              <li>${strengths[0]}</li>
              <li>${strengths[1]}</li>
              <li>${strengths[2]}</li>
            </ul>
            <div class="meta" style="margin-top:6px">Corrige les libellés directement dans METRONOME pour améliorer la qualité globale.</div>
          </div>
        `;
        if(!issueAreas.length){
          listEl.innerHTML = summary + "<div class='muted' style='margin-top:10px'>Aucune faute détectée.</div>";
          return;
        }
        const escapeHtml = (v) => String(v || "").replace(/[&<>"']/g, (m) => ({
          "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"
        })[m]);
        const sections = issueAreas.map(area => {
          const items = (issuesByArea[area] || []).map(it => {
            const text = it.text || it.context || "";
            const offset = it.offset ?? it.context_offset;
            const length = it.length ?? it.context_length;
            const suggestion = it.replacements || it.message || "Suggestion";
            let highlight = escapeHtml(text);
            if(text && offset != null && length != null){
              const safeText = escapeHtml(text);
              const before = safeText.slice(0, offset);
              const mid = safeText.slice(offset, offset + length);
              const after = safeText.slice(offset + length);
              highlight = `${before}<span class="qualityHighlight" data-suggestion="${escapeHtml(suggestion)}">${mid}</span>${after}`;
            }
            return `
              <div class="item">
                <div class="qualityItemTitle">${escapeHtml(it.category || "Suggestion")}</div>
                <div class="qualityFullText">${highlight || "—"}</div>
              </div>
            `;
          }).join("");
          return `
            <div style="margin-top:16px;font-weight:900">Zone : ${escapeHtml(area)}</div>
            ${items}
          `;
        }).join("");
        listEl.innerHTML = summary + "<div style='margin-top:12px;font-weight:900'>Points à corriger</div>" + sections;
      })
      .catch(() => {
        listEl.innerHTML = "<div class='muted'>Impossible d'analyser pour le moment.</div>";
      });
  }

  function close(){ modal.style.display = "none"; }
  document.getElementById('qualityModalClose').onclick = close;
  modal.addEventListener('click', (e)=>{ if(e.target===modal) close(); });
  document.getElementById('btnQualityCheck')?.addEventListener('click', open);
})();
"""

ANALYSIS_MODAL_CSS = r"""
.analysisModal{position:fixed; inset:0; padding:16px 16px 16px 290px; background:rgba(0,0,0,.35); display:none; align-items:flex-start; justify-content:center; overflow:auto; z-index:9997}
.analysisModal .panel{background:#fff; width:min(980px, calc(100vw - 330px)); max-height:calc(100vh - 32px); overflow:auto; border-radius:16px; box-shadow:0 20px 60px rgba(0,0,0,.25)}
@media (max-width:1200px){.analysisModal{padding:16px}.analysisModal .panel{width:min(980px,94vw)}}
.analysisModal .head{display:flex; gap:12px; align-items:center; padding:16px 18px; border-bottom:1px solid #eee}
.analysisModal .list{padding:14px 18px}
.analysisCard{border:1px solid #e2e8f0; border-radius:14px; padding:12px; margin:10px 0; background:#fff}
.analysisGrid{display:grid; grid-template-columns:repeat(3,1fr); gap:10px; margin-top:10px}
.analysisKpi{border:1px solid #e2e8f0; border-radius:12px; padding:10px; background:#f8fafc}
"""

ANALYSIS_MODAL_HTML = r"""
<div class="analysisModal" id="analysisModal">
  <div class="panel">
    <div class="head">
      <h3 style="margin:0">Analyse du compte rendu</h3>
      <div style="margin-left:auto"></div>
      <button class="memoBtn" id="analysisModalClose" type="button">Fermer</button>
    </div>
    <div class="list" id="analysisModalList"></div>
  </div>
</div>
"""

ANALYSIS_MODAL_JS = r"""
(function(){
  const modal = document.getElementById('analysisModal');
  const listEl = document.getElementById('analysisModalList');
  if(!modal || !listEl) return;

  function open(){
    listEl.innerHTML = "<div class='muted'>Analyse en cours…</div>";
    modal.style.display = "flex";
    const qs = new URLSearchParams(window.location.search);
    const meetingId = qs.get("meeting_id") || "";
    const project = qs.get("project") || "";
    fetch(`/api/analysis?meeting_id=${encodeURIComponent(meetingId)}&project=${encodeURIComponent(project)}`)
      .then(r => r.json())
      .then(data => {
        if(data.error){
          listEl.innerHTML = `<div class='muted'>${data.error}</div>`;
          return;
        }
        const k = data.kpis || {};
        const bullets = (data.points || []).map(p => `<li>${p}</li>`).join("");
        const risks = (data.risks || []).map(p => `<li>${p}</li>`).join("");
        const follow = (data.follow_ups || []).map(p => `<li>${p}</li>`).join("");
        const least = (data.least_responsive || []).map(it => `<li>${it.name} (${it.count})</li>`).join("");
        const byArea = data.followups_by_area || {};
        const areaSections = Object.keys(byArea).map(a => {
          const items = (byArea[a] || []).map(t => `<li>${t}</li>`).join("");
          return `<div class="analysisCard"><div style="font-weight:900">Zone : ${a}</div><ul style="margin:6px 0 0 18px">${items || "<li>Aucune action prioritaire.</li>"}</ul></div>`;
        }).join("");
        listEl.innerHTML = `
          <div class="analysisGrid">
            <div class="analysisKpi"><div class="meta">Rappels en retard</div><div style="font-weight:900;font-size:18px">${k.late_tasks ?? 0}</div></div>
            <div class="analysisKpi"><div class="meta">Tâches ouvertes</div><div style="font-weight:900;font-size:18px">${k.open_tasks ?? 0}</div></div>
            <div class="analysisKpi"><div class="meta">À suivre</div><div style="font-weight:900;font-size:18px">${k.followups ?? 0}</div></div>
          </div>
          <div class="analysisCard">
            <div style="font-weight:900">Synthèse rapide</div>
            <ul style="margin:6px 0 0 18px">${bullets || "<li>Aucun point marquant détecté.</li>"}</ul>
          </div>
          <div class="analysisCard">
            <div style="font-weight:900">Points de vigilance</div>
            <ul style="margin:6px 0 0 18px">${risks || "<li>Aucun risque majeur identifié.</li>"}</ul>
          </div>
          <div class="analysisCard">
            <div style="font-weight:900">À relancer à la prochaine réunion</div>
            <ul style="margin:6px 0 0 18px">${follow || "<li>Rien de critique à relancer.</li>"}</ul>
          </div>
          <div class="analysisCard">
            <div style="font-weight:900">Entreprises les moins réactives</div>
            <ul style="margin:6px 0 0 18px">${least || "<li>Aucune entreprise à relancer en priorité.</li>"}</ul>
          </div>
          <div style="margin-top:12px;font-weight:900">Actions attendues par zone</div>
          ${areaSections || "<div class='analysisCard'>Aucune action par zone.</div>"}
        `;
      })
      .catch(() => {
        listEl.innerHTML = "<div class='muted'>Impossible d'analyser pour le moment.</div>";
      });
  }

  function close(){ modal.style.display = "none"; }
  document.getElementById('analysisModalClose').onclick = close;
  modal.addEventListener('click', (e)=>{ if(e.target===modal) close(); });
  document.getElementById('btnAnalysis')?.addEventListener('click', open);
})();
"""

TOOLS_MODAL_CSS = r"""
.toolsModal{position:fixed; inset:0; padding:16px 16px 16px 290px; background:rgba(0,0,0,.35); display:none; align-items:flex-start; justify-content:center; overflow:auto; z-index:9996}
.toolsModal .panel{background:#fff; width:min(980px, calc(100vw - 330px)); max-height:calc(100vh - 32px); overflow:auto; border-radius:16px; box-shadow:0 20px 60px rgba(0,0,0,.25)}
@media (max-width:1200px){.toolsModal{padding:16px}.toolsModal .panel{width:min(980px,94vw)}}
.toolsTabs{display:flex;gap:8px;padding:0 18px 12px;border-bottom:1px solid #eee}
.toolsTab{padding:8px 12px;border:1px solid #cbd5e1;background:#fff;border-radius:10px;font-weight:900;cursor:pointer}
.toolsTab.active{background:#0f172a;color:#fff;border-color:#0f172a}
.toolsBody{padding:14px 18px}
.toolsPane{display:none}
.toolsPane.active{display:block}
"""

TOOLS_MODAL_HTML = r"""
<div class="toolsModal" id="toolsModal">
  <div class="panel">
    <div class="head" style="display:flex;gap:12px;align-items:center;padding:16px 18px;border-bottom:1px solid #eee">
      <h3 style="margin:0">Outils</h3>
      <div style="margin-left:auto"></div>
      <button class="memoBtn" id="toolsModalClose" type="button">Fermer</button>
    </div>
    <div class="toolsTabs">
      <button class="toolsTab active" type="button" data-tools-tab="quality">Qualité du texte</button>
      <button class="toolsTab" type="button" data-tools-tab="analysis">Analyse</button>
    </div>
    <div class="toolsBody">
      <div class="toolsPane active" id="toolsPaneQuality"><div class='muted'>Ouvrez l'onglet pour lancer l'analyse.</div></div>
      <div class="toolsPane" id="toolsPaneAnalysis"><div class='muted'>Ouvrez l'onglet pour lancer l'analyse.</div></div>
    </div>
  </div>
</div>
"""

TOOLS_MODAL_JS = r"""
(function(){
  const modal = document.getElementById('toolsModal');
  const paneQ = document.getElementById('toolsPaneQuality');
  const paneA = document.getElementById('toolsPaneAnalysis');
  if(!modal || !paneQ || !paneA) return;

  const esc = (v) => String(v || '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('\"','&quot;').replaceAll("'",'&#39;');

  function loadQuality(){
    paneQ.innerHTML = "<div class='muted'>Analyse en cours…</div>";
    const qs = new URLSearchParams(window.location.search);
    const meetingId = qs.get('meeting_id') || '';
    const project = qs.get('project') || '';
    fetch(`/api/quality?meeting_id=${encodeURIComponent(meetingId)}&project=${encodeURIComponent(project)}`)
      .then(r => r.json())
      .then(data => {
        if(data.error){ paneQ.innerHTML = `<div class='muted'>${esc(data.error)}</div>`; return; }
        const score = data.score ?? 0;
        const total = data.total ?? 0;
        const issuesByArea = data.issues_by_area || {};
        const issueAreas = Object.keys(issuesByArea);
        const summary = `
          <div class="qualityBadge">Score: <span class="qualityScore">${score}</span>/100</div>
          <div class="qualityGrid">
            <div class="qualityCard"><div class="meta">Erreurs détectées</div><div style="font-weight:900;font-size:18px">${total}</div></div>
            <div class="qualityCard"><div class="meta">Impact</div><div style="font-weight:900;font-size:18px">${score >= 90 ? "Faible" : score >= 75 ? "Moyen" : "Fort"}</div></div>
            <div class="qualityCard"><div class="meta">Relecture</div><div style="font-weight:900;font-size:18px">${score >= 90 ? "OK" : "Recommandée"}</div></div>
          </div>`;
        if(!issueAreas.length){ paneQ.innerHTML = summary + "<div class='muted' style='margin-top:10px'>Aucune faute détectée.</div>"; return; }
        const sections = issueAreas.map(area => {
          const items = (issuesByArea[area] || []).slice(0,8).map(it => `<li>${esc(it.text || it.context || '')}</li>`).join('');
          return `<div class="analysisCard"><div style="font-weight:900">Zone : ${esc(area)}</div><ul style="margin:6px 0 0 18px">${items || '<li>—</li>'}</ul></div>`;
        }).join('');
        paneQ.innerHTML = summary + `<div style="margin-top:12px;font-weight:900">Points à corriger</div>${sections}`;
      })
      .catch(()=>{ paneQ.innerHTML = "<div class='muted'>Impossible d'analyser pour le moment.</div>"; });
  }

  function loadAnalysis(){
    paneA.innerHTML = "<div class='muted'>Analyse en cours…</div>";
    const qs = new URLSearchParams(window.location.search);
    const meetingId = qs.get('meeting_id') || '';
    const project = qs.get('project') || '';
    fetch(`/api/analysis?meeting_id=${encodeURIComponent(meetingId)}&project=${encodeURIComponent(project)}`)
      .then(r => r.json())
      .then(data => {
        if(data.error){ paneA.innerHTML = `<div class='muted'>${esc(data.error)}</div>`; return; }
        const k = data.kpis || {};
        const bullets = (data.points || []).map(p => `<li>${esc(p)}</li>`).join('');
        const risks = (data.risks || []).map(p => `<li>${esc(p)}</li>`).join('');
        paneA.innerHTML = `
          <div class="analysisGrid">
            <div class="analysisKpi"><div class="meta">Rappels en retard</div><div style="font-weight:900;font-size:18px">${k.late_tasks ?? 0}</div></div>
            <div class="analysisKpi"><div class="meta">Tâches ouvertes</div><div style="font-weight:900;font-size:18px">${k.open_tasks ?? 0}</div></div>
            <div class="analysisKpi"><div class="meta">À suivre</div><div style="font-weight:900;font-size:18px">${k.followups ?? 0}</div></div>
          </div>
          <div class="analysisCard"><div style="font-weight:900">Synthèse rapide</div><ul style="margin:6px 0 0 18px">${bullets || '<li>Aucun point marquant détecté.</li>'}</ul></div>
          <div class="analysisCard"><div style="font-weight:900">Points de vigilance</div><ul style="margin:6px 0 0 18px">${risks || '<li>Aucun risque majeur identifié.</li>'}</ul></div>`;
      })
      .catch(()=>{ paneA.innerHTML = "<div class='muted'>Impossible d'analyser pour le moment.</div>"; });
  }

  function activate(tab){
    document.querySelectorAll('.toolsTab').forEach(b => b.classList.toggle('active', b.dataset.toolsTab === tab));
    paneQ.classList.toggle('active', tab === 'quality');
    paneA.classList.toggle('active', tab === 'analysis');
    if(tab === 'quality' && !paneQ.dataset.loaded){ paneQ.dataset.loaded = '1'; loadQuality(); }
    if(tab === 'analysis' && !paneA.dataset.loaded){ paneA.dataset.loaded = '1'; loadAnalysis(); }
  }

  function open(){ modal.style.display = 'flex'; activate('quality'); }
  function close(){ modal.style.display = 'none'; }

  document.getElementById('btnTools')?.addEventListener('click', open);
  document.getElementById('toolsModalClose')?.addEventListener('click', close);
  modal.addEventListener('click', (e)=>{ if(e.target===modal) close(); });
  document.querySelectorAll('.toolsTab').forEach(btn => btn.addEventListener('click', ()=> activate(btn.dataset.toolsTab || 'quality')));
})();
"""

RESIZE_TOP_JS = r"""
(function(){
  const root = document.documentElement;
  const grip = document.getElementById('topPageGrip');
  if(!grip) return;
  let startX = 0;
  let startScale = 1;
  function onMove(e){
    const dx = e.clientX - startX;
    const next = Math.max(0.8, Math.min(1.1, startScale + dx / 500));
    root.style.setProperty('--top-scale', next.toFixed(2));
  }
  function onUp(){
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
  }
  grip.addEventListener('mousedown', (e) => {
    startX = e.clientX;
    const current = parseFloat(getComputedStyle(root).getPropertyValue('--top-scale').trim() || '1');
    startScale = current;
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  });
})();
"""
RESIZE_COLUMNS_JS = r"""
(function(){
  const root = document.documentElement;
  const map = {
    type: '--col-type',
    comment: '--col-comment',
    date: '--col-date',
    date2: '--col-date',
    date3: '--col-date',
    lot: '--col-lot',
    who: '--col-who',
  };
  let active = null;
  let startX = 0;
  let startPct = 0;
  function onMove(e){
    if(!active) return;
    const table = active.closest('table');
    const width = table.getBoundingClientRect().width || 1;
    const dx = e.clientX - startX;
    const deltaPct = (dx / width) * 100;
    const next = Math.max(3, startPct + deltaPct);
    root.style.setProperty(map[active.dataset.col], `${next}%`);
  }
  function onUp(){
    active = null;
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp);
  }
  document.addEventListener('mousedown', (e) => {
    const grip = e.target.closest('.colGrip');
    if(!grip) return;
    active = grip;
    startX = e.clientX;
    const current = getComputedStyle(root).getPropertyValue(map[grip.dataset.col]).trim().replace('%','');
    startPct = parseFloat(current || '0');
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  });
})();
"""

SYNC_EDITABLE_JS = r"""
(function(){
  function syncAll(){
    const groups = new Map();
    document.querySelectorAll('[data-sync]').forEach(el => {
      const key = el.getAttribute('data-sync') || '';
      if(!key || groups.has(key)) return;
      groups.set(key, el.textContent);
    });
    groups.forEach((value, key) => {
      document.querySelectorAll(`[data-sync="${key}"]`).forEach(el => {
        if(el.textContent !== value){ el.textContent = value; }
      });
    });
  }

  function syncValue(el){
    const key = el.getAttribute('data-sync') || '';
    if(!key) return;
    const value = el.textContent;
    document.querySelectorAll(`[data-sync="${key}"]`).forEach(target => {
      if(target !== el){ target.textContent = value; }
    });
  }

  document.addEventListener('input', (e) => {
    const el = e.target.closest('[data-sync]');
    if(el){ syncValue(el); }
  });
  document.addEventListener('blur', (e) => {
    const el = e.target.closest('[data-sync]');
    if(el){ syncValue(el); }
  }, true);
  window.addEventListener('DOMContentLoaded', syncAll);
})();
"""

RANGE_PICKER_JS = r"""
function toggleRangePanel(){
  const panel = document.getElementById('rangePanel');
  if(!panel){ return; }
  const current = panel.style.display;
  panel.style.display = (!current || current === 'none') ? 'flex' : 'none';
}

function applyRange(){
  const start = document.getElementById('rangeStart')?.value || "";
  const end = document.getElementById('rangeEnd')?.value || "";
  const url = new URL(window.location.href);
  if(start){ url.searchParams.set('range_start', start); }
  else{ url.searchParams.delete('range_start'); }
  if(end){ url.searchParams.set('range_end', end); }
  else{ url.searchParams.delete('range_end'); }
  window.location.href = url.toString();
}

function clearRange(){
  const startEl = document.getElementById('rangeStart');
  const endEl = document.getElementById('rangeEnd');
  if(startEl){ startEl.value = ""; }
  if(endEl){ endEl.value = ""; }
  const url = new URL(window.location.href);
  url.searchParams.delete('range_start');
  url.searchParams.delete('range_end');
  window.location.href = url.toString();
}

window.addEventListener('DOMContentLoaded', () => {
  document.getElementById('btnRange')?.addEventListener('click', toggleRangePanel);
});

document.addEventListener('click', (e) => {
  const btn = e.target.closest('#btnRange');
  if(!btn) return;
  toggleRangePanel();
});
"""

PRINT_PREVIEW_TOGGLE_JS = r"""
(function(){
  const btn = document.getElementById('btnPrintPreview');
  if(!btn) return;
  const STORAGE_KEY = 'tempo.print.preview.enabled.v1';

  function loadState(){
    try{ return localStorage.getItem(STORAGE_KEY) === '1'; }
    catch(_){ return false; }
  }

  function saveState(v){
    try{ localStorage.setItem(STORAGE_KEY, v ? '1' : '0'); }
    catch(_){ }
  }

  function apply(enabled){
    document.body.classList.toggle('printPreviewMode', enabled);
    document.body.classList.toggle('printOptimized', enabled);
    btn.textContent = enabled ? 'Aperçu impression : ON' : 'Aperçu impression : OFF';
    btn.classList.toggle('active', enabled);
    if(window.repaginateReport){ window.repaginateReport(); }
  }

  let enabled = loadState();
  apply(enabled);

  btn.addEventListener('click', () => {
    enabled = !enabled;
    saveState(enabled);
    apply(enabled);
  });
})();
"""

CONSTRAINT_TOGGLES_JS = r"""
(function(){
  const panel = document.getElementById('constraintsPanel');
  if(!panel) return;
  const root = document.documentElement;
  const body = document.body;
  const STORAGE_KEY = 'tempo.constraint.toggles.v1';

  const defaultState = {
    fixedA4: true,
    fixedPageHeight: true,
    pageBreaks: true,
    bodyOffset: true,
    pagePadding: true,
    footerReserve: true,
    tableFixed: true,
    printHideUi: true,
    printStickyHeader: true,
    printCompactRows: true,
    printAvoidSplitRows: true,
    keepSessionHeaderWithNext: true,
    printAutoOptimize: true,
    topScale: true,
  };

  function loadState(){
    try{
      const raw = localStorage.getItem(STORAGE_KEY);
      const parsed = raw ? JSON.parse(raw) : {};
      return {...defaultState, ...parsed};
    }catch(_){
      return {...defaultState};
    }
  }

  function saveState(state){
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  }

  function applyConstraint(name, active){
    body.classList.toggle(`constraint-off-${name}`, !active);
  }

  function applyAll(state){
    Object.entries(state).forEach(([k, v]) => applyConstraint(k, !!v));
  }

  const state = loadState();
  panel.querySelectorAll('[data-constraint]').forEach(input => {
    const name = input.getAttribute('data-constraint');
    if(!(name in state)) return;
    input.checked = !!state[name];
    input.addEventListener('change', () => {
      state[name] = !!input.checked;
      applyConstraint(name, state[name]);
      saveState(state);
      if(window.repaginateReport){ window.repaginateReport(); }
    });
  });

  function updateFooterReserveFactor(){
    const input = document.getElementById('footerReserveFactor');
    const value = document.getElementById('footerReserveFactorValue');
    if(!input || !value) return;
    const pct = Math.max(-100, Math.min(150, parseFloat(input.value || '100')));
    const factor = pct / 100;
    value.textContent = `${Math.round(pct)} %`;
    root.style.setProperty('--footer-reserve-factor', factor.toFixed(2));
    try{ localStorage.setItem('tempo.footer.reserve.factor.v1', String(Math.round(pct))); }catch(_){ }
    if(window.repaginateReport){ window.repaginateReport(); }
  }

  const footerReserveInput = document.getElementById('footerReserveFactor');
  if(footerReserveInput){
    let savedPct = null;
    try{ savedPct = localStorage.getItem('tempo.footer.reserve.factor.v1'); }catch(_){ }
    if(savedPct !== null && savedPct !== ''){ footerReserveInput.value = savedPct; }
    footerReserveInput.addEventListener('input', updateFooterReserveFactor);
  }

  document.getElementById('btnConstraints')?.addEventListener('click', () => {
    panel.style.display = panel.style.display === 'none' ? 'flex' : 'none';
  });

  applyAll(state);
  updateFooterReserveFactor();
})();
"""

LAYOUT_CONTROLS_JS = r"""
(function(){
  function closestZone(el){ return el.closest('.zoneBlock'); }

  function mergeDuplicateZones(){
    const zones = Array.from(document.querySelectorAll('.reportPages .zoneBlock'));
    const grouped = new Map();
    zones.forEach(zone => {
      const key = (zone.getAttribute('data-zone-id') || '').trim();
      if(!key) return;
      if(!grouped.has(key)){ grouped.set(key, []); }
      grouped.get(key).push(zone);
    });
    grouped.forEach(group => {
      if(group.length < 2) return;
      const target = group[0];
      const targetBody = target.querySelector('tbody');
      if(!targetBody) return;
      group.slice(1).forEach(zone => {
        const body = zone.querySelector('tbody');
        if(body){
          Array.from(body.children).forEach(row => targetBody.appendChild(row));
        }
        zone.remove();
      });
    });
  }

  function getUniqueZoneBlocks(){
    mergeDuplicateZones();
    const seen = new Set();
    const out = [];
    Array.from(document.querySelectorAll('.reportPages .zoneBlock')).forEach(zone => {
      const key = (zone.getAttribute('data-zone-id') || '').trim();
      if(!key || seen.has(key)) return;
      const hasRows = !!zone.querySelector('tbody tr');
      if(!hasRows) return;
      seen.add(key);
      out.push(zone);
    });
    return out;
  }

  function openZoneOrderModal(){
    const modal = document.getElementById('zoneOrderModal');
    const list = document.getElementById('zoneOrderList');
    if(!modal || !list) return;
    const zones = getUniqueZoneBlocks();
    list.innerHTML = zones.map((zone, idx) => {
      const zoneId = (zone.getAttribute('data-zone-id') || '').trim();
      const title = zone.querySelector('.zoneTitle span');
      const name = (title ? title.textContent : zoneId || `Périmètre ${idx+1}`) || `Périmètre ${idx+1}`;
      return `<li class="zoneOrderItem" draggable="true" data-zone-id="${zoneId}"><span class="zoneOrderGrip">⋮⋮</span><span class="zoneOrderText">${name}</span></li>`;
    }).join('');
    if(!zones.length){
      list.innerHTML = '<li class="zoneOrderItem" draggable="false"><span class="zoneOrderText">Aucun périmètre à réordonner.</span></li>';
    }

    let dragItem = null;
    list.querySelectorAll('.zoneOrderItem[draggable="true"]').forEach(item => {
      item.addEventListener('dragstart', () => {
        dragItem = item;
        item.classList.add('dragging');
      });
      item.addEventListener('dragend', () => {
        item.classList.remove('dragging');
        dragItem = null;
      });
      item.addEventListener('dragover', (ev) => {
        ev.preventDefault();
        if(!dragItem || dragItem === item) return;
        const rect = item.getBoundingClientRect();
        const before = (ev.clientY - rect.top) < (rect.height / 2);
        if(before){
          item.parentNode.insertBefore(dragItem, item);
        }else{
          item.parentNode.insertBefore(dragItem, item.nextSibling);
        }
      });
    });

    modal.style.display = 'flex';
    modal.setAttribute('aria-hidden', 'false');
  }

  function closeZoneOrderModal(){
    const modal = document.getElementById('zoneOrderModal');
    if(!modal) return;
    modal.style.display = 'none';
    modal.setAttribute('aria-hidden', 'true');
  }

  function applyZoneOrder(){
    const list = document.getElementById('zoneOrderList');
    if(!list) return;
    const zones = getUniqueZoneBlocks();
    if(!zones.length) return closeZoneOrderModal();

    const firstPageBlocks = document.querySelector('.reportPages .page--report .reportBlocks');
    if(!firstPageBlocks) return closeZoneOrderModal();

    const anchor = Array.from(firstPageBlocks.querySelectorAll('.reportBlock')).find(b => !b.classList.contains('zoneBlock')) || null;
    const byId = new Map();
    zones.forEach(zone => byId.set((zone.getAttribute('data-zone-id') || '').trim(), zone));

    const orderedIds = Array.from(list.querySelectorAll('.zoneOrderItem[draggable="true"]'))
      .map(el => (el.getAttribute('data-zone-id') || '').trim())
      .filter(Boolean);

    const seen = new Set();
    orderedIds.forEach(id => {
      if(seen.has(id) || !byId.has(id)) return;
      seen.add(id);
      const zone = byId.get(id);
      if(anchor){
        firstPageBlocks.insertBefore(zone, anchor);
      }else{
        firstPageBlocks.appendChild(zone);
      }
    });

    byId.forEach((zone, id) => {
      if(seen.has(id)) return;
      if(anchor){
        firstPageBlocks.insertBefore(zone, anchor);
      }else{
        firstPageBlocks.appendChild(zone);
      }
    });

    closeZoneOrderModal();
    if(window.repaginateReport){ window.repaginateReport(); }
  }

  document.addEventListener('click', (e) => {
    if(e.target.closest('#btnZoneOrder')){
      openZoneOrderModal();
      return;
    }
    if(e.target.closest('#btnZoneOrderClose') || e.target.closest('#btnZoneOrderCancel')){
      closeZoneOrderModal();
      return;
    }
    if(e.target.closest('#btnZoneOrderApply')){
      applyZoneOrder();
      return;
    }
    if(e.target.classList && e.target.classList.contains('zoneOrderModal')){
      closeZoneOrderModal();
    }
  });
})();
"""


DRAGGABLE_IMAGES_JS = r"""
(function(){
  function ensureThumbWrapper(imgSrc){
    return `<span class="thumbAWrap" data-thumb draggable="true"><a class="thumbA" href="${imgSrc}" target="_blank" rel="noopener"><img class="thumb" src="${imgSrc}" alt="" /></a><button type="button" class="thumbRemove noPrint" title="Supprimer">×</button><span class="thumbHandle" title="Redimensionner"></span></span>`;
  }

  function attachResizeBehavior(wrap){
    if(!wrap || wrap.dataset.resizeReady === '1') return;
    wrap.dataset.resizeReady = '1';
    const handle = wrap.querySelector('.thumbHandle');
    const img = wrap.querySelector('.thumb');
    if(!handle || !img) return;

    handle.addEventListener('pointerdown', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const startX = e.clientX;
      const startWidth = img.getBoundingClientRect().width || 160;
      wrap.classList.add('resizing');

      const onMove = (ev) => {
        const nextWidth = Math.min(520, Math.max(70, startWidth + (ev.clientX - startX)));
        img.style.width = `${nextWidth}px`;
        img.style.height = 'auto';
      };

      const onUp = () => {
        wrap.classList.remove('resizing');
        document.removeEventListener('pointermove', onMove);
        document.removeEventListener('pointerup', onUp);
      };

      document.addEventListener('pointermove', onMove);
      document.addEventListener('pointerup', onUp);
    });
  }

  function initGallery(gallery){
    if(!gallery) return;
    gallery.querySelectorAll('.thumbAWrap').forEach(wrap => {
      wrap.setAttribute('draggable', 'true');
      attachResizeBehavior(wrap);
    });
    if(gallery.dataset.dragReady === '1') return;
    gallery.dataset.dragReady = '1';

    let dragEl = null;

    gallery.addEventListener('dragstart', (e) => {
      if(e.target.closest('.thumbHandle')){
        e.preventDefault();
        return;
      }
      const wrap = e.target.closest('.thumbAWrap');
      if(!wrap || wrap.classList.contains('resizing')) return;
      dragEl = wrap;
      wrap.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', 'thumb');
    });

    gallery.addEventListener('dragend', () => {
      if(dragEl){ dragEl.classList.remove('dragging'); }
      dragEl = null;
    });

    gallery.addEventListener('dragover', (e) => {
      if(!dragEl) return;
      e.preventDefault();
      const over = e.target.closest('.thumbAWrap');
      if(!over || over === dragEl) return;
      const rect = over.getBoundingClientRect();
      const before = e.clientX < (rect.left + rect.width / 2);
      gallery.insertBefore(dragEl, before ? over : over.nextSibling);
    });

    gallery.addEventListener('click', (e) => {
      const removeBtn = e.target.closest('.thumbRemove');
      if(!removeBtn) return;
      const wrap = removeBtn.closest('.thumbAWrap');
      if(wrap){ wrap.remove(); }
    });
  }

  function ensureRowGallery(cell){
    let gallery = cell.querySelector('.thumbs[data-gallery]');
    if(!gallery){
      gallery = document.createElement('div');
      gallery.className = 'thumbs';
      gallery.setAttribute('data-gallery', '1');
      const comment = cell.querySelector('.commentText');
      if(comment && comment.nextSibling){
        comment.parentNode.insertBefore(gallery, comment.nextSibling);
      }else{
        cell.appendChild(gallery);
      }
    }
    initGallery(gallery);
    return gallery;
  }

  function setupImageButtons(){
    document.querySelectorAll('.colComment').forEach(cell => {
      const btn = cell.querySelector('.btnAddImage');
      const input = cell.querySelector('.imageInput');
      if(!btn || !input || btn.dataset.ready === '1') return;
      btn.dataset.ready = '1';
      btn.addEventListener('click', () => input.click());
      input.addEventListener('change', (e) => {
        const files = Array.from(e.target.files || []).filter(f => f.type.startsWith('image/'));
        if(!files.length) return;
        const gallery = ensureRowGallery(cell);
        files.forEach(file => {
          const reader = new FileReader();
          reader.onload = () => {
            const src = String(reader.result || '');
            if(!src) return;
            gallery.insertAdjacentHTML('beforeend', ensureThumbWrapper(src));
            const inserted = gallery.lastElementChild;
            if(inserted){ attachResizeBehavior(inserted); }
          };
          reader.readAsDataURL(file);
        });
        input.value = '';
      });
    });
  }

  window.enableDraggableThumbs = function(){
    document.querySelectorAll('.thumbs').forEach(initGallery);
    setupImageButtons();
  };

  window.addEventListener('load', () => {
    window.enableDraggableThumbs();
  });
})();
"""


PRINT_OPTIMIZE_JS = r"""
(function(){
  function optimizeWhitespaceForPrint(){
    if(document.body.classList.contains('constraint-off-printAutoOptimize')){ return; }
    if(document.body.classList.contains('printPreviewMode')){ return; }
    document.body.classList.add('printOptimized');
    if(window.repaginateReport){
      window.repaginateReport();
    }
  }
  function restoreAfterPrint(){
    if(document.body.classList.contains('constraint-off-printAutoOptimize')){ return; }
    if(document.body.classList.contains('printPreviewMode')){ return; }
    document.body.classList.remove('printOptimized');
    if(window.repaginateReport){
      window.repaginateReport();
    }
  }

  window.addEventListener('beforeprint', optimizeWhitespaceForPrint);
  window.addEventListener('afterprint', restoreAfterPrint);
})();
"""

PAGINATION_JS = r"""
(function(){
  function px(value){
    const n = parseFloat(value || "0");
    return Number.isNaN(n) ? 0 : n;
  }

  function calcAvailable(page, includePresence){
    const pageContent = page.querySelector('.pageContent');
    const footer = page.querySelector('.docFooter');
    const header = page.querySelector('.reportHeader');
    const presence = page.querySelector('.presenceWrap');
    const pageRect = page.getBoundingClientRect();
    if(!pageContent) return pageRect.height;
    const styles = window.getComputedStyle(pageContent);
    let available = pageRect.height - px(styles.paddingTop) - px(styles.paddingBottom);
    const reserveFooter = !document.body.classList.contains('constraint-off-footerReserve');
    const rootStyles = getComputedStyle(document.documentElement);
    const reserveFactorRaw = parseFloat((rootStyles.getPropertyValue('--footer-reserve-factor') || '1').trim());
    const reserveFactor = Number.isNaN(reserveFactorRaw) ? 1 : reserveFactorRaw;
    if(reserveFooter && footer){ available -= (footer.getBoundingClientRect().height * reserveFactor); }
    if(header){ available -= header.getBoundingClientRect().height; }
    if(includePresence && presence){ available -= presence.getBoundingClientRect().height; }
    return available;
  }

  function clearExtraPages(container){
    const pages = Array.from(container.querySelectorAll('.page--report'));
    pages.slice(1).forEach(page => page.remove());
  }

  function mergeZoneBlocks(container){
    const zones = Array.from(container.querySelectorAll('.zoneBlock'));
    const grouped = new Map();
    zones.forEach(zone => {
      const key = zone.getAttribute('data-zone-id') || '';
      if(!grouped.has(key)){ grouped.set(key, []); }
      grouped.get(key).push(zone);
    });
    grouped.forEach(group => {
      if(group.length < 2){ return; }
      const target = group[0];
      const targetBody = target.querySelector('tbody');
      if(!targetBody){ return; }
      group.slice(1).forEach(zone => {
        const body = zone.querySelector('tbody');
        if(body){
          Array.from(body.children).forEach(row => targetBody.appendChild(row));
        }
        zone.remove();
      });
    });
  }

  function mergeAdjacentZoneChunksOnSamePage(container){
    const reportBlocks = Array.from(container.querySelectorAll('.page--report .reportBlocks'));
    reportBlocks.forEach(blocks => {
      let prevZone = null;
      Array.from(blocks.children).forEach(node => {
        if(!(node instanceof Element)) return;
        if(!node.classList.contains('zoneBlock')){
          prevZone = null;
          return;
        }
        if(!prevZone){
          prevZone = node;
          return;
        }
        const prevKey = prevZone.getAttribute('data-zone-id') || '';
        const curKey = node.getAttribute('data-zone-id') || '';
        if(prevKey && curKey && prevKey === curKey){
          const prevBody = prevZone.querySelector('tbody');
          const body = node.querySelector('tbody');
          if(prevBody && body){
            Array.from(body.children).forEach(row => prevBody.appendChild(row));
          }
          node.remove();
          return;
        }
        prevZone = node;
      });
    });
  }

  function getZoneSplitData(zone){
    const title = zone.querySelector('.zoneTitle');
    const table = zone.querySelector('table.crTable');
    const tbody = table?.querySelector('tbody');
    const rows = tbody ? Array.from(tbody.children) : [];
    const rowHeights = rows.map(row => row.getBoundingClientRect().height || row.offsetHeight || 0);
    const tableRect = table?.getBoundingClientRect().height || table?.offsetHeight || 0;
    const rowsSum = rowHeights.reduce((sum, h) => sum + h, 0);
    const tableOverhead = Math.max(0, tableRect - rowsSum);
    const titleHeight = title?.getBoundingClientRect().height || title?.offsetHeight || 0;
    return {rows, rowHeights, tableOverhead, titleHeight};
  }

  function cloneZoneShell(zone){
    const clone = zone.cloneNode(true);
    const tbody = clone.querySelector('tbody');
    if(tbody){ tbody.innerHTML = ''; }
    return clone;
  }

  function buildZoneChunk(zone, data, startIndex, maxHeight){
    const {rows, rowHeights, tableOverhead, titleHeight} = data;
    const total = rows.length;
    let height = titleHeight + tableOverhead;
    let endIndex = startIndex;
    while(endIndex < total){
      const rowHeight = rowHeights[endIndex] || 0;
      if(endIndex > startIndex && height + rowHeight > maxHeight){ break; }
      height += rowHeight;
      endIndex += 1;
      if(endIndex === startIndex + 1 && height > maxHeight){ break; }
    }
    const keepSessionHeaderWithNext = !document.body.classList.contains('constraint-off-keepSessionHeaderWithNext');
    if(keepSessionHeaderWithNext && endIndex < total){
      while(endIndex > startIndex + 1 && rows[endIndex - 1]?.classList.contains('sessionSubRow')){
        endIndex -= 1;
      }
    }
    if(endIndex === startIndex && rows[startIndex]?.classList.contains('sessionSubRow') && startIndex + 1 < total){
      endIndex = Math.min(startIndex + 2, total);
    }
    height = titleHeight + tableOverhead;
    for(let i=startIndex;i<endIndex;i++){
      height += rowHeights[i] || 0;
    }
    const chunk = cloneZoneShell(zone);
    const tbody = chunk.querySelector('tbody');
    for(let i=startIndex;i<endIndex;i++){
      tbody.appendChild(rows[i]);
    }
    return {chunk, nextIndex: endIndex, height};
  }

  function updatePageNumbers(){
    const pages = Array.from(document.querySelectorAll('.wrap .page'));
    const total = pages.length;
    pages.forEach((page, idx) => {
      const num = page.querySelector('.pageNum');
      if(!num) return;
      num.textContent = idx === 0 ? '' : `${idx + 1}/${total}`;
    });
  }

  function paginate(){
    const container = document.querySelector('.reportPages');
    const firstPage = container?.querySelector('.page--report');
    if(!container || !firstPage) return;
    const blocksContainer = firstPage.querySelector('.reportBlocks');
    if(!blocksContainer) return;
    mergeZoneBlocks(container);
    const blocks = Array.from(container.querySelectorAll('.reportBlock')).map(block => ({
      node: block,
      height: block.getBoundingClientRect().height || block.offsetHeight || 0,
      splitData: block.classList.contains('zoneBlock') ? getZoneSplitData(block) : null,
    }));

    blocks.forEach(({node}) => node.remove());
    clearExtraPages(container);

    let currentPage = firstPage;
    let currentBlocks = blocksContainer;
    let available = calcAvailable(currentPage, true);
    let used = 0;
    const template = document.getElementById('report-page-template');

    blocks.forEach(({node, height, splitData}) => {
      if(splitData && splitData.rows.length){
        let rowIndex = 0;
        while(rowIndex < splitData.rows.length){
          const remaining = available - used;
          if(remaining <= splitData.titleHeight + splitData.tableOverhead && template && used > 0){
            const clone = template.content.firstElementChild.cloneNode(true);
            container.appendChild(clone);
            currentPage = clone;
            currentBlocks = clone.querySelector('.reportBlocks');
            available = calcAvailable(currentPage, false);
            used = 0;
          }
          const maxHeight = Math.max(available - used, splitData.titleHeight + splitData.tableOverhead);
          const {chunk, nextIndex, height: chunkHeight} = buildZoneChunk(node, splitData, rowIndex, maxHeight);
          if(used > 0 && used + chunkHeight > available && template){
            const clone = template.content.firstElementChild.cloneNode(true);
            container.appendChild(clone);
            currentPage = clone;
            currentBlocks = clone.querySelector('.reportBlocks');
            available = calcAvailable(currentPage, false);
            used = 0;
          }
          currentBlocks.appendChild(chunk);
          const actualHeight = chunk.getBoundingClientRect().height || chunkHeight;
          used += actualHeight;
          rowIndex = nextIndex;
        }
        return;
      }
      if(used > 0 && used + height > available && template){
        const clone = template.content.firstElementChild.cloneNode(true);
        container.appendChild(clone);
        currentPage = clone;
        currentBlocks = clone.querySelector('.reportBlocks');
        available = calcAvailable(currentPage, false);
        used = 0;
      }
      currentBlocks.appendChild(node);
      const actualHeight = node.getBoundingClientRect().height || height;
      used += actualHeight;
    });

    mergeAdjacentZoneChunksOnSamePage(container);
    updatePageNumbers();
  }

  window.repaginateReport = paginate;
  window.refreshPagination = function(){
    if(!window.repaginateReport){ return; }
    requestAnimationFrame(() => window.repaginateReport());
  };
  window.addEventListener('load', () => {
    requestAnimationFrame(paginate);
  });
  window.addEventListener('resize', () => {
    clearTimeout(window.__repaginateTimer);
    window.__repaginateTimer = setTimeout(paginate, 200);
  });
})();
"""

ROW_CONTROL_JS = r"""
(function(){
  const hiddenSet = new Set();

  function rowById(id){ return document.querySelector(`tr[data-row-id="${id}"]`); }

  function syncSessionHeaders(){
    document.querySelectorAll('.crTable tbody').forEach(tbody => {
      const rows = Array.from(tbody.querySelectorAll('tr'));
      for(let i=0;i<rows.length;i++){
        const r = rows[i];
        if(!r.classList.contains('sessionSubRow')) continue;
        let hasVisible = false;
        for(let j=i+1;j<rows.length;j++){
          const n = rows[j];
          if(n.classList.contains('sessionSubRow')) break;
          if(n.classList.contains('rowItem') && !n.classList.contains('rowHidden')){ hasVisible = true; break; }
        }
        r.classList.toggle('rowHidden', !hasVisible);
      }
    });
  }

  function syncZoneVisibility(){
    document.querySelectorAll('.zoneBlock').forEach(zone => {
      const visibleItems = zone.querySelectorAll('tr.rowItem:not(.rowHidden)');
      zone.classList.toggle('rowHidden', visibleItems.length === 0);
    });
  }

  function refreshHiddenSelect(){
    const sel = document.getElementById('hiddenRowsSelect');
    const tools = document.getElementById('hiddenRowsTools');
    if(!sel) return;
    if(tools){ tools.style.display = hiddenSet.size ? 'flex' : 'none'; }
    const current = sel.value || "";
    sel.innerHTML = '<option value="">Lignes masquées…</option>';
    Array.from(hiddenSet).sort().forEach(id => {
      const row = rowById(id);
      const title = row ? (row.querySelector('.commentText')?.textContent || id) : id;
      const opt = document.createElement('option');
      opt.value = id;
      opt.textContent = title.trim().slice(0, 90);
      sel.appendChild(opt);
    });
    if(current && hiddenSet.has(current)){ sel.value = current; }
  }

  function setRowVisibility(id, visible){
    const row = rowById(id);
    if(!row) return;
    row.classList.toggle('noPrintRow', !visible);
    row.classList.toggle('rowHidden', !visible);
    if(visible){ hiddenSet.delete(id); }
    else{ hiddenSet.add(id); }
    const cb = row.querySelector('.rowToggle');
    if(cb){ cb.checked = visible; }
    refreshHiddenSelect();
    syncSessionHeaders();
    syncZoneVisibility();
    if(window.repaginateReport){ window.repaginateReport(); }
  }

  document.addEventListener('change', (e) => {
    const cb = e.target.closest('.rowToggle');
    if(!cb) return;
    const target = cb.dataset.target || "";
    if(!target) return;
    setRowVisibility(target, !!cb.checked);
  });

  window.restoreSelectedRow = function(){
    const sel = document.getElementById('hiddenRowsSelect');
    if(!sel || !sel.value) return;
    setRowVisibility(sel.value, true);
  };

  window.restoreAllHiddenRows = function(){
    Array.from(hiddenSet).forEach(id => setRowVisibility(id, true));
    syncSessionHeaders();
    syncZoneVisibility();
    if(window.repaginateReport){ window.repaginateReport(); }
  };

  syncSessionHeaders();
  syncZoneVisibility();
  refreshHiddenSelect();
})();
"""



# -------------------------
# HOME (selector)
# -------------------------
def render_home(project: Optional[str] = None, print_mode: bool = False) -> str:
    """
    Page d'accueil : choix projet + réunion + boutons Générer / Imprimable.
    (Important) Toute la JS doit rester dans la string HTML -> sinon SyntaxError Python.
    """
    m = get_meetings().copy()
    m[M_COL_PROJECT_TITLE] = m[M_COL_PROJECT_TITLE].fillna("").astype(str).str.strip()
    m = m.loc[m[M_COL_PROJECT_TITLE] != ""].copy()

    projects = sorted(m[M_COL_PROJECT_TITLE].unique().tolist(), key=lambda x: x.lower())
    if project:
        m = m.loc[m[M_COL_PROJECT_TITLE] == project].copy()

    m["__date__"] = m[M_COL_DATE].apply(_parse_date_any)
    m = m.sort_values("__date__", ascending=False)

    project_opts = "".join(
        f'<option value="{_escape(p)}" {"selected" if p==project else ""}>{_escape(p)}</option>'
        for p in projects
    )

    meeting_opts = ""
    for _, r in m.iterrows():
        mid = str(r.get(M_COL_ID, "")).strip()
        d = _parse_date_any(r.get(M_COL_DATE))
        d_txt = _fmt_date(d) or _escape(r.get(M_COL_DATE_DISPLAY, "")) or _escape(r.get(M_COL_DATE, ""))
        proj = project or str(r.get(M_COL_PROJECT_TITLE, "")).strip()
        meeting_opts += f'<option value="{_escape(mid)}">{_escape(d_txt)} — {_escape(proj)}</option>'

    tempo_logo = _logo_data_url(LOGO_TEMPO_PATH)
    logo_html = f"<img src='{tempo_logo}' alt='TEMPO' class='homeLogo' />" if tempo_logo else "<div class='homeLogoText'>TEMPO</div>"
    home_script = r"""
function onProjectChange(){
  const p = document.getElementById('project').value || "";
  const url = p ? `/?project=${encodeURIComponent(p)}` : "/";
  window.location.href = url;
}

function openCR(){
  const meetingEl = document.getElementById('meeting');
  const projectEl = document.getElementById('project');
  if(!meetingEl){ alert("Champ réunion introuvable"); return; }
  const mid = meetingEl.value || "";
  if(!mid){ alert("Choisis une réunion."); return; }
  const p = projectEl ? (projectEl.value || "") : "";
  const url = `/cr?meeting_id=${encodeURIComponent(mid)}&project=${encodeURIComponent(p)}&print=1`;
  window.location.href = url;
}

function renderRows(targetId, rows, leftKey, rightKey){
  const box = document.getElementById(targetId);
  if(!box) return;
  if(!rows || !rows.length){ box.innerHTML = '<div class="empty">Aucune donnée.</div>'; return; }
  box.innerHTML = rows.map(r => {
    const logo = (r.logo || '').trim();
    const logoHtml = /^https?:\/\//i.test(logo) ? `<img class="coMini" src="${logo}" alt="" loading="lazy"/>` : '';
    return `<div class="row"><div class="rowMain">${logoHtml}<span>${r[leftKey]||''}</span></div><strong>${r[rightKey]||0}</strong></div>`;
  }).join('');
}

function currentTimelineView(){
  return document.getElementById('timelineViewMode')?.value || 'frise';
}

function renderDashboardView(data){
  if(currentTimelineView() === 'calendar') renderCalendar(data);
  else renderTimeline(data);
}

function fillSelect(id, options, selectedValue, allLabel){
  const el = document.getElementById(id);
  if(!el) return;
  const base = `<option value="">${allLabel}</option>`;
  const opts = (options || []).map(v => `<option value="${v}" ${v===selectedValue?'selected':''}>${v}</option>`).join('');
  el.innerHTML = base + opts;
}

function _addDaysIso(iso, days){
  if(!iso) return "";
  const d = new Date(iso + "T00:00:00");
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0,10);
}

function timelineTicks(startIso, endIso, zoomMode, pxPerDay){
  const out = [];
  if(!startIso || !endIso) return out;
  let cur = new Date(startIso + "T00:00:00");
  const end = new Date(endIso + "T00:00:00");
  const oneDay = 86400000;

  if(zoomMode === 'year'){
    cur = new Date(cur.getFullYear(), 0, 1);
    while(cur <= end){
      const next = new Date(cur.getFullYear()+1, 0, 1);
      out.push({ iso: cur.toISOString().slice(0,10), next_iso: next.toISOString().slice(0,10), label: String(cur.getFullYear()) });
      cur = next;
    }
    return out;
  }

  if(zoomMode === 'month'){
    cur = new Date(cur.getFullYear(), cur.getMonth(), 1);
    while(cur <= end){
      const next = new Date(cur.getFullYear(), cur.getMonth()+1, 1);
      out.push({ iso: cur.toISOString().slice(0,10), next_iso: next.toISOString().slice(0,10), label: cur.toLocaleDateString('fr-FR',{month:'short',year:'2-digit'}) });
      cur = next;
    }
    return out;
  }

  if(zoomMode === 'week'){
    const day = cur.getDay();
    const diff = (day === 0 ? -6 : 1 - day);
    cur.setDate(cur.getDate() + diff);
    while(cur <= end){
      const next = new Date(cur.getTime() + 7*oneDay);
      const weekLabel = `S${Math.ceil((((cur - new Date(cur.getFullYear(),0,1)) / oneDay) + new Date(cur.getFullYear(),0,1).getDay()+1)/7)}`;
      out.push({ iso: cur.toISOString().slice(0,10), next_iso: next.toISOString().slice(0,10), label: `${weekLabel} ${String(cur.getFullYear()).slice(2)}` });
      cur = next;
    }
    return out;
  }

  // day mode: adapt step to avoid crushed labels
  const targetPx = 70;
  const dayStep = Math.max(1, Math.ceil(targetPx / Math.max(8, pxPerDay || 22)));
  while(cur <= end){
    const next = new Date(cur.getTime() + dayStep*oneDay);
    out.push({ iso: cur.toISOString().slice(0,10), next_iso: next.toISOString().slice(0,10), label: cur.toLocaleDateString('fr-FR',{day:'2-digit',month:'2-digit'}) });
    cur = next;
  }
  return out;
}

function applyTimelineFocus(){}

function getZoomLevel(){
  const el = document.getElementById('timelineScale');
  return Math.max(0, Math.min(3, Number(el?.value || 1)));
}

function zoomModeLabel(level){
  return ['année','mois','semaine','jour'][level] || 'semaine';
}

function zoomPxPerDay(level){
  return [0.22, 2, 7, 22][level] || 7;
}

function syncZoomLabel(){
  const level = getZoomLevel();
  const label = document.getElementById('timelineScaleLabel');
  if(label){ label.textContent = `Échelle: ${zoomModeLabel(level)}`; }
}

function bumpZoom(delta){
  const el = document.getElementById('timelineScale');
  if(!el) return;
  const next = Math.max(0, Math.min(3, Number(el.value || 1) + delta));
  el.value = String(next);
  syncZoomLabel();
  renderDashboardView(window.__homeDashboardData || null);
}

function onScaleChange(){
  syncZoomLabel();
  renderDashboardView(window.__homeDashboardData || null);
}

function enableTimelineDragScroll(){
  const viewport = document.getElementById('timelineViewport');
  if(!viewport || viewport.dataset.dragBound === '1') return;
  viewport.dataset.dragBound = '1';
  let down = false;
  let startX = 0;
  let startLeft = 0;
  viewport.addEventListener('mousedown', (e) => {
    if(e.target.closest('.gBar,[data-drawer-close],.timelineSplitter,button,a,input,select,label')) return;
    down = true;
    startX = e.pageX;
    startLeft = viewport.scrollLeft;
    viewport.classList.add('dragging');
  });
  window.addEventListener('mouseup', () => { down = false; viewport.classList.remove('dragging'); });
  viewport.addEventListener('mouseleave', () => { down = false; viewport.classList.remove('dragging'); });
  viewport.addEventListener('mousemove', (e) => {
    if(!down) return;
    const dx = e.pageX - startX;
    viewport.scrollLeft = startLeft - dx;
  });
}

function escHtml(v){
  return String(v ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'", '&#39;');
}

function drawerValue(v){
  const txt = String(v ?? '').trim();
  if(!txt) return '—';
  const low = txt.toLowerCase();
  if(low === 'nan' || low === 'null' || low === 'none' || low === 'undefined') return '—';
  return txt;
}

function closeTimelineDrawer(){
  const overlay = document.getElementById('taskDrawerOverlay');
  if(!overlay) return;
  overlay.classList.remove('open');
  overlay.setAttribute('aria-hidden', 'true');
  document.body.classList.remove('drawerOpen');
}

function drawerTimeSignal(task){
  const refIso = (window.__homeDashboardData?.reference_date || '').trim();
  const endIso = String(task.end || '').trim();
  const statusRaw = String(task.status || '').trim().toLowerCase();
  const isClosed = task.completed === true || task.completed === 'true' || statusRaw === 'clos';
  if(!refIso || !endIso) return '—';
  const ref = new Date(refIso + 'T00:00:00');
  const end = new Date(endIso + 'T00:00:00');
  if(isNaN(ref) || isNaN(end)) return '—';
  const diff = Math.ceil((end - ref)/86400000);
  if(diff < 0 && !isClosed) return `🔴 En retard de ${Math.abs(diff)} jour${Math.abs(diff)>1?'s':''}`;
  if(diff > 0) return `🟡 Échéance dans ${diff} jour${diff>1?'s':''}`;
  return isClosed ? '🟢 Traité' : '🟡 Échéance aujourd’hui';
}

function openTimelineDrawer(task){
  const overlay = document.getElementById('taskDrawerOverlay');
  if(!overlay) return;
  const title = drawerValue(task.title);
  const area = drawerValue(task.area);
  const pack = drawerValue(task.package);
  const status = drawerValue(task.status);
  const owner = drawerValue(task.owner);
  const company = drawerValue(task.company);
  const companyLogo = String(task.company_logo || '').trim();
  const startTxt = drawerValue(task.start_txt);
  const endTxt = drawerValue(task.end_txt);
  const comment = drawerValue(task.comment);
  const taskId = drawerValue(task.task_id);
  const subtitle = [area, pack, status].join(' • ');
  const setText = (id, value) => {
    const el = document.getElementById(id);
    if(el) el.textContent = value;
  };
  setText('drawerTaskTitle', title);
  setText('drawerTaskSubline', subtitle);
  setText('drawerTaskTimeSignal', drawerTimeSignal(task));
  setText('drawerTaskTiming', `${startTxt} → ${endTxt}`);
  setText('drawerTaskOwner', owner);
  setText('drawerTaskCompany', company);
  const logoEl = document.getElementById('drawerTaskCompanyLogo');
  if(logoEl){
    if(companyLogo && /^https?:\/\//i.test(companyLogo)){
      logoEl.src = companyLogo;
      logoEl.style.display = 'inline-block';
    } else {
      logoEl.removeAttribute('src');
      logoEl.style.display = 'none';
    }
  }

  const noteBlock = document.getElementById('drawerNoteBlock');
  const noteEl = document.getElementById('drawerTaskComment');
  if(noteBlock && noteEl){
    if(comment === '—'){
      noteBlock.style.display = 'none';
      noteEl.textContent = '';
    } else {
      noteBlock.style.display = '';
      noteEl.textContent = comment;
    }
  }

  const idEl = document.getElementById('drawerTaskId');
  if(idEl) idEl.textContent = taskId;

  overlay.classList.add('open');
  overlay.setAttribute('aria-hidden', 'false');
  document.body.classList.add('drawerOpen');
}

function bindTimelineDrawer(){
  const overlay = document.getElementById('taskDrawerOverlay');
  if(!overlay || overlay.dataset.bound === '1') return;
  overlay.dataset.bound = '1';
  overlay.addEventListener('click', (e) => {
    if(e.target.dataset.drawerClose === '1' || e.target === overlay) closeTimelineDrawer();
  });
  window.addEventListener('keydown', (e) => {
    if(e.key === 'Escape') closeTimelineDrawer();
  });
}

function bindTimelineBarClicks(){
  document.querySelectorAll('.gBar[data-task-id]').forEach((bar) => {
    bar.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      openTimelineDrawer({
        task_id: bar.dataset.taskId,
        title: bar.dataset.taskTitle,
        area: bar.dataset.taskArea,
        package: bar.dataset.taskPackage,
        start_txt: bar.dataset.taskStart,
        end_txt: bar.dataset.taskEnd,
        status: bar.dataset.taskStatus,
        owner: bar.dataset.taskOwner,
        company: bar.dataset.taskCompany,
        company_logo: bar.dataset.taskCompanyLogo,
        comment: bar.dataset.taskComment,
        end: bar.dataset.taskEndIso,
        completed: bar.dataset.taskCompleted,
      });
    });
  });
}


function currentWindowMode(){
  return document.getElementById('timelineWindow')?.value || '3m';
}

function scrollTimelineToDate(dateIso){
  const root = document.getElementById('timelineRoot');
  if(!root || !dateIso) return;
  const startIso = root.dataset.viewStart || root.dataset.start || '';
  const pxPerDay = Number(root.dataset.pxPerDay || 7);
  if(!startIso) return;
  const start = new Date(startIso + 'T00:00:00');
  const target = new Date(dateIso + 'T00:00:00');
  const days = Math.max(0, Math.floor((target - start)/86400000));
  const viewport = document.getElementById('timelineViewport');
  if(!viewport) return;
  viewport.scrollLeft = Math.max(0, days * pxPerDay - (viewport.clientWidth * 0.45));
}

function goToday(){
  const todayIso = new Date().toISOString().slice(0,10);
  scrollTimelineToDate(todayIso);
}

function goMeetingDate(){
  const data = window.__homeDashboardData || {};
  const d = data.reference_date || '';
  if(d) scrollTimelineToDate(d);
}

function goFirstReminder(){
  const data = window.__homeDashboardData || {};
  const timeline = data.timeline || [];
  const first = timeline.find(it => it.status === 'rappel');
  if(first && first.start){ scrollTimelineToDate(first.start); }
}

function setSectionCollapsed(area){
  window.__tlCollapsed = window.__tlCollapsed || {};
  window.__tlCollapsed[area] = !window.__tlCollapsed[area];
  renderDashboardView(window.__homeDashboardData || null);
}

function timelineDisplayState(it){
  if(it.completed) return 'closed';
  const today = new Date(); today.setHours(0,0,0,0);
  const start = new Date((it.start||'') + 'T00:00:00');
  const end = new Date((it.end||'') + 'T00:00:00');
  if(!isNaN(end) && end < today) return 'late';
  if(!isNaN(start) && start > today) return 'future';
  return 'active';
}


const CRITICAL_LATE_DAYS = 10;

function areaRiskStats(items){
  const today = new Date(); today.setHours(0,0,0,0);
  let late = 0;
  let critical = 0;
  let soon = 0;
  items.forEach((i) => {
    const state = timelineDisplayState(i);
    const end = new Date((i.end || '') + 'T00:00:00');
    if(state === 'late'){
      late += 1;
      if(!isNaN(end)){
        const lateDays = Math.ceil((today - end)/86400000);
        if(lateDays > CRITICAL_LATE_DAYS) critical += 1;
      }
      return;
    }
    if(!isNaN(end)){
      const diff = Math.ceil((end - today)/86400000);
      if(diff >= 0 && diff < 5) soon += 1;
    }
  });
  return { late, critical, soon };
}

function areaRiskRank(stats){
  if(stats.critical > 0) return 0;
  if(stats.late > 0) return 1;
  if(stats.soon > 0) return 2;
  return 3;
}

function areaSignalHtml(stats){
  const parts = [];
  if(stats.late > 0) parts.push(`<span class="sig danger">🔴 ${stats.late} retard${stats.late>1?'s':''}</span>`);
  if(stats.critical > 0) parts.push(`<span class="sig critical">⚠ ${stats.critical} critique${stats.critical>1?'s':''}</span>`);
  if(stats.soon > 0) parts.push(`<span class="sig soon">⏳ ${stats.soon} échéance${stats.soon>1?'s':''} &lt;5j</span>`);
  if(!parts.length) parts.push('<span class="sig ok">🟢 OK</span>');
  return parts.join('');
}

function taskTooltip(it){
  const title = (it.title || '').trim() || 'Tâche sans titre';
  const end = new Date((it.end || '') + 'T00:00:00');
  const today = new Date(); today.setHours(0,0,0,0);
  const daysLeft = isNaN(end) ? 'n/a' : Math.ceil((end - today)/86400000);
  const leftTxt = isNaN(end) ? 'n/a' : `${daysLeft}j`;
  return `Titre: ${title}
Zone: ${it.area || 'Général'}
Lot: ${it.package || 'Sans lot'}
Responsable: ${it.owner || 'Non attribué'}
Date début: ${it.start_txt || ''}
Date fin: ${it.end_txt || ''}
Statut: ${it.status || ''}
Jours restants: ${leftTxt}`;
}

const TITLE_COL_KEY = 'metronome_title_column_width';
function getTitleColWidth(){
  const saved = Number(localStorage.getItem(TITLE_COL_KEY) || '320');
  if(Number.isFinite(saved) && saved >= 260 && saved <= 600) return saved;
  return 320;
}
function isResizeEnabled(){
  return window.innerWidth >= 1200;
}
function applyTitleColWidth(px){
  const viewport = document.getElementById('timelineViewport');
  if(!viewport) return;
  const width = isResizeEnabled() ? Math.max(260, Math.min(600, px)) : 300;
  viewport.style.setProperty('--title-col-width', `${width}px`);
  const split = document.getElementById('timelineSplitter');
  if(split){ split.style.left = `${width - 3}px`; }
}
function bindTimelineResizer(){
  const viewport = document.getElementById('timelineViewport');
  const splitter = document.getElementById('timelineSplitter');
  const guide = document.getElementById('timelineSplitGuide');
  if(!viewport || !splitter || !guide) return;
  const enabled = isResizeEnabled();
  splitter.style.display = enabled ? 'block' : 'none';
  if(!enabled){
    applyTitleColWidth(300);
    return;
  }
  applyTitleColWidth(getTitleColWidth());
  if(splitter.dataset.bound === '1') return;
  splitter.dataset.bound = '1';
  let dragging = false;
  splitter.addEventListener('mousedown', (e) => {
    dragging = true;
    viewport.classList.add('resizing');
    guide.style.display = 'block';
    e.preventDefault();
  });
  window.addEventListener('mousemove', (e) => {
    if(!dragging) return;
    const rect = viewport.getBoundingClientRect();
    const next = e.clientX - rect.left;
    const clamped = Math.max(260, Math.min(600, next));
    viewport.style.setProperty('--title-col-width', `${clamped}px`);
    splitter.style.left = `${clamped - 3}px`;
    guide.style.left = `${clamped - 1}px`;
  });
  window.addEventListener('mouseup', () => {
    if(!dragging) return;
    dragging = false;
    viewport.classList.remove('resizing');
    guide.style.display = 'none';
    const current = Number((viewport.style.getPropertyValue('--title-col-width') || '320').replace('px',''));
    if(Number.isFinite(current)) localStorage.setItem(TITLE_COL_KEY, String(current));
  });
}

let __tlTipEl = null;
function ensureTimelineTooltip(){
  if(__tlTipEl) return __tlTipEl;
  __tlTipEl = document.createElement('div');
  __tlTipEl.className = 'tlTooltip';
  document.body.appendChild(__tlTipEl);
  return __tlTipEl;
}
function bindTimelineTooltips(){
  const tip = ensureTimelineTooltip();
  document.querySelectorAll('.gBar[data-tip]').forEach(el => {
    el.addEventListener('mouseenter', () => { tip.textContent = el.dataset.tip || ''; tip.classList.add('show'); });
    el.addEventListener('mousemove', (e) => { tip.style.left = (e.pageX + 14) + 'px'; tip.style.top = (e.pageY + 14) + 'px'; });
    el.addEventListener('mouseleave', () => { tip.classList.remove('show'); });
  });
}

function renderCalendar(data){
  const timelineEl = document.getElementById('timeline');
  if(!timelineEl) return;
  const timeline = (data?.timeline || []).slice().sort((a,b) => String(a.start||'').localeCompare(String(b.start||'')));
  if(!timeline.length){
    timelineEl.innerHTML = '<div class="empty">Aucune donnée calendrier selon les filtres.</div>';
    return;
  }
  const byDay = {};
  timeline.forEach((it) => {
    const key = String(it.start || '').trim();
    if(!key) return;
    if(!byDay[key]) byDay[key] = [];
    byDay[key].push(it);
  });
  const days = Object.keys(byDay).sort((a,b) => a.localeCompare(b));
  timelineEl.innerHTML = `<div class="calendarBoard">${days.map((dayIso) => {
    const items = byDay[dayIso] || [];
    const dayLbl = items[0]?.start_txt || dayIso;
    return `<div class="calDay"><div class="calHead">${dayLbl}</div>${items.map((it) => {
      const cls = it.package_color || 'pkg-default';
      return `<div class="calItem"><span class="calDot ${cls}"></span><span class="calTxt">${it.title || 'Tâche sans titre'}</span></div>`;
    }).join('')}</div>`;
  }).join('')}</div>`;
}

function renderTimeline(data){
  const timelineEl = document.getElementById('timeline');
  if(!timelineEl) return;
  const timeline = data?.timeline || [];
  if(!timeline.length){
    timelineEl.innerHTML = '<div class="empty">Aucun rendu daté selon les filtres.</div>';
    return;
  }

  const zoomLevel = getZoomLevel();
  const zoomMode = ['year','month','week','day'][zoomLevel] || 'week';
  const pxPerDay = zoomPxPerDay(zoomLevel);
  const compact = !!document.getElementById('compactView')?.checked;

  const cal = data?.calendar || {};
  const startIso = cal.start || timeline[0]?.start || '';
  const endIso = cal.end || timeline[timeline.length-1]?.end || startIso;
  const padDays = zoomMode === 'day' ? 8 : (zoomMode === 'week' ? 18 : 30);
  const baseStart = new Date(startIso + 'T00:00:00');
  const baseEnd = new Date(endIso + 'T00:00:00');
  const meetingDate = new Date((data?.reference_date || startIso) + 'T00:00:00');
  meetingDate.setHours(0,0,0,0);
  const today = new Date(); today.setHours(0,0,0,0);
  const windowMode = currentWindowMode();

  let viewStart = new Date(baseStart);
  let viewEnd = new Date(baseEnd);
  if(windowMode === '4w'){
    viewStart = new Date(today); viewStart.setDate(viewStart.getDate() - 7);
    viewEnd = new Date(today); viewEnd.setDate(viewEnd.getDate() + 28);
  } else if(windowMode === '3m'){
    viewStart = new Date(today); viewStart.setDate(viewStart.getDate() - 14);
    viewEnd = new Date(today); viewEnd.setDate(viewEnd.getDate() + 90);
  }
  if(viewStart > baseStart) viewStart = new Date(baseStart);
  if(viewEnd < baseEnd) viewEnd = new Date(baseEnd);
  viewStart.setDate(viewStart.getDate() - padDays);
  viewEnd.setDate(viewEnd.getDate() + padDays);

  const totalDays = Math.max(1, Math.floor((viewEnd - viewStart)/86400000) + 1);
  const totalWidth = Math.max(2200, totalDays * pxPerDay);
  const ticks = timelineTicks(viewStart.toISOString().slice(0,10), viewEnd.toISOString().slice(0,10), zoomMode, pxPerDay);
  const startDate = viewStart;

  const ticksHtml = ticks.map(t => {
    const td = new Date(t.iso + 'T00:00:00');
    const nd = new Date((t.next_iso || t.iso) + 'T00:00:00');
    const offDays = Math.max(0, Math.floor((td - startDate)/86400000));
    const spanDays = Math.max(1, Math.floor((nd - td)/86400000));
    const left = offDays * pxPerDay;
    const width = Math.max(48, spanDays * pxPerDay);
    return `<div class="gTick" style="left:${left}px;width:${width}px"><span>${t.label}</span></div>`;
  }).join('');

  const meetLeft = Math.max(0, Math.floor((meetingDate - startDate)/86400000) * pxPerDay);

  const grouped = {};
  timeline.forEach(it => {
    const k = it.area || 'Général';
    if(!grouped[k]) grouped[k] = [];
    grouped[k].push(it);
  });
  const areas = Object.keys(grouped).sort((a,b) => a.localeCompare(b,'fr'));
  const collapsed = window.__tlCollapsed || {};

  const maxInitial = 200;
  const sourceAreas = areas
    .map(a => ({area:a, items:grouped[a], stats: areaRiskStats(grouped[a])}))
    .sort((a,b) => {
      const ra = areaRiskRank(a.stats);
      const rb = areaRiskRank(b.stats);
      if(ra !== rb) return ra - rb;
      if(a.stats.critical !== b.stats.critical) return b.stats.critical - a.stats.critical;
      if(a.stats.late !== b.stats.late) return b.stats.late - a.stats.late;
      if(a.stats.soon !== b.stats.soon) return b.stats.soon - a.stats.soon;
      return a.area.localeCompare(b.area,'fr');
    });
  const fullCount = timeline.length;
  if(!window.__tlMaxRows || window.__tlMaxRows < maxInitial) window.__tlMaxRows = maxInitial;

  function renderRowsForItems(items){
    return items.map(it => {
      const left = Math.max(0, (Number(it.offset_days || 0) + padDays) * pxPerDay);
      const width = Math.max(18, Number(it.duration_days || 1) * pxPerDay);
      const rawTitle = (it.title || '').trim();
      if(!rawTitle){ console.warn('METRONOME timeline: missing title for task', it); }
      const taskTitle = rawTitle || 'Tâche sans titre';
      const tip = taskTooltip({...it, title: taskTitle});
      const cls = it.package_color || 'pkg-default';
      const dState = timelineDisplayState(it);
      const meetingFx = it.meeting_linked ? 'meetingLinked' : '';
      const end = new Date((it.end || '') + 'T00:00:00');
      const today2 = new Date(); today2.setHours(0,0,0,0);
      const diff = isNaN(end) ? null : Math.ceil((end - today2)/86400000);
      const isLate = dState === 'late';
      const detail = compact
        ? ''
        : `<div class="gMeta"><div>Responsable : ${it.owner || 'Non attribué'}</div><div>Échéance : ${it.end_txt || '-'}</div><div>${isLate ? 'Retard' : 'Restant'} : ${diff===null?'n/a':Math.abs(diff)+'j'}</div></div>`;
      const rowModeCls = compact ? 'compact' : 'detailed';
      return `
        <div class="gRow ${dState} ${rowModeCls}">
          <div class="gItemCol">
            <div class="gTitleLine"><span class="gTitle" title="${taskTitle.replaceAll('"','&quot;')}">${taskTitle}</span></div>
            ${detail}
          </div>
          <div class="gTrack" style="width:${totalWidth}px">
            <div class="gBar ${cls} ${meetingFx}" style="left:${left}px;width:${width}px" data-tip="${tip.replaceAll('"','&quot;')}" data-task-id="${escHtml(it.task_id)}" data-task-title="${escHtml(taskTitle)}" data-task-area="${escHtml(it.area || '')}" data-task-package="${escHtml(it.package || '')}" data-task-start="${escHtml(it.start_txt || '')}" data-task-end="${escHtml(it.end_txt || '')}" data-task-status="${escHtml(it.status || '')}" data-task-owner="${escHtml(it.owner || '')}" data-task-company="${escHtml(it.company || '')}" data-task-company-logo="${escHtml(it.company_logo || '')}" data-task-comment="${escHtml(it.comment || '')}" data-task-end-iso="${escHtml(it.end || '')}" data-task-completed="${String(!!it.completed)}">
              <span class="barTitle">${taskTitle}</span>
            </div>
          </div>
        </div>`;
    }).join('');
  }

  let remaining = window.__tlMaxRows;
  const sectionsHtml = sourceAreas.map(({area, items, stats}) => {
    const isClosed = !!collapsed[area];
    const visibleItems = remaining > 0 ? items.slice(0, remaining) : [];
    remaining -= visibleItems.length;
    const sectionRows = isClosed ? '' : renderRowsForItems(visibleItems);
    const hiddenCount = Math.max(0, items.length - visibleItems.length);
    return `
      <div class="gSection">
        <button class="gSectionHead" type="button" onclick="setSectionCollapsed(decodeURIComponent('${encodeURIComponent(area)}'))">
          <span>${isClosed ? '▸' : '▾'}</span><span>${area}</span><span class="small">${items.length}</span><span class="zoneSignal">${areaSignalHtml(stats)}</span>
        </button>
        ${sectionRows}
        ${(!isClosed && hiddenCount>0) ? `<div class="small" style="padding:4px 10px">+${hiddenCount} tâche(s) masquée(s) (lazy)</div>` : ''}
      </div>`;
  }).join('');

  timelineEl.innerHTML = `
    <div class="gViewport" id="timelineViewport" style="--title-col-width:320px">
      <div class="gTop" id="timelineRoot" data-start="${startIso}" data-end="${endIso}" data-view-start="${viewStart.toISOString().slice(0,10)}" data-px-per-day="${pxPerDay}">
        <div class="gTopLeft">Tâches</div><div class="gTopRight" style="width:${totalWidth}px"><div class="gTicks">${ticksHtml}</div></div>
      </div>
      <div class="gBody"><div id="timelineSplitGuide" class="splitGuide"></div>
        <div class="meetingBg" style="left:${Math.max(0, meetLeft-8)}px"></div><div class="meetingLine" style="left:${meetLeft}px"><span>Réunion</span></div>
        <div class="todayLine" style="left:${Math.max(0, Math.floor((today - startDate)/86400000)*pxPerDay)}px"></div>
        ${sectionsHtml}
      </div>
    <div id="timelineSplitter" class="timelineSplitter" aria-hidden="true"></div></div>
    ${fullCount>window.__tlMaxRows ? `<div class='small' style='margin-top:6px'>Lazy mode actif: ${Math.min(window.__tlMaxRows, fullCount)}/${fullCount} tâches affichées. <button class="btnLite" type="button" onclick="window.__tlMaxRows += 120; renderDashboardView(window.__homeDashboardData || null)">Charger +120</button></div>` : ''}`;

  const viewport = document.getElementById('timelineViewport');
  if(viewport){
    const targetLeft = Math.max(0, meetLeft - (viewport.clientWidth * 0.45));
    viewport.scrollLeft = targetLeft;
  }
  enableTimelineDragScroll();
  bindTimelineTooltips();
  bindTimelineResizer();
  bindTimelineDrawer();
  bindTimelineBarClicks();
}

function _uniqueCompaniesFromDashboard(){
  const data = window.__homeDashboardData || {};
  const fromKpi = (data.kpis?.company_cumulative || []).map(x => String(x.name || '').trim()).filter(Boolean);
  const fromTimeline = (data.timeline || []).map(x => String(x.company || '').trim()).filter(Boolean);
  const merged = [...fromKpi, ...fromTimeline];
  return Array.from(new Set(merged)).sort((a,b) => a.localeCompare(b,'fr'));
}

function toggleMailCompanyMode(){
  const sel = document.getElementById('mailCompanySelect');
  const allChk = document.getElementById('mailAllCompanies');
  if(!sel || !allChk) return;
  sel.disabled = !!allChk.checked;
}

function openCompanyMailModal(){
  const modal = document.getElementById('mailModal');
  const sel = document.getElementById('mailCompanySelect');
  const allChk = document.getElementById('mailAllCompanies');
  if(!modal || !sel) return;
  const companies = _uniqueCompaniesFromDashboard();
  sel.innerHTML = companies.map(c => `<option value="${c}">${c}</option>`).join('');
  if(allChk) allChk.checked = true;
  toggleMailCompanyMode();
  const body = document.getElementById('mailBody');
  const preview = document.getElementById('mailPreview');
  if(body) body.value = '';
  if(preview) preview.srcdoc = '';
  modal.style.display = 'flex';
  generateCompanyMailDraft();
}

function closeCompanyMailModal(){
  const modal = document.getElementById('mailModal');
  if(modal) modal.style.display = 'none';
}

function selectedCompaniesForMail(){
  const sel = document.getElementById('mailCompanySelect');
  const allChk = document.getElementById('mailAllCompanies');
  if(allChk?.checked) return [];
  if(!sel) return [];
  return Array.from(sel.selectedOptions || []).map(o => o.value).filter(Boolean);
}

async function generateCompanyMailDraft(){
  const meeting = document.getElementById('meeting')?.value || '';
  const project = document.getElementById('project')?.value || '';
  if(!meeting){ alert('Choisis une réunion.'); return; }
  const companies = selectedCompaniesForMail();
  const allCompanies = (document.getElementById('mailAllCompanies')?.checked) ? '1' : '0';
  const pStart = document.getElementById('mailPeriodStart')?.value || '';
  const pEnd = document.getElementById('mailPeriodEnd')?.value || '';
  const incTasks = document.getElementById('mailIncludeTasks')?.checked ? '1' : '0';
  const incMemos = document.getElementById('mailIncludeMemos')?.checked ? '1' : '0';
  const incRem = document.getElementById('mailIncludeReminders')?.checked ? '1' : '0';
  const incClosed = document.getElementById('mailIncludeClosed')?.checked ? '1' : '0';
  const incWithoutCreated = document.getElementById('mailIncludeWithoutCreated')?.checked ? '1' : '0';
  const url = `/api/meeting_company_mail_draft?meeting_id=${encodeURIComponent(meeting)}&project=${encodeURIComponent(project)}&selected_companies=${encodeURIComponent(companies.join(','))}&all_companies=${allCompanies}&period_start=${encodeURIComponent(pStart)}&period_end=${encodeURIComponent(pEnd)}&include_tasks=${incTasks}&include_memos=${incMemos}&include_reminders=${incRem}&include_closed=${incClosed}&include_without_created=${incWithoutCreated}`;
  const res = await fetch(url);
  const data = await res.json();
  if(data.error){ alert(data.error); return; }
  const body = document.getElementById('mailBody');
  const preview = document.getElementById('mailPreview');
  if(body) body.value = data.html || '';
  if(preview) preview.srcdoc = data.html || '';
}

async function refreshDashboard(){
  const meeting = document.getElementById('meeting')?.value || '';
  const project = document.getElementById('project')?.value || '';
  const area = document.getElementById('filterArea')?.value || '';
  const pack = document.getElementById('filterPackage')?.value || '';
  const status = document.getElementById('filterStatus')?.value || 'open';
  if(!meeting) return;
  const url = `/api/home_meeting_dashboard?meeting_id=${encodeURIComponent(meeting)}&project=${encodeURIComponent(project)}&area=${encodeURIComponent(area)}&package=${encodeURIComponent(pack)}&status_filter=${encodeURIComponent(status)}`;
  const res = await fetch(url);
  const data = await res.json();
  if(data.error){ console.error(data.error); return; }
  window.__homeDashboardData = data;

  document.getElementById('kpiRem').textContent = data.kpis?.open_reminders ?? 0;
  document.getElementById('kpiFol').textContent = data.kpis?.open_followups ?? 0;
  document.getElementById('kpiDate').textContent = data.reference_date || '-';
  renderRows('companyBox', data.kpis?.company_cumulative || [], 'name', 'count');

  syncZoomLabel();
  renderDashboardView(data);

  fillSelect('filterArea', data.filters?.areas || [], area, 'Toutes les zones');
  fillSelect('filterPackage', data.filters?.packages || [], pack, 'Tous les lots');

  const snap = data.summary_snapshot || {};
  const aiEl = document.getElementById('aiSummary');
  const perims = snap.by_perimeter || [];
  const openByCompany = snap.company_open_reminders || [];
  const closedByCompany = snap.company_closed_reminders || [];
  aiEl.innerHTML = `
    <div><strong>Mini synthèse :</strong> ${snap.open_subjects_total ?? 0} sujet(s) ouvert(s), dont ${snap.reminder_total ?? 0} en rappel.</div>
    ${perims.length ? `<div style="margin-top:8px">${perims.map(p => `<div class="row" style="align-items:flex-start"><div><strong>${p.perimeter}</strong><div class="small">${p.open_subjects} ouverts • ${p.reminders} rappels</div>${(p.reminder_subjects||[]).length ? `<div class="small">Rappels: ${(p.reminder_subjects||[]).join(' • ')}</div>` : ''}</div></div>`).join('')}</div>` : '<div class="empty" style="margin-top:8px">Aucun sujet ouvert selon les filtres.</div>'}
    <div style="margin-top:10px"><strong>Rappels ouverts (entreprises)</strong></div>
    <div>${openByCompany.length ? openByCompany.map(c => `<div class="row"><div class="rowMain">${(/^https?:\/\//i.test(c.logo||'')) ? `<img class="coMini" src="${c.logo}" alt="" loading="lazy"/>` : ''}<span>${c.name||'—'}</span></div><strong>${c.count||0}</strong></div>`).join('') : '<div class="empty">Aucune donnée.</div>'}</div>
    <div style="margin-top:10px"><strong>Rappels fermés (entreprises)</strong></div>
    <div>${closedByCompany.length ? closedByCompany.map(c => `<div class="row"><div class="rowMain">${(/^https?:\/\//i.test(c.logo||'')) ? `<img class="coMini" src="${c.logo}" alt="" loading="lazy"/>` : ''}<span>${c.name||'—'}</span></div><strong>${c.count||0}</strong></div>`).join('') : '<div class="empty">Aucune donnée.</div>'}</div>
  `;
}

window.addEventListener('DOMContentLoaded', () => {
  const allChk = document.getElementById('mailAllCompanies');
  const sel = document.getElementById('mailCompanySelect');
  const pStart = document.getElementById('mailPeriodStart');
  const pEnd = document.getElementById('mailPeriodEnd');
  const incTasks = document.getElementById('mailIncludeTasks');
  const incMemos = document.getElementById('mailIncludeMemos');
  const incRem = document.getElementById('mailIncludeReminders');
  const incClosed = document.getElementById('mailIncludeClosed');
  const incWithoutCreated = document.getElementById('mailIncludeWithoutCreated');
  if(allChk) allChk.addEventListener('change', () => { toggleMailCompanyMode(); generateCompanyMailDraft(); });
  if(sel) sel.addEventListener('change', generateCompanyMailDraft);
  if(pStart) pStart.addEventListener('change', generateCompanyMailDraft);
  if(pEnd) pEnd.addEventListener('change', generateCompanyMailDraft);
  if(incTasks) incTasks.addEventListener('change', generateCompanyMailDraft);
  if(incMemos) incMemos.addEventListener('change', generateCompanyMailDraft);
  if(incRem) incRem.addEventListener('change', generateCompanyMailDraft);
  if(incClosed) incClosed.addEventListener('change', generateCompanyMailDraft);
  if(incWithoutCreated) incWithoutCreated.addEventListener('change', generateCompanyMailDraft);
  refreshDashboard();
});
"""
    return f"""
<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>TEMPO • CR Synthèse</title>
<style>
:root{{--text:#0b1220;--muted:#475569;--border:#e2e8f0;--soft:#f8fafc;--shadow:0 10px 30px rgba(2,6,23,.06);--accent:#0f172a;--ok:#14532d;--warn:#9a3412;--late:#991b1b;}}
*{{box-sizing:border-box}}
body{{margin:0;background:#fff;color:var(--text);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Arial;}}
.wrap{{max-width:1200px;margin:0 auto;padding:26px;display:flex;flex-direction:column;gap:14px}}
.card{{background:#fff;border:1px solid var(--border);border-radius:16px;box-shadow:var(--shadow);padding:16px;}}
.brandline{{display:flex;gap:16px;align-items:center;margin-bottom:12px}}
.homeLogo{{height:44px;width:auto;display:block}}
.homeLogoText{{font-weight:1000;letter-spacing:.18em;font-size:20px}}
.tag{{color:var(--muted);font-weight:800}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}
.grid3{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}}
@media(max-width:900px){{.grid,.grid3{{grid-template-columns:1fr}}}}
label{{display:block;font-weight:900;margin:0 0 6px}}
select{{width:100%;padding:12px 12px;border-radius:12px;border:1px solid var(--border);background:#fff;font-weight:700}}
.btn{{display:inline-flex;align-items:center;justify-content:center;gap:10px;padding:11px 14px;border-radius:12px;border:1px solid var(--border);background:var(--accent);color:#fff;font-weight:950;cursor:pointer;text-decoration:none}}
.btn.secondary{{background:#fff;color:var(--text);font-weight:900}}
.kpi{{border:1px solid var(--border);border-radius:12px;padding:12px;background:#fff}}
.kpi .title{{font-weight:800;color:var(--muted);font-size:12px}}
.kpi .value{{font-weight:1000;font-size:24px;margin-top:6px}}
.listBox{{border:1px solid var(--border);border-radius:12px;padding:10px;background:var(--soft);max-height:230px;overflow:auto}}
.row{{display:flex;justify-content:space-between;gap:10px;padding:7px 0;border-bottom:1px dashed #dbe3ef}}
.rowMain{{display:flex;align-items:center;gap:8px;min-width:0}}
.coMini{{width:18px;height:18px;border-radius:999px;border:1px solid #dbe3ef;object-fit:cover;flex:0 0 auto}}
.row:last-child{{border-bottom:none}}
.badge{{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:900;text-transform:uppercase}}
.b-rappel{{background:#fee2e2;color:var(--late)}}
.b-suivre{{background:#ffedd5;color:var(--warn)}}
.b-clos{{background:#dcfce7;color:var(--ok)}}
.timelineFilters{{display:flex;gap:8px;flex-wrap:wrap}}
.timelineFilters select{{width:auto;min-width:160px;padding:8px 10px}}
.btnLite{{border:1px solid var(--border);background:#fff;border-radius:10px;padding:8px 10px;font-weight:900;cursor:pointer}}
.timelineLegend{{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}}
.lg{{display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;font-size:11px;font-weight:900;border:1px solid rgba(15,23,42,.2)}}
.lg.warn{{background:#fee2e2;color:#991b1b}}
.timelineZoom{{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border:1px solid var(--border);border-radius:10px;background:#fff}}
.timelineZoom button{{border:1px solid var(--border);background:#fff;border-radius:8px;width:26px;height:26px;font-weight:900;cursor:pointer}}
.timelineZoom input[type=range]{{width:110px}}
.timelineZoomLabel{{font-size:12px;font-weight:900;color:var(--muted);min-width:100px}}
.gantt{{border:1px solid var(--border);border-radius:12px;background:#fff;padding:10px;overflow:hidden;position:relative}}
.calendarBoard{{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:10px;max-height:64vh;overflow:auto}}
.calDay{{border:1px solid var(--border);border-radius:10px;padding:8px;background:#fff}}
.calHead{{font-weight:900;margin-bottom:6px}}
.calItem{{display:flex;align-items:flex-start;gap:6px;padding:4px 0}}
.calDot{{width:10px;height:10px;border-radius:999px;margin-top:4px;border:1px solid rgba(15,23,42,.2);flex:0 0 auto}}
.calDot.pkg-cvc{{background:#22d3ee}}
.calDot.pkg-plb{{background:#ff00cc}}
.calDot.pkg-ele{{background:#22c55e}}
.calDot.pkg-goe{{background:#7f1d1d}}
.calDot.pkg-syn{{background:#f59e0b}}
.calDot.pkg-default{{background:#cbd5e1}}
.calTxt{{font-size:13px;line-height:1.3}}
.gViewport{{overflow:auto;max-height:64vh;border:1px solid var(--border);border-radius:10px;scrollbar-gutter:stable both-edges;position:relative;cursor:grab}}
.gViewport.dragging{{cursor:grabbing}}
.timelineSplitter{{position:absolute;top:34px;bottom:0;width:6px;cursor:col-resize;z-index:6;background:transparent;transition:background .15s ease}}
.timelineSplitter:hover{{background:rgba(15,23,42,.08)}}
.splitGuide{{position:absolute;top:0;bottom:0;width:2px;background:rgba(15,23,42,.18);display:none;z-index:6;pointer-events:none}}
.gViewport.resizing .splitGuide{{display:block}}
.gViewport.resizing .timelineSplitter{{background:rgba(15,23,42,.12)}}
@media (max-width:1199px){{.timelineSplitter,.splitGuide{{display:none!important}}.gViewport{{--title-col-width:300px!important}}}}
.gTop{{position:sticky;top:0;z-index:5;background:#fff;display:grid;grid-template-columns:var(--title-col-width,320px) max-content}}
.gTopLeft{{position:sticky;left:0;z-index:6;background:#fff;border-bottom:1px solid var(--border);border-right:1px solid #eef2f7;padding:7px 10px;font-weight:900}}
.gTopRight{{position:relative;height:34px;border-bottom:1px solid var(--border);background:#f8fafc}}
.gTicks{{position:relative;height:100%}}
.gTick{{position:absolute;top:0;bottom:0;border-left:1px solid #cbd5e1;border-right:1px solid #e2e8f0;font-size:11px;font-weight:900;color:#334155;display:flex;align-items:center;justify-content:center;white-space:nowrap;background:rgba(248,250,252,.92);overflow:hidden}}
.gTick span{{padding:0 6px;text-overflow:ellipsis;overflow:hidden}}
.gBody{{position:relative;min-width:max-content}}
.todayLine{{position:absolute;top:0;bottom:0;width:2px;background:#dc2626;opacity:.9;z-index:1}}
.gRow{{display:grid;grid-template-columns:var(--title-col-width,320px) max-content;align-items:stretch;min-height:34px}}
.gItemCol{{position:sticky;left:0;z-index:4;background:#fff;border-right:1px solid #eef2f7;padding:6px 10px;height:100%;display:flex;flex-direction:column;justify-content:center}}
.gTitleLine{{display:flex;align-items:center;gap:6px;min-width:0}}
.gTitle{{font-size:14px;font-weight:600;display:-webkit-box;-webkit-box-orient:vertical;overflow:hidden;line-height:1.2;word-break:break-word}}
.gRow.compact .gTitle{{-webkit-line-clamp:2}}
.gRow.detailed .gTitle{{-webkit-line-clamp:3}}
.gTrack{{position:relative;height:100%;min-height:34px;border-bottom:1px solid #f8fafc;background:repeating-linear-gradient(to right,#fff,#fff 239px,#fcfdff 239px,#fcfdff 240px)}}
.gSection:nth-child(even) .gRow .gTrack{{border-bottom-color:transparent}}
.gSection{{margin-bottom:6px}}
.gSectionHead{{display:flex;align-items:center;gap:8px;width:100%;text-align:left;border:0;background:#f8fafc;border-bottom:1px solid #e2e8f0;padding:7px 10px;font-weight:900;position:sticky;left:0;z-index:6}}
.zoneSignal{{margin-left:auto;font-size:12px;font-weight:900;display:inline-flex;gap:6px;align-items:center;flex-wrap:wrap;justify-content:flex-end}}
.zoneSignal .sig{{display:inline-flex;align-items:center;padding:2px 7px;border-radius:999px;background:#f8fafc;border:1px solid #e2e8f0;color:#334155;white-space:nowrap}}
.zoneSignal .sig.danger{{background:#fff7f7;color:#991b1b;border-color:#fee2e2}}
.zoneSignal .sig.critical{{background:#fffaf0;color:#9a3412;border-color:#ffedd5}}
.zoneSignal .sig.soon{{background:#fffbeb;color:#92400e;border-color:#fef3c7}}
.zoneSignal .sig.ok{{background:#f0fdf4;color:#166534;border-color:#dcfce7}}
.gBar{{position:absolute;min-height:26px;height:26px;top:4px;border-radius:6px;padding:2px 8px;font-size:12px;font-weight:700;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;border:1px solid rgba(15,23,42,.28);color:#0b1220;display:flex;align-items:center;box-shadow:0 1px 4px rgba(15,23,42,.08);transform-origin:center;transition:transform .15s ease, box-shadow .15s ease}}
.gBar:hover{{transform:translateY(-1px);box-shadow:0 4px 10px rgba(15,23,42,.12)}}
.barTitle{{display:inline-block;max-width:100%;overflow:hidden;text-overflow:ellipsis}}
.meetingLinked{{transform:scale(1.03);box-shadow:0 0 0 2px rgba(14,165,233,.25),0 3px 8px rgba(14,165,233,.20)}}
.warnBlink{{display:inline-flex;align-items:center;justify-content:center;width:14px;height:14px;border-radius:999px;background:#ef4444;color:#fff;font-size:10px;font-weight:1000;animation:blinkWarn 1s steps(2,start) infinite}}
.warnBlink.right{{margin-left:auto}}
.tlTooltip{{position:fixed;z-index:99999;max-width:320px;white-space:pre-line;background:#0f172a;color:#fff;padding:8px 10px;border-radius:8px;font-size:12px;line-height:1.35;box-shadow:0 10px 24px rgba(2,6,23,.25);opacity:0;transform:translateY(2px);pointer-events:none;transition:opacity .12s ease, transform .12s ease}}
.tlTooltip.show{{opacity:1;transform:translateY(0)}}
@keyframes blinkWarn{{to{{visibility:hidden}}}}
.gBar.pkg-cvc,.lg.pkg-cvc{{background:#22d3ee}}
.gBar.pkg-plb,.lg.pkg-plb{{background:#ff00cc;color:#fff}}
.gBar.pkg-ele,.lg.pkg-ele{{background:#22c55e;color:#052e16}}
.gBar.pkg-goe,.lg.pkg-goe{{background:#7f1d1d;color:#fff}}
.gBar.pkg-syn,.lg.pkg-syn{{background:#f59e0b;color:#111827}}
.gBar.pkg-default{{background:#cbd5e1}}
.gRow.late .gBar{{outline:2px solid rgba(220,38,38,.72)}}
.gRow.future .gBar{{opacity:.6}}
.gRow.late .gBar:hover{{transform:translateY(-1px);box-shadow:0 8px 16px rgba(220,38,38,.2)}}
.gRow.closed .gBar{{opacity:.3}}
.gRow.closed .barTitle{{text-decoration:line-through}}
.gMeta{{font-size:12px;color:#94a3b8;padding-top:2px;line-height:1.25}}
.meetingBg{{position:absolute;top:0;bottom:0;width:16px;background:rgba(14,165,233,.02);z-index:2}}
.meetingLine{{position:absolute;top:0;bottom:0;width:4px;background:#0ea5e9;z-index:3;box-shadow:0 0 0 1px rgba(14,165,233,.15)}}
.meetingLine span{{position:sticky;top:2px;display:inline-block;transform:translateX(6px);background:#0ea5e9;color:#fff;font-size:10px;font-weight:900;padding:2px 6px;border-radius:999px}}
.small{{font-size:12px;color:var(--muted);font-weight:700}}
.empty{{color:var(--muted);font-style:italic}}
.mailModal{{position:fixed;inset:0;background:rgba(0,0,0,.35);display:none;align-items:center;justify-content:center;z-index:10050}}
.mailPanel{{background:#fff;width:min(1420px,99vw);max-height:97vh;overflow:auto;border:1px solid var(--border);border-radius:14px;box-shadow:0 20px 50px rgba(2,6,23,.2);padding:16px}}
.mailGrid{{display:grid;grid-template-columns:minmax(260px,30%) minmax(700px,70%);gap:14px}}
.mailField{{display:grid;gap:6px}}
.mailField label{{margin:0;font-size:12px;color:var(--muted)}}
.mailPeriodRow{{display:flex;gap:8px;align-items:center;flex-wrap:wrap}}
.mailPeriodRow label{{display:grid;gap:4px;font-size:12px;color:var(--muted)}}
.mailPeriodRow input{{padding:7px 9px;border:1px solid var(--border);border-radius:8px;font:12px/1.3 system-ui,-apple-system,Segoe UI,Roboto,Arial;}}
.mailTypeRow{{display:flex;gap:12px;align-items:center;flex-wrap:wrap}}
.mailTypeRow label{{display:inline-flex;gap:6px;align-items:center;font-size:12px;color:var(--muted)}}
.mailField textarea,.mailField select{{width:100%;padding:10px;border-radius:10px;border:1px solid var(--border);font:13px/1.4 system-ui,-apple-system,Segoe UI,Roboto,Arial;}}
#mailCompanySelect{{height:190px}}
#mailBody{{min-height:520px;font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;white-space:pre;tab-size:2}}
#mailPreview{{width:100%;min-height:640px;border:1px solid var(--border);border-radius:10px;background:#fff}}


.drawerOverlay{{position:fixed;inset:0;background:rgba(15,23,42,.22);z-index:10040;opacity:0;pointer-events:none;transition:opacity .16s ease}}
.drawerOverlay.open{{opacity:1;pointer-events:auto}}
.taskDrawer{{position:fixed;top:0;right:0;height:100vh;width:min(440px,92vw);background:#fff;border-left:1px solid var(--border);box-shadow:-8px 0 28px rgba(2,6,23,.16);transform:translateX(100%);transition:transform .18s ease;display:flex;flex-direction:column}}
.drawerOverlay.open .taskDrawer{{transform:translateX(0)}}
.drawerHead{{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:14px 16px;border-bottom:1px solid var(--border);font-weight:1000}}
.drawerClose{{border:1px solid var(--border);background:#fff;border-radius:10px;width:32px;height:32px;font-size:18px;font-weight:900;cursor:pointer}}
.drawerBody{{padding:16px 18px 14px;overflow:auto;display:grid;gap:12px;background:#fff}}
.drawerTitle{{font-size:18px;line-height:1.28;font-weight:700;margin:0;word-break:break-word;letter-spacing:-.003em}}
.drawerSubline{{font-size:14px;color:#64748b;font-weight:700}}
.drawerTimeSignal{{font-size:14px;font-weight:900;background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:8px 10px;display:inline-flex;max-width:max-content}}
.drawerInfo{{display:grid;gap:6px}}
.drawerLine{{display:flex;gap:10px;align-items:flex-start;flex-wrap:wrap}}
.drawerLine .k{{font-size:12px;color:#64748b;font-weight:900;min-width:86px;text-transform:none;letter-spacing:.01em}}
.drawerLine .v{{font-size:15px;font-weight:700;color:#0f172a;word-break:break-word;white-space:pre-wrap;flex:1}}
.drawerMeta{{font-size:9px;color:#94a3b8;font-weight:600;text-align:right}}
.drawerCoLogo{{width:24px;height:24px;border-radius:999px;object-fit:cover;border:1px solid #e2e8f0;display:none}}
body.drawerOpen{{overflow:hidden}}

</style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="brandline">
        {logo_html}
        <div>
          <div style="font-weight:1000">Compte-rendu • Réunion de synthèse</div>
          <div class="tag">Application TEMPO</div>
        </div>
      </div>

      <div class="grid">
        <div>
          <label>Projet</label>
          <select id="project" onchange="onProjectChange()">
            <option value="">— Choisir —</option>
            {project_opts}
          </select>
        </div>
        <div>
          <label>Réunion</label>
          <select id="meeting" onchange="refreshDashboard()">
            {meeting_opts if meeting_opts else '<option value="">— Sélectionne un projet —</option>'}
          </select>
        </div>
      </div>

      <div style="display:flex;gap:10px;margin-top:14px;flex-wrap:wrap">
        <button class="btn" type="button" onclick="openCR()">Ouvrir le compte-rendu</button><button class="btn secondary" type="button" onclick="openCompanyMailModal()">Préparer mail entreprises</button>
      </div>
    </div>

    <div class="card">
      <div style="font-weight:1000;margin-bottom:10px">KPI réunion sélectionnée</div>
      <div class="grid3">
        <div class="kpi"><div class="title">Rappels ouverts à date</div><div class="value" id="kpiRem">-</div></div>
        <div class="kpi"><div class="title">À suivre ouverts</div><div class="value" id="kpiFol">-</div></div>
        <div class="kpi"><div class="title">Date de référence</div><div class="value" id="kpiDate" style="font-size:18px">-</div></div>
      </div>
      <div style="margin-top:10px">
        <div style="font-weight:900;margin-bottom:6px">Rappels ouverts à date cumulés par entreprise</div>
        <div class="listBox" id="companyBox"><div class="empty">Sélectionnez une réunion.</div></div>
      </div>
    </div>

    <div class="card">
      <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap">
        <div style="font-weight:1000">Calendrier / frise chronologique des rendus</div>
        <div class="timelineFilters">
          <select id="filterArea" onchange="refreshDashboard()"><option value="">Toutes les zones</option></select>
          <select id="filterPackage" onchange="refreshDashboard()"><option value="">Tous les lots</option></select>
          <select id="filterStatus" onchange="refreshDashboard()">
            <option value="open" selected>Sujets ouverts</option>
            <option value="reminders">Rappels uniquement</option>
            <option value="all">Tous</option>
          </select>
          <select id="timelineViewMode" onchange="renderDashboardView(window.__homeDashboardData || null)"><option value="frise" selected>Vue frise chronologique</option><option value="calendar">Vue calendrier</option></select>
          <select id="timelineWindow" onchange="renderDashboardView(window.__homeDashboardData || null)">
            <option value="4w">Prochaines 4 semaines</option>
            <option value="3m" selected>Prochains 3 mois</option>
            <option value="all">Plage complète</option>
          </select>
          <div class="timelineZoom">
            <button type="button" onclick="bumpZoom(-1)">−</button>
            <input id="timelineScale" type="range" min="0" max="3" step="1" value="2" oninput="onScaleChange()" />
            <button type="button" onclick="bumpZoom(1)">+</button>
            <span class="timelineZoomLabel" id="timelineScaleLabel">Échelle: semaine</span>
          </div>
          <button type="button" class="btnLite" onclick="goToday()">Aujourd'hui</button>
          <button type="button" class="btnLite" onclick="goFirstReminder()">Aller aux rappels</button><label class="btnLite" style="display:inline-flex;align-items:center;gap:8px"><input id="compactView" type="checkbox" checked onchange="renderDashboardView(window.__homeDashboardData || null)"/> Vue compacte</label>
        </div>
      </div>
      <div class="timelineLegend"><span class="lg pkg-cvc">CVC</span><span class="lg pkg-plb">PLB</span><span class="lg pkg-ele">ELE/CFA/CFO</span><span class="lg pkg-goe">GOE/STR</span><span class="lg pkg-syn">Synthèse</span><span class="lg warn">! Rappel</span></div><div id="timeline" class="gantt" style="margin-top:10px"><div class="empty">Aucune donnée.</div></div>
    </div>

    <div class="card">
      <div style="font-weight:1000;margin-bottom:8px">Synthèse des sujets ouverts par périmètre</div>
      <div id="aiSummary" class="listBox"><div class="empty">Sélectionnez une réunion.</div></div>
    </div>
  </div>


  <div id="taskDrawerOverlay" class="drawerOverlay" aria-hidden="true">
    <aside class="taskDrawer" role="dialog" aria-modal="true" aria-label="Détail tâche">
      <div class="drawerHead"><span>Détail tâche</span><button type="button" class="drawerClose" data-drawer-close="1" aria-label="Fermer">×</button></div>
      <div class="drawerBody">
        <h2 id="drawerTaskTitle" class="drawerTitle">—</h2>
        <div id="drawerTaskSubline" class="drawerSubline">—</div>
        <div id="drawerTaskTimeSignal" class="drawerTimeSignal">—</div>

        <div class="drawerInfo">
          <div class="drawerLine"><div class="k">Timing</div><div id="drawerTaskTiming" class="v">—</div></div>
          <div class="drawerLine"><div class="k">Responsable</div><div id="drawerTaskOwner" class="v">—</div></div>
          <div class="drawerLine"><div class="k">Entreprise</div><img id="drawerTaskCompanyLogo" class="drawerCoLogo" alt="logo entreprise" /><div id="drawerTaskCompany" class="v">—</div></div>
          <div id="drawerNoteBlock" class="drawerLine"><div class="k">Notes</div><div id="drawerTaskComment" class="v"></div></div>
        </div>

        <div class="drawerMeta">ID: <span id="drawerTaskId">—</span></div>
      </div>
    </aside>
  </div>


  <div class="mailModal" id="mailModal">
    <div class="mailPanel">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px"><div style="font-weight:1000">Mail entreprises</div><button class="btnLite" type="button" onclick="closeCompanyMailModal()">Fermer</button></div>
      <div class="mailGrid">
        <div class="mailField">
          <label><input id="mailAllCompanies" type="checkbox" checked /> Toutes les entreprises (sinon sélection multiple)</label>
          <div class="mailPeriodRow"><label>Du <input id="mailPeriodStart" type="date" /></label><label>Au <input id="mailPeriodEnd" type="date" /></label></div>
          <div class="mailTypeRow"><label><input id="mailIncludeTasks" type="checkbox" checked /> Tâches</label><label><input id="mailIncludeMemos" type="checkbox" checked /> Mémos</label><label><input id="mailIncludeReminders" type="checkbox" checked /> Rappels</label><label><input id="mailIncludeClosed" type="checkbox" checked /> Tâches clôturées</label><label><input id="mailIncludeWithoutCreated" type="checkbox" checked /> Inclure hors réunion (sans écrit le)</label></div>
          <select id="mailCompanySelect" multiple></select>
        </div>
        <div class="mailField">
          <textarea id="mailBody" rows="20" style="display:none"></textarea>
          <label>Aperçu mail</label>
          <iframe id="mailPreview" title="Aperçu email"></iframe>
        </div>
      </div>
    </div>
  </div>

<script>{home_script}</script>

</body>
</html>
"""


def render_missing_data_page(err: MissingDataError) -> str:
    hint = f"Définis la variable d'environnement {err.env_var} pour pointer vers le fichier CSV."
    return f"""
<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Erreur de données — TEMPO</title>
  <style>
    :root{{--text:#0b1220;--muted:#475569;--border:#e2e8f0;--soft:#f8fafc;--shadow:0 10px 30px rgba(2,6,23,.06);--accent:#0f172a;}}
    *{{box-sizing:border-box}}
    body{{margin:0;background:#fff;color:var(--text);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Arial;}}
    .wrap{{max-width:900px;margin:0 auto;padding:26px;}}
    .card{{background:#fff;border:1px solid var(--border);border-radius:16px;box-shadow:var(--shadow);padding:16px;}}
    .title{{font-weight:1000;font-size:20px;margin:0 0 10px 0;}}
    .muted{{color:var(--muted);font-weight:700;}}
    .mono{{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="title">Fichier CSV introuvable</div>
      <div class="muted">Impossible de charger la source de données requise.</div>
      <div style="margin-top:12px">
        <div><strong>Source :</strong> {_escape(err.label)}</div>
        <div><strong>Chemin :</strong> <span class="mono">{_escape(err.path)}</span></div>
      </div>
      <div style="margin-top:12px" class="muted">{_escape(hint)}</div>
    </div>
  </div>
</body>
</html>
"""


# -------------------------
# CR RENDER
# -------------------------
def render_cr(
    meeting_id: str,
    project: str = "",
    print_mode: bool = False,
    pinned_memos: str = "",
    range_start: str = "",
    range_end: str = "",
) -> str:
    mrow = meeting_row(meeting_id)
    meeting_entries = entries_for_meeting(meeting_id)

    project = (project or str(mrow.get(M_COL_PROJECT_TITLE, ""))).strip()
    meet_date = _parse_date_any(mrow.get(M_COL_DATE))
    ref_date = meet_date or date.today()
    date_txt = _fmt_date(meet_date) or _escape(mrow.get(M_COL_DATE_DISPLAY, "")) or _escape(mrow.get(M_COL_DATE, ""))
    range_start_date = _parse_date_any(range_start) if range_start else None
    range_end_date = _parse_date_any(range_end) if range_end else None
    if range_start_date is not None and range_end_date is None:
        range_end_date = ref_date
    range_active = range_start_date is not None or range_end_date is not None
    range_start_value = range_start_date.isoformat() if range_start_date else ""
    range_end_value = range_end_date.isoformat() if range_end_date else ""
    range_ref_date = range_end_date or ref_date if range_active else ref_date
    if range_active:
        project_entries = get_entries().copy()
        project_entries = project_entries.loc[
            project_entries[E_COL_PROJECT_TITLE].fillna("").astype(str).str.strip() == project
        ].copy()
        project_entries = _filter_entries_by_created_range(project_entries, range_start_date, range_end_date)
        edf = pd.concat([project_entries, meeting_entries], ignore_index=False)
        if E_COL_ID in edf.columns:
            edf["__id__"] = _series(edf, E_COL_ID, "").fillna("").astype(str).str.strip()
            edf = edf.loc[~edf["__id__"].duplicated(keep="first")].copy()
        else:
            edf = edf.drop_duplicates()
    else:
        edf = meeting_entries

    pinned_set = {p.strip() for p in str(pinned_memos or "").split(",") if p.strip()}

    # Project header info (Projects.csv)
    pinfo = project_info_by_title(project)
    proj_img = pinfo.get("image", "")
    proj_desc = pinfo.get("desc", "")
    proj_tl = " ".join([x for x in [pinfo.get("start", ""), pinfo.get("end", "")] if x]).strip()
    proj_status = pinfo.get("status", "")

    # Exclude duplicates in "À suivre": tasks already listed in CURRENT meeting only
    current_meeting_entry_ids = set(
        _series(meeting_entries, E_COL_ID, "").fillna("").astype(str).str.strip().tolist()
    )

    att, miss = compute_presence_lists(mrow)
    stats = kpis(mrow, edf, ref_date=range_ref_date)

    # Project-wide reminders / follow-ups
    rem_df = reminders_for_project(
        project_title=project,
        ref_date=range_ref_date,
        max_level=8,
        start_date=range_start_date,
        end_date=range_end_date,
    )
    fol_df = followups_for_project(
        project_title=project,
        ref_date=range_ref_date,
        exclude_entry_ids=current_meeting_entry_ids,
        start_date=range_start_date,
        end_date=range_end_date,
    )

    closed_recent_df = pd.DataFrame()
    project_history = get_entries().copy()
    project_history = project_history.loc[
        project_history[E_COL_PROJECT_TITLE].fillna("").astype(str).str.strip() == project
    ].copy()
    if not project_history.empty:
        edf2 = project_history.copy()
        edf2["__is_task__"] = _series(edf2, E_COL_IS_TASK, False).apply(_bool_true)
        edf2["__completed__"] = _series(edf2, E_COL_COMPLETED, False).apply(_bool_true)
        edf2["__deadline__"] = _series(edf2, E_COL_DEADLINE, None).apply(_parse_date_any)
        edf2["__done__"] = _series(edf2, E_COL_COMPLETED_END, None).apply(_parse_date_any)
        edf2.loc[edf2["__done__"].notna(), "__completed__"] = True
        edf2 = edf2.loc[(edf2["__is_task__"] == True) & (edf2["__completed__"] == True)].copy()
        edf2 = edf2.loc[edf2["__done__"].notna()].copy()
        days_since_done = pd.to_datetime(ref_date) - pd.to_datetime(edf2["__done__"])
        edf2 = edf2.loc[(days_since_done.dt.days >= 0) & (days_since_done.dt.days <= 14)].copy()
        edf2["__reminder__"] = edf2.apply(lambda r: reminder_level_at_done(r.get("__deadline__"), r.get("__done__")), axis=1)
        edf2 = _explode_areas(edf2)
        closed_recent_df = edf2

    closed_recent_ids: set[str] = set()
    if not closed_recent_df.empty:
        closed_recent_ids = set(_series(closed_recent_df, E_COL_ID, "").fillna("").astype(str).str.strip())
        closed_recent_ids.discard("")

    rem_company = reminders_by_company(rem_df)[:12]
    areas = group_meeting_by_area(edf)

    # ensure zones that exist only in reminders/follow-ups are also shown
    extra_zones = (
        set(rem_df["__area_list__"].astype(str).tolist())
        | set(fol_df["__area_list__"].astype(str).tolist())
        | set(closed_recent_df["__area_list__"].astype(str).tolist())
    )
    zone_names = [a for a, _ in areas]
    for z in sorted(extra_zones):
        if z not in zone_names:
            areas.append((z, edf.iloc[0:0].copy()))
    areas.sort(key=lambda x: (0 if x[0].lower() == "général" else 1, x[0].lower()))

    # Meeting labels for grouping rows by séance (notes/mémos/tâches)
    meetings_df = get_meetings().copy()
    if not meetings_df.empty:
        meetings_df = meetings_df.loc[
            meetings_df[M_COL_PROJECT_TITLE].fillna("").astype(str).str.strip() == project
        ].copy()
        meetings_df["__mid__"] = _series(meetings_df, M_COL_ID, "").fillna("").astype(str).str.strip()
        meetings_df["__mdate__"] = _series(meetings_df, M_COL_DATE, None).apply(_parse_date_any)
    meeting_date_by_id: Dict[str, Optional[date]] = {}
    if not meetings_df.empty:
        for _, mr in meetings_df.iterrows():
            mid = str(mr.get("__mid__", "")).strip()
            if not mid:
                continue
            mdate = mr.get("__mdate__")
            meeting_date_by_id[mid] = mdate
    meeting_index, _meeting_total = _meeting_sequence_for_project(meetings_df, meeting_id)
    cr_number_default = f"{meeting_index:02d}"

    # Pinned memos across history (editor helper)
    pinned_df = pd.DataFrame()
    img_col_pinned = None
    if pinned_set:
        pe = get_entries().copy()
        pe = pe.loc[pe[E_COL_PROJECT_TITLE].fillna("").astype(str).str.strip() == project].copy()
        pe["__id__"] = _series(pe, E_COL_ID, "").fillna("").astype(str).str.strip()
        pe = pe.loc[pe["__id__"].isin(pinned_set)].copy()
        pe["__is_task__"] = _series(pe, E_COL_IS_TASK, False).apply(_bool_true)
        pe = pe.loc[pe["__is_task__"] == False].copy()
        pe = _explode_areas(pe)
        pinned_df = pe
        img_col_pinned = detect_images_column(pinned_df)

    # -------------------------
    # Presence table (no KPI block)
    # -------------------------
    kpi_table_html = ""
    reminders_kpi_html = ""

    def render_presence_rows(items: List[Dict], label: str) -> str:
        if not items:
            return f"<tr><td>{_escape(label)} (0)</td><td class='muted'>—</td></tr>"
        rows = []
        for it in items:
            name = _escape(it.get("name", ""))
            logo = (it.get("logo", "") or "").strip()
            logo_html = f"<img class='coLogo' src='{_escape(logo)}' alt='' loading='lazy' />" if logo.startswith("http") else ""
            rows.append(f"<li class='presenceLine'>{logo_html}<span>{name}</span></li>")
        return f"<tr><td>{_escape(label)} ({len(items)})</td><td><ul class='presenceList'>{''.join(rows)}</ul></td></tr>"

    presence_html = f"""
      <div class="presenceWrap">
        <table class="annexTable coverTable presenceTable">
          <thead>
            <tr><th>Type</th><th>Entreprises</th></tr>
          </thead>
          <tbody>
            {render_presence_rows(att, "Présentes")}
            {render_presence_rows(miss, "Absentes / Excusées")}
          </tbody>
        </table>
      </div>
    """

    actions_html = f"""
      <div class="actions noPrint">
        <button class="btn" type="button" onclick="window.print()">Imprimer / PDF</button>
        <button class="btn secondary editCompact" id="btnPrintPreview" type="button">Aperçu impression : OFF</button>
        <button class="btn secondary editCompact" type="button" onclick="window.refreshPagination && window.refreshPagination()">Recalculer la mise en page</button>
        <button class="btn secondary editCompact" id="btnTools" type="button">Outils</button>
        <button class="btn secondary editCompact" id="btnRange" type="button" onclick="toggleRangePanel()">Choisir une période</button>
        <button class="btn secondary editCompact" id="btnConstraints" type="button">Contraintes HTML / impression</button>
        <button class="btn secondary editCompact" id="btnZoneOrder" type="button">Réordonner les périmètres</button>
        <div id="hiddenRowsTools" style="display:none;gap:8px;flex-direction:column;">
          <select id="hiddenRowsSelect" class="hiddenRowsSelect" title="Lignes masquées">
            <option value="">Lignes masquées…</option>
          </select>
          <button class="btn secondary editCompact" type="button" onclick="restoreSelectedRow()">Réafficher la ligne</button>
          <button class="btn secondary editCompact" type="button" onclick="restoreAllHiddenRows()">Réafficher tout</button>
        </div>
        <a class="btn secondary" href="/">Changer de réunion</a>
      </div>
      <div class="rangePanel noPrint" id="rangePanel" style="display:{'flex' if range_active else 'none'}">
        <div class="rangeFields">
          <div class="rangeField">
            <label for="rangeStart">Du</label>
            <input type="date" id="rangeStart" value="{_escape(range_start_value)}" />
          </div>
          <div class="rangeField">
            <label for="rangeEnd">Au</label>
            <input type="date" id="rangeEnd" value="{_escape(range_end_value)}" />
          </div>
        </div>
        <div class="rangeActions">
          <button class="btn secondary" type="button" onclick="toggleRangePanel()">Fermer</button>
          <button class="btn secondary" type="button" onclick="clearRange()">Réinitialiser</button>
          <button class="btn" type="button" onclick="applyRange()">Appliquer</button>
        </div>
      </div>
      <div class="constraintsPanel noPrint" id="constraintsPanel" style="display:none">
        <div class="panelTitle">Détection des contraintes de mise en page</div>
        <div class="muted small">Désactive une contrainte pour voir immédiatement son effet sur l'affichage HTML et/ou l'impression.</div>
        <div class="constraintList">
          <label><input type="checkbox" data-constraint="fixedA4" checked /> Gabarit A4 fixe (largeur 210mm)</label>
          <label><input type="checkbox" data-constraint="fixedPageHeight" checked /> Hauteur de page forcée (297mm)</label>
          <label><input type="checkbox" data-constraint="pageBreaks" checked /> Sauts de page forcés entre sections</label>
          <label><input type="checkbox" data-constraint="bodyOffset" checked /> Décalage du body (panneau d'actions à gauche)</label>
          <label><input type="checkbox" data-constraint="pagePadding" checked /> Padding interne de la page</label>
          <label><input type="checkbox" data-constraint="footerReserve" checked /> Réserver l'espace avant footer (anti-chevauchement)</label>
          <label class="constraintSubControl">Niveau de réserve footer
            <input type="range" min="-100" max="150" step="5" value="100" id="footerReserveFactor" />
            <span id="footerReserveFactorValue">100 %</span>
          </label>
          <label><input type="checkbox" data-constraint="tableFixed" checked /> Colonnes de tableau en layout fixe</label>
          <label><input type="checkbox" data-constraint="printHideUi" checked /> Masquer les outils UI à l'impression</label>
          <label><input type="checkbox" data-constraint="printStickyHeader" checked /> Header sticky en impression</label>
          <label><input type="checkbox" data-constraint="printCompactRows" checked /> Compactage des lignes pour imprimer</label>
          <label><input type="checkbox" data-constraint="printAvoidSplitRows" checked /> Empêcher la coupure de lignes/blocs</label>
          <label><input type="checkbox" data-constraint="keepSessionHeaderWithNext" checked /> Ne pas laisser « En séance du » seul en bas de page</label>
          <label><input type="checkbox" data-constraint="printAutoOptimize" checked /> Optimisation auto avant impression</label>
          <label><input type="checkbox" data-constraint="topScale" checked /> Mise à l'échelle du bandeau haut</label>
        </div>
      </div>
      <div class="zoneOrderModal noPrint" id="zoneOrderModal" style="display:none" aria-hidden="true">
        <div class="zoneOrderCard" role="dialog" aria-modal="true" aria-labelledby="zoneOrderTitle">
          <div class="zoneOrderHead">
            <div>
              <div class="panelTitle" id="zoneOrderTitle">Réordonner les périmètres</div>
              <div class="muted small">Glissez-déposez les périmètres présents dans le compte-rendu pour modifier leur ordre.</div>
            </div>
            <button class="btn secondary" type="button" id="btnZoneOrderClose">Fermer</button>
          </div>
          <div class="zoneOrderListWrap">
            <ol class="zoneOrderList" id="zoneOrderList"></ol>
          </div>
          <div class="zoneOrderActions">
            <button class="btn secondary" type="button" id="btnZoneOrderCancel">Annuler</button>
            <button class="btn" type="button" id="btnZoneOrderApply">Appliquer l'ordre</button>
          </div>
        </div>
      </div>
    """

    # Card renderer for tasks outside the meeting (rappels / à-suivre) — NO BADGES
    def render_task_card_from_row(r, tag: str, extra_class: str, img_col: Optional[str]) -> str:
        title = _format_entry_text_html(r.get(E_COL_TITLE, ""))
        company = _escape(r.get(E_COL_COMPANY_TASK, ""))
        owner = _escape(r.get(E_COL_OWNER, ""))
        deadline = _fmt_date(_parse_date_any(r.get(E_COL_DEADLINE)))
        done = ""
        if _bool_true(r.get(E_COL_COMPLETED)):
            done = _fmt_date(_parse_date_any(r.get(E_COL_COMPLETED_END)))

        concerne = " • ".join([x for x in [company, owner] if x])
        status_txt = _escape(r.get(E_COL_STATUS, "")) or ("Terminé" if _bool_true(r.get(E_COL_COMPLETED)) else "Non terminé")

        img_urls = parse_image_urls_any(r.get(img_col)) if img_col else []
        images_html = render_images_gallery(img_urls, print_mode=print_mode)
        comment_html = render_task_comment(r)

        return f"""
          <div class="topic {extra_class}">
            <div class="topicTop">
              <div class="topicTitle">{title}</div>
              <div class="topicRight">
                <div class="rRow"><div class="rLab">Type</div><div class="rVal">Tâche</div></div>
                <div class="rRow"><div class="rLab">Tag</div><div class="rVal">{_escape(tag) or "—"}</div></div>
                <div class="rRow"><div class="rLab">Statut</div><div class="rVal">{status_txt}</div></div>
              </div>
            </div>

            <div class="meta4">
              <div><div class="metaLabel">Pour le</div><div class="metaVal">{deadline or "—"}</div></div>
              <div><div class="metaLabel">Fait le</div><div class="metaVal">{done or "—"}</div></div>
              <div><div class="metaLabel">Concerne</div><div class="metaVal">{concerne or "—"}</div></div>
              <div><div class="metaLabel">Lot</div><div class="metaVal">{_lot_abbrev_list(r.get(E_COL_PACKAGES, "")) or "—"}</div></div>
            </div>

            {images_html}
            {comment_html}
          </div>
        """

    # Pre-detect image column for each dataset
    img_col_meeting = detect_images_column(edf)
    img_col_memo = detect_memo_images_column(edf)
    img_col_rem = detect_images_column(rem_df)
    img_col_fol = detect_images_column(fol_df)

    # -------------------------
    # PDF TABLE RENDER (NO CARDS)
    # -------------------------
    def render_task_row_tr(
        r,
        tag_text: str,
        img_col: Optional[str] = None,
        is_meeting: bool = False,
        reminder_closed: bool = False,
        completed_recent: bool = False,
        row_id: str = "",
    ) -> str:
        title = _format_entry_text_html(r.get(E_COL_TITLE, ""))
        company = _escape(r.get(E_COL_COMPANY_TASK, ""))
        packages = _escape(r.get(E_COL_PACKAGES, ""))
        concerne_display = _concerne_trigram(company)

        created = _fmt_date(_parse_date_any(r.get(E_COL_CREATED)))
        deadline = _fmt_date(_parse_date_any(r.get(E_COL_DEADLINE)))

        done = ""
        if _bool_true(r.get(E_COL_COMPLETED)):
            done = _fmt_date(_parse_date_any(r.get(E_COL_COMPLETED_END)))

        is_task = _bool_true(r.get(E_COL_IS_TASK))
        deadline_display = (deadline or "—") if is_task else "PM"
        done_display = done or "—" if is_task else "/"
        lot_display = _lot_abbrev_list(packages) or "—"
        if not is_task and _has_multiple_companies(company):
            concerne_display = "PE"
        else:
            concerne_display = concerne_display or "PE"

        memo_img_col = img_col_memo if (not is_task and img_col_memo) else img_col
        img_urls = parse_image_urls_any(r.get(memo_img_col)) if memo_img_col else []
        thumbs = ""
        if img_urls:
            thumbs_imgs = "".join(
                f"<span class='thumbAWrap' data-thumb><a class='thumbA' href='{_escape(u)}' target='_blank' rel='noopener'><img class='thumb' src='{_escape(u)}' alt='' /></a><button type='button' class='thumbRemove noPrint' title='Supprimer'>×</button><span class='thumbHandle' title='Déplacer / redimensionner'></span></span>"
                for u in img_urls[:6]
            )
            thumbs = f"<div class='thumbs' data-gallery>{thumbs_imgs}</div>"

        row_cls = "rowItem rowMeeting" if is_meeting else "rowItem"
        if completed_recent:
            row_cls += " rowDoneRecent"

        tag_display = _escape(tag_text).replace(" ", "&nbsp;")
        tag_class = "tagReminderGreen" if tag_text.lower().startswith("rappel") and reminder_closed else "tagReminder"
        tag_html = (
            f"<span class='{tag_class}'>{tag_display}</span>"
            if tag_text.lower().startswith("rappel")
            else tag_display
        )

        safe_row_id = _escape(row_id) or _escape(str(r.get(E_COL_ID, "")))
        toggle_html = f"<input type='checkbox' class='rowToggle noPrint' data-target='{safe_row_id}' checked />"
        return f"""
          <tr class="{row_cls} compactRow" data-row-id="{safe_row_id}" data-entry-type="{"task" if is_task else "memo"}">
            <td class="colType">{toggle_html}<div>{tag_html or "—"}</div></td>
            <td class="colComment">
              <div class="rowImageTools noPrint"><button type="button" class="btnAddImage">+ Image</button><input type="file" class="imageInput" accept="image/*" multiple hidden /></div>
              <div class="commentText">{title}</div>
              {thumbs}
              {render_entry_comment(r)}
            </td>
            <td class="colDate">{created or "—"}</td>
            <td class="colDate">{deadline_display}</td>
            <td class="colDate">{done_display}</td>
            <td class="colLot editableCell" contenteditable="true">{lot_display}</td>
            <td class="colWho editableCell" contenteditable="true">{concerne_display}</td>
          </tr>
        """

    def render_session_subheader_tr(session_label: str, is_current_session: bool = False) -> str:
        return f"""
          <tr class="sessionSubRow{' sessionSubRowCurrent' if is_current_session else ''}">
            <td class="colType">—</td>
            <td class="colComment" colspan="6"><strong>{_escape(session_label)}</strong></td>
          </tr>
        """

    def _meeting_sort_and_label(r) -> Tuple[Optional[date], str]:
        mid = str(r.get(E_COL_MEETING_ID, "")).strip()
        created_d = _parse_date_any(r.get(E_COL_CREATED))
        if mid and mid in meeting_date_by_id and meeting_date_by_id[mid] is not None:
            d = meeting_date_by_id[mid]
        else:
            d = created_d
        if d:
            return d, f"En séance du {d.strftime('%d/%m/%Y')} :"
        return None, "Hors séance :"

    def render_zone_table(area_name: str, rows_html: str) -> str:
        if not rows_html.strip():
            return ""
        zt = _escape(area_name)
        return f"""
        <div class="zoneBlock reportBlock" data-zone-id="{zt}">
          <div class="zoneTitle">
            <span>{zt}</span>
            <div class="zoneTools noPrint">
              <button class="btnAddMemo" type="button" data-area="{zt}">+ Ajouter mémo</button>
            </div>
          </div>
          <table class="crTable">
            <colgroup>
              <col style="width:var(--col-type)" />
              <col style="width:var(--col-comment)" />
              <col style="width:var(--col-date)" />
              <col style="width:var(--col-date)" />
              <col style="width:var(--col-date)" />
              <col style="width:var(--col-lot)" />
              <col style="width:var(--col-who)" />
            </colgroup>
            <thead>
              <tr>
                <th class="colType">Type <span class="colGrip" data-col="type"></span></th>
                <th class="colComment">Commentaires et observations <span class="colGrip" data-col="comment"></span></th>
                <th class="colDate">Écrit le <span class="colGrip" data-col="date"></span></th>
                <th class="colDate">Pour le <span class="colGrip" data-col="date2"></span></th>
                <th class="colDate">Fait le <span class="colGrip" data-col="date3"></span></th>
                <th class="colLot">Lot <span class="colGrip" data-col="lot"></span></th>
                <th class="colWho">Concerne <span class="colGrip" data-col="who"></span></th>
              </tr>
            </thead>
            <tbody>
              {rows_html}
            </tbody>
          </table>
        </div>
        """

    # Build per-zone blocks
    zones_html_parts: List[str] = []

    current_session_label = (
        f"En séance du {(meet_date or ref_date).strftime('%d/%m/%Y')} :" if (meet_date or ref_date) else ""
    )

    def _entry_id_value(r) -> str:
        return str(r.get(E_COL_ID, "")).strip()

    def _is_completed_recent_row(r) -> bool:
        rid = _entry_id_value(r)
        return bool(rid and rid in closed_recent_ids)

    for area_name, g in areas:
        grouped_rows: List[Tuple[Optional[date], str, str]] = []
        seen_entry_ids: set[str] = set()

        rem_zone = rem_df.loc[rem_df["__area_list__"].astype(str) == str(area_name)].copy()
        if not rem_zone.empty:
            for idx, r in rem_zone.iterrows():
                rid = _entry_id_value(r)
                row_html = render_task_row_tr(
                    r,
                    f"Rappel {int(r.get('__reminder__') or 1)}",
                    img_col=img_col_rem,
                    is_meeting=False,
                    completed_recent=_is_completed_recent_row(r),
                    row_id=f"rem-{area_name}-{idx}",
                )
                sort_d, label = _meeting_sort_and_label(r)
                grouped_rows.append((sort_d, label, row_html))
                if rid:
                    seen_entry_ids.add(rid)

        fol_zone = fol_df.loc[fol_df["__area_list__"].astype(str) == str(area_name)].copy()
        if not fol_zone.empty:
            for idx, r in fol_zone.iterrows():
                rid = _entry_id_value(r)
                row_html = render_task_row_tr(
                    r,
                    "Tâche",
                    img_col=img_col_fol,
                    is_meeting=False,
                    completed_recent=_is_completed_recent_row(r),
                    row_id=f"fol-{area_name}-{idx}",
                )
                sort_d, label = _meeting_sort_and_label(r)
                grouped_rows.append((sort_d, label, row_html))
                if rid:
                    seen_entry_ids.add(rid)

        if pinned_set and (not pinned_df.empty):
            pin_zone = pinned_df.loc[pinned_df["__area_list__"].astype(str) == str(area_name)].copy()
            if not pin_zone.empty:
                for idx, r in pin_zone.iterrows():
                    rid = _entry_id_value(r)
                    row_html = render_task_row_tr(
                        r,
                        "Mémo",
                        img_col=img_col_pinned,
                        is_meeting=False,
                        row_id=f"pin-{area_name}-{idx}",
                    )
                    sort_d, label = _meeting_sort_and_label(r)
                    grouped_rows.append((sort_d, label, row_html))
                    if rid:
                        seen_entry_ids.add(rid)

        if not g.empty:
            g_view = g.copy().sort_values(by=E_COL_CREATED, na_position="last")
            for idx, r in g_view.iterrows():
                rid = _entry_id_value(r)
                tag = "Tâche" if _bool_true(r.get(E_COL_IS_TASK)) else "Mémo"
                is_meeting_entry = str(r.get(E_COL_MEETING_ID, "")).strip() == str(meeting_id)
                row_html = render_task_row_tr(
                    r,
                    tag,
                    img_col=img_col_meeting,
                    is_meeting=is_meeting_entry,
                    completed_recent=_is_completed_recent_row(r),
                    row_id=f"meet-{area_name}-{idx}",
                )
                sort_d, label = _meeting_sort_and_label(r)
                grouped_rows.append((sort_d, label, row_html))
                if rid:
                    seen_entry_ids.add(rid)

        closed_zone = (
            closed_recent_df.loc[closed_recent_df["__area_list__"].astype(str) == str(area_name)].copy()
            if not closed_recent_df.empty
            else pd.DataFrame()
        )
        if not closed_zone.empty:
            for idx, r in closed_zone.iterrows():
                rid = _entry_id_value(r)
                if rid and rid in seen_entry_ids:
                    continue
                lvl = r.get("__reminder__")
                tag = f"Rappel {int(lvl)}" if pd.notna(lvl) else "Tâche"
                row_html = render_task_row_tr(
                    r,
                    tag,
                    img_col=img_col_meeting,
                    is_meeting=False,
                    reminder_closed=True,
                    completed_recent=True,
                    row_id=f"closed-{area_name}-{idx}",
                )
                sort_d, label = _meeting_sort_and_label(r)
                grouped_rows.append((sort_d, label, row_html))
                if rid:
                    seen_entry_ids.add(rid)

        grouped_rows.sort(key=lambda item: (item[0] is None, item[0] or date.max, item[1]))
        rows_parts: List[str] = []
        current_label = None
        for _, label, row_html in grouped_rows:
            if label != current_label:
                rows_parts.append(render_session_subheader_tr(label, is_current_session=(label == current_session_label)))
                current_label = label
            rows_parts.append(row_html)

        zone_table_html = render_zone_table(area_name, "\n".join(rows_parts))
        if zone_table_html:
            zones_html_parts.append(zone_table_html)

    zones_html = "".join(zones_html_parts)
    report_note_html = ""

    # -------------------------
    # CSS
    # -------------------------
    css = f"""
:root{{
  --bg:#ffffff;
  --text:#0b1220;
  --muted:#475569;
  --border:#e2e8f0;
  --soft:#f8fafc;
  --shadow:0 10px 30px rgba(2,6,23,.06);
  --accent:#0f172a;
  --blueSoft:#eff6ff;
  --blueBorder:#bfdbfe;
  --col-type:7%;
  --col-comment:53%;
  --col-date:8%;
  --col-lot:8%;
  --col-who:8%;
  --a4-width:210mm;
  --a4-padding-x:6mm;
  --kpi-cols:4;
  --top-scale:1;
  --footer-reserve-factor:1;
}}
*{{box-sizing:border-box}}
html,body{{margin:0;padding:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Arial;-webkit-print-color-adjust:exact;print-color-adjust:exact;}}
body{{padding:14px 14px 14px 280px;}}
.wrap{{display:flex;flex-direction:column;gap:12px;align-items:center;}}
.page{{width:210mm;height:297mm;min-height:297mm;position:relative;background:#fff;overflow:visible;break-after:page;page-break-after:always;}}
.page:last-child{{break-after:auto;page-break-after:auto;}}
.pageContent{{padding:10mm 8mm 34mm 8mm;}}
.page--cover .pageContent{{padding-top:0;}}
.muted{{color:var(--muted)}}
.small{{font-size:12px}}
.noPrint{{}}
@media print{{ .noPrint{{display:none!important}} }}
@media print{{body{{padding:0;background:#fff}} .page{{margin:0;box-shadow:none}}}}
body.printOptimized .reportBlocks{{gap:0!important}}
body.printOptimized .zoneBlock{{margin:0!important}}
body.printOptimized .crTable th, body.printOptimized .crTable td{{padding:4px 5px!important;line-height:1.16!important}}
body.printOptimized .reportHeader{{margin-bottom:4px!important}}
body.printOptimized .thumb{{height:64px!important;max-width:110px!important}}
body.printPreviewMode .rowToggle,
body.printPreviewMode .rowImageTools,
body.printPreviewMode .thumbRemove,
body.printPreviewMode .btnAddMemo,
body.printPreviewMode .colGrip{{display:none!important}}
body.printPreviewMode .editableCell{{background:transparent!important;box-shadow:none!important}}
body.printPreviewMode .editableCell:focus{{box-shadow:none!important}}
body.printPreviewMode .noPrintRow{{display:none!important}}
@media screen{{body{{background:#e5e7eb;}} .page{{box-shadow:0 14px 30px rgba(15,23,42,.16)}}}}
.topPage{{transform:scale(var(--top-scale));transform-origin:top left}}
@media print{{.topPage{{margin:0;}}}}
.reportTables{{margin-top:0}}
.coverHero{{position:relative;overflow:hidden;background:#fff;min-height:420px}}
.coverHeroImg{{position:relative;min-height:430px;background-size:cover;background-position:center}}
.coverHeroFade{{position:absolute;inset:0;background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,0));}}
.coverHeroCurve{{position:absolute;left:50%;bottom:-95px;width:135%;height:190px;transform:translateX(-50%);background:#fff;border-radius:50% 50% 0 0 / 100% 100% 0 0;z-index:2}}
.coverHeroLogoWrap{{position:absolute;left:50%;bottom:18px;transform:translateX(-50%);z-index:4;background:#fff;padding:10px 18px;border-radius:8px;box-shadow:0 6px 18px rgba(2,6,23,.12)}}
.coverHeroLogo{{height:110px;width:auto;display:block}}
.coverNoteCenter{{text-align:center;padding:10px 16px 12px 16px;font-weight:900;display:flex;flex-direction:column;align-items:center;gap:10px}}
.coverAppNote{{margin-top:8px;font-family:"Arial Nova Cond Light","Arial Narrow",Arial,sans-serif;font-size:14px;line-height:1.45;color:#f97316;font-style:italic;font-weight:600;max-width:640px}}
.coverUrl{{margin-top:6px;font-weight:900;color:#f97316;text-decoration:underline;text-underline-offset:3px}}
.coverUrl::after{{content:" ↗";font-weight:900}}
.coverProjectTitle{{font-family:"Arial Nova Cond Light","Arial Narrow",Arial,sans-serif;font-size:22px;line-height:1.2;color:#f59e0b;font-weight:700;letter-spacing:.5px;text-transform:uppercase}}
.coverCrTitle{{margin-top:10px;font-family:"Arial Nova Cond Light","Arial Narrow",Arial,sans-serif;font-size:22px;line-height:1.2;color:#0f3a40;font-weight:700}}
.coverCrMeta{{margin-top:8px;font-family:"Arial Nova Cond Light","Arial Narrow",Arial,sans-serif;font-size:22px;line-height:1.2;color:#0f3a40;font-weight:700}}
.editInline{{display:inline-block;min-width:40px;padding:0 4px;border-bottom:2px dashed #cbd5e1;outline:none}}
@media print{{.editInline{{border-bottom:none}}}}
.nextMeetingBox{{margin:18px auto 0 auto;max-width:78%;border:2px solid #111;padding:12px 10px;font-weight:1000}}
.nextMeetingLine1{{font-family:"Arial Nova Cond Light","Arial Narrow",Arial,sans-serif;font-size:18px}}
.nextMeetingLine2{{font-family:"Arial Nova Cond Light","Arial Narrow",Arial,sans-serif;font-size:18px;color:#ef4444;margin-top:5px}}
.nextMeetingLine3{{font-family:"Arial Nova Cond Light","Arial Narrow",Arial,sans-serif;font-size:18px;color:#111;margin-top:4px;outline:none}}
@media print{{.coverHeroImg{{min-height:390px}} .coverProjectTitle{{font-size:44px}} .coverCrTitle{{font-size:33px}} .coverCrMeta{{font-size:36px}} .nextMeetingLine1{{font-size:18px}} .nextMeetingLine2{{font-size:32px}} .nextMeetingLine3{{font-size:27px}}}}

/* PROJECT BANNER */
.banner{{
  border:1px solid var(--border);
  border-radius:18px;
  overflow:hidden;
  background:linear-gradient(180deg,#fff, var(--soft));
}}
.bannerImg{{position:relative;min-height:260px;background-size:cover;background-position:center;}}
.bannerOverlay{{position:absolute;inset:0;background:linear-gradient(90deg, rgba(2,6,23,.78), rgba(2,6,23,.10));}}
.bannerContent{{position:relative;padding:18px;color:#fff;max-width:900px;}}
.bannerKicker{{font-weight:800;opacity:.9}}
.bannerTitle{{font-size:26px;font-weight:1000;letter-spacing:.2px;margin-top:6px}}
.bannerMeta{{margin-top:10px;display:flex;flex-wrap:wrap;gap:10px}}
.bannerChip{{background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.18);padding:7px 10px;border-radius:999px;font-weight:700;}}
.bannerDesc{{margin-top:10px;opacity:.95}}
@media print{{.bannerImg{{min-height:300px}} .bannerTitle{{font-size:22px}} .bannerContent{{padding:14px}}}}

/* BANNER LOGO */
.bannerLogoWrap{{display:flex;justify-content:flex-start;margin-bottom:8px}}
.bannerLogo{{height:72px;width:auto;display:block}}

/* KPI */
.card{{background:#fff;border:1px solid var(--border);border-radius:16px;box-shadow:var(--shadow);padding:16px;margin-top:14px;}}
.kpis{{display:grid;grid-template-columns:repeat(var(--kpi-cols),1fr);gap:10px;margin-top:12px}}
.kpi{{border:1px solid var(--border);border-radius:14px;background:#fff;padding:10px}}
.kpi_t{{color:var(--muted);font-weight:700;font-size:11px}}
.kpi_v{{font-weight:1000;font-size:20px;margin-top:6px}}
.topGrip{{height:8px;width:120px;background:#e2e8f0;border-radius:999px;margin:8px auto 0;cursor:ns-resize}}
@media (max-width: 980px){{.kpis{{grid-template-columns:repeat(3,1fr)}}}}
@media print{{.kpis{{grid-template-columns:repeat(4,1fr);gap:6px}} .kpi{{padding:6px}} .kpi_v{{font-size:16px}}}}

/* Sections */
.section{{margin-top:18px}}
.sectionTitle{{
  display:flex;align-items:center;gap:10px;
  padding:14px 14px;border:1px solid var(--border);border-radius:16px;
  background:linear-gradient(180deg,#fff, var(--soft));
  font-weight:1000;font-size:16px;letter-spacing:.2px;
  border-left:6px solid #0f172a;
}}
.zoneTitle{{
  display:flex;align-items:center;gap:10px;
  padding:4px 8px;border:1px solid var(--border);border-bottom:none;
  background:#f59e0b;color:#ffffff;font-weight:900;font-size:10px;text-transform:uppercase;
}}
.zoneTitle button{{margin-left:auto}}
.zoneTools{{display:flex;align-items:center;gap:6px;margin-left:auto}}
.zoneBlock.pageBreakBefore{{page-break-before:always}}
.u-page-break{{break-before:page;page-break-before:always;}}
.u-avoid-break{{break-inside:avoid;page-break-inside:avoid;}}

.presGrid{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:10px}}
@media (max-width: 780px){{.presGrid{{grid-template-columns:1fr}}}}
.subcard{{border:1px solid var(--border);border-radius:14px;background:#fff;padding:12px}}
.subhead{{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px;font-weight:900}}
.chips{{display:flex;flex-wrap:wrap;gap:8px}}
.chip{{padding:7px 10px;border-radius:999px;border:1px solid var(--border);background:#fff;font-weight:700;display:inline-flex;align-items:center;gap:8px;}}
.coLogo{{width:18px;height:18px;border-radius:6px;object-fit:cover;display:block}}

/* Topics */
.topics{{display:flex;flex-direction:column;gap:12px;margin-top:10px}}
.topic{{border:1px solid var(--border);border-radius:14px;background:#fff;padding:12px}}
.topicTop{{display:grid;grid-template-columns:1fr 210px;gap:12px;align-items:start;}}
.topicTitle{{font-weight:600;font-size:15px;line-height:1.25}}
.topicRight{{display:flex;flex-direction:column;gap:8px;align-items:stretch;}}
.rRow{{display:flex;justify-content:space-between;gap:10px;border:1px solid var(--border);border-radius:12px;padding:6px 8px;background:#fff}}
.rLab{{color:var(--muted);font-weight:800;font-size:11px}}
.rVal{{font-weight:900;font-size:12px;text-align:right;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}

.meta4{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:10px}}
@media (max-width: 900px){{.meta4{{grid-template-columns:repeat(2,1fr)}}}}
.metaLabel{{color:var(--muted);font-weight:700;font-size:11px}}
.metaVal{{font-weight:700}}
.topicComment{{margin-top:10px;border-top:1px dashed var(--border);padding-top:10px}}

.imgRow{{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}}
.imgThumb{{display:block;width:320px;height:200px;border-radius:12px;overflow:hidden;border:1px solid var(--border);background:#fff}}
.imgThumb img{{width:100%;height:100%;object-fit:cover;display:block}}
.imgThumb{{position:relative;resize:both;overflow:auto}}
.imgGrip{{position:absolute;right:6px;bottom:6px;width:14px;height:14px;border:2px solid rgba(15,23,42,.45);border-top:none;border-left:none;pointer-events:none}}

.actions{{position:fixed;top:14px;left:14px;z-index:9999;display:flex;flex-direction:column;gap:8px;width:248px;padding:10px;border:1px solid var(--border);border-radius:12px;background:rgba(255,255,255,.97);box-shadow:0 8px 24px rgba(2,6,23,.12)}}
.actions .btn,.actions .hiddenRowsSelect{{width:100%}}
.btn{{display:inline-flex;align-items:center;justify-content:center;gap:10px;padding:11px 14px;border-radius:12px;border:1px solid var(--border);background:var(--accent);color:#fff;font-weight:950;cursor:pointer;text-decoration:none}}
.btn.secondary{{background:#fff;color:var(--text);font-weight:900}}
#btnPrintPreview.active{{background:#0f172a;color:#fff;border-color:#0f172a}}
.rangePanel{{position:fixed;top:14px;left:14px;z-index:10001;width:248px;border:1px solid var(--border);border-radius:14px;padding:12px;background:#fff;display:flex;flex-direction:column;gap:10px;box-shadow:0 8px 24px rgba(2,6,23,.12);max-height:calc(100vh - 32px);overflow:auto}}
.constraintsPanel{{position:fixed;top:14px;left:276px;z-index:10001;width:420px;border:1px solid var(--border);border-radius:14px;padding:12px;background:#fff;display:flex;flex-direction:column;gap:10px;box-shadow:0 8px 24px rgba(2,6,23,.12);max-height:calc(100vh - 32px);overflow:auto}}
.panelTitle{{font-weight:900;font-size:13px}}
.constraintList{{display:grid;grid-template-columns:1fr;gap:6px}}
.constraintList label{{display:flex;align-items:flex-start;gap:8px;font-size:12px;line-height:1.25}}
.constraintList label.constraintSubControl{{display:grid;grid-template-columns:130px 1fr auto;align-items:center;gap:8px;margin-left:22px}}
.constraintList label.constraintSubControl input[type="range"]{{width:100%}}
.rangeFields{{display:flex;gap:12px;flex-wrap:wrap}}
.rangeField{{display:flex;flex-direction:column;gap:6px;min-width:180px}}
.rangeField label{{font-weight:900;font-size:12px}}
.rangeField input{{padding:8px 10px;border-radius:10px;border:1px solid var(--border);font-weight:700}}
.rangeActions{{display:flex;gap:10px;flex-wrap:wrap}}
.hiddenRowsSelect{{padding:9px 10px;border:1px solid var(--border);border-radius:10px;font-weight:700;background:#fff;min-width:220px}}
.zoneOrderModal{{position:fixed;inset:0;z-index:10020;background:rgba(2,6,23,.45);display:none;align-items:center;justify-content:center;padding:20px}}
.zoneOrderCard{{width:min(760px,96vw);max-height:90vh;display:flex;flex-direction:column;gap:12px;border:1px solid var(--border);border-radius:16px;background:#fff;padding:14px;box-shadow:0 24px 50px rgba(2,6,23,.35)}}
.zoneOrderHead{{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}}
.zoneOrderListWrap{{overflow:auto;border:1px solid var(--border);border-radius:12px;padding:10px;background:#f8fafc}}
.zoneOrderList{{margin:0;padding:0;list-style:none;display:flex;flex-direction:column;gap:8px}}
.zoneOrderItem{{display:flex;align-items:center;gap:10px;padding:10px 12px;border:1px solid #dbe3ef;border-radius:10px;background:#fff;font-weight:800;cursor:grab}}
.zoneOrderItem.dragging{{opacity:.5}}
.zoneOrderGrip{{color:#64748b;font-size:15px;line-height:1}}
.zoneOrderText{{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.zoneOrderActions{{display:flex;justify-content:flex-end;gap:10px;flex-wrap:wrap}}
@media print{{.actions{{margin:8px 0}} .btn{{padding:8px 10px;font-size:12px}}}}

/* Bleu = sujets réunion */
.newItem{{border-color: var(--blueBorder);background: linear-gradient(180deg, #ffffff, var(--blueSoft));box-shadow: 0 0 0 2px rgba(59,130,246,.05);}}
.reminderItem{{border-left:4px solid #ef4444;}}
.followItem{{border-left:4px solid #f59e0b;}}

/* KPI list */
.kpiList{{display:flex;flex-direction:column;gap:8px}}
.kpiRow{{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:8px 10px;border:1px solid var(--border);border-radius:12px;background:#fff}}
.kpiCo{{display:inline-flex;align-items:center;gap:10px;font-weight:900}}
.kpiCount{{font-weight:1000}}

/* PRINT TABLE */
@page {{ size: A4 portrait; margin: 0; }}
body.constraint-off-fixedA4 .page{{width:auto!important}}
body.constraint-off-fixedPageHeight .page{{height:auto!important;min-height:auto!important}}
body.constraint-off-pageBreaks .page,body.constraint-off-pageBreaks .page:last-child{{break-after:auto!important;page-break-after:auto!important}}
body.constraint-off-bodyOffset{{padding:14px!important}}
body.constraint-off-pagePadding .pageContent{{padding:0!important}}
body.constraint-off-footerReserve .pageContent{{padding-bottom:8mm!important}}
body.constraint-off-tableFixed .crTable{{table-layout:auto!important}}
body.constraint-off-printStickyHeader .printHeaderFixed{{position:static!important;top:auto!important}}
body.constraint-off-printCompactRows.printOptimized .crTable th,body.constraint-off-printCompactRows.printOptimized .crTable td{{padding:7px 8px!important;line-height:1.3!important}}
body.constraint-off-printCompactRows.printOptimized .thumb{{height:80px!important;max-width:140px!important}}
body.constraint-off-topScale .topPage{{transform:none!important}}
@media print{{body.constraint-off-printHideUi .actions,body.constraint-off-printHideUi .rangePanel,body.constraint-off-printHideUi .constraintsPanel{{display:flex!important}}}}
@media print{{body.constraint-off-printAvoidSplitRows .sessionSubRow,body.constraint-off-printAvoidSplitRows .zoneTitle{{break-inside:auto!important;page-break-inside:auto!important;break-after:auto!important;page-break-after:auto!important}}}}

.zoneBlock{{margin:0}}
.zoneBlock + .zoneBlock{{margin-top:0}}
.reportBlocks{{display:flex;flex-direction:column;gap:0}}
.reportBlock{{break-inside:auto;page-break-inside:auto}}
.reportNote{{margin-top:12px}}
.crTable{{width:100%;border-collapse:collapse;table-layout:fixed;border:1px solid var(--border);margin-top:-1px;}}
.crTable thead{{display:table-header-group}}
.crTable tfoot{{display:table-footer-group}}
.crTable th, .crTable td{{border:1px solid var(--border);padding:6px 7px;vertical-align:top;page-break-inside:auto;break-inside:auto;}}
.crTable tr{{page-break-inside:auto;break-inside:auto;}}
.annexTable tr{{page-break-inside:auto;break-inside:auto;}}
.crTable th{{background:#1f4e4f;color:#fff;text-align:center;font-weight:900;font-size:11px;line-height:1.2;white-space:nowrap}}
.crTable td{{font-size:11px;line-height:1.24;word-break:normal;overflow-wrap:break-word;hyphens:none}}
.crTable td.colDate, .crTable th.colDate{{padding:6px 4px}}

.sessionSubRow td{{background:#ffffff;}}
.sessionSubRow td.colType{{color:#94a3b8;font-weight:700;}}
.sessionSubRow td.colComment{{font-size:12px;color:#111827;font-weight:900;text-decoration:none;}}
.sessionSubRowCurrent td.colComment{{color:#1d4ed8;text-decoration:underline;text-underline-offset:2px;}}
.colType{{text-align:center;font-weight:1000;white-space:nowrap;position:relative}}
.colComment{{white-space:normal;position:relative}}
.rowImageTools{{display:flex;justify-content:flex-end;margin-bottom:4px}}
.btnAddImage{{border:1px solid #d1d5db;background:#fff;border-radius:8px;padding:2px 8px;font-size:11px;font-weight:800;cursor:pointer}}
.btnAddImage:hover{{background:#f8fafc}}
.colDate{{text-align:center;font-variant-numeric: tabular-nums;white-space:nowrap;position:relative}}
.colLot{{text-align:center;white-space:nowrap;position:relative}}
.colWho{{text-align:center;white-space:nowrap;position:relative}}
.rowToggle{{width:14px;height:14px;accent-color:#0f172a;cursor:pointer}}
.editableCell{{background:#fff7ed;outline:none}}
.editableCell:focus{{box-shadow:inset 0 0 0 2px #fb923c}}
.noPrintRow{{opacity:.4}}
.rowDoneRecent td{{background:none!important}}
.crTable tr.rowDoneRecent td.colType{{box-shadow:inset 4px 0 0 #16a34a;}}
.crTable tr.rowDoneRecent td.colType div{{color:#15803d;font-weight:900;}}
.rowHidden{{display:none!important}}
.colGrip{{position:absolute;top:0;right:-6px;width:12px;height:100%;cursor:col-resize}}
.colGrip::after{{content:"";position:absolute;top:3px;bottom:3px;left:5px;width:2px;background:#cbd5f5;border-radius:2px;opacity:.7}}

@media print{{ .rowToggle{{display:none}} .noPrintRow{{display:none}} .editableCell{{background:transparent}} .rowImageTools{{display:none!important}} .thumbRemove{{display:none!important}} }}
@media print{{ .sessionSubRow{{break-inside:avoid;page-break-inside:avoid}} .zoneTitle{{break-after:avoid-page;page-break-after:avoid}} }}


.crTable tr.rowMeeting td{{background:#eef8ff;}}
.crTable tr.rowMeeting td.colType{{box-shadow:inset 4px 0 0 #2563eb;}}

.thumbs{{margin-top:6px;display:flex;flex-wrap:wrap;gap:8px;align-items:flex-start}}
.thumb{{width:160px;height:auto;max-width:100%;border:1px solid var(--border);border-radius:8px;display:block;object-fit:cover;background:#fff}}
.entryComment{{margin-top:8px;padding-left:12px;border-left:3px solid #e2e8f0}}
.tagReminderGreen{{color:#16a34a;font-weight:900}}
.thumbA{{display:inline-flex;cursor:grab}}
.commentText{{font-weight:400;line-height:1.24;white-space:normal}}
.tagReminder{{color:#b91c1c;font-weight:900}}
.thumbAWrap{{position:relative;display:inline-flex;touch-action:none;max-width:100%;align-items:flex-start}}
.thumbAWrap.dragging{{opacity:.7;z-index:5}}
.thumbAWrap.resizing{{outline:2px solid #60a5fa;outline-offset:1px}}
.thumbHandle{{position:absolute;right:4px;bottom:4px;width:14px;height:14px;border:2px solid rgba(15,23,42,.45);border-top:none;border-left:none;cursor:nwse-resize;background:rgba(255,255,255,.7)}}
.thumbRemove{{position:absolute;top:2px;right:2px;width:18px;height:18px;border:none;border-radius:999px;background:rgba(15,23,42,.72);color:#fff;font-weight:900;line-height:18px;padding:0;cursor:pointer}}
.thumbRemove:hover{{background:#dc2626}}
.colComment br + br{{display:none}}
.compactRow .colComment{{line-height:1.22}}
.compactRow .colComment .entryComment{{margin-top:6px}}

@media print{{
  .page{{height:auto;min-height:0}}
  .pageContent{{padding:8mm 7mm 30mm 7mm}}
  .crTable th, .crTable td{{padding:5px 6px}}
  .zoneTitle{{padding:5px 7px}}
  .reportHeader{{margin-bottom:6px}}
  .thumb{{height:72px;max-width:130px}}
  .thumbHandle{{display:none}}
  .thumbRemove{{display:none}}
  .btnAddImage{{display:none}}
}}
.annexTable{{width:100%;border-collapse:collapse;font-size:12px;table-layout:fixed;border:1px solid var(--border)}}
.annexTable thead{{display:table-header-group}}
.annexTable th,.annexTable td{{border-bottom:1px solid var(--border);padding:8px 6px;text-align:left;vertical-align:top}}
.annexTable td:first-child{{width:90px;color:#2563eb;font-weight:900}}
.annexTable td:last-child{{text-align:right}}
.annexTable td:last-child .annexLink{{display:inline-block;text-align:right}}
.annexTable th{{font-weight:900;background:#1f4e4f;color:#fff}}
.annexTable .annexLink{{color:#f97316;font-weight:800;text-decoration:underline;text-underline-offset:3px;cursor:pointer}}
.annexTable .annexLink::after{{content:" ↗";font-weight:900;color:#f97316}}
.annexTable tr:last-child td{{border-bottom:none}}
.coverTable{{margin:10px 0 12px 0}}
.coverTable td:first-child{{width:260px;color:#0b1220;font-weight:900}}
.coverTable td.kpiNum{{text-align:right;font-weight:1000}}
.coverTable .chips{{display:flex;flex-wrap:wrap;gap:8px}}
.coverTable .chip{{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--border);border-radius:999px;padding:6px 10px;font-weight:800;background:#fff}}
.coverNote{{margin-top:12px;border:1px solid var(--border);border-radius:14px;padding:12px;background:#fff;line-height:1.5}}
.coverNoteTitle{{font-weight:1000;margin-bottom:6px}}
.reportHeader{{font-family:"Arial Nova Cond Light","Arial Narrow",Arial,sans-serif;font-size:11px;font-weight:400;color:#0b1220;text-align:center;margin:0 0 10px 0;}}
@media print{{.printHeaderFixed{{position:sticky;top:0;background:#fff;padding:1mm 0;z-index:20;}}}}
.reportHeader .accent{{color:#f59e0b;font-weight:900}}
.presenceTable .presenceList{{margin:0;padding-left:0;list-style:none;display:flex;flex-direction:column;gap:6px}}
.presenceTable .presenceLine{{display:flex;align-items:center;gap:8px;font-weight:700}}
.docFooter{{position:absolute;left:0;right:0;bottom:0;height:24mm;display:grid;grid-template-columns:120px 1fr 120px;align-items:center;gap:10px;padding:3mm 10mm;border-top:1px solid #dbe5f0;background:#fff;overflow:hidden;width:100%;box-sizing:border-box}}
.docFooter::before{{content:"";position:absolute;left:0;bottom:0;width:170px;height:42px;background:#123f45;clip-path:polygon(0 100%,100% 100%,0 0)}}
.docFooter::after{{content:"";position:absolute;right:0;bottom:0;width:260px;height:70px;background:#123f45;clip-path:polygon(100% 0,100% 100%,0 100%)}}
.footLeft,.footCenter,.footRight{{position:relative;z-index:2}}
.footLeft{{justify-self:start}}
.footCenter{{text-align:center;justify-self:center}}
.footRight{{justify-self:end;width:120px;display:flex;justify-content:flex-end}}
.pageNum{{font-family:'Arial Nova Cond Light','Arial Narrow',Arial,sans-serif;font-size:13px;font-weight:700;color:rgba(255,255,255,.82);line-height:1;letter-spacing:.2px;padding-right:14px;padding-bottom:6px}}
.tempoLegal{{font-family:"Arial Nova Cond Light","Arial Narrow",Arial,sans-serif;font-size:10px;line-height:1.3;color:#6b7280;font-weight:600}}
.footImg{{display:block;max-height:32px;width:auto}}
.footMark{{max-height:48px}}
.footRythme{{max-height:28px;margin:6px auto 0 auto}}
.footTempo{{max-height:28px;margin-left:auto}}
@media print{{body{{padding:0}} .actions,.rangePanel,.constraintsPanel{{display:none!important}} .page{{width:210mm;min-height:297mm;margin:0;box-shadow:none;break-after:page;page-break-after:always;}} .page:last-child{{break-after:auto;page-break-after:auto;}}}}

{EDITOR_MEMO_MODAL_CSS}
{QUALITY_MODAL_CSS}
{ANALYSIS_MODAL_CSS}
{TOOLS_MODAL_CSS}
"""

    # Banner / cover HTML
    bg_style = (
        f"background-image:url('{_escape(proj_img)}');"
        if proj_img.startswith("http")
        else "background:linear-gradient(90deg,#cfe8ff,#ffffff);"
    )

    tempo_logo = _logo_data_url(LOGO_TEMPO_PATH)
    logo_rythme = _logo_data_url(LOGO_RYTHME_PATH)
    logo_tmark = _logo_data_url(LOGO_T_MARK_PATH)
    cover_html = ""

    next_meeting_date = (meet_date or ref_date) + timedelta(days=7)
    next_meeting_date_txt = next_meeting_date.strftime("%d/%m/%Y")
    cr_date_txt = (meet_date or ref_date).strftime("%d/%m/%Y")

    cover_hero_html = f"""
      <div class='coverHero'>
        <div class='coverHeroImg' style="{bg_style}">
          <div class='coverHeroLogoWrap'>{("<img class='coverHeroLogo' src='" + tempo_logo + "' alt='TEMPO' />") if tempo_logo else ""}</div>
          <div class='coverHeroFade'></div>
        </div>
        <div class='coverHeroCurve'></div>
      </div>
    """

    cover_note_html = f"""
      <div class='coverNoteCenter'>
        <div class='coverProjectTitle' contenteditable='true'>{_escape(project)}</div>
        <div class='coverCrTitle' contenteditable='true'>CR REUNION DE SYNTHESE TECHNIQUE</div>
        <div class='coverCrMeta'>
          N°<span contenteditable='true' class='editInline' data-sync='cr-number'>{_escape(cr_number_default)}</span>
          du <strong>{_escape(cr_date_txt)}</strong>
        </div>
        <div class='nextMeetingBox'>
          <div class='nextMeetingLine1'>La prochaine réunion de synthèse est fixée au</div>
          <div class='nextMeetingLine2'>
            <span contenteditable='true' class='editInline'>{_escape(next_meeting_date_txt)}</span>
            à
            <span contenteditable='true' class='editInline'>14h00</span>
          </div>
          <div contenteditable='true' class='nextMeetingLine3'>BASE VIE — adresse à compléter</div>
        </div>
        <div class='coverAppNote'>
          Téléchargez gratuitement l’application de gestion de projet METRONOME. L’application développée par TEMPO
          dédiée à la gestion de projet. Celle-ci vous permettra de retrouver l’intégralité des réunions de synthèse, comptes rendu,
          planning et suivi des tâches depuis votre smartphone ou votre ordinateur.
        </div>
        <a class='coverUrl' href='https://app.atelier-tempo.fr' target='_blank' rel='noopener'>app.atelier-tempo.fr</a>
      </div>
    """

    report_header_html = f"""
      <div class='reportHeader printHeaderFixed'>
        {_escape(project)} <span class='accent'>— Compte Rendu</span> n°<span contenteditable='true' class='editInline' data-sync='cr-number'>{_escape(cr_number_default)}</span> — Réunion de Synthèse du {_escape(cr_date_txt)}
      </div>
    """

    top_html = f"""
      <div class="topPage">
        {cover_hero_html}
        {cover_note_html}
      </div>
    """
    annexes_html = ""
    try:
        docs = get_documents().copy()
        if not docs.empty:
            meeting_col = next((c for c in docs.columns if "Meeting/ID" in str(c)), None)
            project_col = next((c for c in docs.columns if "Project/Title" in str(c)), None)
            title_col = next((c for c in docs.columns if "Title" in str(c)), None)
            url_col = next((c for c in docs.columns if "URL" in str(c)), None)
            if not url_col:
                url_col = next((c for c in docs.columns if "Link" in str(c)), None)
            if meeting_col:
                docs = docs.loc[docs[meeting_col].astype(str) == str(meeting_id)].copy()
            elif project_col:
                docs = docs.loc[docs[project_col].fillna("").astype(str).str.strip() == project].copy()
            items = []
            for _, r in docs.iterrows():
                title = _escape(r.get(title_col, "") if title_col else r.get("Title", ""))
                url = _escape(r.get(url_col, "") if url_col else "")
                if title or url:
                    link = (
                        f"<a class='annexLink' href='{url}' target='_blank' rel='noopener'>{title or url}</a>"
                        if url
                        else "—"
                    )
                    label = f"{len(items) + 1}."
                    items.append(
                        f"""
              <tr>
                <td>{label} Annexe</td>
                <td>{link}</td>
              </tr>
                        """
                    )
            if items:
                annexes_html = f"""
      <div class="section reportBlock">
        <table class="annexTable">
          <thead>
            <tr>
              <th>Document</th>
              <th>Lien</th>
            </tr>
          </thead>
          <tbody>
            {''.join(items)}
          </tbody>
        </table>
      </div>
                """
    except MissingDataError:
        annexes_html = ""

    return f"""
<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>CR Synthèse — {_escape(project)} — {_escape(date_txt)}</title>
<style>
{css}
</style>
</head>
<body class="{'pdf' if print_mode else ''}">
  {actions_html}
  <div class="wrap">
    <section class="page page--cover">
      <div class="pageContent">
        {cover_html}
        {top_html}
      </div>
      <div class="docFooter">
        <div class="footLeft">{"<img class='footImg footMark' src='" + logo_tmark + "' alt='' />" if logo_tmark else ""}</div>
        <div class="footCenter"><div style="font-family:'Arial Nova Cond Light','Arial Narrow',Arial,sans-serif;font-size:12px;font-weight:700;color:#111">TEMPO</div><div class="tempoLegal">35, rue Beaubourg, 75003 Paris<br/>SAS au capital de 1 000 Euros - RCS Créteil N° 892 046 301 - APE 7112 B</div>{("<img class='footImg footRythme' src='" + logo_rythme + "' alt='' />") if logo_rythme else ""}</div>
        <div class="footRight"><span class="pageNum"></span></div>
      </div>
    </section>

    <div class="reportPages">
      <section class="page page--report">
        <div class="pageContent">
          <div class="reportTables">
            {report_header_html}
            {presence_html}
            <div class="reportBlocks">
              {zones_html}
              {annexes_html}
              {report_note_html}
            </div>
          </div>
        </div>
        <div class="docFooter">
          <div class="footLeft">{"<img class='footImg footMark' src='" + logo_tmark + "' alt='' />" if logo_tmark else ""}</div>
          <div class="footCenter"><div style="font-family:'Arial Nova Cond Light','Arial Narrow',Arial,sans-serif;font-size:12px;font-weight:700;color:#111">TEMPO</div><div class="tempoLegal">35, rue Beaubourg, 75003 Paris<br/>SAS au capital de 1 000 Euros - RCS Créteil N° 892 046 301 - APE 7112 B</div>{("<img class='footImg footRythme' src='" + logo_rythme + "' alt='' />") if logo_rythme else ""}</div>
          <div class="footRight"><span class="pageNum"></span></div>
        </div>
      </section>
    </div>
  </div>

  <template id="report-page-template">
    <section class="page page--report">
      <div class="pageContent">
        <div class="reportTables">
          {report_header_html}
          <div class="reportBlocks"></div>
        </div>
      </div>
      <div class="docFooter">
        <div class="footLeft">{"<img class='footImg footMark' src='" + logo_tmark + "' alt='' />" if logo_tmark else ""}</div>
        <div class="footCenter"><div style="font-family:'Arial Nova Cond Light','Arial Narrow',Arial,sans-serif;font-size:12px;font-weight:700;color:#111">TEMPO</div><div class="tempoLegal">35, rue Beaubourg, 75003 Paris<br/>SAS au capital de 1 000 Euros - RCS Créteil N° 892 046 301 - APE 7112 B</div>{("<img class='footImg footRythme' src='" + logo_rythme + "' alt='' />") if logo_rythme else ""}</div>
        <div class="footRight"><span class="pageNum"></span></div>
      </div>
    </section>
  </template>

{EDITOR_MEMO_MODAL_HTML}
{QUALITY_MODAL_HTML}
{ANALYSIS_MODAL_HTML}
{TOOLS_MODAL_HTML}
<script>{EDITOR_MEMO_MODAL_JS}</script>
<script>{QUALITY_MODAL_JS}</script>
<script>{ANALYSIS_MODAL_JS}</script>
<script>{TOOLS_MODAL_JS}</script>
<script>{SYNC_EDITABLE_JS}</script>
<script>{RANGE_PICKER_JS}</script>
<script>{PRINT_PREVIEW_TOGGLE_JS}</script>
<script>{CONSTRAINT_TOGGLES_JS}</script>
<script>{LAYOUT_CONTROLS_JS}</script>
<script>{DRAGGABLE_IMAGES_JS}</script>
<script>{PAGINATION_JS}</script>
<script>{PRINT_OPTIMIZE_JS}</script>
<script>{ROW_CONTROL_JS}</script>
<script>{RESIZE_COLUMNS_JS}</script>
<script>{RESIZE_TOP_JS}</script>
</body>
</html>
"""


# -------------------------
# ROUTES
# -------------------------
@app.get("/", response_class=HTMLResponse)
def home(project: Optional[str] = Query(default=None)):
    try:
        return HTMLResponse(render_home(project=project))
    except MissingDataError as err:
        return HTMLResponse(render_missing_data_page(err), status_code=503)


@app.get("/cr", response_class=HTMLResponse)
def cr(
    meeting_id: str = Query(...),
    project: str = Query(default=""),
    print: int = Query(default=0),
    pinned_memos: str = Query(default=""),
    range_start: str = Query(default=""),
    range_end: str = Query(default=""),
):
    try:
        return HTMLResponse(
            render_cr(
                meeting_id=meeting_id,
                project=project,
                print_mode=bool(print),
                pinned_memos=pinned_memos,
                range_start=range_start,
                range_end=range_end,
            )
        )
    except MissingDataError as err:
        return HTMLResponse(render_missing_data_page(err), status_code=503)


@app.get("/health", response_class=JSONResponse)
def health():
    data = {}
    for k, p in [
        ("entries", ENTRIES_PATH),
        ("meetings", MEETINGS_PATH),
        ("companies", COMPANIES_PATH),
        ("projects", PROJECTS_PATH),
    ]:
        try:
            ok = os.path.exists(p)
            mt = _mtime(p)
            data[k] = {"path": p, "exists": ok, "mtime": mt}
        except Exception as e:
            data[k] = {"path": p, "exists": False, "error": str(e)}
    return {"ok": True, "files": data}


@app.get("/api/memos", response_class=JSONResponse)
def api_memos(project: str = Query("", alias="project"), area: str = Query("", alias="area")):
    """
    Return list of MEMOS for a given project and area.
    IMPORTANT: the memo must belong to the zones of THE SAME PROJECT.
    """
    try:
        e = get_entries().copy()
        e = e.loc[e[E_COL_PROJECT_TITLE].fillna("").astype(str).str.strip() == str(project).strip()].copy()
        e = _explode_areas(e)
        if area:
            e = e.loc[e["__area_list__"].astype(str) == str(area)].copy()

        e["__is_task__"] = _series(e, E_COL_IS_TASK, False).apply(_bool_true)
        e = e.loc[e["__is_task__"] == False].copy()

        e["__created__"] = _series(e, E_COL_CREATED, None).apply(_parse_date_any)
        e = e.sort_values(by=["__created__"], ascending=[False])

        items = []
        for _, r in e.iterrows():
            rid = str(r.get(E_COL_ID, "")).strip()
            if not rid:
                continue
            items.append(
                {
                    "id": rid,
                    "title": str(r.get(E_COL_TITLE, "") or "").strip(),
                    "created": _fmt_date(_parse_date_any(r.get(E_COL_CREATED))),
                    "company": str(r.get(E_COL_COMPANY_TASK, "") or "").strip(),
                    "owner": str(r.get(E_COL_OWNER, "") or "").strip(),
                }
            )
        return {"items": items}
    except MissingDataError as err:
        return JSONResponse(
            {"error": str(err), "label": err.label, "path": err.path, "env_var": err.env_var},
            status_code=503,
        )
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)


def _quality_payload(
    text: str,
    language: str = "fr",
    ignore_terms: Optional[set[str]] = None,
) -> Dict[str, object]:
    cleaned_text = re.sub(r"\bnan\b", "", text, flags=re.IGNORECASE).strip()
    if not cleaned_text:
        return {"score": 100, "total": 0, "issues": []}
    ignore_terms = {t.lower() for t in (ignore_terms or set()) if t}
    url = "https://api.languagetool.org/v2/check"
    data = urllib.parse.urlencode({"language": language, "text": cleaned_text}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    matches = payload.get("matches", [])
    words = max(1, len(re.findall(r"\w+", cleaned_text)))
    errors = 0
    score = max(0, int(100 - (errors / words) * 100))
    issues = []
    for m in matches:
        offset = m.get("offset")
        length = m.get("length")
        match_text = (
            cleaned_text[offset : offset + length] if offset is not None and length is not None else ""
        )
        match_text_stripped = match_text.strip()
        if not match_text_stripped:
            continue
        match_lower = match_text_stripped.lower()
        if match_lower == "nan" or match_lower in ignore_terms:
            continue
        if match_text_stripped.isupper() and len(match_text_stripped) > 2:
            continue
        if match_text_stripped.istitle() and len(match_text_stripped) > 2:
            continue
        context = m.get("context", {}) or {}
        repl = ", ".join([r.get("value", "") for r in m.get("replacements", []) if r.get("value")])
        category = (m.get("rule", {}) or {}).get("category", {}) or {}
        errors += 1
        issues.append(
            {
                "message": m.get("message", ""),
                "context": context.get("text", ""),
                "context_offset": context.get("offset"),
                "context_length": context.get("length"),
                "replacements": repl,
                "category": category.get("name", ""),
                "offset": offset,
                "length": length,
                "text": cleaned_text,
            }
        )
    score = max(0, int(100 - (errors / words) * 100))
    return {"score": score, "total": errors, "issues": issues}


@app.get("/api/quality", response_class=JSONResponse)
def api_quality(
    meeting_id: str = Query(...),
    project: str = Query(default=""),
):
    try:
        mrow = meeting_row(meeting_id)
        edf = entries_for_meeting(meeting_id)
        project = (project or str(mrow.get(M_COL_PROJECT_TITLE, ""))).strip()
        meeting_date = _parse_date_any(mrow.get(M_COL_DATE)) or date.today()
        ref_date = date.today()
        rem_df = reminders_for_project(project_title=project, ref_date=ref_date, max_level=8)
        fol_df = followups_for_project(project_title=project, ref_date=ref_date, exclude_entry_ids=set())

        def _items(df: pd.DataFrame, ignore_terms: set[str]) -> List[Dict[str, str]]:
            if df.empty:
                return []
            df = _explode_areas(df.copy())
            out = []
            for _, r in df.iterrows():
                title = str(r.get(E_COL_TITLE, "") or "").strip()
                comment = str(r.get(E_COL_TASK_COMMENT_TEXT, "") or "").strip()
                text = " ".join([t for t in [title, comment] if t]).strip()
                text = re.sub(r"\bnan\b", "", text, flags=re.IGNORECASE).strip()
                if not text:
                    continue
                area = str(r.get("__area_list__", "Général"))
                ignore_terms.add(area.lower())
                ignore_terms.update(_split_words(area))
                out.append({"area": area, "text": text})
            return out
        ignore_terms: set[str] = set()
        if project:
            ignore_terms.add(project.lower())
            ignore_terms.update(_split_words(project))
        company_terms = pd.concat(
            [
                edf.get(E_COL_COMPANY_TASK, pd.Series(dtype=str)),
                rem_df.get(E_COL_COMPANY_TASK, pd.Series(dtype=str)),
                fol_df.get(E_COL_COMPANY_TASK, pd.Series(dtype=str)),
            ],
            ignore_index=True,
        )
        owner_terms = pd.concat(
            [
                edf.get(E_COL_OWNER, pd.Series(dtype=str)),
                rem_df.get(E_COL_OWNER, pd.Series(dtype=str)),
                fol_df.get(E_COL_OWNER, pd.Series(dtype=str)),
            ],
            ignore_index=True,
        )
        for val in pd.concat([company_terms, owner_terms], ignore_index=True).dropna().astype(str):
            ignore_terms.add(val.lower())
            ignore_terms.update(_split_words(val))
        items = _items(edf, ignore_terms) + _items(rem_df, ignore_terms) + _items(fol_df, ignore_terms)
        issues_by_area: Dict[str, List[Dict[str, object]]] = {}
        total_errors = 0
        total_words = 0
        for it in items:
            payload = _quality_payload(it["text"], language="fr", ignore_terms=ignore_terms)
            total_errors += int(payload.get("total", 0))
            cleaned = re.sub(r"\bnan\b", "", it["text"], flags=re.IGNORECASE)
            total_words += max(1, len(re.findall(r"\w+", cleaned)))
            if payload.get("issues"):
                issues_by_area.setdefault(it["area"], []).extend(payload["issues"])
        score = max(0, int(100 - (total_errors / max(1, total_words)) * 100))
        return {"score": score, "total": total_errors, "issues_by_area": issues_by_area}
    except MissingDataError as err:
        return JSONResponse(
            {"error": str(err), "label": err.label, "path": err.path, "env_var": err.env_var},
            status_code=503,
        )
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)


def _timeline_package_color(package_label: str) -> str:
    raw = (package_label or "").strip().lower()
    if not raw:
        return "pkg-default"
    if "cvc" in raw:
        return "pkg-cvc"
    if "plb" in raw or "plomberie" in raw:
        return "pkg-plb"
    if (
        "ele" in raw
        or "élec" in raw
        or "electric" in raw
        or "cfa/cfo" in raw
        or "cfa et cfo" in raw
        or re.search(r"\bcfa\b", raw)
        or re.search(r"\bcfo\b", raw)
    ):
        return "pkg-ele"
    if "goe" in raw or "gros oeuvre" in raw or "gros œuvre" in raw or "structure" in raw or re.search(r"\bstr\b", raw):
        return "pkg-goe"
    if "synth" in raw:
        return "pkg-syn"
    return "pkg-default"


def _build_ai_summary_by_area(df: pd.DataFrame, ref_date: date) -> Dict[str, Dict[str, object]]:
    if df.empty:
        return {}
    out: Dict[str, Dict[str, object]] = {}
    for area, g in df.groupby("__area_list__", dropna=False):
        gg = g.copy()
        gg["__completed__"] = _series(gg, E_COL_COMPLETED, False).apply(_bool_true)
        gg["__deadline__"] = _series(gg, E_COL_DEADLINE, None).apply(_parse_date_any)
        gg["__owner__"] = _series(gg, E_COL_OWNER, "").fillna("").astype(str).str.strip()
        gg["__package__"] = _series(gg, E_COL_PACKAGES, "").fillna("").astype(str).str.strip()

        open_df = gg.loc[~gg["__completed__"]].copy()
        open_count = int(len(open_df))
        late_df = open_df.loc[open_df["__deadline__"].notna() & (open_df["__deadline__"] < ref_date)].copy()
        late_count = int(len(late_df))
        soon_df = open_df.loc[
            open_df["__deadline__"].notna()
            & (open_df["__deadline__"] >= ref_date)
            & ((open_df["__deadline__"] - ref_date).apply(lambda d: d.days) < 5)
        ].copy()
        soon_count = int(len(soon_df))

        dep_mask = (
            open_df[E_COL_TITLE]
            .fillna("")
            .astype(str)
            .str.contains(r"stbat|validation|attente|phibor|diffusion", case=False, regex=True)
        )
        dep_count = int(dep_mask.sum()) if not open_df.empty else 0

        owners_overload = int((open_df["__owner__"].value_counts() > 2).sum()) if not open_df.empty else 0
        inter_lot_tension = int((open_df["__package__"].nunique() >= 2) and (open_count >= 3))

        if late_count > 2 or (late_count > 0 and dep_count > 0):
            level = "🔴 Zone à risque"
        elif late_count > 0 or soon_count >= 2 or inter_lot_tension:
            level = "🟠 Zone sous tension"
        else:
            level = "🟢 Zone maîtrisée"

        risk_parts = []
        if dep_count:
            risk_parts.append("Risque de blocage si validations externes non obtenues rapidement")
        if inter_lot_tension:
            risk_parts.append("Tension inter-lots sur la même période")
        if late_count:
            risk_parts.append("Retards cumulés impactant le séquencement")
        if not risk_parts:
            risk_parts.append("Flux globalement maîtrisé à horizon court")

        if dep_count:
            action = "Relancer STBAT/validation sous 48h et verrouiller un jalon de décision"
        elif late_count > 2:
            action = "Arbitrer les priorités en prochaine réunion et isoler un point critique"
        elif soon_count >= 2:
            action = "Prioriser les échéances <5 jours et assigner un responsable unique"
        else:
            action = "Maintenir le rythme et anticiper les validations de la prochaine séquence"

        indicators = f"{open_count} tâches ouvertes | {late_count} retards | {soon_count} échéances <5j | {dep_count} dépendances externes"
        analysis = "; ".join(risk_parts[:3])
        if owners_overload:
            analysis += "; vigilance charge responsable"

        out[str(area)] = {
            "status": level,
            "indicators": indicators,
            "analysis": analysis,
            "action": action,
        }
    return out


@app.get("/api/home_meeting_dashboard", response_class=JSONResponse)
def api_home_meeting_dashboard(
    meeting_id: str = Query(...),
    project: str = Query(default=""),
    area: str = Query(default=""),
    package: str = Query(default=""),
    status_filter: str = Query(default="open"),
):
    try:
        mrow = meeting_row(meeting_id)
        project = (project or str(mrow.get(M_COL_PROJECT_TITLE, ""))).strip()
        meeting_date = _parse_date_any(mrow.get(M_COL_DATE)) or date.today()
        ref_date = date.today()

        rem_df = reminders_for_project(project_title=project, ref_date=ref_date, max_level=8)
        fol_df = followups_for_project(project_title=project, ref_date=ref_date, exclude_entry_ids=set())

        company_counts = reminders_by_company(rem_df)
        company_logo_map = companies_logo_by_name()

        entries = get_entries().copy()
        entries = entries.loc[entries[E_COL_PROJECT_TITLE].fillna("").astype(str).str.strip() == project].copy()
        entries["__is_task__"] = _series(entries, E_COL_IS_TASK, False).apply(_bool_true)
        entries = entries.loc[entries["__is_task__"] == True].copy()
        entries["__deadline__"] = _series(entries, E_COL_DEADLINE, None).apply(_parse_date_any)
        entries = entries.loc[entries["__deadline__"].notna()].copy()
        entries = _explode_areas(entries)
        entries = _explode_packages(entries)

        if area:
            entries = entries.loc[entries["__area_list__"].astype(str) == area].copy()
            rem_df = rem_df.loc[rem_df["__area_list__"].astype(str) == area].copy() if not rem_df.empty else rem_df
            fol_df = fol_df.loc[fol_df["__area_list__"].astype(str) == area].copy() if not fol_df.empty else fol_df
        if package:
            entries = entries.loc[entries["__package_list__"].astype(str) == package].copy()

        # Rappels entreprises recalculés sur le périmètre/filtre courant
        company_counts = reminders_by_company(rem_df)

        timeline = []
        cal_start = None
        cal_end = None
        total_days = 1
        if not entries.empty:
            entries["__completed__"] = _series(entries, E_COL_COMPLETED, False).apply(_bool_true)
            if status_filter == "open":
                entries = entries.loc[entries["__completed__"] == False].copy()
            elif status_filter == "reminders":
                entries = entries.loc[
                    (entries["__completed__"] == False)
                    & (entries["__deadline__"].notna())
                    & (entries["__deadline__"] < ref_date)
                ].copy()

            entries["__company__"] = _series(entries, E_COL_COMPANY_TASK, "").fillna("").astype(str).str.strip()
            entries["__company__"] = entries["__company__"].replace("", "Non renseigné")
            entries["__start__"] = _series(entries, E_COL_CREATED, None).apply(_parse_date_any)
            entries["__start__"] = entries.apply(
                lambda r: r["__start__"] if r["__start__"] is not None else r["__deadline__"] - timedelta(days=7), axis=1
            )
            entries = entries.sort_values("__deadline__", ascending=True)

            min_start = entries["__start__"].min()
            max_end = entries["__deadline__"].max()
            if min_start is None or max_end is None:
                min_start = ref_date - timedelta(days=30)
                max_end = ref_date + timedelta(days=180)
            if min_start > max_end:
                min_start, max_end = max_end, min_start
            cal_start = min_start
            cal_end = max_end
            total_days = max(1, (max_end - min_start).days + 1)

            for _, r in entries.head(120).iterrows():
                d_start = r.get("__start__")
                d_end = r.get("__deadline__")
                if d_start is None or d_end is None:
                    continue
                is_open = not bool(r.get("__completed__", False))
                status = "clos"
                if is_open and d_end < ref_date:
                    status = "rappel"
                elif is_open:
                    status = "a_suivre"
                area_label = str(r.get("__area_list__", "Général"))
                package_label = str(r.get("__package_list__", "Sans lot"))
                timeline.append({
                    "title": str(r.get(E_COL_TITLE, "") or "").strip(),
                    "start": d_start.isoformat(),
                    "end": d_end.isoformat(),
                    "start_txt": _fmt_date(d_start),
                    "end_txt": _fmt_date(d_end),
                    "offset_days": int((d_start - min_start).days),
                    "duration_days": int(max(1, (d_end - d_start).days + 1)),
                    "area": area_label,
                    "package": package_label,
                    "perimeter": area_label,
                    "package_color": _timeline_package_color(package_label),
                    "company": str(r.get("__company__", "Non renseigné")),
                    "company_logo": str(company_logo_map.get(_norm_name(str(r.get("__company__", "") or "")), "") or "").strip(),
                    "owner": str(r.get(E_COL_OWNER, "") or "").strip() or "Non attribué",
                    "task_id": str(r.get(E_COL_ID, "") or "").strip(),
                    "comment": str(r.get(E_COL_TASK_COMMENT_TEXT, "") or "").strip(),
                    "meeting_id": str(r.get(E_COL_MEETING_ID, "") or "").strip(),
                    "meeting_linked": str(r.get(E_COL_MEETING_ID, "") or "").strip() == str(meeting_id),
                    "completed": bool(r.get("__completed__", False)),
                    "status": status,
                })

        area_options = sorted({str(a) for a in entries.get("__area_list__", pd.Series([], dtype=str)).dropna().astype(str) if str(a).strip()})
        package_options = sorted({str(a) for a in entries.get("__package_list__", pd.Series([], dtype=str)).dropna().astype(str) if str(a).strip()})
        ai_summary = _build_ai_summary_by_area(entries, ref_date)

        entries_for_summary = entries.copy()
        if not entries_for_summary.empty:
            entries_for_summary["__completed__"] = _series(entries_for_summary, E_COL_COMPLETED, False).apply(_bool_true)
            entries_for_summary["__company__"] = _series(entries_for_summary, E_COL_COMPANY_TASK, "").fillna("").astype(str).str.strip().replace("", "Non renseigné")
        open_subjects_total = 0
        reminder_total = 0
        by_perimeter = []
        closed_company_counts = []
        if not entries_for_summary.empty:
            open_df_all = entries_for_summary.loc[entries_for_summary["__completed__"] == False].copy()
            open_subjects_total = int(len(open_df_all))
            reminder_total = int((open_df_all["__deadline__"] < ref_date).sum()) if not open_df_all.empty else 0
            if not open_df_all.empty:
                for area_name, g in open_df_all.groupby("__area_list__", dropna=False):
                    rem_g = g.loc[g["__deadline__"] < ref_date]
                    rem_titles = [str(t).strip() for t in rem_g.get(E_COL_TITLE, pd.Series([], dtype=str)).fillna("").astype(str).tolist() if str(t).strip()][:3]
                    by_perimeter.append({
                        "perimeter": str(area_name) if str(area_name).strip() else "Général",
                        "open_subjects": int(len(g)),
                        "reminders": int(len(rem_g)),
                        "reminder_subjects": rem_titles,
                    })
                by_perimeter = sorted(by_perimeter, key=lambda x: (-int(x["reminders"]), -int(x["open_subjects"]), str(x["perimeter"])))
            closed_df = entries_for_summary.loc[(entries_for_summary["__completed__"] == True) & (entries_for_summary["__deadline__"] < ref_date)].copy()
            if not closed_df.empty:
                g = closed_df.groupby("__company__", dropna=False).size().reset_index(name="count")
                g = g.sort_values("count", ascending=False)
                closed_company_counts = [
                    {
                        "name": str(r["__company__"]),
                        "logo": str(company_logo_map.get(_norm_name(str(r["__company__"])), "") or "").strip(),
                        "count": int(r["count"]),
                    }
                    for _, r in g.head(12).iterrows()
                ]

        return {
            "kpis": {
                "open_reminders": int(len(rem_df)),
                "open_followups": int(len(fol_df)),
                "company_cumulative": company_counts,
            },
            "timeline": timeline,
            "calendar": {
                "start": cal_start.isoformat() if cal_start else "",
                "end": cal_end.isoformat() if cal_end else "",
                "total_days": int(total_days),
            },
            "filters": {"areas": area_options, "packages": package_options},
            "ai_summary_by_area": ai_summary,
            "summary_snapshot": {
                "open_subjects_total": int(open_subjects_total),
                "reminder_total": int(reminder_total),
                "by_perimeter": by_perimeter,
                "company_open_reminders": company_counts,
                "company_closed_reminders": closed_company_counts,
            },
            "reference_date": ref_date.isoformat(),
            "meeting_date": meeting_date.isoformat(),
        }
    except MissingDataError as err:
        return JSONResponse(
            {"error": str(err), "label": err.label, "path": err.path, "env_var": err.env_var},
            status_code=503,
        )
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)


@app.post("/api/meeting_package_email", response_class=JSONResponse)
def api_meeting_package_email(
    meeting_id: str = Query(...),
    package: str = Query(...),
    recipients: str = Query(..., description="Comma-separated emails"),
    project: str = Query(default=""),
    send_email: bool = Query(default=False),
):
    try:
        mrow = meeting_row(meeting_id)
        project = (project or str(mrow.get(M_COL_PROJECT_TITLE, ""))).strip()
        meeting_date = _parse_date_any(mrow.get(M_COL_DATE)) or date.today()
        today_ref = date.today()

        edf = entries_for_meeting(meeting_id).copy()
        if project:
            edf = edf.loc[_series(edf, E_COL_PROJECT_TITLE, "").fillna("").astype(str).str.strip() == project].copy()
        if edf.empty:
            return {"meeting_id": meeting_id, "package": package, "rows": 0, "message": "Aucune donnée pour la réunion/projet."}

        edf = _explode_packages(edf)
        pkg = (package or "").strip()
        if pkg:
            edf = edf.loc[edf["__package_list__"].astype(str).str.strip() == pkg].copy()
        if edf.empty:
            return {"meeting_id": meeting_id, "package": package, "rows": 0, "message": "Aucune entrée pour ce lot."}

        edf["__is_task__"] = _series(edf, E_COL_IS_TASK, False).apply(_bool_true)
        edf["__deadline__"] = _series(edf, E_COL_DEADLINE, None).apply(_parse_date_any)
        edf["__completed__"] = _series(edf, E_COL_COMPLETED, False).apply(_bool_true)
        edf["__company__"] = _series(edf, E_COL_COMPANY_TASK, "").fillna("").astype(str).str.strip().replace("", "Non renseigné")

        tasks = edf.loc[edf["__is_task__"] == True].copy()
        memos = edf.loc[edf["__is_task__"] == False].copy()
        reminders = tasks.loc[(tasks["__completed__"] == False) & (tasks["__deadline__"].notna()) & (tasks["__deadline__"] < today_ref)].copy()
        opened = tasks.loc[tasks["__completed__"] == False].copy()
        closed = tasks.loc[tasks["__completed__"] == True].copy()

        def _task_row_payload(r):
            dl = r.get("__deadline__")
            return {
                "id": str(r.get(E_COL_ID, "") or "").strip(),
                "title": str(r.get(E_COL_TITLE, "") or "").strip(),
                "area": str(r.get(E_COL_AREAS, "") or "").strip(),
                "company": str(r.get("__company__", "") or "").strip(),
                "owner": str(r.get(E_COL_OWNER, "") or "").strip(),
                "deadline": dl.isoformat() if isinstance(dl, date) else "",
                "status": str(r.get(E_COL_STATUS, "") or "").strip(),
                "comment": str(r.get(E_COL_TASK_COMMENT_TEXT, "") or "").strip(),
                "completed": bool(r.get("__completed__", False)),
            }

        def _memo_row_payload(r):
            return {
                "id": str(r.get(E_COL_ID, "") or "").strip(),
                "title": str(r.get(E_COL_TITLE, "") or "").strip(),
                "area": str(r.get(E_COL_AREAS, "") or "").strip(),
                "company": str(r.get("__company__", "") or "").strip(),
                "owner": str(r.get(E_COL_OWNER, "") or "").strip(),
                "status": str(r.get(E_COL_STATUS, "") or "").strip(),
                "comment": str(r.get(E_COL_TASK_COMMENT_TEXT, "") or "").strip(),
            }

        payload = {
            "meeting_id": str(meeting_id),
            "project": project,
            "meeting_date": meeting_date.isoformat(),
            "generated_at": today_ref.isoformat(),
            "package": pkg or "Sans lot",
            "kpis": {
                "tasks_total": int(len(tasks)),
                "opened_total": int(len(opened)),
                "closed_total": int(len(closed)),
                "reminders_total": int(len(reminders)),
                "memos_total": int(len(memos)),
            },
            "tasks_opened": [_task_row_payload(r) for _, r in opened.iterrows()],
            "tasks_closed": [_task_row_payload(r) for _, r in closed.iterrows()],
            "tasks_reminders": [_task_row_payload(r) for _, r in reminders.iterrows()],
            "memos": [_memo_row_payload(r) for _, r in memos.iterrows()],
        }

        recips = [x.strip() for x in str(recipients or "").split(",") if x.strip()]
        if not send_email:
            return {"dry_run": True, "recipients": recips, "payload": payload}

        smtp_host = os.getenv("METRONOME_SMTP_HOST", "").strip()
        smtp_port = int(os.getenv("METRONOME_SMTP_PORT", "587") or "587")
        smtp_user = os.getenv("METRONOME_SMTP_USER", "").strip()
        smtp_pass = os.getenv("METRONOME_SMTP_PASS", "").strip()
        smtp_from = os.getenv("METRONOME_SMTP_FROM", smtp_user).strip()
        use_tls = os.getenv("METRONOME_SMTP_TLS", "1").strip() not in {"0", "false", "False"}

        if not smtp_host or not smtp_from or not recips:
            return JSONResponse({"error": "Configuration SMTP ou destinataires manquants.", "payload": payload}, status_code=400)

        msg = EmailMessage()
        msg["Subject"] = f"[METRONOME] Réunion {meeting_id} • Lot {payload['package']}"
        msg["From"] = smtp_from
        msg["To"] = ", ".join(recips)
        msg.set_content(json.dumps(payload, ensure_ascii=False, indent=2))

        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
            if use_tls:
                server.starttls()
            if smtp_user:
                server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        return {"sent": True, "recipients": recips, "kpis": payload["kpis"]}
    except MissingDataError as err:
        return JSONResponse(
            {"error": str(err), "label": err.label, "path": err.path, "env_var": err.env_var},
            status_code=503,
        )
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)


@app.get("/api/meeting_company_mail_draft", response_class=JSONResponse)
def api_meeting_company_mail_draft(
    meeting_id: str = Query(...),
    project: str = Query(default=""),
    companies: str = Query(default=""),
    selected_companies: str = Query(default=""),
    all_companies: bool = Query(default=True),
    period_start: str = Query(default=""),
    period_end: str = Query(default=""),
    include_tasks: bool = Query(default=True),
    include_memos: bool = Query(default=True),
    include_reminders: bool = Query(default=True),
    include_closed: bool = Query(default=True),
    include_without_created: bool = Query(default=True),
):
    try:
        mrow = meeting_row(meeting_id)
        project = (project or str(mrow.get(M_COL_PROJECT_TITLE, ""))).strip()
        meeting_date = _parse_date_any(mrow.get(M_COL_DATE)) or date.today()
        p_start = _parse_date_any(period_start) if str(period_start or "").strip() else None
        p_end = _parse_date_any(period_end) if str(period_end or "").strip() else None
        if p_start and p_end and p_start > p_end:
            p_start, p_end = p_end, p_start

        meeting_df = entries_for_meeting(meeting_id).copy()
        if project:
            meeting_df = meeting_df.loc[_series(meeting_df, E_COL_PROJECT_TITLE, "").fillna("").astype(str).str.strip() == project].copy()

        all_project_df = get_entries().copy()
        if project:
            all_project_df = all_project_df.loc[_series(all_project_df, E_COL_PROJECT_TITLE, "").fillna("").astype(str).str.strip() == project].copy()
        if all_project_df.empty:
            return {"emails_detected": [], "subject": "", "html": "", "text_fallback": "Aucune donnée projet."}

        all_project_df["__company__"] = _series(all_project_df, E_COL_COMPANY_TASK, "").fillna("").astype(str).str.strip().replace("", "Non renseigné")
        all_project_df = _explode_areas(all_project_df)
        all_project_df["__is_task__"] = _series(all_project_df, E_COL_IS_TASK, False).apply(_bool_true)
        all_project_df["__deadline__"] = _series(all_project_df, E_COL_DEADLINE, None).apply(_parse_date_any)
        all_project_df["__created__"] = _series(all_project_df, E_COL_CREATED, None).apply(_parse_date_any)
        all_project_df["__completed__"] = _series(all_project_df, E_COL_COMPLETED, False).apply(_bool_true)
        all_project_df["__done_date__"] = _series(all_project_df, E_COL_COMPLETED_END, None).apply(_parse_date_any)

        raw_selected = selected_companies or companies
        selected = [x.strip() for x in str(raw_selected or "").split(",") if x.strip()]
        if all_companies:
            target_companies = sorted({str(c).strip() for c in all_project_df["__company__"].astype(str).tolist() if str(c).strip()})
        elif selected:
            sn = {_norm_name(x) for x in selected}
            target_companies = sorted({str(c).strip() for c in all_project_df["__company__"].astype(str).tolist() if _norm_name(str(c)) in sn})
        else:
            target_companies = sorted({str(c).strip() for c in _series(meeting_df, E_COL_COMPANY_TASK, "").fillna("").astype(str).tolist() if str(c).strip()})

        if not target_companies:
            return {"emails_detected": [], "subject": "", "html": "", "text_fallback": "Aucune entreprise cible."}

        emails_map = companies_email_by_name()
        contacts_map = _contacts_by_company_name()
        deployed_people = _project_deployed_people_by_company(project)
        users_contacts_map = _users_deployed_contacts_by_company(project)

        recipients_by_company: Dict[str, List[str]] = {}
        for comp in target_companies:
            key = _norm_name(comp)
            people_norm = {_norm_name(x) for x in deployed_people.get(key, [])}
            company_contacts = contacts_map.get(key, [])
            users_contacts = users_contacts_map.get(key, [])

            matched_company_emails: List[str] = []
            for ct in company_contacts:
                nm = str(ct.get("name", "") or "").strip()
                em = str(ct.get("email", "") or "").strip().lower()
                if not em:
                    continue
                nm_norm = _norm_name(nm)
                if people_norm and nm_norm and (nm_norm in people_norm or any((pn and (pn in nm_norm or nm_norm in pn)) for pn in people_norm)):
                    matched_company_emails.append(em)

            users_emails = sorted({str(x.get("email", "") or "").strip().lower() for x in users_contacts if str(x.get("email", "") or "").strip()})
            base_emails = sorted({e.lower() for e in emails_map.get(key, []) if e})
            recipients_by_company[comp] = sorted(set(matched_company_emails or users_emails or base_emails))

        concern_cols = [
            col for col in all_project_df.columns
            if ("concerne" in str(col).lower() or "concerned" in str(col).lower() or "concern" in str(col).lower())
        ]
        # Fallback robustesse: certains exports n'ont pas de colonne "Concerne" explicite.
        # Dans ce cas on se rabat sur la colonne entreprise de la tâche pour ne pas perdre toute la data.
        if not concern_cols and E_COL_COMPANY_TASK in all_project_df.columns:
            concern_cols = [E_COL_COMPANY_TASK]

        target_norm = {_norm_name(x): x for x in target_companies}
        items_all: List[dict] = []
        for _, r in all_project_df.iterrows():
            concerne_raw = _companies_concerned_for_row(r, concern_cols)
            if not concerne_raw:
                # fallback ligne à ligne: si aucune valeur concernée lue mais entreprise tâche présente
                # on évite un mail vide sur des exports incomplets.
                co_task = str(r.get(E_COL_COMPANY_TASK, "") or "").strip()
                if co_task:
                    concerne_raw = [co_task]
            concerne_filtered = []
            seen_cf = set()
            for c in concerne_raw:
                n = _norm_name(c)
                if n in target_norm and n not in seen_cf:
                    seen_cf.add(n)
                    concerne_filtered.append(target_norm[n])
            if not concerne_filtered:
                continue

            is_task = bool(r.get("__is_task__", False))
            is_completed = bool(r.get("__completed__", False))
            due_date = r.get("__deadline__")
            created_date = r.get("__created__")
            done_date = r.get("__done_date__")
            rem_lvl = 0

            if (not include_without_created) and (not isinstance(created_date, date)):
                continue

            if (not is_task) and not include_memos:
                continue

            if p_start or p_end:
                dates_for_match = [d for d in [created_date, due_date, done_date] if isinstance(d, date)]
                if not dates_for_match:
                    continue
                in_period = False
                for dd in dates_for_match:
                    if p_start and dd < p_start:
                        continue
                    if p_end and dd > p_end:
                        continue
                    in_period = True
                    break
                if not in_period:
                    continue

            # Fenêtre par défaut (si aucune période saisie):
            # - entrées disponibles à la date de réunion (open/memos)
            # - rappels
            # - sujets "fait-le" dans les 2 semaines avant la réunion
            if not p_start and not p_end:
                if is_task and is_completed:
                    if not isinstance(done_date, date):
                        continue
                    if not (meeting_date - timedelta(days=14) <= done_date <= meeting_date):
                        continue
                else:
                    if isinstance(created_date, date) and created_date > meeting_date:
                        continue

            is_reminder = bool(is_task and (not is_completed) and isinstance(due_date, date) and due_date < meeting_date)
            if is_task and is_completed and not include_closed:
                continue
            if is_reminder and not include_reminders:
                continue
            if is_task and (not is_completed) and (not is_reminder) and not include_tasks:
                continue

            if is_task and not is_completed and isinstance(due_date, date) and due_date < meeting_date:
                itype = "reminder"
                rem_lvl = int(reminder_level(due_date, False, meeting_date) or 1)
                done_label = f"RAPPEL {rem_lvl}"
            elif is_task and not is_completed:
                itype = "open"
                done_label = ""
            elif is_task and is_completed:
                itype = "done"
                done_label = _fmt_mail_date(done_date) if isinstance(done_date, date) else ""
            else:
                itype = "memo"
                done_label = "/"

            companies_vals = [str(x).strip() for x in concerne_filtered if str(x).strip()]
            if not companies_vals:
                continue

            lot_vals = _split_multi_labels(str(r.get(E_COL_PACKAGES, "") or "").strip())
            lot_vals = [x for x in lot_vals if str(x).strip()]
            if not lot_vals:
                lot_vals = ["Sans lot"]

            items_all.append({
                "type": itype,
                "subject": str(r.get(E_COL_TITLE, "") or "").strip() or "(sans titre)",
                "created_date": created_date,
                "due_date": due_date,
                "done_date": done_date,
                "done_label": done_label,
                "concerne": lot_vals,
                "target_companies": companies_vals,
                "area": _clean_area_name(str(r.get("__area_list__", "") or "").strip() or "Généralité"),
                "reminder_level": rem_lvl,
            })

        subject, html = build_company_email_html(
            meeting={"project": project, "meeting_id": meeting_id, "selected_company_count": len(target_companies)},
            company={"name": "Toutes les entreprises" if len(target_companies) > 1 else target_companies[0]},
            items=items_all,
            meeting_date=meeting_date,
            app_url="https://app.atelier-tempo.fr",
        )
        target_phrase_txt = _target_companies_phrase(len(target_companies))
        text_fallback = (
            f"Bonjour,\n\nVeuillez trouver ci-après la liste des sujets ouverts déployés à la date du {_fmt_mail_date(date.today())} sur l'application METRONOME pour {target_phrase_txt}.\n"
            "(Version texte. Utiliser le HTML pour un rendu complet.)"
        )
        return {
            "meeting_id": meeting_id,
            "project": project,
            "selected_companies": target_companies,
            "subject": subject,
            "html": html,
            "text_fallback": text_fallback,
        }
    except MissingDataError as err:
        return JSONResponse(
            {"error": str(err), "label": err.label, "path": err.path, "env_var": err.env_var},
            status_code=503,
        )
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)


@app.get("/api/analysis", response_class=JSONResponse)
def api_analysis(
    meeting_id: str = Query(...),
    project: str = Query(default=""),
):
    try:
        mrow = meeting_row(meeting_id)
        edf = entries_for_meeting(meeting_id)
        project = (project or str(mrow.get(M_COL_PROJECT_TITLE, ""))).strip()
        ref_date = _parse_date_any(mrow.get(M_COL_DATE)) or date.today()
        rem_df = reminders_for_project(project_title=project, ref_date=ref_date, max_level=8)
        fol_df = followups_for_project(project_title=project, ref_date=ref_date, exclude_entry_ids=set())

        is_task = _series(edf, E_COL_IS_TASK, False).apply(_bool_true)
        tasks = edf[is_task].copy()
        completed = _series(tasks, E_COL_COMPLETED, False).apply(_bool_true)
        open_tasks = tasks[~completed]

        points = []
        risks = []
        follow_ups = []
        late_tasks = len(rem_df)
        followups = len(fol_df)

        if late_tasks:
            points.append(f"{late_tasks} rappel(s) en retard à la date de séance.")
            risks.append("Retards critiques à prioriser avant la prochaine réunion.")
        if len(open_tasks):
            points.append(f"{len(open_tasks)} tâche(s) ouverte(s) dans la séance.")
        if followups:
            follow_ups.append(f"{followups} tâche(s) à suivre sur le projet.")

        if not points:
            points.append("Aucun point bloquant détecté dans la séance.")

        least_responsive = reminders_by_company(rem_df)[:5]
        followups_by_area = {}
        if not fol_df.empty:
            for area, g in fol_df.groupby("__area_list__", dropna=False):
                titles = g.get(E_COL_TITLE, pd.Series([], dtype=str)).fillna("").astype(str).tolist()
                followups_by_area[str(area)] = [t for t in titles if t.strip()][:6]

        return {
            "kpis": {"late_tasks": late_tasks, "open_tasks": int(len(open_tasks)), "followups": followups},
            "points": points,
            "risks": risks,
            "follow_ups": follow_ups,
            "least_responsive": least_responsive,
            "followups_by_area": followups_by_area,
        }
    except MissingDataError as err:
        return JSONResponse(
            {"error": str(err), "label": err.label, "path": err.path, "env_var": err.env_var},
            status_code=503,
        )
    except Exception as ex:
        return JSONResponse({"error": str(ex)}, status_code=500)

@app.get("/api/finance/affaires", response_class=JSONResponse)
def api_finance_affaires(debug: bool = Query(default=False)):
    try:
        data = get_finance_data()
        payload = {
            "count": len(data["affaire_list"]),
            "items": data["affaire_list"],
        }
        if debug:
            payload["metadata"] = data["metadata"]
        return payload
    except MissingDataError as err:
        return JSONResponse(
            {"error": str(err), "label": err.label, "path": err.path, "env_var": err.env_var},
            status_code=503,
        )
    except Exception as ex:
        logger.exception("[FINANCE] Failed to list affaires")
        return JSONResponse({"error": str(ex)}, status_code=500)


@app.get("/api/finance/affaires/{affaire_id}", response_class=JSONResponse)
def api_finance_affaire_detail(affaire_id: str, debug: bool = Query(default=False)):
    try:
        key = str(affaire_id or "").strip()
        if not key:
            raise HTTPException(status_code=400, detail="affaire_id est requis")

        data = get_finance_data()
        affaire = data["affaires"].get(key)
        if affaire is None:
            raise HTTPException(status_code=404, detail=f"Affaire introuvable: {key}")

        payload = {
            "affaire": affaire,
        }
        if debug:
            payload["metadata"] = data["metadata"]
        return payload
    except HTTPException:
        raise
    except MissingDataError as err:
        return JSONResponse(
            {"error": str(err), "label": err.label, "path": err.path, "env_var": err.env_var},
            status_code=503,
        )
    except Exception as ex:
        logger.exception("[FINANCE] Failed to get affaire detail for %s", affaire_id)
        return JSONResponse({"error": str(ex)}, status_code=500)
