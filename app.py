
import csv
import io
import json
import logging
import os
import re
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from openpyxl import load_workbook

APP_TITLE = "Gestion Affaire - Finance"
DEFAULT_WORKBOOK_PATH = r"\\192.168.10.100\03 - finances\10 - TABLEAU DE BORD\01 - TABLEAU ACTIVITEE\TABLEAU ACTIVITEE.xlsx"
DEFAULT_SHEET_NAME = "AFFAIRES 2026"
DEFAULT_CACHE_FILE = "finance_cache.json"

WORKBOOK_PATH = os.getenv("ACTIVITE_XLSX_PATH", DEFAULT_WORKBOOK_PATH)
SHEET_NAME = os.getenv("ACTIVITE_SHEET_NAME", DEFAULT_SHEET_NAME)
CACHE_FILE = os.getenv("FINANCE_CACHE_FILE", DEFAULT_CACHE_FILE)
EXPECTED_SCHEMA_VERSION = "finance_affaires_dataset_v4"
TEMPO_LOGO_PATH = os.getenv("TEMPO_LOGO_PATH", r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Content\T logo.png")
METRONOME_BASE_PATH = os.getenv("METRONOME_BASE_PATH", r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME")
METRONOME_FILES = {
    "projects": "Projects.csv",
    "entries": "Entries (Tasks & Memos).csv",
    "meetings": "Meetings.csv",
    "areas": "Areas.csv",
    "packages": "Packages.csv",
    "companies": "Companies.csv",
    "users": "Users.csv",
    "comments": "Comments.csv",
    "documents": "Documents.csv",
}

METRONOME_COLUMN_ALIASES = {
    "project_title_entries": ["Project/Title", "Project/Title (dev)", "Project"],
    "project_title_projects": ["Title", "Name"],
    "project_name_projects": ["Name", "Title"],
    "project_start": ["Start Date", "StartDate", "Start"],
    "project_end": ["End Date", "EndDate", "End"],
    "entry_done_date": ["Done Date", "DoneDate", "Completed Date", "CompletedDate", "Completed/Declared End", "ClosedDate", "UpdatedAt"],
    "entry_category": ["Category/Name to display", "Category"],
    "entry_project_id": ["Project/ID", "Project"],
    "entry_meeting_id": ["Meeting/ID", "Meeting"],
    "entry_owner_id": ["Owner for Tasks/ID", "Owner/ID", "Assignee"],
    "entry_created_by_id": ["Created by/ID", "CreatedBy/ID"],
    "entry_comment_editor_id": ["Comment for Tasks/Editor ID"],
    "entry_completed_reporting_user_id": ["Completed/Reporting User ID"],
    "entry_completed_declared_user_id": ["Completed/Declared User ID"],
    "entry_task_package_id": ["Packages/ID for Task", "Package", "Package/ID"],
    "entry_memo_package_ids": ["Packages/IDs for Memos"],
    "entry_area_ids": ["Areas/IDs", "Area/ID", "Area"],
    "entry_deadline": ["Deadline & Status for Tasks/Deadline", "DueDate"],
    "entry_status_id": ["Deadline & Status for Tasks/Status ID", "Status ID"],
    "entry_status_label": ["Deadline & Status for Tasks/Status Emoji + Text", "Status"],
    "entry_comment_text": ["Comment for Tasks/Text", "Comment", "Text"],
    "comment_memo_id": ["Memo/ID", "Entry", "Memo"],
    "comment_owner_id": ["Owner/ID", "Owner"],
    "comment_date": ["Date", "Created", "Creation Date", "UpdatedAt"],
    "meeting_date": ["Date", "Meeting Date"],
    "meeting_attending_company_ids": ["Companies/Attending IDs"],
    "meeting_missing_company_ids": ["Companies/Missing IDs", "Companies/Missing Calculated IDs (dev)"],
    "package_company_id": ["Company/ID", "Company"],
    "package_project_id": ["Project/ID"],
    "area_project_id": ["Project/ID"],
    "company_collaborators_ids": ["Collaborators/IDs"],
    "documents_project_id": ["Project/ID"],
    "documents_meeting_id": ["Meeting/ID"],
}

MONTHS = [
    "janvier", "fevrier", "mars", "avril", "mai", "juin",
    "juillet", "aout", "septembre", "octobre", "novembre", "decembre",
]
MONTH_LABELS = {
    "janvier": "Janv.", "fevrier": "Févr.", "mars": "Mars", "avril": "Avr.",
    "mai": "Mai", "juin": "Juin", "juillet": "Juil.", "aout": "Août",
    "septembre": "Sept.", "octobre": "Oct.", "novembre": "Nov.", "decembre": "Déc.",
}
COLUMN_ORDER = [
    "client", "affaire", "tag", "numero", "delai_reglement_jours", "commande_ht",
    "facturation_cumulee_2017", "facturation_cumulee_2018", "facturation_cumulee_2021",
    "facturation_cumulee_2022", "facturation_cumulee_2023", "facturation_cumulee_2024",
    "facturation_cumulee_2025", "facturation_cumulee_2026", "reste_a_facturer",
    "janvier_previsionnel", "janvier_facture", "fevrier_previsionnel", "fevrier_facture",
    "mars_previsionnel", "mars_facture", "avril_previsionnel", "avril_facture",
    "mai_previsionnel", "mai_facture", "juin_previsionnel", "juin_facture",
    "juillet_previsionnel", "juillet_facture", "aout_previsionnel", "aout_facture",
    "septembre_previsionnel", "septembre_facture", "octobre_previsionnel", "octobre_facture",
    "novembre_previsionnel", "novembre_facture", "decembre_previsionnel", "decembre_facture",
    "total_previsionnel", "total_facture",
]

logger = logging.getLogger("finance_cockpit")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

app = FastAPI(title=APP_TITLE)


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", text)


def clean_number(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = clean_text(value).replace("€", "").replace("\u202f", "").replace(" ", "")
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def safe_int(value: Any) -> int:
    return int(round(clean_number(value)))


def slugify(value: Any) -> str:
    text = clean_text(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text or "affaire"


def build_display_name(client: str, affaire: str) -> str:
    parts = [p for p in [clean_text(client), clean_text(affaire)] if p]
    return " - ".join(parts) if parts else "Affaire sans nom"


def month_payload() -> Dict[str, Dict[str, float]]:
    return {m: {"previsionnel": 0.0, "facture": 0.0, "ecart": 0.0} for m in MONTHS}


def row_tuple_to_dict(values: tuple) -> Dict[str, Any]:
    data = {}
    for idx, key in enumerate(COLUMN_ORDER):
        data[key] = values[idx] if idx < len(values) else None
    return data


def anteriorite_from_row(row: Dict[str, Any]) -> float:
    keys = [
        "facturation_cumulee_2017",
        "facturation_cumulee_2018",
        "facturation_cumulee_2021",
        "facturation_cumulee_2022",
        "facturation_cumulee_2023",
        "facturation_cumulee_2024",
        "facturation_cumulee_2025",
    ]
    return sum(clean_number(row.get(k)) for k in keys)


def facture_2026_from_row(row: Dict[str, Any]) -> float:
    return clean_number(row.get("facturation_cumulee_2026"))


class FinanceService:
    def __init__(self, workbook_path: str, sheet_name: str, cache_file: str) -> None:
        self.workbook_path = workbook_path
        self.sheet_name = sheet_name
        self.cache_file = Path(cache_file)
        self._cache: Dict[str, Any] = {
            "status": "idle",
            "generated_at": None,
            "rows_read": 0,
            "rows_kept": 0,
            "affaires_count": 0,
            "warnings": [],
            "items": {},
            "ordered_ids": [],
            "source_path": workbook_path,
            "source_mtime": None,
        }
        self._lock = Lock()

    def workbook_exists(self) -> bool:
        return os.path.exists(self.workbook_path)

    def source_mtime(self) -> Optional[float]:
        try:
            return os.path.getmtime(self.workbook_path)
        except OSError:
            return None

    def cache_status(self) -> Dict[str, Any]:
        cache = self.get_finance_cache()
        return {
            "status": cache.get("status", "unknown"),
            "generated_at": cache.get("generated_at"),
            "rows_read": cache.get("rows_read", 0),
            "rows_kept": cache.get("rows_kept", 0),
            "affaires_count": cache.get("affaires_count", 0),
            "warnings": cache.get("warnings", []),
            "source_path": cache.get("source_path"),
            "source_mtime": cache.get("source_mtime"),
            "cache_file": str(self.cache_file.resolve()),
        }

    def get_finance_cache(self) -> Dict[str, Any]:
        with self._lock:
            cache = dict(self._cache)
        if (
            cache.get("status") == "ready"
            and cache.get("items")
            and cache.get("schema_version") == EXPECTED_SCHEMA_VERSION
        ):
            return cache

        if self.cache_file.exists():
            try:
                disk = json.loads(self.cache_file.read_text(encoding="utf-8"))
                if (
                    disk.get("schema_version") == EXPECTED_SCHEMA_VERSION
                    and disk.get("source_mtime") == self.source_mtime()
                    and disk.get("items")
                ):
                    with self._lock:
                        self._cache = disk
                    return dict(self._cache)
            except Exception:
                logger.exception("Impossible de lire le cache JSON")

        return self.rebuild_finance_cache()

    def rebuild_finance_cache(self) -> Dict[str, Any]:
        if not self.workbook_exists():
            raise FileNotFoundError(f"Fichier introuvable : {self.workbook_path}")

        logger.info("Reconstruction du cache finance depuis %s", self.workbook_path)
        with self._lock:
            self._cache["status"] = "building"

        wb = load_workbook(self.workbook_path, data_only=True, read_only=True)
        if self.sheet_name not in wb.sheetnames:
            raise ValueError(f"Onglet introuvable : {self.sheet_name}")

        ws = wb[self.sheet_name]
        parsed = self.parse_affaires_sheet(ws)

        cache = {
            "schema_version": EXPECTED_SCHEMA_VERSION,
            "status": "ready",
            "generated_at": now_iso(),
            "rows_read": parsed["meta"]["rows_read"],
            "rows_kept": parsed["meta"]["rows_kept"],
            "affaires_count": parsed["meta"]["affaires_count"],
            "warnings": parsed["meta"]["warnings"],
            "source_path": self.workbook_path,
            "source_mtime": self.source_mtime(),
            "sheet_name": self.sheet_name,
            "items": parsed["items"],
            "ordered_ids": parsed["ordered_ids"],
        }
        with self._lock:
            self._cache = cache

        try:
            self.cache_file.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            logger.exception("Impossible d'écrire le cache JSON")

        logger.info(
            "Cache prêt | affaires=%s rows_read=%s rows_kept=%s",
            cache["affaires_count"], cache["rows_read"], cache["rows_kept"],
        )
        return dict(cache)

    def parse_affaires_sheet(self, ws) -> Dict[str, Any]:
        rows_read = 0
        rows_kept = 0
        warnings: List[str] = []
        current_client = ""
        current_parent: Optional[Dict[str, Any]] = None
        blank_streak = 0
        parent_rows: List[Dict[str, Any]] = []

        for row_index, values in enumerate(ws.iter_rows(min_row=13, max_col=len(COLUMN_ORDER), values_only=True), start=13):
            row = row_tuple_to_dict(values)
            raw_client = clean_text(row["client"])
            raw_affaire = clean_text(row["affaire"])
            raw_tag = clean_text(row["tag"])

            if not raw_client and not raw_affaire:
                blank_streak += 1
                if blank_streak >= 6:
                    break
                continue
            blank_streak = 0

            if "pipe operationnel" in slugify(raw_affaire):
                break

            if raw_client:
                current_client = raw_client

            rows_read += 1
            is_parent = bool(raw_client and raw_affaire and not raw_tag)

            if is_parent:
                current_parent = self._normalize_row(row, current_client=current_client)
                current_parent["missions"] = []
                current_parent["_row_index"] = row_index
                parent_rows.append(current_parent)
                rows_kept += 1
                continue

            if not raw_affaire:
                continue

            mission = self._normalize_row(
                row,
                current_client=current_client,
                parent_display=current_parent["display_name"] if current_parent else "",
                parent_id=current_parent["affaire_id"] if current_parent else "",
                force_affaire_name=raw_affaire,
            )

            if current_parent is None:
                mission["missions"] = [self._mission_payload_from_affaire(mission)]
                mission["_row_index"] = row_index
                parent_rows.append(mission)
                current_parent = mission
            else:
                current_parent["missions"].append(self._mission_payload_from_affaire(mission))
            rows_kept += 1

        items: Dict[str, Dict[str, Any]] = {}
        ordered_ids: List[str] = []
        for parent in parent_rows:
            affair = self._finalize_affaire(parent)
            items[affair["affaire_id"]] = affair
            ordered_ids.append(affair["affaire_id"])

        meta = {
            "rows_read": rows_read,
            "rows_kept": rows_kept,
            "affaires_count": len(items),
            "warnings": warnings,
        }
        return {"items": items, "ordered_ids": ordered_ids, "meta": meta}

    def _normalize_row(
        self,
        row: Dict[str, Any],
        current_client: str,
        parent_display: str = "",
        parent_id: str = "",
        force_affaire_name: str = "",
    ) -> Dict[str, Any]:
        client = clean_text(row.get("client")) or current_client
        affaire_name = force_affaire_name or clean_text(row.get("affaire"))
        display_name = build_display_name(client, affaire_name)
        affaire_id = slugify(display_name)

        mensuel = month_payload()
        for month in MONTHS:
            pre = clean_number(row.get(f"{month}_previsionnel"))
            fac = clean_number(row.get(f"{month}_facture"))
            mensuel[month]["previsionnel"] = pre
            mensuel[month]["facture"] = fac
            mensuel[month]["ecart"] = fac - pre

        total_previsionnel = sum(mensuel[m]["previsionnel"] for m in MONTHS)
        total_facture = clean_number(row.get("total_facture")) or sum(mensuel[m]["facture"] for m in MONTHS)
        commande_ht = clean_number(row.get("commande_ht"))
        anteriorite = anteriorite_from_row(row)
        fact_2026 = facture_2026_from_row(row)
        facturation_totale = anteriorite + fact_2026
        reste = clean_number(row.get("reste_a_facturer"))

        return {
            "affaire_id": affaire_id,
            "display_name": display_name,
            "client": client,
            "affaire": affaire_name,
            "numero": clean_text(row.get("numero")),
            "tag": clean_text(row.get("tag")),
            "tags": [clean_text(row.get("tag"))] if clean_text(row.get("tag")) else [],
            "parent_affaire_id": parent_id or affaire_id,
            "parent_display_name": parent_display or display_name,
            "delai_reglement_jours": safe_int(row.get("delai_reglement_jours")),
            "commande_ht": commande_ht,
            "facturation_cumulee_2017": clean_number(row.get("facturation_cumulee_2017")),
            "facturation_cumulee_2018": clean_number(row.get("facturation_cumulee_2018")),
            "facturation_cumulee_2021": clean_number(row.get("facturation_cumulee_2021")),
            "facturation_cumulee_2022": clean_number(row.get("facturation_cumulee_2022")),
            "facturation_cumulee_2023": clean_number(row.get("facturation_cumulee_2023")),
            "facturation_cumulee_2024": clean_number(row.get("facturation_cumulee_2024")),
            "facturation_cumulee_2025": clean_number(row.get("facturation_cumulee_2025")),
            "facturation_cumulee_2026": fact_2026,
            "anteriorite": anteriorite,
            "facture_2026": fact_2026,
            "facturation_totale": facturation_totale,
            "reste_a_facturer": reste,
            "has_reste_value": row.get("reste_a_facturer") not in (None, ""),
            "mensuel": mensuel,
            "total_previsionnel": total_previsionnel,
            "total_facture": total_facture,
            "ecart_previsionnel_vs_facture": total_facture - total_previsionnel,
            "taux_avancement_financier": (facturation_totale / commande_ht) if commande_ht else 0.0,
        }

    def _mission_payload_from_affaire(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "tag": data.get("tag", ""),
            "label": data.get("affaire", ""),
            "numero": data.get("numero", ""),
            "commande_ht": data.get("commande_ht", 0.0),
            "facturation_cumulee_2017": data.get("facturation_cumulee_2017", 0.0),
            "facturation_cumulee_2018": data.get("facturation_cumulee_2018", 0.0),
            "facturation_cumulee_2021": data.get("facturation_cumulee_2021", 0.0),
            "facturation_cumulee_2022": data.get("facturation_cumulee_2022", 0.0),
            "facturation_cumulee_2023": data.get("facturation_cumulee_2023", 0.0),
            "facturation_cumulee_2024": data.get("facturation_cumulee_2024", 0.0),
            "facturation_cumulee_2025": data.get("facturation_cumulee_2025", 0.0),
            "facturation_cumulee_2026": data.get("facturation_cumulee_2026", 0.0),
            "anteriorite": data.get("anteriorite", 0.0),
            "facture_2026": data.get("facture_2026", data.get("facturation_cumulee_2026", 0.0)),
            "facturation_totale": (
                clean_number(data.get("anteriorite", 0.0))
                + clean_number(data.get("facture_2026", data.get("facturation_cumulee_2026", 0.0)))
            ),
            "reste_a_facturer": data.get("reste_a_facturer", 0.0),
            "has_reste_value": data.get("has_reste_value", False),
            "total_previsionnel": data.get("total_previsionnel", 0.0),
            "total_facture": data.get("total_facture", 0.0),
            "mensuel": data.get("mensuel", month_payload()),
        }

    def _finalize_affaire(self, affaire: Dict[str, Any]) -> Dict[str, Any]:
        excel_anteriorite = anteriorite_from_row(affaire)
        excel_facture_2026 = clean_number(affaire.get("facturation_cumulee_2026"))
        excel_reste_a_facturer = clean_number(affaire.get("reste_a_facturer"))

        missions = affaire.get("missions") or []
        if missions:
            monthly = month_payload()
            tags = []
            numero = affaire.get("numero", "")
            commande = 0.0
            facture_2026 = 0.0
            anteriorite = 0.0
            delai_values = []

            for mission in missions:
                commande += clean_number(mission.get("commande_ht"))
                anteriorite += clean_number(mission.get("anteriorite"))
                facture_2026 += clean_number(mission.get("facture_2026", mission.get("facturation_cumulee_2026")))
                if clean_number(mission.get("delai_reglement_jours")) > 0:
                    delai_values.append(safe_int(mission.get("delai_reglement_jours")))
                if mission.get("tag"):
                    tags.append(clean_text(mission["tag"]))
                if not numero and mission.get("numero"):
                    numero = clean_text(mission["numero"])

                mm = mission.get("mensuel") or {}
                for month in MONTHS:
                    monthly[month]["previsionnel"] += clean_number((mm.get(month) or {}).get("previsionnel"))
                    monthly[month]["facture"] += clean_number((mm.get(month) or {}).get("facture"))

            for month in MONTHS:
                monthly[month]["ecart"] = monthly[month]["facture"] - monthly[month]["previsionnel"]

            affaire["commande_ht"] = commande
            affaire["anteriorite"] = anteriorite
            affaire["facture_2026"] = facture_2026
            affaire["facturation_totale"] = anteriorite + facture_2026
            affaire["reste_a_facturer"] = sum(clean_number(m.get("reste_a_facturer")) for m in missions)
            affaire["facturation_cumulee_2026"] = affaire["facture_2026"]
            affaire["mensuel"] = monthly
            affaire["tags"] = sorted(set(t for t in tags if t))
            affaire["numero"] = numero
            affaire["total_previsionnel"] = sum(monthly[m]["previsionnel"] for m in MONTHS)
            affaire["total_facture"] = sum(monthly[m]["facture"] for m in MONTHS)
            affaire["ecart_previsionnel_vs_facture"] = affaire["total_facture"] - affaire["total_previsionnel"]
            affaire["delai_reglement_jours"] = affaire.get("delai_reglement_jours") or (max(delai_values) if delai_values else 0)
            affaire["taux_avancement_financier"] = (affaire["facturation_totale"] / affaire["commande_ht"]) if affaire["commande_ht"] else 0.0
        else:
            affaire["missions"] = []
            if affaire.get("tag") and not affaire.get("tags"):
                affaire["tags"] = [affaire["tag"]]
            affaire["anteriorite"] = excel_anteriorite
            affaire["facture_2026"] = excel_facture_2026
            affaire["facturation_totale"] = affaire["anteriorite"] + affaire["facture_2026"]
            if abs(clean_number(affaire.get("reste_a_facturer"))) < 1e-9 and abs(clean_number(affaire.get("commande_ht"))) > 1e-9:
                affaire["reste_a_facturer"] = clean_number(affaire.get("commande_ht")) - affaire["facturation_totale"]
            affaire["taux_avancement_financier"] = (affaire["facturation_totale"] / clean_number(affaire.get("commande_ht"))) if clean_number(affaire.get("commande_ht")) else 0.0

        affaire["audit"] = {
            "excel_anteriorite": excel_anteriorite,
            "excel_facture_2026": excel_facture_2026,
            "excel_reste_a_facturer": excel_reste_a_facturer,
            "missions_anteriorite": clean_number(affaire.get("anteriorite", 0)),
            "missions_facture_2026": clean_number(affaire.get("facture_2026", 0)),
            "missions_reste_a_facturer": clean_number(affaire.get("reste_a_facturer", 0)),
            "ecart_anteriorite": clean_number(affaire.get("anteriorite", 0)) - excel_anteriorite,
            "ecart_facture_2026": clean_number(affaire.get("facture_2026", 0)) - excel_facture_2026,
            "ecart_reste_a_facturer": clean_number(affaire.get("reste_a_facturer", 0)) - excel_reste_a_facturer,
        }
        affaire["insights"] = self.compute_insights(affaire)
        affaire.pop("has_reste_value", None)
        affaire.pop("tag", None)
        affaire.pop("_row_index", None)
        return affaire

    @staticmethod
    def compute_insights(affaire: Dict[str, Any]) -> List[str]:
        insights: List[str] = []
        commande = clean_number(affaire.get("commande_ht"))
        facture = clean_number(affaire.get("facturation_totale"))
        reste = clean_number(affaire.get("reste_a_facturer"))
        prev = clean_number(affaire.get("total_previsionnel"))
        fact_total = clean_number(affaire.get("total_facture"))
        taux = float(affaire.get("taux_avancement_financier") or 0.0)

        if commande > 0 and facture <= 0:
            insights.append("Aucune facturation enregistrée alors qu'une commande existe.")

        if reste > max(1000.0, commande * 0.4):
            insights.append("Le reste à facturer est élevé.")

        if prev > 0 and fact_total < prev * 0.75:
            insights.append("La facturation est en retard par rapport au prévisionnel.")
        elif prev > 0 and fact_total > prev * 1.1:
            insights.append("La facturation est en avance par rapport au prévisionnel.")

        if commande > 0 and taux >= 0.9:
            insights.append("L'affaire est presque finalisée financièrement.")

        if not insights:
            insights.append("Situation financière stable selon les données disponibles.")
        return insights

    @staticmethod
    def lightweight_affaires(cache: Dict[str, Any], search: str = "") -> List[Dict[str, str]]:
        items = cache.get("items", {}) or {}
        q = slugify(search) if search else ""
        out = []
        for affaire_id, item in items.items():
            display = item.get("display_name") or affaire_id
            if q and q not in slugify(display):
                continue
            out.append({"affaire_id": affaire_id, "display_name": display})
        out.sort(key=lambda x: x["display_name"])
        return out



class MetronomeService:
    _PROJECT_MATCH_IGNORED_TOKENS = {
        "de", "du", "des", "la", "le", "les", "a", "au", "aux", "et",
        "reunion", "syt", "projet", "affaire",
    }

    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)
        self._lock = Lock()
        self._cache: Dict[str, Any] = {"loaded": False, "tables": {}, "mtime": {}}

    def _read_csv_rows(self, path: Path) -> List[Dict[str, str]]:
        raw = path.read_bytes()
        for enc in ("utf-8-sig", "cp1252", "latin-1"):
            try:
                text = raw.decode(enc)
                break
            except Exception:
                continue
        else:
            text = raw.decode("utf-8", errors="ignore")
        return list(csv.DictReader(io.StringIO(text), delimiter=","))

    def _table_path(self, key: str) -> Path:
        return self.base_path / METRONOME_FILES[key]

    def _current_mtime(self) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for key in METRONOME_FILES:
            p = self._table_path(key)
            try:
                out[key] = p.stat().st_mtime
            except OSError:
                out[key] = None
        return out

    def _ensure_loaded(self) -> Dict[str, Any]:
        with self._lock:
            mt = self._current_mtime()
            if self._cache.get("loaded") and self._cache.get("mtime") == mt:
                return dict(self._cache)

            tables: Dict[str, List[Dict[str, str]]] = {}
            missing = []
            for key in METRONOME_FILES:
                p = self._table_path(key)
                if not p.exists():
                    missing.append(str(p))
                    tables[key] = []
                    continue
                tables[key] = self._read_csv_rows(p)

            self._cache = {
                "loaded": True,
                "tables": tables,
                "mtime": mt,
                "missing": missing,
                "loaded_at": now_iso(),
            }
            return dict(self._cache)

    @staticmethod
    def _row_id(row: Dict[str, str]) -> str:
        return clean_text(row.get("🔒 Row ID") or row.get("Row ID") or row.get("ID"))

    @staticmethod
    def _idx(rows: List[Dict[str, str]], key: str = "ID") -> Dict[str, Dict[str, str]]:
        out: Dict[str, Dict[str, str]] = {}
        for r in rows:
            k = clean_text(r.get(key)) or MetronomeService._row_id(r)
            if k:
                out[k] = r
        return out

    def _tokenize_project_name(self, value: str) -> set[str]:
        return {
            token for token in slugify(value).split("-")
            if token and token not in self._PROJECT_MATCH_IGNORED_TOKENS and len(token) > 1
        }

    @staticmethod
    def _get_first_value(row: Dict[str, str], keys: List[str]) -> str:
        for key in keys:
            val = clean_text(row.get(key))
            if val:
                return val
        return ""
    @staticmethod
    def _is_empty_value(value: Any) -> bool:
        txt = clean_text(value).lower()
        return txt in {"", "none", "null", "nan", "na"}

    def _split_multi_values(self, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            parts = value
        else:
            parts = str(value).split(",")
        out: List[str] = []
        for p in parts:
            t = clean_text(p)
            if self._is_empty_value(t):
                continue
            out.append(t)
        return out

    @staticmethod
    def _parse_bool(value: Any) -> bool:
        txt = clean_text(value).lower()
        return txt in {"true", "1", "yes", "oui", "vrai"}

    @staticmethod
    def _parse_date_value(value: str) -> Optional[datetime]:
        txt = clean_text(value)
        if not txt:
            return None
        for fmt in (
            "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%y %H:%M:%S"
        ):
            try:
                return datetime.strptime(txt[:19], fmt)
            except Exception:
                continue
        return None

    @classmethod
    def _parse_date_only(cls, value: str) -> Optional[date]:
        dt = cls._parse_date_value(value)
        return dt.date() if dt else None

    @staticmethod
    def _normalize_company(value: Any) -> str:
        return clean_text(value) or "Non renseigné"

    @staticmethod
    def _normalize_zone(value: Any) -> str:
        return clean_text(value) or "Général"

    @staticmethod
    def _normalize_lot(value: Any) -> str:
        return clean_text(value) or "Sans lot"

    @staticmethod
    def _normalize_owner(value: Any) -> str:
        return clean_text(value) or "Non attribué"

    @staticmethod
    def _company_logo_from_row(row: Dict[str, Any]) -> str:
        for key in ["Logo", "Logo URL", "Avatar", "Avatar URL", "Icon", "Icon URL"]:
            val = clean_text(row.get(key))
            if val:
                return val
        return ""

    @staticmethod
    def reminder_level(deadline: Optional[date], completed: bool, ref_date: date) -> Optional[int]:
        if completed or not deadline:
            return None
        days_late = (ref_date - deadline).days
        if days_late <= 0:
            return None
        return ((days_late - 1) // 7) + 1

    def reminders_by_company(self, rem_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rem_rows:
            return []
        grouped: Dict[str, int] = {}
        logos: Dict[str, str] = {}
        for row in rem_rows:
            company = self._normalize_company(row.get("__company__"))
            grouped[company] = grouped.get(company, 0) + 1
            logos[company] = clean_text(row.get("__logo__"))
        return [
            {"name": name, "count": count, "logo": logos.get(name, "")}
            for name, count in sorted(grouped.items(), key=lambda x: (-x[1], x[0]))
        ]

    def reminders_for_project(
        self,
        project_title: str,
        ref_date: date,
        max_level: int = 8,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        entries_override: Optional[List[Dict[str, Any]]] = None,
        company_logos: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        source = entries_override or []
        rows: List[Dict[str, Any]] = []
        for e in source:
            if not e.get("is_task"):
                continue
            created_date = self._parse_date_only(e.get("meeting_date", ""))
            if start_date and created_date and created_date < start_date:
                continue
            if end_date and created_date and created_date > end_date:
                continue
            completed = bool(e.get("is_closed"))
            done = e.get("done_date")
            if done is not None:
                completed = True
            deadline = self._parse_date_only(e.get("deadline", ""))
            lvl = self.reminder_level(deadline, completed, ref_date)
            if lvl is None:
                continue
            if int(lvl) > max_level:
                continue
            company = self._normalize_company(e.get("company"))
            zones = e.get("area_names") or ["Général"]
            if not zones:
                zones = ["Général"]
            for zone in zones:
                rows.append({
                    **e,
                    "__project__": project_title,
                    "__deadline__": deadline,
                    "__reminder__": int(lvl),
                    "__zone__": self._normalize_zone(zone),
                    "__company__": company,
                    "__logo__": clean_text((company_logos or {}).get(company, "")),
                })
        rows.sort(key=lambda r: (-r["__reminder__"], r.get("__deadline__") or date.max))
        return rows

    def followups_for_project(
        self,
        project_title: str,
        ref_date: date,
        exclude_entry_ids: Optional[List[str]],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        entries_override: Optional[List[Dict[str, Any]]] = None,
        company_logos: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        source = entries_override or []
        excluded = {clean_text(v) for v in (exclude_entry_ids or []) if clean_text(v)}
        rows: List[Dict[str, Any]] = []
        for e in source:
            if not e.get("is_task"):
                continue
            eid = clean_text(e.get("entry_id"))
            if eid and eid in excluded:
                continue
            created_date = self._parse_date_only(e.get("meeting_date", ""))
            if start_date and created_date and created_date < start_date:
                continue
            if end_date and created_date and created_date > end_date:
                continue
            completed = bool(e.get("is_closed"))
            done = e.get("done_date")
            if done is not None:
                completed = True
            if completed:
                continue
            deadline = self._parse_date_only(e.get("deadline", ""))
            if deadline and deadline < ref_date:
                continue
            company = self._normalize_company(e.get("company"))
            zones = e.get("area_names") or ["Général"]
            if not zones:
                zones = ["Général"]
            for zone in zones:
                rows.append({
                    **e,
                    "__id__": eid,
                    "__project__": project_title,
                    "__deadline__": deadline,
                    "__zone__": self._normalize_zone(zone),
                    "__company__": company,
                    "__deadline_sort__": deadline or date.max,
                })
        rows.sort(key=lambda r: (r.get("__deadline_sort__") or date.max, r.get("__company__") or ""))
        return rows

    def meeting_simple_kpis(self, entries_rows: List[Dict[str, Any]], ref_date: date) -> Dict[str, int]:
        tasks = [e for e in entries_rows if e.get("is_task")]
        memos = [e for e in entries_rows if e.get("is_memo")]
        open_tasks = [e for e in tasks if not bool(e.get("is_closed"))]
        closed_tasks = [e for e in tasks if bool(e.get("is_closed"))]
        late_tasks = 0
        for e in open_tasks:
            dl = self._parse_date_only(e.get("deadline", ""))
            if dl and dl < ref_date:
                late_tasks += 1
        return {
            "total_entries": len(entries_rows),
            "tasks_meeting": len(tasks),
            "memos_meeting": len(memos),
            "open_tasks": len(open_tasks),
            "closed_tasks": len(closed_tasks),
            "late_tasks": late_tasks,
        }


    def _score_project_match(self, target_slug: str, target_tokens: set[str], candidate_name: str) -> int:
        candidate_slug = slugify(candidate_name)
        if not candidate_slug:
            return 0
        score = 0
        if candidate_slug == target_slug:
            score = 100
        else:
            if target_slug and target_slug in candidate_slug:
                score = max(score, 80)
            if candidate_slug and candidate_slug in target_slug:
                score = max(score, 78)
            candidate_tokens = self._tokenize_project_name(candidate_name)
            common_tokens = target_tokens & candidate_tokens
            if common_tokens:
                token_score = 45 + len(common_tokens) * 12
                if target_tokens and candidate_tokens and common_tokens == target_tokens == candidate_tokens:
                    token_score += 10
                score = max(score, min(token_score, 95))
        return score

    def _resolve_project(self, projects: List[Dict[str, str]], entries: List[Dict[str, str]], project_name: str) -> Dict[str, Any]:
        target_name = clean_text(project_name)
        target_slug = slugify(target_name)
        target_tokens = self._tokenize_project_name(target_name)
        resolved: Dict[str, Any] = {
            "searched_project_name": target_name,
            "searched_project_slug": target_slug,
            "resolved_project_title": "",
            "resolved_project_id": "",
            "matched_project_name": "",
            "matched_project_slug": "",
            "match_score": 0,
            "resolution_mode": "fallback",
            "best_project_candidates": [],
            "best_entry_candidates": [],
        }
        if not target_slug:
            return resolved

        scored_projects: List[Dict[str, Any]] = []
        best_project: Optional[Dict[str, str]] = None
        best_name = ""
        best_score = 0
        best_len = 10**9

        for project in projects:
            title = self._get_first_value(project, METRONOME_COLUMN_ALIASES["project_title_projects"])
            name = self._get_first_value(project, METRONOME_COLUMN_ALIASES["project_name_projects"])
            project_row_id = self._row_id(project)
            for candidate_name in [title, name]:
                if not candidate_name:
                    continue
                score = self._score_project_match(target_slug, target_tokens, candidate_name)
                if score <= 0:
                    continue
                scored_projects.append({
                    "project_id": project_row_id,
                    "project_name": candidate_name,
                    "project_slug": slugify(candidate_name),
                    "score": score,
                })
                slug_len_delta = abs(len(slugify(candidate_name)) - len(target_slug))
                if score > best_score or (score == best_score and slug_len_delta < best_len):
                    best_project = project
                    best_name = candidate_name
                    best_score = score
                    best_len = slug_len_delta

        scored_projects.sort(key=lambda x: (-x["score"], abs(len(x["project_slug"]) - len(target_slug))))
        resolved["best_project_candidates"] = scored_projects[:5]

        min_reliable_score = 55
        if best_project and best_score >= min_reliable_score:
            resolved_title = self._get_first_value(best_project, METRONOME_COLUMN_ALIASES["project_title_projects"])
            resolved_id = self._row_id(best_project)
            mode = "title" if clean_text(best_project.get("Title")) else "name"
            if resolved_id:
                mode = "id"
            resolved.update({
                "resolved_project_title": resolved_title,
                "resolved_project_id": resolved_id,
                "matched_project_name": best_name,
                "matched_project_slug": slugify(best_name),
                "match_score": best_score,
                "resolution_mode": mode,
            })
            return resolved

        scored_entry_titles: Dict[str, Dict[str, Any]] = {}
        for e in entries:
            entry_title = self._get_first_value(e, METRONOME_COLUMN_ALIASES["project_title_entries"])
            if not entry_title:
                continue
            score = self._score_project_match(target_slug, target_tokens, entry_title)
            if score <= 0:
                continue
            eid = self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_project_id"])
            existing = scored_entry_titles.get(entry_title)
            if not existing or score > existing["score"]:
                scored_entry_titles[entry_title] = {
                    "project_id": eid,
                    "project_name": entry_title,
                    "project_slug": slugify(entry_title),
                    "score": score,
                }

        scored_entries = sorted(scored_entry_titles.values(), key=lambda x: (-x["score"], abs(len(x["project_slug"]) - len(target_slug))))
        resolved["best_entry_candidates"] = scored_entries[:5]

        if scored_entries and scored_entries[0]["score"] >= min_reliable_score:
            best = scored_entries[0]
            resolved.update({
                "resolved_project_title": best["project_name"],
                "resolved_project_id": clean_text(best.get("project_id")),
                "matched_project_name": best["project_name"],
                "matched_project_slug": best["project_slug"],
                "match_score": best["score"],
                "resolution_mode": "entries_fallback",
            })
        return resolved

    def build_project_board(self, project_name: str, start_date: str = "", end_date: str = "") -> Dict[str, Any]:
        cache = self._ensure_loaded()
        t = cache.get("tables", {})
        projects = t.get("projects", [])
        entries = t.get("entries", [])
        meetings_rows = t.get("meetings", [])
        areas_rows = t.get("areas", [])
        packages_rows = t.get("packages", [])
        companies_rows = t.get("companies", [])
        users_rows = t.get("users", [])
        comments_rows = t.get("comments", [])
        documents_rows = t.get("documents", [])

        meetings = self._idx(meetings_rows)
        areas = self._idx(areas_rows)
        packages = self._idx(packages_rows)
        companies = self._idx(companies_rows)
        users = self._idx(users_rows)
        company_logos_by_name: Dict[str, str] = {}
        for comp in companies_rows:
            cname = self._normalize_company(comp.get("Name"))
            logo = self._company_logo_from_row(comp)
            if cname not in company_logos_by_name or logo:
                company_logos_by_name[cname] = logo

        comments_by_entry: Dict[str, List[str]] = {}
        memo_comments_by_entry: Dict[str, List[Dict[str, Any]]] = {}
        for c in comments_rows:
            eid = self._get_first_value(c, METRONOME_COLUMN_ALIASES["comment_memo_id"])
            txt = clean_text(c.get("Comment") or c.get("Text") or c.get("Entry"))
            owner_id = self._get_first_value(c, METRONOME_COLUMN_ALIASES["comment_owner_id"])
            created_at = self._parse_date_value(self._get_first_value(c, METRONOME_COLUMN_ALIASES["comment_date"]))
            if eid and txt:
                comments_by_entry.setdefault(eid, []).append(txt)
                memo_comments_by_entry.setdefault(eid, []).append({
                    "comment_id": self._row_id(c),
                    "memo_id": eid,
                    "text": txt,
                    "owner_user_id": owner_id,
                    "owner_user_name": clean_text(users.get(owner_id, {}).get("Name")),
                    "created_at": created_at.isoformat(sep=" ") if created_at else "",
                    "raw": c,
                })

        target = clean_text(project_name)
        match_debug = self._resolve_project(projects, entries, target)
        resolved_title = clean_text(match_debug.get("resolved_project_title"))
        resolved_id = clean_text(match_debug.get("resolved_project_id"))

        project_info: Dict[str, str] = {}
        if resolved_title:
            for p in projects:
                pt = self._get_first_value(p, METRONOME_COLUMN_ALIASES["project_title_projects"])
                if clean_text(pt) == resolved_title:
                    project_info = p
                    if not resolved_id:
                        resolved_id = self._row_id(p)
                        match_debug["resolved_project_id"] = resolved_id
                    break
        if not project_info and resolved_id:
            for p in projects:
                if self._row_id(p) == resolved_id:
                    project_info = p
                    if not resolved_title:
                        resolved_title = self._get_first_value(p, METRONOME_COLUMN_ALIASES["project_title_projects"])
                        match_debug["resolved_project_title"] = resolved_title
                    break

        if not resolved_title and not resolved_id:
            return {
                "ok": False,
                "project_name": target,
                "reason": "project_not_found",
                "confidence_level": "low",
                "warning": True,
                "warning_message": "Aucune correspondance projet fiable trouvée",
                "match_debug": match_debug,
                "missing_files": cache.get("missing", []),
                "loaded_at": cache.get("loaded_at"),
            }

        rows = []
        today = datetime.now().date()
        rows_filtered_by_title = 0
        rows_filtered_by_id = 0
        filter_mode = "id" if resolved_id else "title"
        match_debug["filter_mode"] = filter_mode

        def parse_date(value: str) -> Optional[date]:
            txt = clean_text(value)
            if not txt:
                return None
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(txt[:10], fmt).date()
                except Exception:
                    continue
            return None

        selected_entries: List[Dict[str, Any]] = []

        for e in entries:
            entry_project_title = self._get_first_value(e, METRONOME_COLUMN_ALIASES["project_title_entries"])
            entry_project_id = self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_project_id"])

            use_row = False
            if resolved_id:
                if entry_project_id == resolved_id:
                    rows_filtered_by_id += 1
                    use_row = True
            elif resolved_title and entry_project_title == resolved_title:
                rows_filtered_by_title += 1
                use_row = True

            if not use_row:
                continue

            completed_flag = self._parse_bool(self._get_first_value(e, ["Completed/true/false", "Completed"]))
            done_date = self._parse_date_only(self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_done_date"]))
            is_closed = bool(completed_flag)
            if done_date is not None:
                is_closed = True

            entry_id = self._row_id(e)
            area_ids = self._split_multi_values(self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_area_ids"]))
            area = areas.get(area_ids[0], {}) if area_ids else {}
            task_pkg_id = self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_task_package_id"])
            pkg = packages.get(task_pkg_id, {})
            company_id_for_task = self._get_first_value(e, ["Company/ID"])
            company_name_for_task = self._get_first_value(e, ["Company/Name for Tasks", "Company"])
            pkg_company_id = self._get_first_value(pkg, METRONOME_COLUMN_ALIASES["package_company_id"])
            company = companies.get(company_id_for_task) or companies.get(pkg_company_id) or {}
            owner_user_id = self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_owner_id"])
            user = users.get(owner_user_id, {})
            meeting_id = self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_meeting_id"])
            meeting = meetings.get(meeting_id, {})
            due = self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_deadline"])
            meeting_date = self._get_first_value(meeting, METRONOME_COLUMN_ALIASES["meeting_date"])
            request_date = parse_date(due) or parse_date(meeting_date)

            category_label = self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_category"]) or ""
            category_slug = slugify(category_label)
            is_task_entry = "tache" in category_slug
            is_memo_entry = "memo" in category_slug
            memo_package_ids = self._split_multi_values(self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_memo_package_ids"]))
            memo_package_labels = [clean_text(packages.get(pid, {}).get("Name")) for pid in memo_package_ids if pid]
            created_by_id = self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_created_by_id"])
            last_comment_text = clean_text(self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_comment_text"]))
            memo_comments = memo_comments_by_entry.get(entry_id, []) if is_memo_entry else []
            selected_entries.append({
                "entry_id": entry_id,
                "entry_title": clean_text(e.get("Title")),
                "entry_type": "Tâche" if is_task_entry else ("Mémo" if is_memo_entry else category_label),
                "is_task": is_task_entry,
                "is_memo": is_memo_entry,
                "project_id": resolved_id,
                "project_title": resolved_title,
                "project_archived": self._parse_bool(self._get_first_value(project_info, ["Archived", "Is Archived"])),
                "meeting_id": meeting_id,
                "meeting_date": meeting_date,
                "created_by_user_id": created_by_id,
                "created_by_user_name": clean_text(users.get(created_by_id, {}).get("Name")),
                "package_id": task_pkg_id,
                "package_name": clean_text(pkg.get("Name")),
                "package_label": clean_text(pkg.get("Label") or pkg.get("Name")),
                "package_company_id": pkg_company_id,
                "package_company_name": clean_text(companies.get(pkg_company_id, {}).get("Name")),
                "memo_package_ids": memo_package_ids,
                "memo_package_labels": [x for x in memo_package_labels if x],
                "area_ids": area_ids,
                "area_names": [clean_text(areas.get(aid, {}).get("Name")) for aid in area_ids if aid],
                "owner_user_id": owner_user_id,
                "owner_full_name": clean_text(user.get("Name")),
                "deadline": due,
                "status_id": self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_status_id"]),
                "status_label": self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_status_label"]),
                "completed": is_closed,
                "completed_state": "closed" if is_closed else "open",
                "completed_at": done_date.isoformat() if done_date else "",
                "first_response_at": memo_comments[0]["created_at"] if memo_comments else "",
                "last_activity_at": (memo_comments[-1]["created_at"] if memo_comments else (done_date.isoformat() if done_date else "")),
                "last_comment_text": last_comment_text or (memo_comments[-1]["text"] if memo_comments else ""),
                "last_comment_date": memo_comments[-1]["created_at"] if memo_comments else "",
                "last_comment_user_id": memo_comments[-1]["owner_user_id"] if memo_comments else self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_comment_editor_id"]),
                "last_comment_user_name": memo_comments[-1]["owner_user_name"] if memo_comments else clean_text(users.get(self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_comment_editor_id"]), {}).get("Name")),
                "memo_comments_count": len(memo_comments),
                "memo_comments": memo_comments,
                "request_date": request_date,
                "done_date": done_date,
                "is_closed": is_closed,
                "company": clean_text(company_name_for_task) or clean_text(company.get("Name")) or clean_text(companies.get(pkg_company_id, {}).get("Name")) or "Non renseigné",
                "raw": e,
            })

            if is_closed:
                continue

            overdue = False
            if due:
                due_date = parse_date(due)
                overdue = bool(due_date and due_date < today)

            rows.append({
                "zone": self._normalize_zone(area.get("Name")),
                "lot": self._normalize_lot(pkg.get("Name")),
                "sujet": clean_text(e.get("Title")),
                "entreprise": self._normalize_company(clean_text(company_name_for_task) or clean_text(company.get("Name"))),
                "responsable": self._normalize_owner(user.get("Name")),
                "statut": clean_text(e.get("Status")),
                "date_echeance": due,
                "reunion_origine": meeting_date,
                "commentaire": " | ".join(comments_by_entry.get(entry_id, [])),
                "overdue": overdue,
            })

        match_debug["rows_filtered_by_title"] = rows_filtered_by_title
        match_debug["rows_filtered_by_id"] = rows_filtered_by_id

        def count_by(field: str) -> List[Dict[str, Any]]:
            agg: Dict[str, int] = {}
            for r in rows:
                k = clean_text(r.get(field)) or "Non renseigné"
                agg[k] = agg.get(k, 0) + 1
            return [{"label": k, "count": v} for k, v in sorted(agg.items(), key=lambda x: (-x[1], x[0]))]

        by_meeting = count_by("reunion_origine")
        by_company = count_by("entreprise")

        range_start = parse_date(start_date) if start_date else None
        range_end = parse_date(end_date) if end_date else None
        meeting_dates = [parse_date(e.get("meeting_date", "")) for e in selected_entries if parse_date(e.get("meeting_date", ""))]
        meeting_ref_date = max(meeting_dates) if meeting_dates else None
        reference_date = range_end or meeting_ref_date or today
        reference_date_text = reference_date.strftime("%d/%m/%Y")

        current_meeting_entry_ids: List[str] = []
        if meeting_ref_date:
            current_meeting_entry_ids = [
                clean_text(e.get("entry_id"))
                for e in selected_entries
                if parse_date(e.get("meeting_date", "")) == meeting_ref_date and clean_text(e.get("entry_id"))
            ]

        rem_rows = self.reminders_for_project(
            resolved_title or target,
            reference_date,
            max_level=8,
            start_date=range_start,
            end_date=range_end,
            entries_override=selected_entries,
            company_logos=company_logos_by_name,
        )
        fol_rows = self.followups_for_project(
            resolved_title or target,
            reference_date,
            exclude_entry_ids=current_meeting_entry_ids,
            start_date=range_start,
            end_date=range_end,
            entries_override=selected_entries,
        )
        due_by_company_list = self.reminders_by_company(rem_rows)
        meeting_simple_kpis = self.meeting_simple_kpis(selected_entries, reference_date)

        reminder_threshold_days = 14
        open_stats: Dict[str, Dict[str, int]] = {}
        delay_stats: Dict[str, Dict[str, int]] = {}
        for item in selected_entries:
            company = item["company"]
            request_date = item.get("request_date")
            done_date = item.get("done_date")
            if not item.get("is_closed"):
                row = open_stats.setdefault(company, {"open_count": 0, "reminder_count": 0})
                row["open_count"] += 1
                if request_date and (today - request_date).days >= reminder_threshold_days:
                    row["reminder_count"] += 1
            elif request_date and done_date:
                delay_days = max(0, (done_date - request_date).days)
                row = delay_stats.setdefault(company, {"sum_days": 0, "count": 0})
                row["sum_days"] += delay_days
                row["count"] += 1

        open_tasks_by_company = [
            {"label": k, "open_count": v["open_count"], "reminder_count": v["reminder_count"]}
            for k, v in sorted(open_stats.items(), key=lambda x: (-x[1]["open_count"], -x[1]["reminder_count"], x[0]))
        ]
        average_processing_days_by_company = [
            {"label": k, "avg_days": round(v["sum_days"] / max(1, v["count"]), 1), "closed_count": v["count"]}
            for k, v in sorted(delay_stats.items(), key=lambda x: (x[1]["sum_days"] / max(1, x[1]["count"]), x[0]))
        ]

        fact_tasks: List[Dict[str, Any]] = []
        fact_memos: List[Dict[str, Any]] = []
        fact_memo_comments: List[Dict[str, Any]] = []
        fact_interactions: List[Dict[str, Any]] = []

        for item in selected_entries:
            deadline_dt = parse_date(item.get("deadline", ""))
            request_dt = item.get("request_date")
            done_dt = item.get("done_date")
            first_response_dt = self._parse_date_value(item.get("first_response_at", "")) if item.get("first_response_at") else None
            last_activity_dt = self._parse_date_value(item.get("last_activity_at", "")) if item.get("last_activity_at") else None

            if item.get("is_task"):
                is_open = not item.get("is_closed")
                is_closed = bool(item.get("is_closed"))
                is_late = bool(is_open and deadline_dt and deadline_dt < today)
                is_closed_late = bool(is_closed and deadline_dt and done_dt and done_dt > deadline_dt)
                is_closed_on_time = bool(is_closed and deadline_dt and done_dt and done_dt <= deadline_dt)
                days_late = max(0, (today - deadline_dt).days) if is_late and deadline_dt else 0
                age_days = max(0, (today - request_dt).days) if is_open and request_dt else 0
                days_to_deadline = (deadline_dt - today).days if deadline_dt else None
                response_delay_days = (first_response_dt.date() - request_dt).days if (first_response_dt and request_dt) else None

                fact_tasks.append({
                    **item,
                    "is_open": is_open,
                    "is_closed": is_closed,
                    "is_late": is_late,
                    "is_closed_late": is_closed_late,
                    "is_closed_on_time": is_closed_on_time,
                    "days_late": days_late,
                    "age_days": age_days,
                    "days_to_deadline": days_to_deadline,
                    "response_delay_days": response_delay_days,
                    "completed_at": item.get("completed_at") or (done_dt.isoformat() if done_dt else ""),
                    "first_response_at": item.get("first_response_at") or (first_response_dt.isoformat(sep=" ") if first_response_dt else ""),
                    "last_activity_at": item.get("last_activity_at") or (last_activity_dt.isoformat(sep=" ") if last_activity_dt else ""),
                })
            elif item.get("is_memo"):
                fact_memos.append(item)
                for c in item.get("memo_comments", []):
                    fact_memo_comments.append(c)

            fact_interactions.append({
                "entry_id": item.get("entry_id"),
                "project_id": item.get("project_id"),
                "meeting_id": item.get("meeting_id"),
                "company": item.get("company"),
                "event_type": "entry_closed" if item.get("is_closed") else "entry_open",
                "event_at": item.get("completed_at") or item.get("meeting_date") or "",
            })

        meetings_count = 0
        fact_meetings: List[Dict[str, Any]] = []
        meeting_rows_for_project = []
        for m in meetings_rows:
            pid = self._get_first_value(m, ["Project/ID", "Project"])
            if resolved_id and pid and pid != resolved_id:
                continue
            meeting_rows_for_project.append(m)

        for m in meeting_rows_for_project:
            mid = self._row_id(m)
            mdate = self._get_first_value(m, METRONOME_COLUMN_ALIASES["meeting_date"])
            meetings_count += 1
            fact_meetings.append({
                "meeting_id": mid,
                "meeting_date": mdate,
                "project_id": resolved_id,
                "project_title": resolved_title,
                "attending_company_ids": self._split_multi_values(self._get_first_value(m, METRONOME_COLUMN_ALIASES["meeting_attending_company_ids"])),
                "missing_company_ids": self._split_multi_values(self._get_first_value(m, METRONOME_COLUMN_ALIASES["meeting_missing_company_ids"])),
                "raw": m,
            })

        documents_for_project = [
            d for d in documents_rows
            if (not resolved_id or self._get_first_value(d, METRONOME_COLUMN_ALIASES["documents_project_id"]) in {"", resolved_id})
        ]

        project_start = parse_date(self._get_first_value(project_info, METRONOME_COLUMN_ALIASES["project_start"]))
        project_end = parse_date(self._get_first_value(project_info, METRONOME_COLUMN_ALIASES["project_end"]))
        if project_start and project_end and project_end > project_start:
            total_days = (project_end - project_start).days
            elapsed_days = min(max((today - project_start).days, 0), total_days)
            progress_percent = round((elapsed_days / total_days) * 100, 1)
        else:
            total_days = 0
            elapsed_days = 0
            progress_percent = 0.0

        total_tasks = len(fact_tasks)
        closed_tasks = sum(1 for t in fact_tasks if t.get("is_closed"))
        open_tasks = sum(1 for t in fact_tasks if t.get("is_open"))
        late_open_tasks = sum(1 for t in fact_tasks if t.get("is_late"))
        closure_rate = round((closed_tasks / total_tasks) * 100, 1) if total_tasks else 0.0
        on_time_closed = sum(1 for t in fact_tasks if t.get("is_closed_on_time"))
        on_time_closure_rate = round((on_time_closed / closed_tasks) * 100, 1) if closed_tasks else 0.0
        avg_task_age_open = round(sum(t.get("age_days", 0) for t in fact_tasks if t.get("is_open")) / max(1, open_tasks), 1) if open_tasks else 0.0
        avg_days_late = round(sum(t.get("days_late", 0) for t in fact_tasks if t.get("is_late")) / max(1, late_open_tasks), 1) if late_open_tasks else 0.0
        tasks_per_meeting = round(total_tasks / max(1, meetings_count), 2)
        memos_per_meeting = round(len(fact_memos) / max(1, meetings_count), 2)
        operational_progress_pct = round((closed_tasks / total_tasks) * 100, 1) if total_tasks else 0.0
        calendar_progress_pct = progress_percent
        progress_gap_pct = round(operational_progress_pct - calendar_progress_pct, 1)
        recent_threshold = today - timedelta(days=30)
        closure_velocity_30d = sum(1 for t in fact_tasks if t.get("is_closed") and t.get("done_date") and t.get("done_date") >= recent_threshold)
        project_health_score = max(0.0, min(100.0, round(0.45*closure_rate + 0.3*on_time_closure_rate + 0.25*max(0,100-(late_open_tasks*100/max(1,total_tasks))), 1)))

        kpi_project_summary = {
            "total_tasks": total_tasks,
            "open_tasks": open_tasks,
            "closed_tasks": closed_tasks,
            "late_open_tasks": late_open_tasks,
            "closure_rate": closure_rate,
            "on_time_closure_rate": on_time_closure_rate,
            "avg_task_age_open": avg_task_age_open,
            "avg_days_late": avg_days_late,
            "meetings_count": meetings_count,
            "tasks_per_meeting": tasks_per_meeting,
            "memos_per_meeting": memos_per_meeting,
            "calendar_progress_pct": calendar_progress_pct,
            "operational_progress_pct": operational_progress_pct,
            "progress_gap_pct": progress_gap_pct,
            "closure_velocity_30d": closure_velocity_30d,
            "project_health_score": project_health_score,
        }

        company_task_map: Dict[str, List[Dict[str, Any]]] = {}
        for t in fact_tasks:
            company_task_map.setdefault(clean_text(t.get("company")) or "Non renseigné", []).append(t)

        attendance_by_company: Dict[str, Dict[str, int]] = {}
        for m in fact_meetings:
            for cid in m.get("attending_company_ids", []):
                cname = clean_text(companies.get(cid, {}).get("Name")) or cid
                rec = attendance_by_company.setdefault(cname, {"attended": 0, "missing": 0})
                rec["attended"] += 1
            for cid in m.get("missing_company_ids", []):
                cname = clean_text(companies.get(cid, {}).get("Name")) or cid
                rec = attendance_by_company.setdefault(cname, {"attended": 0, "missing": 0})
                rec["missing"] += 1

        kpi_company_summary: List[Dict[str, Any]] = []
        kpi_reactivity_company: List[Dict[str, Any]] = []
        for cname, tasks in company_task_map.items():
            total = len(tasks)
            open_n = sum(1 for x in tasks if x.get("is_open"))
            late_open = sum(1 for x in tasks if x.get("is_late"))
            closed = sum(1 for x in tasks if x.get("is_closed"))
            on_time = sum(1 for x in tasks if x.get("is_closed_on_time"))
            delays = [x.get("days_late", 0) for x in tasks if x.get("is_late")]
            response = [x.get("response_delay_days") for x in tasks if x.get("response_delay_days") is not None]
            under3 = sum(1 for v in response if v <= 3)
            no_response = sum(1 for x in tasks if x.get("response_delay_days") is None)
            attend = attendance_by_company.get(cname, {"attended": 0, "missing": 0})
            meetings_total_for_company = attend["attended"] + attend["missing"]
            reminder_count = sum(1 for x in tasks if x.get("is_open") and x.get("age_days", 0) >= reminder_threshold_days)

            rec = {
                "company": cname,
                "total_tasks": total,
                "open_tasks": open_n,
                "late_open_tasks": late_open,
                "closure_rate": round((closed / total) * 100, 1) if total else 0.0,
                "on_time_closure_rate": round((on_time / closed) * 100, 1) if closed else 0.0,
                "avg_days_late": round(sum(delays)/len(delays),1) if delays else 0.0,
                "avg_first_response_delay_days": round(sum(response)/len(response),1) if response else None,
                "no_response_rate": round((no_response/total)*100,1) if total else 0.0,
                "meetings_attended": attend["attended"],
                "meetings_missing": attend["missing"],
                "attendance_rate": round((attend["attended"]/meetings_total_for_company)*100,1) if meetings_total_for_company else None,
                "reminder_count": reminder_count,
                "reminder_per_task_ratio": round(reminder_count/total,2) if total else 0.0,
            }
            reactivity_score = 100.0
            if rec["avg_first_response_delay_days"] is not None:
                reactivity_score -= min(40.0, rec["avg_first_response_delay_days"] * 4)
            reactivity_score -= min(35.0, rec["no_response_rate"] * 0.35)
            reactivity_score += min(20.0, (under3 / max(1, len(response))) * 20 if response else 0)
            rec["reactivity_score"] = round(max(0.0, min(100.0, reactivity_score)), 1)
            kpi_company_summary.append(rec)
            kpi_reactivity_company.append({
                "company": cname,
                "avg_first_response_delay_days": rec["avg_first_response_delay_days"],
                "reaction_under_3d_rate": round((under3/len(response))*100,1) if response else 0.0,
                "tasks_without_response_rate": rec["no_response_rate"],
                "avg_processing_days": round(sum((x.get("done_date")-x.get("request_date")).days for x in tasks if x.get("done_date") and x.get("request_date")) / max(1, sum(1 for x in tasks if x.get("done_date") and x.get("request_date"))),1) if any(x.get("done_date") and x.get("request_date") for x in tasks) else None,
                "reactivity_score": rec["reactivity_score"],
            })

        kpi_company_summary.sort(key=lambda x: (-x["open_tasks"], x["company"]))
        kpi_reactivity_company.sort(key=lambda x: (x["reactivity_score"] if x["reactivity_score"] is not None else 999, x["company"]))

        package_map: Dict[str, List[Dict[str, Any]]] = {}
        for t in fact_tasks:
            package_label = clean_text(t.get("package_label") or t.get("package_name") or "Non renseigné")
            package_map.setdefault(package_label, []).append(t)
        kpi_package_summary: List[Dict[str, Any]] = []
        for plabel, tasks in package_map.items():
            total = len(tasks)
            open_n = sum(1 for x in tasks if x.get("is_open"))
            late_open = sum(1 for x in tasks if x.get("is_late"))
            closed = sum(1 for x in tasks if x.get("is_closed"))
            memos_count = sum(1 for m in fact_memos if plabel in [clean_text(v) for v in m.get("memo_package_labels", [])])
            impacted_areas_count = len({a for x in tasks for a in x.get("area_names", []) if clean_text(a)})
            criticality = round(min(100.0, late_open*12 + open_n*3 + impacted_areas_count*5),1)
            kpi_package_summary.append({
                "package": plabel,
                "total_tasks": total,
                "open_tasks": open_n,
                "late_open_tasks": late_open,
                "closure_rate": round((closed/total)*100,1) if total else 0.0,
                "avg_days_late": round(sum(x.get("days_late",0) for x in tasks if x.get("is_late"))/max(1,late_open),1) if late_open else 0.0,
                "tasks_due_7d": sum(1 for x in tasks if x.get("days_to_deadline") is not None and 0 <= x.get("days_to_deadline") <= 7),
                "memos_count": memos_count,
                "impacted_areas_count": impacted_areas_count,
                "package_criticality_score": criticality,
            })
        kpi_package_summary.sort(key=lambda x: (-x["late_open_tasks"], -x["open_tasks"], x["package"]))

        zone_map: Dict[str, List[Dict[str, Any]]] = {}
        for t in fact_tasks:
            names = t.get("area_names") or ["Non renseigné"]
            for z in names:
                zone = clean_text(z) or "Non renseigné"
                zone_map.setdefault(zone, []).append(t)
        kpi_zone_summary: List[Dict[str, Any]] = []
        for zname, tasks in zone_map.items():
            total = len(tasks)
            open_n = sum(1 for x in tasks if x.get("is_open"))
            late_open = sum(1 for x in tasks if x.get("is_late"))
            closed = sum(1 for x in tasks if x.get("is_closed"))
            critical_packages_count = len({clean_text(x.get("package_label")) for x in tasks if x.get("is_late")})
            kpi_zone_summary.append({
                "zone": zname,
                "total_tasks": total,
                "open_tasks": open_n,
                "late_open_tasks": late_open,
                "closure_rate": round((closed/total)*100,1) if total else 0.0,
                "critical_packages_count": critical_packages_count,
            })
        kpi_zone_summary.sort(key=lambda x: (-x["late_open_tasks"], -x["open_tasks"], x["zone"]))

        kpi_project_progress = {
            "calendar_progress_pct": calendar_progress_pct,
            "operational_progress_pct": operational_progress_pct,
            "progress_gap_pct": progress_gap_pct,
            "start_date": project_start.isoformat() if project_start else "",
            "end_date": project_end.isoformat() if project_end else "",
            "elapsed_days": elapsed_days,
            "total_days": total_days,
        }

        for task in fact_tasks:
            dline = task.get("deadline")
            deadline_dt = parse_date(dline) if dline else None
            is_done = bool(task.get("is_closed"))
            if is_done:
                timeline_status = "clos"
            elif deadline_dt and deadline_dt < reference_date:
                timeline_status = "rappel"
            else:
                timeline_status = "a_suivre"
            start_dt = task.get("request_date")
            if not start_dt and deadline_dt:
                start_dt = deadline_dt - timedelta(days=7)
            task["timeline_status"] = timeline_status
            task["timeline_start"] = start_dt.isoformat() if start_dt else ""
            task["timeline_end"] = deadline_dt.isoformat() if deadline_dt else ""
            task["company"] = self._normalize_company(task.get("company"))
            task["owner_full_name"] = self._normalize_owner(task.get("owner_full_name"))

        reminder_log: List[Dict[str, Any]] = []
        rid = 0
        for task in fact_tasks:
            if task.get("is_open") and task.get("age_days", 0) >= reminder_threshold_days:
                rid += 1
                reminder_log.append({
                    "reminder_id": f"theoretical-{rid}",
                    "project_id": task.get("project_id"),
                    "meeting_id": task.get("meeting_id"),
                    "entry_id": task.get("entry_id"),
                    "company_id": task.get("package_company_id") or "",
                    "company_name": task.get("company") or "Non renseigné",
                    "sent_at": "",
                    "sent_by_user_id": "",
                    "sent_by_user_name": "",
                    "channel": "",
                    "reminder_type": "theoretical",
                    "template_name": "",
                    "status": "pending",
                })
            txt = clean_text(task.get("last_comment_text")).lower()
            if txt and ("relance" in txt or "rappel" in txt):
                rid += 1
                reminder_log.append({
                    "reminder_id": f"real-{rid}",
                    "project_id": task.get("project_id"),
                    "meeting_id": task.get("meeting_id"),
                    "entry_id": task.get("entry_id"),
                    "company_id": task.get("package_company_id") or "",
                    "company_name": task.get("company") or "Non renseigné",
                    "sent_at": task.get("last_comment_date") or task.get("last_activity_at") or "",
                    "sent_by_user_id": task.get("last_comment_user_id") or "",
                    "sent_by_user_name": task.get("last_comment_user_name") or "",
                    "channel": "memo_comment",
                    "reminder_type": "real",
                    "template_name": "",
                    "status": "sent",
                })

        reminder_count_theoretical = sum(1 for r in reminder_log if r.get("reminder_type") == "theoretical")
        reminder_count_real = sum(1 for r in reminder_log if r.get("reminder_type") == "real")
        for c in kpi_company_summary:
            cname = c.get("company")
            c["reminder_count_theoretical"] = sum(1 for r in reminder_log if r.get("reminder_type") == "theoretical" and r.get("company_name") == cname)
            c["reminder_count_real"] = sum(1 for r in reminder_log if r.get("reminder_type") == "real" and r.get("company_name") == cname)
        open_entries_count = rows_filtered_by_title + rows_filtered_by_id
        if project_info and match_debug.get("match_score", 0) >= 80:
            confidence_level = "high"
        elif open_entries_count > 0 or match_debug.get("resolution_mode") == "entries_fallback":
            confidence_level = "medium"
        else:
            confidence_level = "low"

        warning = bool(match_debug.get("resolution_mode") == "entries_fallback" or not project_info)
        warning_message = "Projet trouvé via entrées METRONOME (fallback)" if warning else ""

        project_display_name = resolved_title or target
        project_id = resolved_id or self._row_id(project_info)
        return {
            "ok": True,
            "project_name": project_display_name,
            "project_id": project_id,
            "confidence_level": confidence_level,
            "warning": warning,
            "warning_message": warning_message,
            "match_debug": match_debug,
            "kpis_meeting_simple": meeting_simple_kpis,
            "kpis": {
                "open_topics": len(rows),
                "overdue_topics": sum(1 for r in rows if r.get("overdue")),
                "by_company": by_company,
                "by_package": count_by("lot"),
                "by_meeting": by_meeting,
            },
            "kpis_pilotage": {
                "reminders_open_count": len(rem_rows),
                "followups_open_count": len(fol_rows),
                "reference_date": reference_date.isoformat(),
                "reference_date_text": reference_date_text,
                "reminders_by_company": due_by_company_list,
                "open_tasks_by_company": open_tasks_by_company,
                "average_processing_days_by_company": average_processing_days_by_company,
                "rappels_ouverts_a_date": len(rem_rows),
                "a_suivre_ouverts": len(fol_rows),
                "date_reference": reference_date.isoformat(),
                "rappels_cumules_par_entreprise": due_by_company_list,
            },
            "analytics": {
                "fact_tasks": fact_tasks,
                "fact_memos": fact_memos,
                "fact_memo_comments": fact_memo_comments,
                "fact_meetings": fact_meetings,
                "fact_interactions": fact_interactions,
                "kpi_project_summary": kpi_project_summary,
                "kpi_company_summary": kpi_company_summary,
                "kpi_package_summary": kpi_package_summary,
                "kpi_zone_summary": kpi_zone_summary,
                "kpi_project_progress": kpi_project_progress,
                "kpi_reactivity_company": kpi_reactivity_company,
                "reminder_log": reminder_log,
                "reminder_count_theoretical": reminder_count_theoretical,
                "reminder_count_real": reminder_count_real,
                "documents_count": len(documents_for_project),
            },
            "rows": rows,
            "missing_files": cache.get("missing", []),
            "loaded_at": cache.get("loaded_at"),
        }


service = FinanceService(WORKBOOK_PATH, SHEET_NAME, CACHE_FILE)
metronome_service = MetronomeService(METRONOME_BASE_PATH)


def landing_html() -> str:
    return """<!doctype html>
<html lang='fr'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Gestion Affaire - Accueil</title>
<style>
:root{--panel:#fff;--line:#dfe5ef;--ink:#122033;--muted:#6e7a90;--accent:#ef8d00;--shadow:0 16px 48px rgba(18,32,51,.09)}
*{box-sizing:border-box}body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:linear-gradient(180deg,#f5f7fb 0%,#eef2f7 100%);color:var(--ink)}
.wrap{max-width:1260px;margin:30px auto 52px;padding:0 20px}.hero,.card{background:var(--panel);border:1px solid rgba(20,32,51,.05);border-radius:24px;box-shadow:var(--shadow)}
.hero{padding:28px}.hero-top{display:flex;align-items:center;gap:16px}.logo{width:56px;height:56px;object-fit:contain;border-radius:10px;background:#fff7ef;border:1px solid #f8ddb4;padding:6px}
.eyebrow{font-size:12px;font-weight:800;color:var(--accent);text-transform:uppercase;letter-spacing:.14em}h1{margin:8px 0 12px;font-size:44px;line-height:1.05}.sub{font-size:20px;color:var(--muted);max-width:930px}
.selector{margin-top:22px;display:grid;grid-template-columns:2fr 1fr;gap:12px}.search{height:52px;border:1px solid var(--line);border-radius:14px;padding:0 14px;font-size:16px;width:100%}
.badge{display:inline-flex;align-items:center;justify-content:center;padding:0 14px;height:52px;border-radius:14px;background:#f9fbff;color:#41547b;font-weight:700;border:1px solid var(--line)}
.grid{margin-top:18px;display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}.card{padding:20px;display:flex;flex-direction:column;justify-content:space-between;min-height:220px}.card h3{margin:0;font-size:21px}.card p{margin:10px 0 18px;color:var(--muted);font-size:14px;min-height:44px}.card .btn{width:100%}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;height:44px;padding:0 16px;border-radius:12px;text-decoration:none;border:none;font-weight:800;cursor:pointer}
.btn.primary,.btn.dark{background:var(--accent);color:#fff}.btn.disabled{background:#ecf0f7;color:#8a94a8;cursor:not-allowed}
.state{margin-top:14px;padding:14px 16px;border-radius:14px;background:#f8faff;border:1px solid var(--line);color:#4f5d78;font-weight:600}
@media (max-width:980px){.grid{grid-template-columns:repeat(2,1fr)}.selector{grid-template-columns:1fr}}@media (max-width:640px){h1{font-size:34px}.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
  <div class='wrap'>
    <section class='hero'>
      <div class='hero-top'>
        <img class='logo' src='/assets/logo-tempo' alt='Logo Tempo'>
        <div><div class='eyebrow'>Gestion affaire</div><h1>Accueil multiprojets</h1></div>
      </div>
      <div class='sub'>Choisissez une affaire en saisissant ses premières lettres, puis accédez directement aux modules disponibles.</div>
      <div class='selector'>
        <input id='projectSearch' class='search' list='projectList' placeholder='Tapez les premières lettres de l'affaire'>
        <datalist id='projectList'></datalist>
        <div id='projectBadge' class='badge'>Aucune affaire sélectionnée</div>
      </div>
      <div id='state' class='state'>Sélectionnez une affaire pour activer les modules.</div>
    </section>

    <section class='grid'>
      <article class='card'>
        <h3>Tableau de bord</h3><p>Porte d'entrée de pilotage de l'affaire.</p>
        <a id='dashboardLink' class='btn disabled' href='javascript:void(0)' aria-disabled='true'>Ouvrir le tableau de bord</a>
      </article>
      <article class='card'>
        <h3>Finances</h3><p>Cockpit financier détaillé de l'affaire.</p>
        <a id='financeLink' class='btn disabled' href='javascript:void(0)' aria-disabled='true'>Ouvrir Finances</a>
      </article>
      <article class='card'><h3>Gestion de projet</h3><p>Planification, jalons et coordination.</p><a id='pmLink' class='btn disabled' href='javascript:void(0)' aria-disabled='true'>Ouvrir</a></article>
      <article class='card'><h3>Imputation</h3><p>Suivi des temps et affectations.</p><button class='btn disabled' disabled>Ouvrir</button></article>
    </section>
  </div>
<script>
const state={projects:[],selectedId:'',selectedLabel:''};
function esc(v){return String(v||'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));}
function setModuleLink(id,base){const el=document.getElementById(id);if(state.selectedId){el.className='btn primary';el.href=`/${base}?affaire_id=${encodeURIComponent(state.selectedId)}`;el.removeAttribute('aria-disabled');}else{el.className='btn disabled';el.href='javascript:void(0)';el.setAttribute('aria-disabled','true');}}
function updateUi(){document.getElementById('projectBadge').textContent=state.selectedLabel?`Affaire : ${state.selectedLabel}`:'Aucune affaire sélectionnée';document.getElementById('state').textContent=state.selectedLabel?`Vous naviguez sur l'affaire ${state.selectedLabel}.`:'Sélectionnez une affaire pour activer les modules.';setModuleLink('financeLink','finance');setModuleLink('dashboardLink','dashboard');setModuleLink('pmLink','gestion-projet');if(state.selectedId){localStorage.setItem('selectedAffaireId',state.selectedId);} }
async function loadProjects(){try{const res=await fetch('/api/finance/affaires');const data=await res.json();state.projects=(data.items||[]).map(x=>({id:x.affaire_id,label:x.display_name})).filter(x=>x.id&&x.label).sort((a,b)=>a.label.localeCompare(b.label,'fr'));}catch(_){state.projects=[];}
const list=document.getElementById('projectList');list.innerHTML=state.projects.map(p=>`<option value="${esc(p.label)}"></option>`).join('');const savedId=localStorage.getItem('selectedAffaireId')||'';const selected=state.projects.find(x=>x.id===savedId);if(selected){state.selectedId=selected.id;state.selectedLabel=selected.label;document.getElementById('projectSearch').value=selected.label;}updateUi();}
document.getElementById('projectSearch').addEventListener('input',ev=>{const q=(ev.target.value||'').trim().toLowerCase();const selected=state.projects.find(p=>p.label.toLowerCase()===q)||state.projects.find(p=>p.label.toLowerCase().startsWith(q));state.selectedId=selected?.id||'';state.selectedLabel=selected?.label||'';updateUi();});
loadProjects();
</script>
</body>
</html>"""



def finance_html() -> str:
    return """
<!doctype html>
<html lang='fr'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Finance Cockpit</title>
<style>
:root{--bg:#eef2f7;--panel:#fff;--panel2:#f7f9fc;--line:#dfe5ef;--ink:#122033;--muted:#6e7a90;--blue:#ef8d00;--green:#1d9a5b;--amber:#c48716;--red:#c84c4c;--shadow:0 14px 40px rgba(19,31,53,.08)}
*{box-sizing:border-box}body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:linear-gradient(180deg,#f5f7fb 0%,#eef2f7 100%);color:var(--ink)}
.container{max-width:1440px;margin:22px auto 36px;padding:0 18px}.topbar,.hero,.section,.kpis{background:var(--panel);border:1px solid rgba(22,34,51,.04);box-shadow:var(--shadow);border-radius:24px}
.topbar{display:flex;gap:16px;align-items:center;padding:18px 20px;position:sticky;top:14px;z-index:10}.brand{min-width:200px}.brand h1{margin:0;font-size:36px;line-height:1}.brand p{margin:6px 0 0;color:var(--muted);font-size:14px}
.controls{display:flex;gap:12px;align-items:center;flex:1;flex-wrap:wrap}.search,.select{height:48px;border-radius:14px;border:1px solid var(--line);background:#fff;padding:0 14px;color:var(--ink);font-size:15px}.search{min-width:280px;flex:1}.select{min-width:340px;flex:1}
.btn{height:48px;border:none;border-radius:14px;padding:0 16px;font-weight:700;cursor:pointer}.btn.primary{background:var(--blue);color:#fff}.btn.dark{background:#ef8d00;color:#fff}
.badge{display:inline-flex;align-items:center;gap:8px;padding:10px 12px;border-radius:999px;background:var(--panel2);color:var(--muted);font-size:13px;font-weight:700}.dot{width:10px;height:10px;border-radius:50%}.dot.ready{background:var(--green)}.dot.building{background:var(--amber)}.dot.error{background:var(--red)}.dot.idle{background:#9aa6b8}
.hero{margin-top:18px;padding:26px 28px}.hero-top{display:flex;justify-content:space-between;gap:16px;align-items:flex-start;flex-wrap:wrap}.hero h2{margin:8px 0 4px;font-size:42px;line-height:1.02}.eyebrow{font-size:12px;font-weight:800;color:var(--blue);letter-spacing:.14em;text-transform:uppercase}
.meta-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin-top:18px}.meta-pill{background:var(--panel2);border:1px solid var(--line);border-radius:18px;padding:14px 16px}.meta-pill .label{font-size:12px;color:var(--muted);text-transform:uppercase;font-weight:800;letter-spacing:.08em}.meta-pill .value{margin-top:6px;font-size:16px;font-weight:700}
.health{padding:10px 14px;border-radius:999px;font-size:13px;font-weight:800}.health.ok{background:#eaf8f0;color:var(--green)}.health.warn{background:#fff6e6;color:var(--amber)}.health.bad{background:#fff0f0;color:var(--red)}
.kpis{margin-top:18px;padding:16px}.kpi-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}.kpi{background:linear-gradient(180deg,#fff,#f8faff);border:1px solid var(--line);border-radius:20px;padding:18px;min-height:132px}.kpi .label{font-size:13px;color:var(--muted);font-weight:800;text-transform:uppercase;letter-spacing:.06em}.kpi .value{margin-top:10px;font-size:34px;font-weight:800;line-height:1}.kpi .sub{margin-top:10px;font-size:13px;color:var(--muted)}
.kpi.good{background:linear-gradient(180deg,#ffffff,#ebf9f1);border-color:#8fd5b0}.kpi.warn{background:linear-gradient(180deg,#ffffff,#fff4e1);border-color:#f1c171}.kpi.bad{background:linear-gradient(180deg,#ffffff,#ffe9e9);border-color:#ee9a9a}
.layout{display:grid;grid-template-columns:2fr 1fr;gap:18px;margin-top:18px}.section{padding:18px 18px 22px}.section h3{margin:0 0 14px;font-size:24px}
.chart-card{min-height:560px}.chart-wrap{height:320px;border-radius:18px;background:linear-gradient(180deg,#f7f9fc 0%,#f3f6fb 100%);border:1px solid var(--line);padding:14px}.cum-wrap{margin-top:14px;padding:14px;border:1px solid var(--line);border-radius:16px;background:#f9fbff}.cum-title{margin:0 0 10px;font-size:16px;font-weight:800}.cum-row{display:grid;grid-template-columns:160px 1fr 120px;gap:10px;align-items:center;margin:8px 0}.cum-track{height:12px;background:#e7edf7;border-radius:999px;overflow:hidden}.cum-fill{height:100%}.cum-pre{background:#b9c9ea}.cum-fac{background:#ef8d00}
.legend{display:flex;gap:18px;align-items:center;font-size:13px;color:var(--muted);font-weight:700;margin-top:10px}.legend span{display:inline-flex;align-items:center;gap:8px}.swatch{display:inline-block;width:14px;height:14px;border-radius:4px}
.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:18px}table{width:100%;border-collapse:collapse}th,td{padding:14px;border-bottom:1px solid var(--line);font-size:14px;text-align:left}th{background:#f7f9fc;color:#536079;font-size:12px;text-transform:uppercase;letter-spacing:.08em}tr:last-child td{border-bottom:none}td.num{text-align:right;font-variant-numeric:tabular-nums}
.delta.pos{color:var(--green);font-weight:800}.delta.neg{color:var(--red);font-weight:800}.insights{display:flex;flex-wrap:wrap;gap:10px}.insight{padding:12px 14px;border-radius:16px;font-size:14px;font-weight:700;border:1px solid var(--line);background:var(--panel2)}
.notice{padding:14px 16px;border-radius:16px;background:#fff7e7;color:#8c6211;border:1px solid #f0dcab}.error{padding:14px 16px;border-radius:16px;background:#fff0f0;color:#992f2f;border:1px solid #f1c6c6}.empty{padding:28px;border:1px dashed var(--line);border-radius:18px;color:var(--muted);text-align:center;background:#fafbfd}.small{font-size:13px;color:var(--muted)}.footer-row{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-top:12px}
@media (max-width:1200px){.kpi-grid{grid-template-columns:repeat(2,1fr)}.layout{grid-template-columns:1fr}.meta-grid{grid-template-columns:repeat(2,1fr)}}@media (max-width:720px){.topbar{position:static}.kpi-grid{grid-template-columns:1fr}.meta-grid{grid-template-columns:1fr}.hero h2{font-size:30px}.select,.search{min-width:100%}}
</style>
</head>
<body>
<div class='container'>
  <div class='topbar'>
    <div class='brand'><div class='eyebrow'>Gestion affaire</div><h1>Finance</h1><p>Cockpit financier par affaire</p></div>
    <div class='controls'>
      <input id='searchInput' class='search' type='search' placeholder='Rechercher une affaire, un client, un mot-clé…'>
      <select id='affaireSelect' class='select'><option value=''>Sélectionnez une affaire</option></select>
      <button id='reloadBtn' class='btn primary'>Reconstruire le cache</button>
      <button id='exportBtn' class='btn dark' disabled>Exporter CSV</button>
      <a id='dashboardBtn' class='btn dark' href='/dashboard'>Tableau de bord</a>
      <a id='pmBtn' class='btn dark' href='/gestion-projet'>Gestion de projet</a>
      <a class='btn dark' href='/'>Accueil</a>
      <div id='cacheBadge' class='badge'><span class='dot idle'></span><span>Cache : attente</span></div>
    </div>
  </div>

  <div id='errorBox' style='display:none;margin-top:18px' class='error'></div>
  <div id='noticeBox' style='display:none;margin-top:18px' class='notice'></div>

  <div class='hero'>
    <div class='hero-top'>
      <div>
        <div class='eyebrow'>Affaire sélectionnée</div>
        <h2 id='heroTitle'>Sélectionnez une affaire</h2>
        <div id='heroSubtitle' class='small'>Le cockpit se remplit à partir du cache du tableau activité.</div>
      </div>
      <div id='heroHealth' class='health warn'>En attente</div>
    </div>
    <div class='meta-grid'>
      <div class='meta-pill'><div class='label'>CLIENT</div><div class='value' id='metaClient'>-</div></div>
      <div class='meta-pill'><div class='label'>PROJET</div><div class='value' id='metaProject'>-</div></div>
      <div class='meta-pill'><div class='label'>MISSIONS</div><div class='value' id='metaMissions'>-</div></div>
      <div class='meta-pill'><div class='label'>STATUT</div><div class='value' id='metaStatus'>🟠 Attention</div></div>
    </div>
  </div>

  <div class='kpis'><div class='kpi-grid'>
    <div class='kpi' id='kpiCommandeCard'><div class='label'>💰 Commande HT</div><div class='value' id='kpiCommande'>0 €</div><div class='sub'>Montant contractualisé</div></div>
    <div class='kpi' id='kpiAnterioriteCard'><div class='label'>📚 Antériorité</div><div class='value' id='kpiAnteriorite'>0 €</div><div class='sub'>Somme G → M</div></div>
    <div class='kpi' id='kpiFacture2026Card'><div class='label'>📈 Facturé 2026</div><div class='value' id='kpiFacture2026'>0 €</div><div class='sub'>Colonne N</div></div>
    <div class='kpi' id='kpiFacturationTotaleCard'><div class='label'>🧾 Facturation totale</div><div class='value' id='kpiFacturationTotale'>0 €</div><div class='sub'>📚 Antériorité + 2026</div></div>
    <div class='kpi' id='kpiResteCard'><div class='label'>⚠ Reste à facturer</div><div class='value' id='kpiReste'>0 €</div><div class='sub'>Solde estimé</div></div>
    <div class='kpi' id='kpiAvanceCard'><div class='label'>✅ Avancement financier</div><div class='value' id='kpiAvance'>0 %</div><div class='sub'>Facturé / commande</div></div>
  </div></div>

  <div class='layout'>
    <div class='section chart-card'>
      <h3>Facturation mensuelle</h3>
      <div class='chart-wrap'><svg id='monthlyChart' width='100%' height='100%' viewBox='0 0 980 320' preserveAspectRatio='none'></svg></div>
      <div class='legend'><span><i class='swatch' style='background:#dbe6ff'></i>Prévisionnel</span><span><i class='swatch' style='background:#ef8d00'></i>Facturation</span></div><div class='cum-wrap'><div class='cum-title'>Graphique cumulatif</div><div id='cumulativeChart'></div></div>
    </div>
    <div class='section'><h3>Insights / alertes</h3><div id='insightsBox' class='insights'><div class='empty' style='width:100%'>Sélectionnez une affaire.</div></div><div class='footer-row'><div class='small' id='statusMeta'>Cache en attente.</div></div></div>
  </div>

  <div class='section' style='margin-top:18px'><h3>Mensuel détaillé</h3><div id='monthlyTableWrap' class='table-wrap'><div class='empty'>Sélectionnez une affaire pour afficher le détail mensuel.</div></div></div>
  <div class='section' style='margin-top:18px'><div class='footer-row'><h3 style='margin:0'>Détail des missions</h3><div class='small' id='missionsMeta'>0 mission</div></div><div id='missionsTableWrap' class='table-wrap'><div class='empty'>Sélectionnez une affaire pour afficher les missions.</div></div></div>
</div>

<script>
const MONTHS=["janvier","fevrier","mars","avril","mai","juin","juillet","aout","septembre","octobre","novembre","decembre"];
const MONTH_LABELS={"janvier":"Janv.","fevrier":"Févr.","mars":"Mars","avril":"Avr.","mai":"Mai","juin":"Juin","juillet":"Juil.","aout":"Août","septembre":"Sept.","octobre":"Oct.","novembre":"Nov.","decembre":"Déc."};
const state={cacheStatus:null,affaires:[],selectedAffaireId:"",selectedAffaire:null};
function euro(v){return new Intl.NumberFormat('fr-FR',{style:'currency',currency:'EUR',maximumFractionDigits:0}).format(Number(v||0));}
function pct(v){return new Intl.NumberFormat('fr-FR',{style:'percent',maximumFractionDigits:1}).format(Number(v||0));}
function fmt(v){return new Intl.NumberFormat('fr-FR',{maximumFractionDigits:0}).format(Number(v||0));}
function esc(v){return String(v??'').replace(/[&<>]/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]||ch));}
function showError(msg){const b=document.getElementById('errorBox');b.textContent=msg||'Erreur';b.style.display='block';}
function clearError(){document.getElementById('errorBox').style.display='none';}
function showNotice(msg){const b=document.getElementById('noticeBox');b.textContent=msg||'';b.style.display=msg?'block':'none';}
function setCacheBadge(status,label){const c=status==='ready'?'ready':status==='building'?'building':status==='error'?'error':'idle';document.getElementById('cacheBadge').innerHTML=`<span class="dot ${c}"></span><span>${esc(label)}</span>`;}
async function api(url,options){const r=await fetch(url,options||{});const data=await r.json().catch(()=>({}));if(!r.ok) throw new Error(data.error||data.detail||data.message||`HTTP ${r.status}`);return data;}
function healthClass(a){const reste=Number(a.reste_a_facturer||0),taux=Number(a.taux_avancement_financier||0);if(taux>=0.9)return{label:'Presque soldée',cls:'ok'};if(reste>0&&taux<0.35)return{label:'À surveiller',cls:'warn'};if(reste<0)return{label:'Incohérence à vérifier',cls:'bad'};return{label:'Stable',cls:'ok'};}
async function loadCacheStatus(){const d=await api('/api/finance/cache-status');state.cacheStatus=d;const label=d.status==='ready'?`Cache prêt · ${d.affaires_count} affaires`:d.status==='building'?'Cache en reconstruction…':`Cache : ${d.status}`;setCacheBadge(d.status,label);document.getElementById('statusMeta').textContent=`${d.affaires_count||0} affaires · ${d.rows_kept||0} lignes utiles · ${d.generated_at||'pas encore généré'}`;}
async function loadAffairesList(search=''){const d=await api(`/api/finance/affaires?search=${encodeURIComponent(search)}`);state.affaires=d.items||[];const sel=document.getElementById('affaireSelect');const prev=state.selectedAffaireId;sel.innerHTML=`<option value=''>Sélectionnez une affaire</option>`+state.affaires.map(x=>`<option value="${esc(x.affaire_id)}">${esc(x.display_name)}</option>`).join('');if(prev&&state.affaires.some(x=>x.affaire_id===prev)){sel.value=prev;}else{state.selectedAffaireId='';state.selectedAffaire=null;}showNotice(state.affaires.length?`${state.affaires.length} affaire(s) disponible(s)`:'Aucune affaire trouvée pour ce filtre.');}
async function loadSelectedAffaire(id){if(!id){state.selectedAffaireId='';state.selectedAffaire=null;renderAll();return;}const d=await api(`/api/finance/affaire/${encodeURIComponent(id)}`);state.selectedAffaireId=id;state.selectedAffaire=d.affaire||null;renderAll();localStorage.setItem('selectedAffaireId',id);}
function setHeroEmpty(){document.getElementById('heroTitle').textContent='Sélectionnez une affaire';document.getElementById('heroSubtitle').textContent='Le cockpit se remplit à partir du cache du tableau activité.';document.getElementById('metaClient').textContent='-';document.getElementById('metaProject').textContent='-';document.getElementById('metaMissions').textContent='-';document.getElementById('metaStatus').textContent='🟠 Attention';const h=document.getElementById('heroHealth');h.textContent='En attente';h.className='health warn';}
function cardTone(id,tone){const el=document.getElementById(id);el.classList.remove('good','warn','bad');if(tone)el.classList.add(tone);}
function renderHero(){const a=state.selectedAffaire;if(!a){setHeroEmpty();return;}document.getElementById('heroTitle').textContent=a.display_name||'-';document.getElementById('heroSubtitle').textContent=`Client ${a.client||'-'} · ${(a.missions||[]).length} mission(s)`;document.getElementById('metaClient').textContent=a.client||'-';document.getElementById('metaProject').textContent=a.affaire||'-';document.getElementById('metaMissions').textContent=String((a.missions||[]).length||0);const hh=healthClass(a),el=document.getElementById('heroHealth');el.textContent=hh.label;el.className=`health ${hh.cls}`;document.getElementById('metaStatus').textContent=hh.cls==='ok'?'🟢 Stable':(hh.cls==='warn'?'🟠 Attention':'🔴 Risque');}
function renderKpis(){const a=state.selectedAffaire||{commande_ht:0,anteriorite:0,facture_2026:0,facturation_totale:0,reste_a_facturer:0,taux_avancement_financier:0};
document.getElementById('kpiCommande').textContent=euro(a.commande_ht);
document.getElementById('kpiAnteriorite').textContent=euro(a.anteriorite||0);
document.getElementById('kpiFacture2026').textContent=euro(a.facture_2026||a.facturation_cumulee_2026||0);
document.getElementById('kpiFacturationTotale').textContent=euro(a.facturation_totale||0);
document.getElementById('kpiReste').textContent=euro(a.reste_a_facturer);
document.getElementById('kpiAvance').textContent=pct(a.taux_avancement_financier);
cardTone('kpiCommandeCard','good');
cardTone('kpiAnterioriteCard',(a.anteriorite||0)>0?'good':'warn');
cardTone('kpiFacture2026Card',(a.facture_2026||0)>0?'good':'warn');
cardTone('kpiFacturationTotaleCard',(a.facturation_totale||0)>0?'good':'warn');
cardTone('kpiResteCard',a.reste_a_facturer<0?'bad':(a.reste_a_facturer>(a.commande_ht||0)*0.5?'warn':'good'));
cardTone('kpiAvanceCard',a.taux_avancement_financier>0.85?'good':(a.taux_avancement_financier<0.35?'warn':''));}
function renderFinanceChart(){const root=document.getElementById('monthlyChart');const a=state.selectedAffaire;if(!a){root.innerHTML=`<text x="490" y="160" text-anchor="middle" fill="#6e7a90" font-size="18">Sélectionnez une affaire</text>`;return;}const s=MONTHS.map(m=>({label:MONTH_LABELS[m],pre:Number((((a.mensuel||{})[m]||{}).previsionnel)||0),fac:Number((((a.mensuel||{})[m]||{}).facture)||0)}));const maxVal=Math.max(1,...s.flatMap(x=>[x.pre,x.fac]));const left=56,top=16,width=880,height=250,step=width/s.length,barW=step*0.48;let grid='',bars='',labels='';const points=[];for(let i=0;i<=4;i++){const y=top+(height/4)*i,val=Math.round(maxVal*(1-i/4));grid+=`<line x1="${left}" y1="${y}" x2="${left+width}" y2="${y}" stroke="#dfe5ef" stroke-width="1"/><text x="${left-10}" y="${y+4}" text-anchor="end" fill="#8090a8" font-size="12">${fmt(val)}</text>`;}s.forEach((it,i)=>{const x=left+i*step+(step-barW)/2;const h=(it.pre/maxVal)*height;const y=top+height-h;const py=top+height-(it.fac/maxVal)*height;bars+=`<rect x="${x}" y="${y}" width="${barW}" height="${Math.max(h,0.5)}" rx="8" fill="#dbe6ff"><title>${it.label} prévisionnel: ${euro(it.pre)}</title></rect>`;points.push(`${x+barW/2},${py}`);labels+=`<text x="${x+barW/2}" y="${top+height+22}" text-anchor="middle" fill="#66748b" font-size="12">${it.label}</text>`;});root.innerHTML=`${grid}<line x1="${left}" y1="${top+height}" x2="${left+width}" y2="${top+height}" stroke="#b8c3d4" stroke-width="1.2"/>${bars}<polyline points="${points.join(' ')}" fill="none" stroke="#ef8d00" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>${points.map((p,i)=>{const q=p.split(',');return `<circle cx="${q[0]}" cy="${q[1]}" r="4.5" fill="#fff" stroke="#ef8d00" stroke-width="3"><title>${s[i].label} facturé: ${euro(s[i].fac)}</title></circle>`;}).join('')}${labels}`;}
function renderCumulativeChart(){const root=document.getElementById('cumulativeChart');const a=state.selectedAffaire;if(!a){root.innerHTML="<div class='small'>Sélectionnez une affaire.</div>";return;}const pre=Number(a.total_previsionnel||0);const fac=Number(a.total_facture||0);const max=Math.max(1,pre,fac);const prePct=(pre/max)*100;const facPct=(fac/max)*100;root.innerHTML=`<div class='cum-row'><div>Prévisionnel cumulé</div><div class='cum-track'><div class='cum-fill cum-pre' style='width:${prePct}%'></div></div><div>${euro(pre)}</div></div><div class='cum-row'><div>Facturation cumulée</div><div class='cum-track'><div class='cum-fill cum-fac' style='width:${facPct}%'></div></div><div>${euro(fac)}</div></div>`;}
function renderMonthlyTable(){const root=document.getElementById('monthlyTableWrap');const a=state.selectedAffaire;if(!a){root.innerHTML=`<div class='empty'>Sélectionnez une affaire pour afficher le détail mensuel.</div>`;return;}let rows='';MONTHS.forEach(m=>{const pre=Number((((a.mensuel||{})[m]||{}).previsionnel)||0),fac=Number((((a.mensuel||{})[m]||{}).facture)||0),ec=fac-pre;rows+=`<tr><td>${MONTH_LABELS[m]}</td><td class='num'>${euro(pre)}</td><td class='num'>${euro(fac)}</td><td class='num delta ${ec>=0?'pos':'neg'}'>${euro(ec)}</td></tr>`;});rows+=`<tr><td><strong>Total</strong></td><td class='num'><strong>${euro(a.total_previsionnel||0)}</strong></td><td class='num'><strong>${euro(a.total_facture||0)}</strong></td><td class='num delta ${Number(a.ecart_previsionnel_vs_facture||0)>=0?'pos':'neg'}'><strong>${euro(a.ecart_previsionnel_vs_facture||0)}</strong></td></tr>`;root.innerHTML=`<table><thead><tr><th>Mois</th><th class='num'>Prévisionnel</th><th class='num'>Facturé</th><th class='num'>Écart</th></tr></thead><tbody>${rows}</tbody></table>`;}
function renderMissions(){const root=document.getElementById('missionsTableWrap');const meta=document.getElementById('missionsMeta');const a=state.selectedAffaire;if(!a){meta.textContent='0 mission';root.innerHTML=`<div class='empty'>Sélectionnez une affaire pour afficher les missions.</div>`;return;}const missions=a.missions||[];meta.textContent=`${missions.length} mission(s)`;if(!missions.length){root.innerHTML=`<div class='empty'>Aucune mission détaillée sur cette affaire.</div>`;return;}root.innerHTML=`<table><thead><tr><th>Tag</th><th>Mission</th><th>N°</th><th class='num'>Commande</th><th class='num'>🧾 Facturation totale</th><th class='num'>Reste</th><th class='num'>Prévisionnel</th><th class='num'>Facturé</th></tr></thead><tbody>${missions.map(m=>`<tr><td>${esc(m.tag||'')}</td><td>${esc(m.label||'')}</td><td>${esc(m.numero||'')}</td><td class='num'>${euro(m.commande_ht)}</td><td class='num'>${euro(m.facturation_totale||((m.anteriorite||0)+(m.facture_2026||m.facturation_cumulee_2026||0)))}</td><td class='num'>${euro(m.reste_a_facturer)}</td><td class='num'>${euro(m.total_previsionnel)}</td><td class='num'>${euro(m.total_facture)}</td></tr>`).join('')}</tbody></table>`;}
function renderInsights(){const root=document.getElementById('insightsBox');const a=state.selectedAffaire;if(!a){root.innerHTML=`<div class='empty' style='width:100%'>Sélectionnez une affaire.</div>`;return;}const items=a.insights||[];root.innerHTML=items.map(x=>`<div class='insight'>${esc(x)}</div>`).join('');}
function renderAll(){renderHero();renderKpis();renderFinanceChart();renderCumulativeChart();renderMonthlyTable();renderMissions();renderInsights();document.getElementById('exportBtn').disabled=!state.selectedAffaireId;const dash=document.getElementById('dashboardBtn');dash.href=state.selectedAffaireId?`/dashboard?affaire_id=${encodeURIComponent(state.selectedAffaireId)}`:'/dashboard';const pm=document.getElementById('pmBtn');pm.href=state.selectedAffaireId?`/gestion-projet?affaire_id=${encodeURIComponent(state.selectedAffaireId)}`:'/gestion-projet';}
async function rebuildCache(){clearError();showNotice('Reconstruction du cache en cours…');await api('/api/finance/rebuild-cache',{method:'POST'});await loadCacheStatus();await loadAffairesList(document.getElementById('searchInput').value||'');if(state.selectedAffaireId&&state.affaires.some(x=>x.affaire_id===state.selectedAffaireId)){await loadSelectedAffaire(state.selectedAffaireId);}else{state.selectedAffaireId='';state.selectedAffaire=null;renderAll();}showNotice('Cache reconstruit avec succès.');}
async function initFinancePage(){clearError();try{await loadCacheStatus();await loadAffairesList('');const params=new URLSearchParams(window.location.search);const affairFromUrl=params.get('affaire_id');const affairFromStorage=localStorage.getItem('selectedAffaireId')||'';const preselected=affairFromUrl||affairFromStorage;if(preselected&&state.affaires.some(x=>x.affaire_id===preselected)){document.getElementById('affaireSelect').value=preselected;await loadSelectedAffaire(preselected);}else{renderAll();}}catch(err){showError(err.message||'Erreur de chargement');}
document.getElementById('searchInput').addEventListener('input',async ev=>{clearError();try{await loadAffairesList(ev.target.value||'');if(state.selectedAffaireId&&!state.affaires.some(x=>x.affaire_id===state.selectedAffaireId)){state.selectedAffaireId='';state.selectedAffaire=null;document.getElementById('affaireSelect').value='';renderAll();}}catch(err){showError(err.message||'Erreur de recherche');}});
document.getElementById('affaireSelect').addEventListener('change',async ev=>{clearError();try{await loadSelectedAffaire(ev.target.value||'');}catch(err){showError(err.message||'Erreur de chargement affaire');}});
document.getElementById('reloadBtn').addEventListener('click',async()=>{try{await rebuildCache();}catch(err){showError(err.message||'Erreur de reconstruction du cache');}});
document.getElementById('exportBtn').addEventListener('click',()=>{if(!state.selectedAffaireId)return;window.location.href=`/api/finance/affaire/${encodeURIComponent(state.selectedAffaireId)}/export-csv`;});}
initFinancePage();
</script>
</body>
</html>
"""


def gestion_projet_html() -> str:
    return """<!doctype html>
<html lang='fr'>
<head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Gestion de projet</title>
<style>
:root{--line:#dfe5ef;--ink:#122033;--muted:#6e7a90;--accent:#ef8d00;--panel:#fff;--shadow:0 12px 34px rgba(18,32,51,.07)}
*{box-sizing:border-box}body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:#f3f6fb;color:var(--ink)}
.wrap{max-width:1480px;margin:20px auto;padding:0 16px}.top,.kpis,.section{background:var(--panel);border:1px solid var(--line);border-radius:22px;box-shadow:var(--shadow)}
.top{padding:14px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}.search,.select{height:44px;border:1px solid var(--line);border-radius:12px;padding:0 12px;min-width:280px}
.btn{height:44px;border-radius:12px;border:none;padding:0 14px;background:var(--accent);color:#fff;text-decoration:none;display:inline-flex;align-items:center;font-weight:800;cursor:pointer}
.kpis{margin-top:12px;padding:14px}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.k{border:1px solid var(--line);border-radius:14px;padding:14px;background:#fbfdff}.k .v{font-size:34px;font-weight:900;margin-top:6px}
.section{margin-top:12px;padding:14px}.subgrid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:14px}table{width:100%;border-collapse:collapse}th,td{padding:10px 12px;border-bottom:1px solid var(--line);font-size:13px;text-align:left}th{background:#f7f9fc;font-size:12px;color:#5b6880;text-transform:uppercase}.small{color:var(--muted);font-size:13px}
.bar{height:10px;background:#edf1f8;border-radius:999px;overflow:hidden}.fill{height:100%;background:#ef8d00}
.loading-wrap{margin-top:8px;padding:8px 10px;border:1px solid #d8e0ee;border-radius:12px;background:#fff}.loading-label{font-size:12px;color:#5b6880;margin-bottom:6px;font-weight:700}.loading-track{height:8px;border-radius:999px;background:#eef2f8;overflow:hidden}.loading-bar{height:100%;width:35%;background:linear-gradient(90deg,#ef8d00,#ffd08a);animation:loadmove 1.2s infinite ease-in-out}@keyframes loadmove{0%{margin-left:-35%}100%{margin-left:100%}}.match-box{margin-top:10px;border:1px solid var(--line);border-radius:10px;padding:8px;background:#fffaf2}.match-box.ok{border-color:#49a66a;background:#f4fcf6}.match-box.warn{border-color:#ef8d00;background:#fff7ec}.match-title{font-weight:700;margin-bottom:4px;font-size:13px}.conf-badge{display:inline-block;margin-left:8px;padding:1px 8px;border-radius:999px;font-size:11px;font-weight:800;background:#eef2f8;color:#3b4f6f}.match-grid{display:none}.match-item{font-size:12px;color:#30425f}.match-item b{display:block;color:#6e7a90;font-size:11px;margin-bottom:2px}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;word-break:break-word}.pilot-box{margin-top:12px;border:1px solid #cfd8e8;border-radius:18px;padding:16px;background:#f8fbff}.pilot-title{font-size:34px;font-weight:900;margin:4px 0 10px}.pilot-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.pilot-card{background:#fff;border:1px solid #d8e0ee;border-radius:18px;padding:14px}.pilot-card .t{font-size:15px;color:#4b5d7a;font-weight:800}.pilot-card .v{font-size:44px;font-weight:900;margin-top:8px}.pilot-list{margin-top:12px;border:1px solid #d8e0ee;border-radius:18px;background:#fff;padding:8px 14px}.pilot-row{display:flex;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px dashed #d8e0ee}.pilot-row:last-child{border-bottom:none}.pilot-row .name{font-weight:700}.company-cell{display:flex;align-items:center;gap:10px}.company-logo{width:28px;height:28px;border-radius:50%;border:1px solid #d8e0ee;background:#fff;object-fit:cover;display:inline-flex;align-items:center;justify-content:center;font-size:11px;color:#6e7a90;font-weight:800}.proj-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.progress-card{border:1px solid #d8e0ee;border-radius:14px;padding:12px;background:#fff}.progress-title{font-weight:800;margin-bottom:8px}.progress-row{display:grid;grid-template-columns:180px 1fr 70px;gap:10px;align-items:center;padding:6px 0}.progress-track{height:10px;border-radius:999px;background:#edf1f8;overflow:hidden}.progress-fill{height:100%;background:#1b6ef3}.curve-wrap{border:1px solid #d8e0ee;border-radius:14px;padding:12px;background:#fff}
@media (max-width:980px){.grid{grid-template-columns:repeat(2,1fr)}.subgrid{grid-template-columns:1fr}}@media (max-width:640px){.grid{grid-template-columns:1fr}}
</style></head>
<body><div class='wrap'>
  <div class='top'>
    <a class='btn' href='/'>Accueil</a>
    <input id='searchInput' class='search' type='search' placeholder='Rechercher une affaire'>
    <select id='affaireSelect' class='select'><option value=''>Sélectionnez une affaire</option></select>
    <a id='financeBtn' class='btn' href='/finance'>Finances</a>
    <a id='dashboardBtn' class='btn' href='/dashboard'>Tableau de bord</a>
  </div>
  <div id='loadingWrap' class='loading-wrap' style='display:none'><div id='loadingLabel' class='loading-label'>Chargement des indicateurs projet…</div><div class='loading-track'><div class='loading-bar'></div></div></div>

  <div class='kpis'><div class='grid'>
    <div class='k'><div class='small'>Sujets ouverts</div><div id='kOpen' class='v'>0</div></div>
    <div class='k'><div class='small'>Sujets en retard</div><div id='kLate' class='v'>0</div></div>
    <div class='k'><div class='small'>Projet METRONOME</div><div id='kProject' class='v' style='font-size:22px'>-</div></div>
    <div class='k'><div class='small'>Chargement</div><div id='kLoad' class='v' style='font-size:18px'>-</div></div>
  </div>
    <div id='matchBox' class='match-box warn'>
      <div id='matchStatus' class='match-title'>Diagnostic de matching METRONOME</div>
      <div id='matchReason' class='small'>Aucune affaire sélectionnée.</div>
      <div class='match-grid'>
        <div id='matchSearchName' class='match-item'><b>Projet recherché</b>-</div>
        <div id='matchSearchSlug' class='match-item'><b>Slug recherché</b><span class='mono'>-</span></div>
        <div id='matchFoundName' class='match-item'><b>Projet matché</b>-</div>
        <div id='matchFoundSlug' class='match-item'><b>Slug matché</b><span class='mono'>-</span></div>
        <div id='matchResolvedTitle' class='match-item'><b>Projet résolu (title)</b>-</div>
        <div id='matchResolutionMode' class='match-item'><b>Mode de résolution</b>-</div>
        <div id='matchScore' class='match-item'><b>Score de match</b>-</div>
        <div id='rowsByTitle' class='match-item'><b>Lignes filtrées par titre</b>-</div>
        <div id='rowsById' class='match-item'><b>Lignes filtrées par ID</b>-</div>
        <div id='missingFiles' class='match-item'><b>Fichiers CSV manquants</b>-</div>
      </div>
    </div>
  </div>

  <div class='section'>
    <div class='pilot-box'>
      <h3 style='margin:0 0 10px'>KPI réunion sélectionnée</h3>
      <div class='pilot-grid'>
        <div class='pilot-card'><div class='t'>Rappels ouverts à date</div><div id='kRappelsDate' class='v'>0</div></div>
        <div class='pilot-card'><div class='t'>À suivre ouverts</div><div id='kASuivre' class='v'>0</div></div>
        <div class='pilot-card'><div class='t'>Date de référence</div><div id='kDateRef' class='v' style='font-size:34px'>-</div></div>
      </div>
      <h3 style='margin:14px 0 8px'>Rappels ouverts à date cumulés par entreprise</h3>
      <div id='pilotByCompany' class='pilot-list'><div class='small'>Aucune donnée</div></div>
    </div>
  </div>

  <div class='section'>
    <h3>Pilotage global projet</h3>
    <div class='proj-grid'>
      <div id='projectProgressBars' class='progress-card'><div class='small'>Aucune donnée</div></div>
      <div id='projectProgressCurve' class='curve-wrap'><div class='small'>Aucune donnée</div></div>
    </div>
  </div>

  <div class='section'>
    <h3>Tâches ouvertes par entreprise (avec rappels)</h3>
    <div id='companyOpenList' class='pilot-list'><div class='small'>Aucune donnée</div></div>
  </div>

  <div class='section'>
    <h3>Réactivité entreprises (délai moyen de traitement)</h3>
    <div id='companyDelayList' class='pilot-list'><div class='small'>Aucune donnée</div></div>
  </div>
</div>
<script>
const state={affaires:[],selectedId:'',board:null};
function esc(v){return String(v??'').replace(/[&<>]/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]||ch));}
async function api(u){const r=await fetch(u);const d=await r.json();if(!r.ok) throw new Error(d.detail||'Erreur API');return d;}
function renderBars(id,items){const root=document.getElementById(id);if(!items||!items.length){root.innerHTML="<div class='small'>Aucune donnée</div>";return;}const max=Math.max(1,...items.map(x=>Number(x.count||0)));root.innerHTML=items.map(x=>`<div style='margin:8px 0'><div style='display:flex;justify-content:space-between;gap:8px'><span>${esc(x.label)}</span><strong>${x.count}</strong></div><div class='bar'><div class='fill' style='width:${(Number(x.count||0)/max)*100}%'></div></div></div>`).join('');}
function renderTable(rows){const root=document.getElementById('boardTable');if(!rows||!rows.length){root.innerHTML="<div class='small' style='padding:12px'>Aucun sujet ouvert.</div>";return;}root.innerHTML=`<table><thead><tr><th>Zone</th><th>Lot</th><th>Sujet</th><th>Entreprise</th><th>Responsable</th><th>Statut</th><th>Date échéance</th><th>Réunion origine</th><th>Commentaire</th></tr></thead><tbody>${rows.map(r=>`<tr><td>${esc(r.zone)}</td><td>${esc(r.lot)}</td><td>${esc(r.sujet)}</td><td>${esc(r.entreprise)}</td><td>${esc(r.responsable)}</td><td>${esc(r.statut)}</td><td>${esc(r.date_echeance)}</td><td>${esc(r.reunion_origine)}</td><td>${esc(r.commentaire)}</td></tr>`).join('')}</tbody></table>`;}
function setText(id,value){const el=document.getElementById(id);if(el) el.textContent=value;}
function setHtml(id,value){const el=document.getElementById(id);if(el) el.innerHTML=value;}
function showLoading(on,label='Chargement des indicateurs projet…'){const w=document.getElementById('loadingWrap');if(!w) return;w.style.display=on?'block':'none';setText('loadingLabel',label);}
function renderOpenTasksByCompany(b){const p=(b&&b.kpis_pilotage)||{};const analytics=(b&&b.analytics)||{};const companies=analytics.kpi_company_summary||[];const items=companies.length?companies.map(c=>({label:c.company,open_count:c.open_tasks,reminder_theoretical:c.reminder_count_theoretical||0,reminder_real:c.reminder_count_real||0})):((p.open_tasks_by_company||[]).map(x=>({label:x.label,open_count:x.open_count,reminder_theoretical:x.reminder_count||0,reminder_real:0})));const root=document.getElementById('companyOpenList');if(!items.length){root.innerHTML="<div class='small'>Aucune donnée</div>";return;}const seuil=p.reminder_threshold_weeks||2;root.innerHTML=items.map(x=>`<div class='pilot-row'><span class='name'>${esc(x.label)}</span><span><strong>${x.open_count}</strong> ouvertes · <strong>${x.reminder_theoretical}</strong> rappels théoriques (>${seuil} sem.) · <strong>${x.reminder_real}</strong> rappels réels</span></div>`).join('');}
function renderAvgDelayByCompany(b){const p=(b&&b.kpis_pilotage)||{};const items=p.average_processing_days_by_company||[];const root=document.getElementById('companyDelayList');if(!items.length){root.innerHTML="<div class='small'>Pas assez de sujets clôturés pour calculer ce KPI.</div>";return;}root.innerHTML=items.map(x=>`<div class='pilot-row'><span class='name'>${esc(x.label)}</span><span><strong>${x.avg_days}</strong> jours (sur ${x.closed_count})</span></div>`).join('');}
function initials(v){const parts=String(v||'').trim().split(/\s+/).slice(0,2);return parts.map(x=>x[0]||'').join('').toUpperCase()||'?';}
function companyLogoHtml(item){const name=item.name||item.label||'';const logo=item.logo||'';if(logo){return `<img class='company-logo' src='${esc(logo)}' alt='${esc(name)}'>`;}return `<span class='company-logo'>${esc(initials(name))}</span>`;}
function renderPilotageKpis(b){const p=(b&&b.kpis_pilotage)||{};setText('kRappelsDate',String(p.reminders_open_count||p.rappels_ouverts_a_date||0));setText('kASuivre',String(p.followups_open_count||p.a_suivre_ouverts||0));setText('kDateRef',String(p.reference_date_text||p.reference_date||p.date_reference||'-'));const root=document.getElementById('pilotByCompany');const items=p.reminders_by_company||p.rappels_cumules_par_entreprise||[];if(!items.length){root.innerHTML="<div class='small'>Aucune donnée</div>";}else{root.innerHTML=items.map(x=>`<div class='pilot-row'><span class='company-cell'>${companyLogoHtml(x)}<span class='name'>${esc(x.name||x.label)}</span></span><strong>${x.count}</strong></div>`).join('');}}
function renderMatchDiagnostics(b){const md=(b&&b.match_debug)||{};const box=document.getElementById('matchBox');
  setHtml('matchSearchName',`<b>Projet recherché</b>${esc(md.searched_project_name||b?.project_name||'-')}`);
  setHtml('matchSearchSlug',`<b>Slug recherché</b><span class='mono'>${esc(md.searched_project_slug||'-')}</span>`);
  setHtml('matchFoundName',`<b>Projet matché</b>${esc(md.matched_project_name||'-')}`);
  setHtml('matchFoundSlug',`<b>Slug matché</b><span class='mono'>${esc(md.matched_project_slug||'-')}</span>`);
  setHtml('matchResolvedTitle',`<b>Projet résolu (title)</b>${esc(md.resolved_project_title||'-')}`);
  setHtml('matchResolutionMode',`<b>Mode de résolution</b>${esc(md.resolution_mode||'-')}`);
  setHtml('matchScore',`<b>Score de match</b>${md.match_score===0||md.match_score?esc(String(md.match_score)):'-'}`);
  setHtml('rowsByTitle',`<b>Lignes filtrées par titre</b>${md.rows_filtered_by_title===0||md.rows_filtered_by_title?esc(String(md.rows_filtered_by_title)):'-'}`);
  setHtml('rowsById',`<b>Lignes filtrées par ID</b>${md.rows_filtered_by_id===0||md.rows_filtered_by_id?esc(String(md.rows_filtered_by_id)):'-'}`);
  const missing=(b&&Array.isArray(b.missing_files)&&b.missing_files.length)?b.missing_files.map(x=>esc(x)).join(' | '):'-';
  setHtml('missingFiles',`<b>Fichiers CSV manquants</b>${missing}`);
  if(!b||!b.ok){
    const g=document.querySelector('#matchBox .match-grid');if(g)g.style.display='grid';
    box.classList.remove('ok');box.classList.add('warn');
    const cf=esc((b&&b.confidence_level)||'low');setHtml('matchStatus',`⚠ Projet METRONOME non trouvé <span class='conf-badge'>confiance: ${cf}</span>`);
    const reason=b?.reason||'project_not_found';
    const loaded=b?.loaded_at?` · Chargement: ${b.loaded_at}`:'';
    setText('matchReason',`Raison: ${reason}${loaded}`);
    return;
  }
  const g=document.querySelector('#matchBox .match-grid');if(g)g.style.display='grid';
  box.classList.remove('warn');box.classList.add('ok');
  const cf=esc((b&&b.confidence_level)||'high');setHtml('matchStatus',`✅ Matching METRONOME OK <span class='conf-badge'>confiance: ${cf}</span>`);
  const loaded=b.loaded_at?` · Chargement: ${b.loaded_at}`:'';
  const warn=b&&b.warning?` · ${b.warning_message||'matching partiel'}`:'';setText('matchReason',`Projet recherché et projet matché résolus.${loaded}${warn}`);
}
function renderProjectProgress(b){const a=(b&&b.analytics)||{};const p=(a.kpi_project_progress)||{};const sum=(a.kpi_project_summary)||{};const bars=document.getElementById('projectProgressBars');const curve=document.getElementById('projectProgressCurve');if(!bars||!curve)return;const cal=Number(p.calendar_progress_pct||0);const op=Number(p.operational_progress_pct||0);const gap=Number(p.progress_gap_pct||0);const health=Number(sum.project_health_score||0);const rows=[['Avancement calendrier',cal],['Avancement opérationnel',op],['Écart (op - cal)',Math.max(0,Math.min(100,gap+50))],['Santé projet',health]];bars.innerHTML=`<div class='progress-title'>Barres de progression</div>${rows.map(r=>`<div class='progress-row'><div>${esc(r[0])}</div><div class='progress-track'><div class='progress-fill' style='width:${Math.max(0,Math.min(100,Number(r[1]||0)))}%'></div></div><div><strong>${Number(r[1]||0).toFixed(1)}%</strong></div></div>`).join('')}`;const points=[{x:10,y:90},{x:40,y:90-cal*0.8},{x:70,y:90-op*0.8},{x:95,y:90-health*0.8}];curve.innerHTML=`<div class='progress-title'>Courbe synthétique projet</div><svg viewBox='0 0 100 100' style='width:100%;height:180px'><polyline points='${points.map(p=>`${p.x},${p.y}`).join(' ')}' fill='none' stroke='#ef8d00' stroke-width='2'/><line x1='10' y1='90' x2='95' y2='90' stroke='#ccd6e8' stroke-width='1'/>${points.map(p=>`<circle cx='${p.x}' cy='${p.y}' r='2.2' fill='#1b6ef3'/>`).join('')}<text x='10' y='98' font-size='4'>Démarrage</text><text x='38' y='98' font-size='4'>Calendrier</text><text x='66' y='98' font-size='4'>Opérationnel</text><text x='90' y='98' font-size='4'>Santé</text></svg><div class='small'>Période projet: ${esc(p.start_date||'-')} → ${esc(p.end_date||'-')} (${esc(String(p.elapsed_days||0))}/${esc(String(p.total_days||0))} jours)</div>`;}
function renderBoard(){const b=state.board;if(!b||!b.ok){setText('kOpen','0');setText('kLate','0');setText('kProject','Projet METRONOME non trouvé');setText('kLoad',b&&b.loaded_at?b.loaded_at:'-');renderPilotageKpis({});renderProjectProgress({});renderOpenTasksByCompany({});renderAvgDelayByCompany({});renderMatchDiagnostics(b||{});return;}const k=b.kpis||{};setText('kOpen',String(k.open_topics||0));setText('kLate',String(k.overdue_topics||0));setText('kProject',b.warning?`${b.project_name||'-'} (partiel)`: (b.project_name||'-'));setText('kLoad',b.loaded_at||'-');renderPilotageKpis(b);renderProjectProgress(b);renderOpenTasksByCompany(b);renderAvgDelayByCompany(b);renderMatchDiagnostics(b);} 
async function loadBoard(id){if(!id){state.selectedId='';state.board=null;renderBoard();return;}state.selectedId=id;localStorage.setItem('selectedAffaireId',id);document.getElementById('financeBtn').href=`/finance?affaire_id=${encodeURIComponent(id)}`;document.getElementById('dashboardBtn').href=`/dashboard?affaire_id=${encodeURIComponent(id)}`;showLoading(true);try{state.board=await api(`/api/project-management/board?affaire_id=${encodeURIComponent(id)}`);}catch(err){state.board=null;showPageError(err);}finally{showLoading(false);}renderBoard();}
async function loadAffaires(search=''){const d=await api(`/api/finance/affaires?search=${encodeURIComponent(search)}`);state.affaires=d.items||[];const sel=document.getElementById('affaireSelect');sel.innerHTML=`<option value=''>Sélectionnez une affaire</option>`+state.affaires.map(x=>`<option value="${esc(x.affaire_id)}">${esc(x.display_name)}</option>`).join('');if(state.selectedId&&state.affaires.some(x=>x.affaire_id===state.selectedId)){sel.value=state.selectedId;}}
function showPageError(err){const box=document.getElementById('matchBox');if(box){box.classList.remove('ok');box.classList.add('warn');}setText('matchStatus','⚠ Erreur de chargement');setText('matchReason','Erreur de chargement : '+((err&&err.message)?err.message:String(err||'Erreur inconnue')));}
async function init(){
  try{
    await loadAffaires('');
    const params=new URLSearchParams(window.location.search);
    const urlId=params.get('affaire_id');
    const initialId=urlId||localStorage.getItem('selectedAffaireId')||'';
    if(initialId){
      state.selectedId=initialId;
      const sel=document.getElementById('affaireSelect');
      const found=state.affaires.find(a=>a.affaire_id===initialId);
      if(found){
        state.selectedId=found.affaire_id;
        sel.value=found.affaire_id;
        localStorage.setItem('selectedAffaireId',found.affaire_id);
      }
      await loadBoard(initialId);
    }else{renderBoard();}
    document.getElementById('searchInput').addEventListener('input',async e=>{await loadAffaires(e.target.value||'');});
    document.getElementById('affaireSelect').addEventListener('change',async e=>{await loadBoard(e.target.value||'');});
  }catch(err){console.error(err);renderBoard();showPageError(err);}
}
init();
</script></body></html>"""



def dashboard_html() -> str:
    return """<!doctype html>
<html lang='fr'>
<head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Gestion Affaire - Tableau de bord</title>
<style>
:root{--line:#dfe5ef;--ink:#122033;--muted:#6e7a90;--accent:#ef8d00;--panel:#fff;--shadow:0 12px 34px rgba(18,32,51,.07)}
*{box-sizing:border-box}body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:#f3f6fb;color:var(--ink)}
.wrap{max-width:1320px;margin:22px auto;padding:0 16px}.top,.hero,.kpis,.chart{background:var(--panel);border:1px solid var(--line);border-radius:22px;box-shadow:var(--shadow)}
.top{padding:14px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}.search,.select{height:44px;border:1px solid var(--line);border-radius:12px;padding:0 12px;min-width:280px}
.btn{height:44px;border-radius:12px;border:none;padding:0 14px;background:var(--accent);color:#fff;text-decoration:none;display:inline-flex;align-items:center;font-weight:800;cursor:pointer}
.hero{padding:18px;margin-top:14px}.eyeb{font-size:12px;font-weight:800;letter-spacing:.12em;color:var(--accent);text-transform:uppercase}.title{font-size:36px;font-weight:900;margin:6px 0}.muted{color:var(--muted)}
.kpis{padding:14px;margin-top:14px}.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.k{border:1px solid var(--line);border-radius:14px;padding:14px;background:#fbfdff}.k .v{font-size:36px;font-weight:900;margin-top:6px}
.chart{padding:14px;margin-top:14px}.chart-wrap{height:340px;border:1px solid var(--line);border-radius:14px;padding:8px;background:#f9fbff}
@media (max-width:900px){.grid{grid-template-columns:1fr}.title{font-size:28px}}
</style></head>
<body>
<div class='wrap'>
  <div class='top'>
    <a class='btn' href='/'>Accueil</a>
    <input id='searchInput' class='search' type='search' placeholder='Rechercher une affaire'>
    <select id='affaireSelect' class='select'><option value=''>Sélectionnez une affaire</option></select>
    <a id='financeBtn' class='btn' href='/finance'>Finances</a>
    <a id='pmBtn' class='btn' href='/gestion-projet'>Gestion de projet</a>
    <button id='exportBtn' class='btn' disabled>Exporter CSV</button>
  </div>
  <div class='hero'>
    <div class='eyeb'>Tableau de bord</div>
    <div id='title' class='title'>Sélectionnez une affaire</div>
    <div id='subtitle' class='muted'>Synthèse principale : client, commande, antériorité, facturé 2026, facturation totale et reste à facturer.</div>
  </div>
  <div class='kpis'><div class='grid'>
    <div class='k'><div class='muted'>💰 Commande HT</div><div id='kCommande' class='v'>0 €</div></div>
    <div class='k'><div class='muted'>Facturation cumulée</div><div id='kFacture' class='v'>0 €</div></div>
    <div class='k'><div class='muted'>⚠ Reste à facturer</div><div id='kReste' class='v'>0 €</div></div>
  </div></div>
  <div class='chart'><h3>Prévisionnel vs facturation (mois restants)</h3><div class='chart-wrap'><svg id='chart' width='100%' height='320' viewBox='0 0 980 320' preserveAspectRatio='none'></svg></div></div>
</div>
<script>
const MONTHS=['janvier','fevrier','mars','avril','mai','juin','juillet','aout','septembre','octobre','novembre','decembre'];
const MONTH_LABELS={janvier:'Janv.',fevrier:'Févr.',mars:'Mars',avril:'Avr.',mai:'Mai',juin:'Juin',juillet:'Juil.',aout:'Août',septembre:'Sept.',octobre:'Oct.',novembre:'Nov.',decembre:'Déc.'};
const state={affaires:[],selectedId:'',selected:null};
function euro(v){return new Intl.NumberFormat('fr-FR',{style:'currency',currency:'EUR',maximumFractionDigits:0}).format(Number(v||0));}
function esc(v){return String(v||'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));}
async function api(u){const r=await fetch(u);const d=await r.json();if(!r.ok) throw new Error(d.detail||'Erreur API');return d;}
async function loadAffairesList(search=''){const d=await api(`/api/finance/affaires?search=${encodeURIComponent(search)}`);state.affaires=d.items||[];const sel=document.getElementById('affaireSelect');sel.innerHTML=`<option value=''>Sélectionnez une affaire</option>`+state.affaires.map(x=>`<option value="${esc(x.affaire_id)}">${esc(x.display_name)}</option>`).join('');if(state.selectedId&&state.affaires.some(x=>x.affaire_id===state.selectedId)){sel.value=state.selectedId;}}
async function loadSelectedAffaire(id){if(!id){state.selectedId='';state.selected=null;render();return;}const d=await api(`/api/finance/affaire/${encodeURIComponent(id)}`);state.selectedId=id;state.selected=d.affaire;localStorage.setItem('selectedAffaireId',id);render();}
function renderChart(){const root=document.getElementById('chart');const a=state.selected;if(!a){root.innerHTML='';return;}const monthIdx=(new Date().getMonth());const data=MONTHS.slice(monthIdx).map(m=>({label:MONTH_LABELS[m],pre:Number(((a.mensuel||{})[m]||{}).previsionnel||0),fac:Number(((a.mensuel||{})[m]||{}).facture||0)}));const max=Math.max(1,...data.flatMap(x=>[x.pre,x.fac]));const left=56,top=20,width=880,height=240,step=width/Math.max(1,data.length),bw=step*0.46;let out='';for(let i=0;i<=4;i++){const y=top+(height/4)*i;out+=`<line x1="${left}" y1="${y}" x2="${left+width}" y2="${y}" stroke="#dfe5ef"/><text x="${left-8}" y="${y+4}" text-anchor="end" fill="#8190a8" font-size="12">${Math.round(max*(1-i/4))}</text>`;}const pts=[];data.forEach((d,i)=>{const x=left+i*step+(step-bw)/2;const h=(d.pre/max)*height;const y=top+height-h;const py=top+height-(d.fac/max)*height;out+=`<rect x="${x}" y="${y}" width="${bw}" height="${Math.max(h,1)}" rx="7" fill="#fbd8a8"></rect><text x="${x+bw/2}" y="${top+height+18}" text-anchor="middle" fill="#6f7f98" font-size="12">${d.label}</text>`;pts.push(`${x+bw/2},${py}`);});out+=`<polyline points="${pts.join(' ')}" fill="none" stroke="#ef8d00" stroke-width="4"/>`;root.innerHTML=out;}
function render(){const a=state.selected;document.getElementById('financeBtn').href=state.selectedId?`/finance?affaire_id=${encodeURIComponent(state.selectedId)}`:'/finance';document.getElementById('pmBtn').href=state.selectedId?`/gestion-projet?affaire_id=${encodeURIComponent(state.selectedId)}`:'/gestion-projet';document.getElementById('exportBtn').disabled=!state.selectedId;document.getElementById('title').textContent=a?(a.display_name||'-'):'Sélectionnez une affaire';document.getElementById('subtitle').textContent=a?`Client ${a.client||'-'} · ${a.affaire||'-'}`:'Synthèse principale : client, commande, antériorité, facturé 2026, facturation totale et reste à facturer.';document.getElementById('kCommande').textContent=euro(a?a.commande_ht:0);document.getElementById('kFacture').textContent=euro(a?(a.facturation_totale||((a.anteriorite||0)+(a.facture_2026||a.facturation_cumulee_2026||0))):0);document.getElementById('kReste').textContent=euro(a?a.reste_a_facturer:0);renderChart();}
async function init(){await loadAffairesList('');const params=new URLSearchParams(window.location.search);const pre=params.get('affaire_id')||localStorage.getItem('selectedAffaireId')||'';if(pre&&state.affaires.some(x=>x.affaire_id===pre)){document.getElementById('affaireSelect').value=pre;await loadSelectedAffaire(pre);}else{render();}document.getElementById('searchInput').addEventListener('input',async e=>{await loadAffairesList(e.target.value||'');});document.getElementById('affaireSelect').addEventListener('change',async e=>loadSelectedAffaire(e.target.value||''));document.getElementById('exportBtn').addEventListener('click',()=>{if(state.selectedId)window.location.href=`/api/finance/affaire/${encodeURIComponent(state.selectedId)}/export-csv`;});}
init();
</script>
</body>
</html>"""



@app.get("/assets/logo-tempo")
def tempo_logo():
    if os.path.exists(TEMPO_LOGO_PATH):
        return FileResponse(TEMPO_LOGO_PATH)
    raise HTTPException(status_code=404, detail="Logo Tempo introuvable")


@app.get("/", response_class=HTMLResponse)
def landing_page():
    return landing_html()


@app.get("/finance", response_class=HTMLResponse)
def finance_page():
    return finance_html()


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_page():
    return dashboard_html()


@app.get("/gestion-projet", response_class=HTMLResponse)
def gestion_projet_page():
    return gestion_projet_html()


@app.get("/health", response_class=JSONResponse)
def health():
    return {
        "status": "ok",
        "service": "finance",
        "workbook_exists": service.workbook_exists(),
        "workbook_path": service.workbook_path,
        "sheet_name": service.sheet_name,
    }


@app.get("/api/finance", response_class=JSONResponse)
def finance_index():
    return {
        "service": "finance",
        "status": "ok",
        "endpoints": {
            "landing": "/",
            "finance_ui": "/finance",
            "dashboard_ui": "/dashboard",
            "project_management_ui": "/gestion-projet",
            "cache_status": "/api/finance/cache-status",
            "rebuild_cache": "/api/finance/rebuild-cache",
            "affaires": "/api/finance/affaires?search=...",
            "affaire": "/api/finance/affaire/{affaire_id}",
            "affaire_export_csv": "/api/finance/affaire/{affaire_id}/export-csv",
            "health": "/health",
        },
    }


@app.get("/api/finance/cache-status", response_class=JSONResponse)
def api_cache_status():
    return service.cache_status()


@app.post("/api/finance/rebuild-cache", response_class=JSONResponse)
@app.get("/api/finance/rebuild-cache", response_class=JSONResponse)
def api_rebuild_cache():
    cache = service.rebuild_finance_cache()
    return {
        "status": "rebuilt",
        "generated_at": cache.get("generated_at"),
        "rows_read": cache.get("rows_read"),
        "rows_kept": cache.get("rows_kept"),
        "affaires_count": cache.get("affaires_count"),
        "warnings": cache.get("warnings", []),
    }


@app.get("/api/finance/affaires", response_class=JSONResponse)
def api_affaires(search: str = Query(default="")):
    cache = service.get_finance_cache()
    items = FinanceService.lightweight_affaires(cache, search=search)
    return {"ok": True, "count": len(items), "items": items}


@app.get("/api/finance/affaire/{affaire_id}", response_class=JSONResponse)
def api_affaire_detail(affaire_id: str):
    cache = service.get_finance_cache()
    item = cache.get("items", {}).get(affaire_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"Affaire introuvable : {affaire_id}")
    return {"ok": True, "affaire": item}


@app.get("/api/project-management/board", response_class=JSONResponse)
def api_project_management_board(affaire_id: str = Query(default=""), affaire_name: str = Query(default=""), start_date: str = Query(default=""), end_date: str = Query(default="")):
    name = clean_text(affaire_name)
    if affaire_id and not name:
        cache = service.get_finance_cache()
        item = cache.get("items", {}).get(affaire_id)
        if not item:
            raise HTTPException(status_code=404, detail=f"Affaire introuvable : {affaire_id}")
        name = clean_text(item.get("display_name")) or clean_text(item.get("affaire"))
    if not name:
        raise HTTPException(status_code=400, detail="affaire_id ou affaire_name requis")
    return metronome_service.build_project_board(name, start_date=start_date, end_date=end_date)


@app.get("/api/finance/affaire/{affaire_id}/export-csv")
def api_affaire_export_csv(affaire_id: str):
    cache = service.get_finance_cache()
    item = cache.get("items", {}).get(affaire_id)
    if not item:
        raise HTTPException(status_code=404, detail=f"Affaire introuvable : {affaire_id}")

    out = io.StringIO()
    writer = csv.writer(out, delimiter=";")
    writer.writerow(["Affaire", item.get("display_name", "")])
    writer.writerow(["Client", item.get("client", "")])
    writer.writerow(["Mission", item.get("affaire", "")])
    writer.writerow([])
    writer.writerow(["Mois", "Prévisionnel", "Facturé", "Écart"])
    for month in MONTHS:
        row = item.get("mensuel", {}).get(month, {})
        writer.writerow([MONTH_LABELS[month], row.get("previsionnel", 0), row.get("facture", 0), row.get("ecart", 0)])
    writer.writerow([])
    writer.writerow(["Tag", "Mission", "Numero", "💰 Commande HT", "🧾 Facturation totale", "⚠ Reste à facturer", "Total prévisionnel", "Total facturé"])
    for mission in item.get("missions", []):
        writer.writerow([
            mission.get("tag", ""),
            mission.get("label", ""),
            mission.get("numero", ""),
            mission.get("commande_ht", 0),
            mission.get("facturation_totale", (clean_number(mission.get("anteriorite")) + clean_number(mission.get("facture_2026", mission.get("facturation_cumulee_2026", 0))))),
            mission.get("reste_a_facturer", 0),
            mission.get("total_previsionnel", 0),
            mission.get("total_facture", 0),
        ])
    out.seek(0)
    return StreamingResponse(
        iter([out.getvalue().encode("utf-8-sig")]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=finance-{affaire_id}.csv"},
    )
