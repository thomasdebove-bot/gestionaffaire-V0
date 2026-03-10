
import csv
import io
import json
import logging
import os
import re
import unicodedata
from datetime import date, datetime
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
}

METRONOME_COLUMN_ALIASES = {
    "project_title_entries": ["Project/Title", "Project/Title (dev)", "Project"],
    "project_title_projects": ["Title", "Name"],
    "project_name_projects": ["Name", "Title"],
    "project_start": ["Start Date", "StartDate", "Start"],
    "project_end": ["End Date", "EndDate", "End"],
    "entry_done_date": ["Done Date", "DoneDate", "Completed Date", "CompletedDate", "ClosedDate", "UpdatedAt"],
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
    def _idx(rows: List[Dict[str, str]], key: str = "ID") -> Dict[str, Dict[str, str]]:
        out: Dict[str, Dict[str, str]] = {}
        for r in rows:
            k = clean_text(r.get(key))
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
        }
        if not target_slug:
            return resolved

        best_project: Optional[Dict[str, str]] = None
        best_name = ""
        best_score = 0
        best_len = 10**9

        for project in projects:
            title = self._get_first_value(project, METRONOME_COLUMN_ALIASES["project_title_projects"])
            name = self._get_first_value(project, METRONOME_COLUMN_ALIASES["project_name_projects"])
            for candidate_name in [title, name]:
                if not candidate_name:
                    continue
                score = self._score_project_match(target_slug, target_tokens, candidate_name)
                if score <= 0:
                    continue
                slug_len_delta = abs(len(slugify(candidate_name)) - len(target_slug))
                if score > best_score or (score == best_score and slug_len_delta < best_len):
                    best_project = project
                    best_name = candidate_name
                    best_score = score
                    best_len = slug_len_delta

        min_reliable_score = 55
        if best_project and best_score >= min_reliable_score:
            resolved_title = self._get_first_value(best_project, METRONOME_COLUMN_ALIASES["project_title_projects"])
            resolved_id = self._get_first_value(best_project, ["ID"])
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

        best_entry_title = ""
        best_entry_slug = ""
        best_entry_score = 0
        for e in entries:
            entry_title = self._get_first_value(e, METRONOME_COLUMN_ALIASES["project_title_entries"])
            if not entry_title:
                continue
            score = self._score_project_match(target_slug, target_tokens, entry_title)
            if score > best_entry_score:
                best_entry_title = entry_title
                best_entry_slug = slugify(entry_title)
                best_entry_score = score

        if best_entry_title and best_entry_score >= min_reliable_score:
            resolved.update({
                "resolved_project_title": best_entry_title,
                "matched_project_name": best_entry_title,
                "matched_project_slug": best_entry_slug,
                "match_score": best_entry_score,
                "resolution_mode": "fallback",
            })
        return resolved

    def build_project_board(self, project_name: str) -> Dict[str, Any]:
        cache = self._ensure_loaded()
        t = cache.get("tables", {})
        projects = t.get("projects", [])
        entries = t.get("entries", [])
        meetings = self._idx(t.get("meetings", []))
        areas = self._idx(t.get("areas", []))
        packages = self._idx(t.get("packages", []))
        companies = self._idx(t.get("companies", []))
        users = self._idx(t.get("users", []))

        comments_by_entry: Dict[str, List[str]] = {}
        for c in t.get("comments", []):
            eid = clean_text(c.get("Entry"))
            txt = clean_text(c.get("Comment") or c.get("Entry") or c.get("Text"))
            if eid and txt:
                comments_by_entry.setdefault(eid, []).append(txt)

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
                        resolved_id = clean_text(p.get("ID"))
                        match_debug["resolved_project_id"] = resolved_id
                    break
        if not project_info and resolved_id:
            for p in projects:
                if clean_text(p.get("ID")) == resolved_id:
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
                "match_debug": match_debug,
                "missing_files": cache.get("missing", []),
                "loaded_at": cache.get("loaded_at"),
            }

        rows = []
        today = datetime.now().date()
        rows_filtered_by_title = 0
        rows_filtered_by_id = 0

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
            entry_project_id = clean_text(e.get("Project"))

            use_row = False
            if resolved_title and entry_project_title == resolved_title:
                rows_filtered_by_title += 1
                use_row = True
            elif not resolved_title and resolved_id and entry_project_id == resolved_id:
                rows_filtered_by_id += 1
                use_row = True
            elif resolved_id and entry_project_id == resolved_id and rows_filtered_by_title == 0:
                rows_filtered_by_id += 1
                use_row = True

            if not use_row:
                continue

            status = clean_text(e.get("Status")).lower()
            is_closed = status in {"closed", "close", "done", "termine", "terminé"}

            entry_id = clean_text(e.get("ID"))
            area = areas.get(clean_text(e.get("Area")), {})
            pkg = packages.get(clean_text(e.get("Package")), {})
            company = companies.get(clean_text(e.get("Company")), {})
            user = users.get(clean_text(e.get("Assignee")), {})
            meeting = meetings.get(clean_text(e.get("Meeting")), {})
            due = clean_text(e.get("DueDate"))
            meeting_date = clean_text(meeting.get("Date"))
            done_date = parse_date(self._get_first_value(e, METRONOME_COLUMN_ALIASES["entry_done_date"]))
            request_date = parse_date(due) or parse_date(meeting_date)

            selected_entries.append({
                "company": clean_text(company.get("Name")) or "Non défini",
                "request_date": request_date,
                "done_date": done_date,
                "is_closed": is_closed,
            })

            if is_closed:
                continue

            overdue = False
            if due:
                due_date = parse_date(due)
                overdue = bool(due_date and due_date < today)

            rows.append({
                "zone": clean_text(area.get("Name")),
                "lot": clean_text(pkg.get("Name")),
                "sujet": clean_text(e.get("Title")),
                "entreprise": clean_text(company.get("Name")),
                "responsable": clean_text(user.get("Name")),
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
                k = clean_text(r.get(field)) or "Non défini"
                agg[k] = agg.get(k, 0) + 1
            return [{"label": k, "count": v} for k, v in sorted(agg.items(), key=lambda x: (-x[1], x[0]))]

        by_meeting = count_by("reunion_origine")
        by_company = count_by("entreprise")
        due_rows = [r for r in rows if (parse_date(r.get("date_echeance", "")) or today) <= today]
        due_by_company: Dict[str, int] = {}
        for r in due_rows:
            label = clean_text(r.get("entreprise")) or "Non défini"
            due_by_company[label] = due_by_company.get(label, 0) + 1
        due_by_company_list = [
            {"label": k, "count": v} for k, v in sorted(due_by_company.items(), key=lambda x: (-x[1], x[0]))
        ]

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

        project_display_name = resolved_title or target
        project_id = resolved_id or clean_text(project_info.get("ID"))
        return {
            "ok": True,
            "project_name": project_display_name,
            "project_id": project_id,
            "match_debug": match_debug,
            "kpis": {
                "open_topics": len(rows),
                "overdue_topics": sum(1 for r in rows if r.get("overdue")),
                "by_company": by_company,
                "by_package": count_by("lot"),
                "by_meeting": by_meeting,
            },
            "kpis_pilotage": {
                "rappels_ouverts_a_date": len(due_rows),
                "a_suivre_ouverts": len(rows),
                "date_reference": today.isoformat(),
                "rappels_cumules_par_entreprise": due_by_company_list,
                "open_tasks_by_company": open_tasks_by_company,
                "average_processing_days_by_company": average_processing_days_by_company,
                "reminder_threshold_weeks": round(reminder_threshold_days / 7),
                "timeline_progress": {
                    "start_date": project_start.isoformat() if project_start else "",
                    "end_date": project_end.isoformat() if project_end else "",
                    "elapsed_days": elapsed_days,
                    "total_days": total_days,
                    "progress_percent": progress_percent,
                },
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
.loading-wrap{margin-top:8px;padding:8px 10px;border:1px solid #d8e0ee;border-radius:12px;background:#fff}.loading-label{font-size:12px;color:#5b6880;margin-bottom:6px;font-weight:700}.loading-track{height:8px;border-radius:999px;background:#eef2f8;overflow:hidden}.loading-bar{height:100%;width:35%;background:linear-gradient(90deg,#ef8d00,#ffd08a);animation:loadmove 1.2s infinite ease-in-out}@keyframes loadmove{0%{margin-left:-35%}100%{margin-left:100%}}.match-box{margin-top:10px;border:1px solid var(--line);border-radius:10px;padding:8px;background:#fffaf2}.match-box.ok{border-color:#49a66a;background:#f4fcf6}.match-box.warn{border-color:#ef8d00;background:#fff7ec}.match-title{font-weight:700;margin-bottom:4px;font-size:13px}.match-grid{display:none}.match-item{font-size:12px;color:#30425f}.match-item b{display:block;color:#6e7a90;font-size:11px;margin-bottom:2px}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;word-break:break-word}.pilot-box{margin-top:12px;border:1px solid #cfd8e8;border-radius:18px;padding:16px;background:#f8fbff}.pilot-title{font-size:34px;font-weight:900;margin:4px 0 10px}.pilot-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}.pilot-card{background:#fff;border:1px solid #d8e0ee;border-radius:18px;padding:14px}.pilot-card .t{font-size:15px;color:#4b5d7a;font-weight:800}.pilot-card .v{font-size:44px;font-weight:900;margin-top:8px}.pilot-list{margin-top:12px;border:1px solid #d8e0ee;border-radius:18px;background:#fff;padding:8px 14px}.pilot-row{display:flex;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px dashed #d8e0ee}.pilot-row:last-child{border-bottom:none}.pilot-row .name{font-weight:700}
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
function renderOpenTasksByCompany(b){const p=(b&&b.kpis_pilotage)||{};const items=p.open_tasks_by_company||[];const root=document.getElementById('companyOpenList');if(!items.length){root.innerHTML="<div class='small'>Aucune donnée</div>";return;}const seuil=p.reminder_threshold_weeks||2;root.innerHTML=items.map(x=>`<div class='pilot-row'><span class='name'>${esc(x.label)}</span><span><strong>${x.open_count}</strong> ouvertes · <strong>${x.reminder_count}</strong> rappels (>${seuil} sem.)</span></div>`).join('');}
function renderAvgDelayByCompany(b){const p=(b&&b.kpis_pilotage)||{};const items=p.average_processing_days_by_company||[];const root=document.getElementById('companyDelayList');if(!items.length){root.innerHTML="<div class='small'>Pas assez de sujets clôturés pour calculer ce KPI.</div>";return;}root.innerHTML=items.map(x=>`<div class='pilot-row'><span class='name'>${esc(x.label)}</span><span><strong>${x.avg_days}</strong> jours (sur ${x.closed_count})</span></div>`).join('');}
function renderPilotageKpis(b){const p=(b&&b.kpis_pilotage)||{};setText('kRappelsDate',String(p.rappels_ouverts_a_date||0));setText('kASuivre',String(p.a_suivre_ouverts||0));const t=p.timeline_progress||{};setText('kDateRef',`${t.progress_percent||0}%`);const root=document.getElementById('pilotByCompany');const items=p.rappels_cumules_par_entreprise||[];if(!items.length){root.innerHTML="<div class='small'>Aucune donnée</div>";}else{root.innerHTML=items.map(x=>`<div class='pilot-row'><span class='name'>${esc(x.label)}</span><strong>${x.count}</strong></div>`).join('');}const lbl=(t.start_date&&t.end_date)?`Période projet: ${t.start_date} → ${t.end_date} (${t.elapsed_days||0}/${t.total_days||0} jours)`:'Période projet non disponible';setHtml('pilotByCompany',`<div class='small' style='margin-bottom:8px'>${esc(lbl)}</div>`+root.innerHTML);}
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
    const g=document.querySelector('#matchBox .match-grid');if(g)g.style.display='none';
    box.classList.remove('ok');box.classList.add('warn');
    setText('matchStatus','⚠ Projet METRONOME non trouvé');
    const reason=b?.reason||'project_not_found';
    const loaded=b?.loaded_at?` · Chargement: ${b.loaded_at}`:'';
    setText('matchReason',`Raison: ${reason}${loaded}`);
    return;
  }
  const g=document.querySelector('#matchBox .match-grid');if(g)g.style.display='none';
  box.classList.remove('warn');box.classList.add('ok');
  setText('matchStatus','✅ Matching METRONOME OK');
  const loaded=b.loaded_at?` · Chargement: ${b.loaded_at}`:'';
  setText('matchReason',`Projet recherché et projet matché résolus.${loaded}`);
}
function renderBoard(){const b=state.board;if(!b||!b.ok){setText('kOpen','0');setText('kLate','0');setText('kProject','Projet METRONOME non trouvé');setText('kLoad',b&&b.loaded_at?b.loaded_at:'-');renderPilotageKpis({});renderOpenTasksByCompany({});renderAvgDelayByCompany({});renderMatchDiagnostics(b||{});return;}const k=b.kpis||{};setText('kOpen',String(k.open_topics||0));setText('kLate',String(k.overdue_topics||0));setText('kProject',b.project_name||'-');setText('kLoad',b.loaded_at||'-');renderPilotageKpis(b);renderOpenTasksByCompany(b);renderAvgDelayByCompany(b);renderMatchDiagnostics(b);} 
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
def api_project_management_board(affaire_id: str = Query(default=""), affaire_name: str = Query(default="")):
    name = clean_text(affaire_name)
    if affaire_id and not name:
        cache = service.get_finance_cache()
        item = cache.get("items", {}).get(affaire_id)
        if not item:
            raise HTTPException(status_code=404, detail=f"Affaire introuvable : {affaire_id}")
        name = clean_text(item.get("display_name")) or clean_text(item.get("affaire"))
    if not name:
        raise HTTPException(status_code=400, detail="affaire_id ou affaire_name requis")
    return metronome_service.build_project_board(name)


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
