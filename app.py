
import csv
import io
import json
import logging
import os
import re
import unicodedata
from datetime import datetime
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
TEMPO_LOGO_PATH = os.getenv("TEMPO_LOGO_PATH", r"\\192.168.10.100\02 - affaires\02.2 - SYNTHESE\ZZ - METRONOME\Content\T logo.png")

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


def cumulative_facturation_from_row(row: Dict[str, Any]) -> float:
    return sum(
        clean_number(value)
        for key, value in row.items()
        if isinstance(key, str) and key.startswith("facturation_cumulee_")
    )


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
        if cache.get("status") == "ready" and cache.get("items"):
            return cache

        if self.cache_file.exists():
            try:
                disk = json.loads(self.cache_file.read_text(encoding="utf-8"))
                if disk.get("source_mtime") == self.source_mtime() and disk.get("items"):
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
            "schema_version": "finance_affaires_dataset_v3",
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
            pre_raw = clean_number(row.get(f"{month}_previsionnel"))
            fac = clean_number(row.get(f"{month}_facture"))
            pre = pre_raw if abs(fac) < 1e-9 else 0.0
            mensuel[month]["previsionnel"] = pre
            mensuel[month]["facture"] = fac
            mensuel[month]["ecart"] = fac - pre

        total_previsionnel = sum(mensuel[m]["previsionnel"] for m in MONTHS)
        total_facture = clean_number(row.get("total_facture")) or sum(mensuel[m]["facture"] for m in MONTHS)
        commande_ht = clean_number(row.get("commande_ht"))
        fact_2026 = clean_number(row.get("facturation_cumulee_2026"))
        total_facturation_cumulee = cumulative_facturation_from_row(row)
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
            "total_facturation_cumulee": total_facturation_cumulee,
            "reste_a_facturer": reste,
            "has_reste_value": row.get("reste_a_facturer") not in (None, ""),
            "mensuel": mensuel,
            "total_previsionnel": total_previsionnel,
            "total_facture": total_facture,
            "ecart_previsionnel_vs_facture": total_facture - total_previsionnel,
            "taux_avancement_financier": (total_facturation_cumulee / commande_ht) if commande_ht else 0.0,
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
            "total_facturation_cumulee": data.get("total_facturation_cumulee", data.get("facturation_cumulee_2026", 0.0)),
            "reste_a_facturer": data.get("reste_a_facturer", 0.0),
            "has_reste_value": data.get("has_reste_value", False),
            "total_previsionnel": data.get("total_previsionnel", 0.0),
            "total_facture": data.get("total_facture", 0.0),
            "mensuel": data.get("mensuel", month_payload()),
        }

    def _finalize_affaire(self, affaire: Dict[str, Any]) -> Dict[str, Any]:
        missions = affaire.get("missions") or []
        if missions:
            monthly = month_payload()
            tags = []
            numero = affaire.get("numero", "")
            commande = 0.0
            facture_2026 = 0.0
            facture_cumulee = 0.0
            delai_values = []
            for mission in missions:
                commande += clean_number(mission.get("commande_ht"))
                facture_2026 += clean_number(mission.get("facturation_cumulee_2026"))
                facture_cumulee += cumulative_facturation_from_row(mission)
                if clean_number(mission.get("delai_reglement_jours")) > 0:
                    delai_values.append(safe_int(mission.get("delai_reglement_jours")))
                if mission.get("tag"):
                    tags.append(clean_text(mission["tag"]))
                if not numero and mission.get("numero"):
                    numero = clean_text(mission["numero"])
                mm = mission.get("mensuel") or {}
                for month in MONTHS:
                    monthly[month]["previsionnel"] += clean_number(((mm.get(month) or {}).get("previsionnel")))
                    monthly[month]["facture"] += clean_number(((mm.get(month) or {}).get("facture")))
            for month in MONTHS:
                monthly[month]["ecart"] = monthly[month]["facture"] - monthly[month]["previsionnel"]

            parent_commande = clean_number(affaire.get("commande_ht"))
            parent_facture_2026 = clean_number(affaire.get("facturation_cumulee_2026"))
            parent_facture_cumulee = cumulative_facturation_from_row(affaire)
            affaire["commande_ht"] = parent_commande if abs(parent_commande) > 1e-9 else commande
            affaire["facturation_cumulee_2026"] = parent_facture_2026 if abs(parent_facture_2026) > 1e-9 else facture_2026
            affaire["total_facturation_cumulee"] = parent_facture_cumulee if abs(parent_facture_cumulee) > 1e-9 else facture_cumulee
            has_parent_reste = bool(affaire.get("has_reste_value", False))
            reste_parent = clean_number(affaire.get("reste_a_facturer"))
            reste_missions = sum(clean_number(m.get("reste_a_facturer")) for m in missions)
            affaire["reste_a_facturer"] = reste_parent if has_parent_reste else reste_missions
            affaire["mensuel"] = monthly
            affaire["tags"] = sorted(set([t for t in tags if t]))
            affaire["numero"] = numero
            affaire["total_previsionnel"] = sum(monthly[m]["previsionnel"] for m in MONTHS)
            affaire["total_facture"] = sum(monthly[m]["facture"] for m in MONTHS)
            affaire["ecart_previsionnel_vs_facture"] = affaire["total_facture"] - affaire["total_previsionnel"]
            affaire["delai_reglement_jours"] = affaire.get("delai_reglement_jours") or (max(delai_values) if delai_values else 0)
            affaire["taux_avancement_financier"] = (affaire["total_facturation_cumulee"] / affaire["commande_ht"]) if affaire["commande_ht"] else 0.0
        else:
            affaire["missions"] = []
            if affaire.get("tag") and not affaire.get("tags"):
                affaire["tags"] = [affaire["tag"]]

        affaire["total_facturation_cumulee"] = clean_number(affaire.get("total_facturation_cumulee")) or cumulative_facturation_from_row(affaire)
        affaire["insights"] = self.compute_insights(affaire)
        affaire.pop("has_reste_value", None)
        affaire.pop("tag", None)
        affaire.pop("_row_index", None)
        return affaire

    @staticmethod
    def compute_insights(affaire: Dict[str, Any]) -> List[str]:
        insights: List[str] = []
        commande = clean_number(affaire.get("commande_ht"))
        facture = clean_number(affaire.get("total_facturation_cumulee"))
        reste = clean_number(affaire.get("reste_a_facturer"))
        prev = clean_number(affaire.get("total_previsionnel"))
        fact_total = clean_number(affaire.get("total_facture"))
        taux = float(affaire.get("taux_avancement_financier") or 0.0)
        active_months = [m for m in MONTHS if clean_number((affaire.get("mensuel", {}).get(m) or {}).get("previsionnel")) > 0]

        if commande > 0 and facture <= 0:
            insights.append("Aucune facturation 2026 détectée malgré une commande active.")
        if prev > 0 and fact_total < prev * 0.8:
            insights.append("Le facturé est en retard sur le prévisionnel annuel.")
        if prev > 0 and fact_total > prev * 1.1:
            insights.append("Le facturé dépasse sensiblement le prévisionnel.")
        if commande > 0 and reste / commande > 0.5:
            insights.append("Le reste à facturer reste élevé.")
        if commande > 0 and taux >= 0.9:
            insights.append("Affaire presque soldée financièrement.")
        if prev > 0 and len(active_months) <= 2:
            insights.append("Le prévisionnel est concentré sur très peu de mois.")
        if not insights:
            insights.append("Situation globalement stable selon les données disponibles.")
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


service = FinanceService(WORKBOOK_PATH, SHEET_NAME, CACHE_FILE)


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
.grid{margin-top:18px;display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}.card{padding:20px}.card h3{margin:0;font-size:21px}.card p{margin:10px 0 18px;color:var(--muted);font-size:14px;min-height:60px}
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
      <article class='card'><h3>Gestion de projet</h3><p>Bientôt disponible.</p><button class='btn disabled' disabled>Bientôt disponible</button></article>
      <article class='card'><h3>Imputation</h3><p>Bientôt disponible.</p><button class='btn disabled' disabled>Bientôt disponible</button></article>
    </section>
  </div>
<script>
const state={projects:[],selectedId:'',selectedLabel:''};
function esc(v){return String(v||'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));}
function setModuleLink(id,base){const el=document.getElementById(id);if(state.selectedId){el.className='btn primary';el.href=`/${base}?affaire_id=${encodeURIComponent(state.selectedId)}`;el.removeAttribute('aria-disabled');}else{el.className='btn disabled';el.href='javascript:void(0)';el.setAttribute('aria-disabled','true');}}
function updateUi(){document.getElementById('projectBadge').textContent=state.selectedLabel?`Affaire : ${state.selectedLabel}`:'Aucune affaire sélectionnée';document.getElementById('state').textContent=state.selectedLabel?`Vous naviguez sur l'affaire ${state.selectedLabel}.`:'Sélectionnez une affaire pour activer les modules.';setModuleLink('financeLink','finance');setModuleLink('dashboardLink','dashboard');if(state.selectedId){localStorage.setItem('selectedAffaireId',state.selectedId);} }
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
.kpi.good{background:linear-gradient(180deg,#fff,#edf8f2)}.kpi.warn{background:linear-gradient(180deg,#fff,#fff8eb)}.kpi.bad{background:linear-gradient(180deg,#fff,#fff1f1)}
.layout{display:grid;grid-template-columns:2fr 1fr;gap:18px;margin-top:18px}.section{padding:18px 18px 22px}.section h3{margin:0 0 14px;font-size:24px}
.chart-card{height:420px}.chart-wrap{height:340px;border-radius:18px;background:linear-gradient(180deg,#f7f9fc 0%,#f3f6fb 100%);border:1px solid var(--line);padding:14px}
.legend{display:flex;gap:18px;align-items:center;font-size:13px;color:var(--muted);font-weight:700;margin-top:10px}.legend span{display:inline-flex;align-items:center;gap:8px}.swatch{display:inline-block;width:14px;height:14px;border-radius:4px}
.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:18px}table{width:100%;border-collapse:collapse}th,td{padding:14px;border-bottom:1px solid var(--line);font-size:14px;text-align:left}th{background:#f7f9fc;color:#536079;font-size:12px;text-transform:uppercase;letter-spacing:.08em}tr:last-child td{border-bottom:none}td.num{text-align:right;font-variant-numeric:tabular-nums}
.delta.pos{color:var(--green);font-weight:800}.delta.neg{color:var(--red);font-weight:800}.insights{display:flex;flex-wrap:wrap;gap:10px}.insight{padding:12px 14px;border-radius:16px;font-size:14px;font-weight:700;border:1px solid var(--line);background:var(--panel2)}
.notice{padding:14px 16px;border-radius:16px;background:#fff7e7;color:#8c6211;border:1px solid #f0dcab}.error{padding:14px 16px;border-radius:16px;background:#fff0f0;color:#992f2f;border:1px solid #f1c6c6}.empty{padding:28px;border:1px dashed var(--line);border-radius:18px;color:var(--muted);text-align:center;background:#fafbfd}.small{font-size:13px;color:var(--muted)}.footer-row{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-top:12px}
@media (max-width:1200px){.kpi-grid{grid-template-columns:repeat(3,1fr)}.layout{grid-template-columns:1fr}.meta-grid{grid-template-columns:repeat(2,1fr)}}@media (max-width:720px){.topbar{position:static}.kpi-grid{grid-template-columns:1fr}.meta-grid{grid-template-columns:1fr}.hero h2{font-size:30px}.select,.search{min-width:100%}}
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
      <div class='meta-pill'><div class='label'>Client</div><div class='value' id='metaClient'>-</div></div>
      <div class='meta-pill'><div class='label'>Mission</div><div class='value' id='metaAffaire'>-</div></div>
      <div class='meta-pill'><div class='label'>Tags</div><div class='value' id='metaTags'>-</div></div>
      <div class='meta-pill'><div class='label'>Cache généré</div><div class='value' id='metaCache'>-</div></div>
    </div>
  </div>

  <div class='kpis'><div class='kpi-grid'>
    <div class='kpi' id='kpiCommandeCard'><div class='label'>Commande HT</div><div class='value' id='kpiCommande'>0 €</div><div class='sub'>Montant contractualisé</div></div>
    <div class='kpi' id='kpiFactureCard'><div class='label'>Facturation cumulée 2017-2026</div><div class='value' id='kpiFacture'>0 €</div><div class='sub'>Antériorité incluse</div></div>
    <div class='kpi' id='kpiResteCard'><div class='label'>Reste à facturer</div><div class='value' id='kpiReste'>0 €</div><div class='sub'>Solde estimé</div></div>
    <div class='kpi' id='kpiAvanceCard'><div class='label'>Avancement financier</div><div class='value' id='kpiAvance'>0 %</div><div class='sub'>Facturé / commande</div></div>
  </div></div>

  <div class='layout'>
    <div class='section chart-card'>
      <h3>Facturation mensuelle</h3>
      <div class='chart-wrap'><svg id='monthlyChart' width='100%' height='100%' viewBox='0 0 980 320' preserveAspectRatio='none'></svg></div>
      <div class='legend'><span><i class='swatch' style='background:#dbe6ff'></i>Prévisionnel</span><span><i class='swatch' style='background:#ef8d00'></i>Facturation</span></div>
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
function esc(v){return String(v??'').replace(/[&<>"]/g,s=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[s]));}
function showError(msg){const b=document.getElementById('errorBox');b.textContent=msg||'Erreur';b.style.display='block';}
function clearError(){document.getElementById('errorBox').style.display='none';}
function showNotice(msg){const b=document.getElementById('noticeBox');b.textContent=msg||'';b.style.display=msg?'block':'none';}
function setCacheBadge(status,label){const c=status==='ready'?'ready':status==='building'?'building':status==='error'?'error':'idle';document.getElementById('cacheBadge').innerHTML=`<span class="dot ${c}"></span><span>${esc(label)}</span>`;}
async function api(url,options){const r=await fetch(url,options||{});const data=await r.json().catch(()=>({}));if(!r.ok) throw new Error(data.error||data.detail||data.message||`HTTP ${r.status}`);return data;}
function healthClass(a){const reste=Number(a.reste_a_facturer||0),taux=Number(a.taux_avancement_financier||0);if(taux>=0.9)return{label:'Presque soldée',cls:'ok'};if(reste>0&&taux<0.35)return{label:'À surveiller',cls:'warn'};if(reste<0)return{label:'Incohérence à vérifier',cls:'bad'};return{label:'Stable',cls:'ok'};}
async function loadCacheStatus(){const d=await api('/api/finance/cache-status');state.cacheStatus=d;const label=d.status==='ready'?`Cache prêt · ${d.affaires_count} affaires`:d.status==='building'?'Cache en reconstruction…':`Cache : ${d.status}`;setCacheBadge(d.status,label);document.getElementById('metaCache').textContent=d.generated_at||'-';document.getElementById('statusMeta').textContent=`${d.affaires_count||0} affaires · ${d.rows_kept||0} lignes utiles · ${d.generated_at||'pas encore généré'}`;}
async function loadAffairesList(search=''){const d=await api(`/api/finance/affaires?search=${encodeURIComponent(search)}`);state.affaires=d.items||[];const sel=document.getElementById('affaireSelect');const prev=state.selectedAffaireId;sel.innerHTML=`<option value=''>Sélectionnez une affaire</option>`+state.affaires.map(x=>`<option value="${esc(x.affaire_id)}">${esc(x.display_name)}</option>`).join('');if(prev&&state.affaires.some(x=>x.affaire_id===prev)){sel.value=prev;}else{state.selectedAffaireId='';state.selectedAffaire=null;}showNotice(state.affaires.length?`${state.affaires.length} affaire(s) disponible(s)`:'Aucune affaire trouvée pour ce filtre.');}
async function loadSelectedAffaire(id){if(!id){state.selectedAffaireId='';state.selectedAffaire=null;renderAll();return;}const d=await api(`/api/finance/affaire/${encodeURIComponent(id)}`);state.selectedAffaireId=id;state.selectedAffaire=d.affaire||null;renderAll();localStorage.setItem('selectedAffaireId',id);}
function setHeroEmpty(){document.getElementById('heroTitle').textContent='Sélectionnez une affaire';document.getElementById('heroSubtitle').textContent='Le cockpit se remplit à partir du cache du tableau activité.';document.getElementById('metaClient').textContent='-';document.getElementById('metaAffaire').textContent='-';document.getElementById('metaTags').textContent='-';const h=document.getElementById('heroHealth');h.textContent='En attente';h.className='health warn';}
function cardTone(id,tone){const el=document.getElementById(id);el.classList.remove('good','warn','bad');if(tone)el.classList.add(tone);}
function renderHero(){const a=state.selectedAffaire;if(!a){setHeroEmpty();return;}document.getElementById('heroTitle').textContent=a.display_name||'-';document.getElementById('heroSubtitle').textContent=`Client ${a.client||'-'} · ${(a.missions||[]).length} mission(s)`;document.getElementById('metaClient').textContent=a.client||'-';document.getElementById('metaAffaire').textContent=a.affaire||'-';document.getElementById('metaTags').textContent=(a.tags||[]).join(' · ')||'-';document.getElementById('metaCache').textContent=(state.cacheStatus&&state.cacheStatus.generated_at)||'-';const hh=healthClass(a),el=document.getElementById('heroHealth');el.textContent=hh.label;el.className=`health ${hh.cls}`;}
function renderKpis(){const a=state.selectedAffaire||{commande_ht:0,total_facturation_cumulee:0,reste_a_facturer:0,taux_avancement_financier:0};document.getElementById('kpiCommande').textContent=euro(a.commande_ht);document.getElementById('kpiFacture').textContent=euro(a.total_facturation_cumulee||a.facturation_cumulee_2026||0);document.getElementById('kpiReste').textContent=euro(a.reste_a_facturer);document.getElementById('kpiAvance').textContent=pct(a.taux_avancement_financier);cardTone('kpiCommandeCard','good');cardTone('kpiFactureCard',(a.total_facturation_cumulee||0)>0?'good':'warn');cardTone('kpiResteCard',a.reste_a_facturer<0?'bad':(a.reste_a_facturer>(a.commande_ht||0)*0.5?'warn':'good'));cardTone('kpiAvanceCard',a.taux_avancement_financier>0.85?'good':(a.taux_avancement_financier<0.35?'warn':''));}
function renderFinanceChart(){const root=document.getElementById('monthlyChart');const a=state.selectedAffaire;if(!a){root.innerHTML=`<text x="490" y="160" text-anchor="middle" fill="#6e7a90" font-size="18">Sélectionnez une affaire</text>`;return;}const s=MONTHS.map(m=>({label:MONTH_LABELS[m],pre:Number((((a.mensuel||{})[m]||{}).previsionnel)||0),fac:Number((((a.mensuel||{})[m]||{}).facture)||0)}));const maxVal=Math.max(1,...s.flatMap(x=>[x.pre,x.fac]));const left=56,top=16,width=880,height=250,step=width/s.length,barW=step*0.48;let grid='',bars='',labels='';const points=[];for(let i=0;i<=4;i++){const y=top+(height/4)*i,val=Math.round(maxVal*(1-i/4));grid+=`<line x1="${left}" y1="${y}" x2="${left+width}" y2="${y}" stroke="#dfe5ef" stroke-width="1"/><text x="${left-10}" y="${y+4}" text-anchor="end" fill="#8090a8" font-size="12">${fmt(val)}</text>`;}s.forEach((it,i)=>{const x=left+i*step+(step-barW)/2;const h=(it.pre/maxVal)*height;const y=top+height-h;const py=top+height-(it.fac/maxVal)*height;bars+=`<rect x="${x}" y="${y}" width="${barW}" height="${Math.max(h,0.5)}" rx="8" fill="#dbe6ff"><title>${it.label} prévisionnel: ${euro(it.pre)}</title></rect>`;points.push(`${x+barW/2},${py}`);labels+=`<text x="${x+barW/2}" y="${top+height+22}" text-anchor="middle" fill="#66748b" font-size="12">${it.label}</text>`;});root.innerHTML=`${grid}<line x1="${left}" y1="${top+height}" x2="${left+width}" y2="${top+height}" stroke="#b8c3d4" stroke-width="1.2"/>${bars}<polyline points="${points.join(' ')}" fill="none" stroke="#ef8d00" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>${points.map((p,i)=>{const q=p.split(',');return `<circle cx="${q[0]}" cy="${q[1]}" r="4.5" fill="#fff" stroke="#ef8d00" stroke-width="3"><title>${s[i].label} facturé: ${euro(s[i].fac)}</title></circle>`;}).join('')}${labels}`;}
function renderMonthlyTable(){const root=document.getElementById('monthlyTableWrap');const a=state.selectedAffaire;if(!a){root.innerHTML=`<div class='empty'>Sélectionnez une affaire pour afficher le détail mensuel.</div>`;return;}let rows='';MONTHS.forEach(m=>{const pre=Number((((a.mensuel||{})[m]||{}).previsionnel)||0),fac=Number((((a.mensuel||{})[m]||{}).facture)||0),ec=fac-pre;rows+=`<tr><td>${MONTH_LABELS[m]}</td><td class='num'>${euro(pre)}</td><td class='num'>${euro(fac)}</td><td class='num delta ${ec>=0?'pos':'neg'}'>${euro(ec)}</td></tr>`;});rows+=`<tr><td><strong>Total</strong></td><td class='num'><strong>${euro(a.total_previsionnel||0)}</strong></td><td class='num'><strong>${euro(a.total_facture||0)}</strong></td><td class='num delta ${Number(a.ecart_previsionnel_vs_facture||0)>=0?'pos':'neg'}'><strong>${euro(a.ecart_previsionnel_vs_facture||0)}</strong></td></tr>`;root.innerHTML=`<table><thead><tr><th>Mois</th><th class='num'>Prévisionnel</th><th class='num'>Facturé</th><th class='num'>Écart</th></tr></thead><tbody>${rows}</tbody></table>`;}
function renderMissions(){const root=document.getElementById('missionsTableWrap');const meta=document.getElementById('missionsMeta');const a=state.selectedAffaire;if(!a){meta.textContent='0 mission';root.innerHTML=`<div class='empty'>Sélectionnez une affaire pour afficher les missions.</div>`;return;}const missions=a.missions||[];meta.textContent=`${missions.length} mission(s)`;if(!missions.length){root.innerHTML=`<div class='empty'>Aucune mission détaillée sur cette affaire.</div>`;return;}root.innerHTML=`<table><thead><tr><th>Tag</th><th>Mission</th><th>N°</th><th class='num'>Commande</th><th class='num'>Fact. cumulée</th><th class='num'>Reste</th><th class='num'>Prévisionnel</th><th class='num'>Facturé</th></tr></thead><tbody>${missions.map(m=>`<tr><td>${esc(m.tag||'')}</td><td>${esc(m.label||'')}</td><td>${esc(m.numero||'')}</td><td class='num'>${euro(m.commande_ht)}</td><td class='num'>${euro(m.total_facturation_cumulee||m.facturation_cumulee_2026)}</td><td class='num'>${euro(m.reste_a_facturer)}</td><td class='num'>${euro(m.total_previsionnel)}</td><td class='num'>${euro(m.total_facture)}</td></tr>`).join('')}</tbody></table>`;}
function renderInsights(){const root=document.getElementById('insightsBox');const a=state.selectedAffaire;if(!a){root.innerHTML=`<div class='empty' style='width:100%'>Sélectionnez une affaire.</div>`;return;}const items=a.insights||[];root.innerHTML=items.map(x=>`<div class='insight'>${esc(x)}</div>`).join('');}
function renderAll(){renderHero();renderKpis();renderFinanceChart();renderMonthlyTable();renderMissions();renderInsights();document.getElementById('exportBtn').disabled=!state.selectedAffaireId;const dash=document.getElementById('dashboardBtn');dash.href=state.selectedAffaireId?`/dashboard?affaire_id=${encodeURIComponent(state.selectedAffaireId)}`:'/dashboard';}
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
    <button id='exportBtn' class='btn' disabled>Exporter CSV</button>
  </div>
  <div class='hero'>
    <div class='eyeb'>Tableau de bord</div>
    <div id='title' class='title'>Sélectionnez une affaire</div>
    <div id='subtitle' class='muted'>Synthèse principale : client, commande, facturation cumulée, reste à facturer.</div>
  </div>
  <div class='kpis'><div class='grid'>
    <div class='k'><div class='muted'>Commande HT</div><div id='kCommande' class='v'>0 €</div></div>
    <div class='k'><div class='muted'>Facturation cumulée</div><div id='kFacture' class='v'>0 €</div></div>
    <div class='k'><div class='muted'>Reste à facturer</div><div id='kReste' class='v'>0 €</div></div>
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
function render(){const a=state.selected;document.getElementById('financeBtn').href=state.selectedId?`/finance?affaire_id=${encodeURIComponent(state.selectedId)}`:'/finance';document.getElementById('exportBtn').disabled=!state.selectedId;document.getElementById('title').textContent=a?(a.display_name||'-'):'Sélectionnez une affaire';document.getElementById('subtitle').textContent=a?`Client ${a.client||'-'} · ${a.affaire||'-'}`:'Synthèse principale : client, commande, facturation cumulée, reste à facturer.';document.getElementById('kCommande').textContent=euro(a?a.commande_ht:0);document.getElementById('kFacture').textContent=euro(a?(a.total_facturation_cumulee||a.facturation_cumulee_2026):0);document.getElementById('kReste').textContent=euro(a?a.reste_a_facturer:0);renderChart();}
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
    writer.writerow(["Tag", "Mission", "Numero", "Commande HT", "Facturation cumulée", "Reste à facturer", "Total prévisionnel", "Total facturé"])
    for mission in item.get("missions", []):
        writer.writerow([
            mission.get("tag", ""),
            mission.get("label", ""),
            mission.get("numero", ""),
            mission.get("commande_ht", 0),
            mission.get("total_facturation_cumulee", mission.get("facturation_cumulee_2026", 0)),
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
