import csv
import hashlib
import io
import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote

DEFAULT_WORKBOOK_PATH = r"\\192.168.10.100\03 - finances\10 - TABLEAU DE BORD\01 - TABLEAU ACTIVITEE\TABLEAU ACTIVITEE.xlsx"
DEFAULT_SHEET_NAME = "AFFAIRES 2026"
DEFAULT_CACHE_FILE = "finance_cache.json"
CACHE_KEY = "finance_affaires_dataset_v2"

COLUMN_MAPPING = {
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

MONTHS = [
    "janvier", "fevrier", "mars", "avril", "mai", "juin",
    "juillet", "aout", "septembre", "octobre", "novembre", "decembre",
]

CUMULATIVE_FIELDS = [
    "facturation_cumulee_2017", "facturation_cumulee_2018", "facturation_cumulee_2021", "facturation_cumulee_2022",
    "facturation_cumulee_2023", "facturation_cumulee_2024", "facturation_cumulee_2025", "facturation_cumulee_2026",
]


@dataclass
class NormalizedRow:
    excel_row: int
    row_kind: str
    data: Dict[str, Any]


class InMemoryCache:
    def __init__(self) -> None:
        self._store: Dict[str, Any] = {}

    def get(self, key: str) -> Any:
        return self._store.get(key)

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def clean_number(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = clean_text(value).replace(" ", "").replace("\u202f", "")
    text = text.replace("€", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def slugify_affaire_name(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_text.lower()).strip("-")
    return slug or "affaire"


def build_affaire_id(client: str, affaire: str) -> str:
    base = f"{slugify_affaire_name(client)}-{slugify_affaire_name(affaire)}"
    digest = hashlib.md5(f"{client}|{affaire}".encode("utf-8")).hexdigest()[:8]
    return f"{base}-{digest}"


def is_empty_row(row: Dict[str, Any]) -> bool:
    return all(clean_text(value) == "" for value in row.values())


def is_parent_row(row: Dict[str, Any]) -> bool:
    has_affaire = clean_text(row.get("affaire")) != ""
    return has_affaire


def is_total_row(row: Dict[str, Any]) -> bool:
    head = " ".join(clean_text(row.get(k)).lower() for k in ["client", "affaire", "tag", "numero"])
    return "total" in head


def is_detail_row(row: Dict[str, Any], current_affaire_id: Optional[str]) -> bool:
    if not current_affaire_id:
        return False
    if clean_text(row.get("affaire")):
        return False
    numeric_fields = ["commande_ht", "reste_a_facturer", "total_previsionnel", "total_facture", "facturation_cumulee_2026"]
    return any(clean_number(row.get(k)) != 0.0 for k in numeric_fields)


def get_file_signature(path: str) -> str:
    file_path = Path(path)
    stat = file_path.stat()
    payload = f"{file_path}:{stat.st_mtime_ns}:{stat.st_size}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class FinanceService:
    def __init__(self, config: Dict[str, Any], cache_backend: Optional[Any] = None, logger: Optional[logging.Logger] = None) -> None:
        self.config = config
        self.cache = cache_backend or InMemoryCache()
        self.logger = logger or logging.getLogger("finance")

    def load_finance_source(self) -> Any:
        from openpyxl import load_workbook

        workbook_path = self.config["FINANCE_WORKBOOK_PATH"]
        return load_workbook(filename=workbook_path, data_only=True, read_only=True)

    def parse_affaires_sheet(self, workbook: Any) -> Dict[str, Any]:
        sheet_name = self.config["FINANCE_SHEET_NAME"]
        if sheet_name not in workbook.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' introuvable")
        ws = workbook[sheet_name]

        headers = {
            "main": self._read_row_map(ws, 11),
            "sub": self._read_row_map(ws, 12),
        }

        rows: List[Dict[str, Any]] = []
        for row_number in range(14, ws.max_row + 1):
            row = self._extract_row_data(ws, row_number)
            if is_empty_row(row):
                continue
            row["_excel_row"] = row_number
            rows.append(row)
        return {"headers": headers, "rows": rows, "rows_read": max(ws.max_row - 13, 0)}

    def normalize_finance_rows(self, raw_rows: List[Dict[str, Any]]) -> Tuple[List[NormalizedRow], List[str]]:
        normalized: List[NormalizedRow] = []
        warnings: List[str] = []
        current_affaire_id: Optional[str] = None

        for row in raw_rows:
            excel_row = row["_excel_row"]
            client = clean_text(row.get("client"))
            affaire = clean_text(row.get("affaire"))
            tag = clean_text(row.get("tag"))
            numero = clean_text(row.get("numero"))

            normalized_data = {
                "client": client,
                "affaire": affaire,
                "tag": tag,
                "numero": numero,
                "delai_reglement_jours": safe_int(row.get("delai_reglement_jours")),
                **{f: clean_number(row.get(f)) for f in CUMULATIVE_FIELDS},
                "commande_ht": clean_number(row.get("commande_ht")),
                "reste_a_facturer": clean_number(row.get("reste_a_facturer")),
                "total_previsionnel": clean_number(row.get("total_previsionnel")),
                "total_facture": clean_number(row.get("total_facture")),
                "mensuel": {
                    month: {
                        "previsionnel": clean_number(row.get(f"{month}_previsionnel")),
                        "facture": clean_number(row.get(f"{month}_facture")),
                    }
                    for month in MONTHS
                },
            }

            if is_total_row(row):
                normalized.append(NormalizedRow(excel_row=excel_row, row_kind="total", data=normalized_data))
                continue

            if is_parent_row(row):
                if not client and current_affaire_id:
                    warnings.append(f"Ligne {excel_row}: parent sans client, héritage implicite")
                current_affaire_id = build_affaire_id(client or "inconnu", affaire)
                normalized_data["affaire_id"] = current_affaire_id
                normalized.append(NormalizedRow(excel_row=excel_row, row_kind="parent", data=normalized_data))
                continue

            if is_detail_row(row, current_affaire_id):
                normalized_data["affaire_id"] = current_affaire_id
                normalized.append(NormalizedRow(excel_row=excel_row, row_kind="detail", data=normalized_data))
                continue

            normalized.append(NormalizedRow(excel_row=excel_row, row_kind="skip", data=normalized_data))
        return normalized, warnings

    def group_rows_by_affaire(self, rows: List[NormalizedRow]) -> Tuple[Dict[str, Dict[str, Any]], int]:
        grouped: Dict[str, Dict[str, Any]] = {}
        kept = 0

        for row in rows:
            if row.row_kind not in {"parent", "detail", "total"}:
                continue

            affaire_id = row.data.get("affaire_id")
            if row.row_kind == "total" and not affaire_id and grouped:
                affaire_id = list(grouped.keys())[-1]

            if not affaire_id:
                continue

            kept += 1
            bucket = grouped.setdefault(
                affaire_id,
                {"parents": [], "details": [], "totals": [], "tags": set(), "client": "", "affaire": "", "numero": ""},
            )

            if row.data.get("client"):
                bucket["client"] = row.data["client"]
            if row.data.get("affaire"):
                bucket["affaire"] = row.data["affaire"]
            if row.data.get("numero"):
                bucket["numero"] = row.data["numero"]
            if row.data.get("tag"):
                bucket["tags"].add(row.data["tag"])

            if row.row_kind == "parent":
                bucket["parents"].append(row.data)
            elif row.row_kind == "detail":
                bucket["details"].append(row.data)
            else:
                bucket["totals"].append(row.data)

        return grouped, kept

    def build_affaire_payload(self, affaire_id: str, grouped: Dict[str, Any]) -> Dict[str, Any]:
        client = grouped.get("client") or "INCONNU"
        affaire = grouped.get("affaire") or "AFFAIRE"
        numero = grouped.get("numero") or ""
        tags = sorted(t for t in grouped.get("tags", set()) if t)

        source_rows = grouped["details"] if grouped["details"] else grouped["parents"]

        def sum_field(field: str) -> float:
            return sum(clean_number(row.get(field)) for row in source_rows)

        mensuel = {}
        for month in MONTHS:
            p = sum(clean_number(r["mensuel"][month]["previsionnel"]) for r in source_rows)
            f = sum(clean_number(r["mensuel"][month]["facture"]) for r in source_rows)
            mensuel[month] = {"previsionnel": p, "facture": f, "ecart": f - p}

        commande_ht = sum_field("commande_ht")
        fact_2026 = sum_field("facturation_cumulee_2026")
        total_previsionnel = sum_field("total_previsionnel")
        total_facture = sum_field("total_facture")

        reste_excel = sum_field("reste_a_facturer")
        reste_calc = max(commande_ht - fact_2026, 0.0)
        reste_a_facturer = reste_calc if abs(reste_excel - reste_calc) > max(1.0, 0.05 * max(commande_ht, 1.0)) else max(reste_excel, 0.0)

        taux_avancement = 0.0 if commande_ht <= 0 else max(0.0, min(1.0, fact_2026 / commande_ht))

        missions = []
        for mission in grouped["details"]:
            mission_monthly = {}
            for month in MONTHS:
                mp = clean_number(mission["mensuel"][month]["previsionnel"])
                mf = clean_number(mission["mensuel"][month]["facture"])
                mission_monthly[month] = {"previsionnel": mp, "facture": mf, "ecart": mf - mp}
            missions.append(
                {
                    "tag": mission.get("tag"),
                    "numero": mission.get("numero"),
                    "commande_ht": clean_number(mission.get("commande_ht")),
                    "facturation_cumulee_2026": clean_number(mission.get("facturation_cumulee_2026")),
                    "reste_a_facturer": max(clean_number(mission.get("reste_a_facturer")), 0.0),
                    "total_previsionnel": clean_number(mission.get("total_previsionnel")),
                    "total_facture": clean_number(mission.get("total_facture")),
                    "mensuel": mission_monthly,
                }
            )

        payload = {
            "affaire_id": affaire_id,
            "display_name": f"{client} - {affaire}",
            "client": client,
            "affaire": affaire,
            "numero": numero,
            "tags": tags,
            "delai_reglement_jours": next((r.get("delai_reglement_jours") for r in source_rows if r.get("delai_reglement_jours") is not None), None),
            "commande_ht": commande_ht,
            **{f: sum_field(f) for f in CUMULATIVE_FIELDS},
            "reste_a_facturer": reste_a_facturer,
            "taux_avancement_financier": taux_avancement,
            "ecart_previsionnel_vs_facture": total_facture - total_previsionnel,
            "mensuel": mensuel,
            "total_previsionnel": total_previsionnel,
            "total_facture": total_facture,
            "missions": missions,
        }
        return payload

    def build_finance_cache(self) -> Dict[str, Any]:
        workbook_path = self.config["FINANCE_WORKBOOK_PATH"]
        sheet_name = self.config["FINANCE_SHEET_NAME"]
        signature = f"{get_file_signature(workbook_path)}:{sheet_name}"
        warnings: List[str] = []

        workbook = self.load_finance_source()
        try:
            parsed = self.parse_affaires_sheet(workbook)
            normalized_rows, norm_warnings = self.normalize_finance_rows(parsed["rows"])
            warnings.extend(norm_warnings)
            grouped, rows_kept = self.group_rows_by_affaire(normalized_rows)
            items = {affaire_id: self.build_affaire_payload(affaire_id, group) for affaire_id, group in grouped.items()}
            sorted_items = dict(sorted(items.items(), key=lambda kv: kv[1]["display_name"].lower()))

            cache_payload = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "ready",
                "signature": signature,
                "sheet": sheet_name,
                "rows_read": parsed["rows_read"],
                "rows_kept": rows_kept,
                "affaires_count": len(sorted_items),
                "warnings": warnings,
                "headers": parsed["headers"],
                "items": sorted_items,
            }
            self.logger.info(
                "Finance cache built | rows_read=%s rows_kept=%s affaires_count=%s warnings=%s",
                cache_payload["rows_read"],
                cache_payload["rows_kept"],
                cache_payload["affaires_count"],
                len(warnings),
            )
            self._persist_cache_file(cache_payload)
            return cache_payload
        finally:
            workbook.close()

    def _persist_cache_file(self, payload: Dict[str, Any]) -> None:
        cache_file = self.config.get("FINANCE_CACHE_FILE")
        if not cache_file:
            return
        try:
            Path(cache_file).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as exc:
            self.logger.warning("Impossible d'écrire le cache JSON '%s': %s", cache_file, exc)

    def _load_cache_file(self) -> Optional[Dict[str, Any]]:
        cache_file = self.config.get("FINANCE_CACHE_FILE")
        if not cache_file:
            return None
        path = Path(cache_file)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def get_finance_cache(self) -> Dict[str, Any]:
        workbook_path = self.config["FINANCE_WORKBOOK_PATH"]
        sheet_name = self.config["FINANCE_SHEET_NAME"]
        expected_signature = f"{get_file_signature(workbook_path)}:{sheet_name}"

        mem = self.cache.get(CACHE_KEY)
        if mem and mem.get("signature") == expected_signature:
            return mem

        disk = self._load_cache_file()
        if disk and disk.get("signature") == expected_signature and disk.get("status") == "ready":
            self.cache.set(CACHE_KEY, disk)
            self.logger.info("Finance cache loaded from disk")
            return disk

        rebuilt = self.build_finance_cache()
        self.cache.set(CACHE_KEY, rebuilt)
        return rebuilt

    def rebuild_finance_cache(self) -> Dict[str, Any]:
        rebuilt = self.build_finance_cache()
        self.cache.set(CACHE_KEY, rebuilt)
        return rebuilt

    @staticmethod
    def lightweight_affaires(cache_payload: Dict[str, Any], search: str = "") -> List[Dict[str, str]]:
        items = []
        search_lower = search.lower().strip()
        for affaire in cache_payload.get("items", {}).values():
            display_name = affaire.get("display_name", "")
            if search_lower and search_lower not in display_name.lower():
                continue
            items.append({"affaire_id": affaire["affaire_id"], "display_name": display_name})
        items.sort(key=lambda x: x["display_name"].lower())
        return items

    @staticmethod
    def compute_insights(affaire: Dict[str, Any]) -> List[str]:
        insights: List[str] = []
        commande = clean_number(affaire.get("commande_ht"))
        fact = clean_number(affaire.get("facturation_cumulee_2026"))
        reste = clean_number(affaire.get("reste_a_facturer"))
        ecart = clean_number(affaire.get("ecart_previsionnel_vs_facture"))

        if commande > 0 and fact <= 0:
            insights.append("Aucune facturation détectée malgré une commande active.")
        if reste > 0 and commande > 0 and reste / commande > 0.6:
            insights.append("Reste à facturer élevé : suivre le rythme de facturation.")
        if commande > 0 and fact / commande >= 0.9:
            insights.append("Affaire presque soldée financièrement.")
        if ecart > 0:
            insights.append("Facturation en avance sur le prévisionnel.")
        if ecart < 0:
            insights.append("Facturation en retard par rapport au prévisionnel.")

        active_months = [m for m, v in affaire.get("mensuel", {}).items() if clean_number(v.get("previsionnel")) > 0]
        if 0 < len(active_months) <= 2:
            insights.append("Prévisionnel concentré sur peu de mois.")

        return insights


class FinanceASGIApp:
    def __init__(self, cache_backend: Optional[Any] = None) -> None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        self.logger = logging.getLogger("finance")
        self.config = {
            "FINANCE_WORKBOOK_PATH": os.getenv("FINANCE_WORKBOOK_PATH", DEFAULT_WORKBOOK_PATH),
            "FINANCE_SHEET_NAME": os.getenv("FINANCE_SHEET_NAME", DEFAULT_SHEET_NAME),
            "FINANCE_DEBUG_PARSE": os.getenv("FINANCE_DEBUG_PARSE", "0") == "1",
            "FINANCE_CACHE_FILE": os.getenv("FINANCE_CACHE_FILE", DEFAULT_CACHE_FILE),
        }
        self.service = FinanceService(config=self.config, cache_backend=cache_backend, logger=self.logger)

    async def __call__(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self._send_json(send, 404, {"error": "Unsupported scope"})
            return

        method = scope.get("method", "GET")
        path = unquote(scope.get("path", ""))
        query_params = parse_qs(scope.get("query_string", b"").decode("utf-8"))

        if method == "GET" and path == "/":
            await self._send_html(send, 200, self._landing_page())
            return

        if method == "GET" and path == "/finance":
            await self._send_html(send, 200, self._finance_cockpit_page())
            return

        if method == "GET" and path == "/health":
            await self._send_json(send, 200, {"status": "ok", "service": "finance"})
            return

        if method == "GET" and path == "/api/finance":
            await self._send_json(send, 200, self._service_index())
            return

        if method == "GET" and path == "/api/finance/cache-status":
            await self._handle_cache_status(send)
            return

        if method in {"GET", "POST"} and path == "/api/finance/rebuild-cache":
            await self._handle_rebuild_cache(send)
            return

        if method == "GET" and path == "/api/finance/affaires":
            search = query_params.get("search", [""])[0]
            await self._handle_affaires(send, search)
            return

        if method == "GET" and path.startswith("/api/finance/affaire/") and path.endswith("/export-csv"):
            affaire_id = path.removeprefix("/api/finance/affaire/").removesuffix("/export-csv")
            await self._handle_affaire_export_csv(send, affaire_id.strip("/"))
            return

        if method == "GET" and path.startswith("/api/finance/affaire/"):
            affaire_id = path.split("/api/finance/affaire/", 1)[1]
            await self._handle_affaire(send, affaire_id)
            return

        await self._send_json(send, 404, {"error": "Not found", "path": path})

    async def _handle_cache_status(self, send: Any) -> None:
        try:
            cache = self.service.get_finance_cache()
            status = {
                "status": cache.get("status", "unknown"),
                "generated_at": cache.get("generated_at"),
                "rows_read": cache.get("rows_read", 0),
                "rows_kept": cache.get("rows_kept", 0),
                "affaires_count": cache.get("affaires_count", 0),
                "warnings": cache.get("warnings", []),
            }
            await self._send_json(send, 200, status)
        except Exception as exc:
            await self._send_json(send, 500, {"error": str(exc)})

    async def _handle_rebuild_cache(self, send: Any) -> None:
        try:
            cache = self.service.rebuild_finance_cache()
            await self._send_json(
                send,
                200,
                {
                    "status": "rebuilt",
                    "generated_at": cache.get("generated_at"),
                    "rows_read": cache.get("rows_read", 0),
                    "rows_kept": cache.get("rows_kept", 0),
                    "affaires_count": cache.get("affaires_count", 0),
                    "warnings": cache.get("warnings", []),
                },
            )
        except Exception as exc:
            await self._send_json(send, 500, {"error": str(exc)})

    async def _handle_affaires(self, send: Any, search: str) -> None:
        try:
            cache = self.service.get_finance_cache()
            affaires = FinanceService.lightweight_affaires(cache, search=search)
            self.logger.info("Affaires list loaded | count=%s search=%s", len(affaires), search)
            await self._send_json(send, 200, affaires)
        except Exception as exc:
            await self._send_json(send, 500, {"error": str(exc)})

    async def _handle_affaire(self, send: Any, affaire_id: str) -> None:
        try:
            cache = self.service.get_finance_cache()
            affaire = cache.get("items", {}).get(affaire_id)
            if not affaire:
                await self._send_json(send, 404, {"error": "Affaire introuvable", "affaire_id": affaire_id})
                return
            payload = dict(affaire)
            payload["insights"] = FinanceService.compute_insights(affaire)
            await self._send_json(send, 200, payload)
        except Exception as exc:
            await self._send_json(send, 500, {"error": str(exc)})

    async def _handle_affaire_export_csv(self, send: Any, affaire_id: str) -> None:
        try:
            cache = self.service.get_finance_cache()
            affaire = cache.get("items", {}).get(affaire_id)
            if not affaire:
                await self._send_json(send, 404, {"error": "Affaire introuvable", "affaire_id": affaire_id})
                return

            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["type", "tag", "numero", "commande_ht", "facturation_cumulee_2026", "reste_a_facturer", "total_previsionnel", "total_facture"])
            writer.writerow(["affaire", "", affaire.get("numero"), affaire.get("commande_ht"), affaire.get("facturation_cumulee_2026"), affaire.get("reste_a_facturer"), affaire.get("total_previsionnel"), affaire.get("total_facture")])
            for m in affaire.get("missions", []):
                writer.writerow(["mission", m.get("tag"), m.get("numero"), m.get("commande_ht"), m.get("facturation_cumulee_2026"), m.get("reste_a_facturer"), m.get("total_previsionnel"), m.get("total_facture")])

            data = output.getvalue().encode("utf-8")
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"content-type", b"text/csv; charset=utf-8"],
                    [b"content-disposition", f"attachment; filename=finance-{affaire_id}.csv".encode("utf-8")],
                ],
            })
            await send({"type": "http.response.body", "body": data})
        except Exception as exc:
            await self._send_json(send, 500, {"error": str(exc)})

    async def _send_json(self, send: Any, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        await send({"type": "http.response.start", "status": status, "headers": [[b"content-type", b"application/json; charset=utf-8"]]})
        await send({"type": "http.response.body", "body": body})

    async def _send_html(self, send: Any, status: int, html: str) -> None:
        await send({"type": "http.response.start", "status": status, "headers": [[b"content-type", b"text/html; charset=utf-8"]]})
        await send({"type": "http.response.body", "body": html.encode("utf-8")})

    def _service_index(self) -> Dict[str, Any]:
        return {
            "service": "finance",
            "status": "ok",
            "endpoints": {
                "cache_status": "/api/finance/cache-status",
                "rebuild_cache": "/api/finance/rebuild-cache",
                "affaires": "/api/finance/affaires?search=...",
                "affaire": "/api/finance/affaire/{affaire_id}",
                "affaire_export_csv": "/api/finance/affaire/{affaire_id}/export-csv",
                "finance_ui": "/finance",
            },
        }

    def _landing_page(self) -> str:
        return """<!doctype html><html><head><meta charset='utf-8'><title>Gestion Affaire - Finance</title></head>
<body style='font-family:Inter,Arial,sans-serif;padding:24px'>
<h1>Gestion Affaire - Finance</h1>
<p>Module cockpit financier prêt.</p>
<ul>
<li><a href='/finance'>Ouvrir le cockpit Finance</a></li>
<li><a href='/api/finance'>Index API Finance</a></li>
<li><a href='/api/finance/cache-status'>Statut du cache</a></li>
</ul>
</body></html>"""

    def _finance_cockpit_page(self) -> str:
        return """<!doctype html>
<html lang='fr'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>Finance Cockpit</title>
<style>
:root { --bg:#f4f7fb; --card:#fff; --ink:#172032; --muted:#6d7482; --primary:#2d5bff; --ok:#1d9a5b; --bad:#d04646; }
*{box-sizing:border-box} body{margin:0;background:var(--bg);font-family:Inter,Segoe UI,Arial,sans-serif;color:var(--ink)}
.container{max-width:1300px;margin:22px auto;padding:0 18px}
.top{background:var(--card);border-radius:16px;padding:16px;display:flex;gap:12px;align-items:center;box-shadow:0 8px 25px rgba(13,27,62,.08);flex-wrap:wrap}
.top h1{margin:0 14px 0 0;font-size:26px}
input,select,button{border:1px solid #d8dfeb;border-radius:12px;padding:10px 12px;background:#fff}
button{background:var(--primary);color:#fff;border:none;font-weight:600;cursor:pointer}
.status{padding:6px 10px;border-radius:999px;background:#e8f0ff;color:#2b4fcb;font-size:12px}
.grid{display:grid;gap:14px;margin-top:16px;grid-template-columns:repeat(6,minmax(0,1fr))}
.kpi{background:var(--card);border-radius:16px;padding:16px;box-shadow:0 8px 25px rgba(13,27,62,.07)}
.kpi .l{font-size:12px;color:var(--muted)} .kpi .v{font-size:26px;font-weight:700;margin-top:6px}
.card{background:var(--card);border-radius:16px;padding:18px;box-shadow:0 8px 25px rgba(13,27,62,.07);margin-top:16px}
canvas{width:100%;height:290px;background:#fbfcff;border-radius:12px}
table{width:100%;border-collapse:collapse;font-size:14px} th,td{padding:10px;border-bottom:1px solid #eef2f8;text-align:right} th:first-child,td:first-child{text-align:left}
.pos{color:var(--ok);font-weight:600}.neg{color:var(--bad);font-weight:600}
.muted{color:var(--muted)}
.message{padding:12px;border-radius:12px;margin-top:12px}.err{background:#ffe9e9;color:#8b2626}.ok{background:#e7f8ee;color:#1e6f43}
@media (max-width:1100px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
</style>
</head>
<body>
<div class='container'>
  <div class='top'>
    <h1>Finance</h1>
    <input id='searchAffaire' placeholder='Recherche affaire...' />
    <select id='affaireSelect'></select>
    <button id='reloadBtn'>Reconstruire cache</button>
    <span id='cacheBadge' class='status'>Cache: ...</span>
    <span id='feedback' class='muted'></span>
  </div>

  <div id='errorBox' style='display:none' class='message err'></div>
  <div id='loadingBox' style='display:none' class='message ok'>Chargement...</div>

  <div class='grid' id='kpiGrid'></div>

  <div class='card'><h3>Rythme mensuel</h3><canvas id='financeChart' width='1200' height='320'></canvas></div>
  <div class='card'><h3>Mensuel détaillé</h3><div id='monthlyTable'></div></div>
  <div class='card'><h3>Détail missions</h3><div id='missionsTable'></div></div>
  <div class='card'><h3>Insights / alertes</h3><div id='insightsBox' class='muted'>Sélectionnez une affaire.</div></div>
</div>
<script>
let financeState = { cacheStatus:null, affaires:[], selectedAffaireId:null, selectedAffaire:null };

function euro(v){return new Intl.NumberFormat('fr-FR',{style:'currency',currency:'EUR',maximumFractionDigits:0}).format(Number(v||0));}
function pct(v){return `${(Number(v||0)*100).toFixed(1)}%`;}

function showFinanceError(message){const el=document.getElementById('errorBox');el.textContent=message;el.style.display='block';}
function clearFinanceError(){const el=document.getElementById('errorBox');el.style.display='none';el.textContent='';}
function showFinanceLoading(on){document.getElementById('loadingBox').style.display=on?'block':'none';}

async function loadFinanceCacheStatus(){
  const r=await fetch('/api/finance/cache-status');
  if(!r.ok) throw new Error('Impossible de charger le statut cache');
  financeState.cacheStatus=await r.json();
  console.debug('cache status chargé', financeState.cacheStatus);
  const b=document.getElementById('cacheBadge');
  b.textContent=`Cache: ${financeState.cacheStatus.status} | ${financeState.cacheStatus.affaires_count} affaires`;
}

async function loadAffairesList(search=''){
  const qs = search ? `?search=${encodeURIComponent(search)}` : '';
  const r=await fetch(`/api/finance/affaires${qs}`);
  if(!r.ok) throw new Error('Impossible de charger la liste des affaires');
  financeState.affaires=await r.json();
  console.debug('nombre d\'affaires chargées', financeState.affaires.length);
  const sel=document.getElementById('affaireSelect');
  sel.innerHTML='';
  if(financeState.affaires.length===0){
    const o=document.createElement('option');o.text='Aucune affaire trouvée';o.value='';sel.appendChild(o);return;
  }
  for(const a of financeState.affaires){const o=document.createElement('option');o.value=a.affaire_id;o.textContent=a.display_name;sel.appendChild(o);}
  financeState.selectedAffaireId = financeState.selectedAffaireId || financeState.affaires[0].affaire_id;
  sel.value = financeState.selectedAffaireId;
}

async function loadSelectedAffaire(){
  if(!financeState.selectedAffaireId){ return; }
  const r=await fetch(`/api/finance/affaire/${encodeURIComponent(financeState.selectedAffaireId)}`);
  if(!r.ok){ if(r.status===404){throw new Error('Affaire introuvable');} throw new Error('Erreur API affaire'); }
  financeState.selectedAffaire=await r.json();
  console.debug('affaire sélectionnée', financeState.selectedAffaireId);
  console.debug('payload affaire reçu', financeState.selectedAffaire);
}

function renderFinanceKpis(){
  const a=financeState.selectedAffaire; const grid=document.getElementById('kpiGrid');
  if(!a){grid.innerHTML='';return;}
  const kpis=[
    ['Commande HT', euro(a.commande_ht)],
    ['Facturation cumulée 2026', euro(a.facturation_cumulee_2026)],
    ['Reste à facturer', euro(a.reste_a_facturer)],
    ['Avancement financier', pct(a.taux_avancement_financier)],
    ['Total prévisionnel', euro(a.total_previsionnel)],
    ['Écart prévisionnel vs facturé', euro(a.ecart_previsionnel_vs_facture)],
  ];
  grid.innerHTML = kpis.map(k=>`<div class='kpi'><div class='l'>${k[0]}</div><div class='v'>${k[1]}</div></div>`).join('');
}

function renderFinanceChart(){
  const canvas=document.getElementById('financeChart'); const ctx=canvas.getContext('2d');
  ctx.clearRect(0,0,canvas.width,canvas.height);
  const a=financeState.selectedAffaire; if(!a){return;}
  const months=Object.keys(a.mensuel||{});
  const prev=months.map(m=>Number(a.mensuel[m].previsionnel||0));
  const fact=months.map(m=>Number(a.mensuel[m].facture||0));
  const maxV=Math.max(1,...prev,...fact); const left=40, top=20, w=canvas.width-70, h=canvas.height-50;
  ctx.strokeStyle='#d8deeb'; ctx.beginPath(); ctx.moveTo(left,top); ctx.lineTo(left,top+h); ctx.lineTo(left+w,top+h); ctx.stroke();
  const bw = w/(months.length*1.5);
  months.forEach((m,i)=>{const x=left+i*(w/months.length)+8; const bh=(prev[i]/maxV)*h; ctx.fillStyle='rgba(45,91,255,.35)'; ctx.fillRect(x,top+h-bh,bw,bh); ctx.fillStyle='#6d7482'; ctx.font='11px sans-serif'; ctx.fillText(m.slice(0,3),x,top+h+14);});
  ctx.strokeStyle='#1d9a5b'; ctx.lineWidth=2; ctx.beginPath();
  months.forEach((m,i)=>{const x=left+i*(w/months.length)+8+bw/2; const y=top+h-(fact[i]/maxV)*h; if(i===0){ctx.moveTo(x,y);}else{ctx.lineTo(x,y);} });
  ctx.stroke();
}

function renderFinanceMonthlyTable(){
  const a=financeState.selectedAffaire; const root=document.getElementById('monthlyTable');
  if(!a){root.innerHTML='<p class="muted">Aucune donnée mensuelle.</p>';return;}
  const rows=Object.entries(a.mensuel||{}).map(([m,v])=>{const e=(Number(v.facture)-Number(v.previsionnel)); const cls=e>=0?'pos':'neg'; return `<tr><td>${m}</td><td>${euro(v.previsionnel)}</td><td>${euro(v.facture)}</td><td class='${cls}'>${euro(e)}</td></tr>`;}).join('');
  root.innerHTML=`<table><thead><tr><th>Mois</th><th>Prévisionnel</th><th>Facturé</th><th>Écart</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function renderFinanceMissions(){
  const a=financeState.selectedAffaire; const root=document.getElementById('missionsTable');
  if(!a || !a.missions || a.missions.length===0){root.innerHTML='<p class="muted">Aucune mission détaillée.</p>';return;}
  const rows=a.missions.map(m=>`<tr><td>${m.tag||''}</td><td>${m.numero||''}</td><td>${euro(m.commande_ht)}</td><td>${euro(m.facturation_cumulee_2026)}</td><td>${euro(m.reste_a_facturer)}</td><td>${euro(m.total_previsionnel)}</td><td>${euro(m.total_facture)}</td></tr>`).join('');
  root.innerHTML=`<table><thead><tr><th>Tag</th><th>Numéro</th><th>Commande HT</th><th>Fact. 2026</th><th>Reste</th><th>Total prév.</th><th>Total fact.</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function renderFinanceInsights(){
  const a=financeState.selectedAffaire; const root=document.getElementById('insightsBox');
  if(!a){root.textContent='Sélectionnez une affaire.'; return;}
  const insights=(a.insights||[]);
  root.innerHTML = insights.length ? `<ul>${insights.map(i=>`<li>${i}</li>`).join('')}</ul>` : '<span class="muted">Aucune alerte majeure détectée.</span>';
}

async function initFinancePage(){
  clearFinanceError(); showFinanceLoading(true);
  try {
    await loadFinanceCacheStatus();
    await loadAffairesList();
    await loadSelectedAffaire();
    renderFinanceKpis(); renderFinanceChart(); renderFinanceMonthlyTable(); renderFinanceMissions(); renderFinanceInsights();
    document.getElementById('feedback').textContent='Cockpit prêt';
  } catch (e) {
    showFinanceError(e.message || 'Erreur de chargement Finance');
  } finally { showFinanceLoading(false); }

  document.getElementById('affaireSelect').addEventListener('change', async (ev)=>{
    financeState.selectedAffaireId=ev.target.value;
    showFinanceLoading(true); clearFinanceError();
    try { await loadSelectedAffaire(); renderFinanceKpis(); renderFinanceChart(); renderFinanceMonthlyTable(); renderFinanceMissions(); renderFinanceInsights(); }
    catch(e){ showFinanceError(e.message || 'Erreur chargement affaire'); }
    finally { showFinanceLoading(false); }
  });

  document.getElementById('searchAffaire').addEventListener('input', async (ev)=>{
    try{ await loadAffairesList(ev.target.value||''); }
    catch(e){ showFinanceError(e.message || 'Erreur recherche'); }
  });

  document.getElementById('reloadBtn').addEventListener('click', async ()=>{
    showFinanceLoading(true); clearFinanceError();
    try {
      const r=await fetch('/api/finance/rebuild-cache', {method:'POST'});
      if(!r.ok) throw new Error('Reconstruction cache en erreur');
      await loadFinanceCacheStatus();
      await loadAffairesList(document.getElementById('searchAffaire').value||'');
      await loadSelectedAffaire();
      renderFinanceKpis(); renderFinanceChart(); renderFinanceMonthlyTable(); renderFinanceMissions(); renderFinanceInsights();
      document.getElementById('feedback').textContent='Cache reconstruit';
    } catch(e){ showFinanceError(e.message || 'Erreur rebuild cache'); }
    finally { showFinanceLoading(false); }
  });
}

initFinancePage();
</script>
</body>
</html>"""

    def _read_row_map(self, ws: Any, row_index: int) -> Dict[str, Any]:
        return {COLUMN_MAPPING[col]: ws[f"{col}{row_index}"].value for col in COLUMN_MAPPING}

    def _extract_row_data(self, ws: Any, row_index: int) -> Dict[str, Any]:
        return {field: ws[f"{col}{row_index}"].value for col, field in COLUMN_MAPPING.items()}


app = FinanceASGIApp()
