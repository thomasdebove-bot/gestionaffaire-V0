import hashlib
import json
import logging
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote


DEFAULT_WORKBOOK_PATH = r"\\192.168.10.100\03 - finances\10 - TABLEAU DE BORD\01 - TABLEAU ACTIVITEE\TABLEAU ACTIVITEE.xlsx"
DEFAULT_SHEET_NAME = "AFFAIRES 2026"
CACHE_KEY = "finance_affaires_dataset_v1"

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
    "janvier",
    "fevrier",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "aout",
    "septembre",
    "octobre",
    "novembre",
    "decembre",
]

CUMULATIVE_FIELDS = [
    "facturation_cumulee_2017",
    "facturation_cumulee_2018",
    "facturation_cumulee_2021",
    "facturation_cumulee_2022",
    "facturation_cumulee_2023",
    "facturation_cumulee_2024",
    "facturation_cumulee_2025",
    "facturation_cumulee_2026",
]


@dataclass
class ParsedRow:
    excel_row: int
    row_type: str
    data: Dict[str, Any]


class InMemoryCache:
    def __init__(self) -> None:
        self._store: Dict[str, Any] = {}

    def get(self, key: str) -> Any:
        return self._store.get(key)

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value


class FinanceASGIApp:
    def __init__(self, cache_backend: Optional[Any] = None) -> None:
        self.config = {
            "FINANCE_WORKBOOK_PATH": os.getenv("FINANCE_WORKBOOK_PATH", DEFAULT_WORKBOOK_PATH),
            "FINANCE_SHEET_NAME": os.getenv("FINANCE_SHEET_NAME", DEFAULT_SHEET_NAME),
            "FINANCE_DEBUG_PARSE": os.getenv("FINANCE_DEBUG_PARSE", "0") == "1",
        }
        self.cache = cache_backend or InMemoryCache()
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        self.logger = logging.getLogger("finance")

    async def __call__(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self._send_json(send, 404, {"error": "Unsupported scope"})
            return

        method = scope.get("method", "GET")
        path = unquote(scope.get("path", ""))

        if method == "GET" and path == "/":
            await self._send_html(send, 200, self._service_homepage())
            return

        if method == "GET" and path == "/api/finance":
            await self._send_json(send, 200, self._service_index())
            return

        if method == "GET" and path == "/health":
            await self._send_json(send, 200, {"status": "ok", "service": "finance"})
            return

        if method == "GET" and path == "/api/finance/affaires":
            await self._handle_affaires(send)
            return

        if method == "GET" and path.startswith("/api/finance/affaires/"):
            affaire_id = path.split("/api/finance/affaires/", 1)[1]
            await self._handle_affaire_detail(send, affaire_id)
            return

        if method == "GET" and path == "/api/finance/debug/parse":
            await self._handle_debug(send)
            return

        await self._send_json(send, 404, {"error": "Not found", "path": path})

    async def _handle_affaires(self, send: Any) -> None:
        try:
            dataset = self.get_finance_dataset()
            await self._send_json(send, 200, dataset["affaires_list"])
        except Exception as exc:
            await self._send_json(send, 500, {"error": str(exc)})

    async def _handle_affaire_detail(self, send: Any, affaire_id: str) -> None:
        try:
            dataset = self.get_finance_dataset()
            affaire = dataset["affaires_by_id"].get(affaire_id)
            if affaire is None:
                await self._send_json(send, 404, {"error": "Affaire introuvable", "affaire_id": affaire_id})
                return
            await self._send_json(send, 200, affaire)
        except Exception as exc:
            await self._send_json(send, 500, {"error": str(exc)})

    async def _handle_debug(self, send: Any) -> None:
        try:
            dataset = self.get_finance_dataset()
            await self._send_json(send, 200, dataset["debug"])
        except Exception as exc:
            await self._send_json(send, 500, {"error": str(exc)})

    async def _send_json(self, send: Any, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [[b"content-type", b"application/json; charset=utf-8"]],
            }
        )
        await send({"type": "http.response.body", "body": body})


    async def _send_html(self, send: Any, status: int, html: str) -> None:
        body = html.encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [[b"content-type", b"text/html; charset=utf-8"]],
            }
        )
        await send({"type": "http.response.body", "body": body})

    def _service_index(self) -> Dict[str, Any]:
        return {
            "service": "finance",
            "status": "ok",
            "endpoints": {
                "service_index": "/api/finance",
                "list_affaires": "/api/finance/affaires",
                "affaire_detail": "/api/finance/affaires/{affaire_id}",
                "parse_debug": "/api/finance/debug/parse",
                "health": "/health",
            },
        }

    def _service_homepage(self) -> str:
        return """<!doctype html>
<html lang="fr">
<head><meta charset="utf-8"><title>Finance API</title></head>
<body>
<h1>Finance API</h1>
<ul>
  <li><a href="/api/finance">GET /api/finance</a></li>
  <li><a href="/health">GET /health</a></li>
  <li><a href="/api/finance/affaires">GET /api/finance/affaires</a></li>
  <li><a href="/api/finance/debug/parse">GET /api/finance/debug/parse</a></li>
</ul>
</body>
</html>"""

    def get_finance_dataset(self) -> Dict[str, Any]:
        workbook_path = self.config["FINANCE_WORKBOOK_PATH"]
        sheet_name = self.config["FINANCE_SHEET_NAME"]
        file_signature = get_file_signature(workbook_path)
        cache_signature = f"{file_signature}:{sheet_name}"

        cached_value = self.cache.get(CACHE_KEY)
        if cached_value and cached_value.get("signature") == cache_signature:
            self.logger.debug("Finance dataset loaded from cache")
            return cached_value["dataset"]

        self.logger.info("Parsing workbook '%s' sheet '%s'", workbook_path, sheet_name)
        dataset = parse_finance_sheet(workbook_path, sheet_name, self.config["FINANCE_DEBUG_PARSE"], self.logger)
        self.cache.set(CACHE_KEY, {"signature": cache_signature, "dataset": dataset})
        return dataset


def get_file_signature(path: str) -> str:
    file_path = Path(path)
    stat = file_path.stat()
    payload = f"{file_path}:{stat.st_mtime_ns}:{stat.st_size}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_finance_sheet(workbook_path: str, sheet_name: str, debug_parse: bool, logger: logging.Logger) -> Dict[str, Any]:
    from openpyxl import load_workbook

    workbook = load_workbook(filename=workbook_path, data_only=True, read_only=True)
    try:
        if sheet_name not in workbook.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' introuvable dans '{workbook_path}'")

        ws = workbook[sheet_name]
        main_headers = read_row_map(ws, 11)
        sub_headers = read_row_map(ws, 12)

        parsed_rows: List[ParsedRow] = []
        affaires: Dict[str, Dict[str, Any]] = {}
        current_affaire_id: Optional[str] = None
        stats = {"rows_total": 0, "rows_skipped": 0, "rows_parent": 0, "rows_mission": 0, "rows_total_line": 0}

        for row_number in range(14, ws.max_row + 1):
            row_data = extract_row_data(ws, row_number)
            stats["rows_total"] += 1

            if is_row_empty(row_data):
                stats["rows_skipped"] += 1
                continue

            row_type = classify_row(row_data, current_affaire_id)
            if row_type == "skip":
                stats["rows_skipped"] += 1
                continue

            if row_type == "parent":
                stats["rows_parent"] += 1
                affaire_id, affaire_payload = upsert_parent_affaire(row_data, affaires)
                current_affaire_id = affaire_id
                parsed_rows.append(ParsedRow(excel_row=row_number, row_type=row_type, data=affaire_payload))
                continue

            if row_type == "total_line":
                stats["rows_total_line"] += 1
                if current_affaire_id and current_affaire_id in affaires:
                    affaires[current_affaire_id]["totaux"] = extract_totals(row_data)
                parsed_rows.append(ParsedRow(excel_row=row_number, row_type=row_type, data=row_data))
                continue

            if row_type == "mission" and current_affaire_id and current_affaire_id in affaires:
                stats["rows_mission"] += 1
                mission_data = normalize_mission_row(row_data, row_number)
                affaires[current_affaire_id]["missions"].append(mission_data)
                parsed_rows.append(ParsedRow(excel_row=row_number, row_type=row_type, data=mission_data))
                continue

            stats["rows_skipped"] += 1

        affaires_list = [
            {
                "id": affaire_id,
                "label": f"{payload.get('numero') or ''} - {payload.get('affaire') or ''}".strip(" -"),
                "client": payload.get("client"),
                "affaire": payload.get("affaire"),
                "numero": payload.get("numero"),
                "tag": payload.get("tag"),
            }
            for affaire_id, payload in affaires.items()
        ]

        debug_payload = {
            "headers": {"main": main_headers, "sub": sub_headers},
            "stats": stats,
        }
        if debug_parse:
            debug_payload["rows"] = [asdict(row) for row in parsed_rows]

        logger.info("Finance parsing complete: %s", stats)
        return {
            "affaires_by_id": affaires,
            "affaires_list": affaires_list,
            "debug": debug_payload,
        }
    finally:
        workbook.close()


def upsert_parent_affaire(row_data: Dict[str, Any], affaires: Dict[str, Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    affaire = row_data.get("affaire") or ""
    numero = row_data.get("numero") or ""
    key_seed = f"{numero}|{affaire}".lower().strip()
    affaire_id = hashlib.md5(key_seed.encode("utf-8")).hexdigest()

    payload = {
        "id": affaire_id,
        "client": row_data.get("client"),
        "affaire": affaire,
        "tag": row_data.get("tag"),
        "numero": numero,
        "delai_reglement_jours": row_data.get("delai_reglement_jours"),
        "commande_ht": row_data.get("commande_ht"),
        "cumul_facturation": {field: row_data.get(field) for field in CUMULATIVE_FIELDS},
        "reste_a_facturer": row_data.get("reste_a_facturer"),
        "mensuel": {
            month: {
                "previsionnel": row_data.get(f"{month}_previsionnel"),
                "facture": row_data.get(f"{month}_facture"),
            }
            for month in MONTHS
        },
        "totaux": {
            "previsionnel": row_data.get("total_previsionnel"),
            "facture": row_data.get("total_facture"),
        },
        "missions": [],
    }

    if affaire_id in affaires:
        existing = affaires[affaire_id]
        for key, value in payload.items():
            if key == "missions":
                continue
            if existing.get(key) in (None, "") and value not in (None, ""):
                existing[key] = value
        return affaire_id, existing

    affaires[affaire_id] = payload
    return affaire_id, payload


def normalize_mission_row(row_data: Dict[str, Any], row_number: int) -> Dict[str, Any]:
    mission_name = row_data.get("tag") or row_data.get("affaire") or f"mission_{row_number}"
    return {
        "excel_row": row_number,
        "mission": mission_name,
        "numero": row_data.get("numero"),
        "commande_ht": row_data.get("commande_ht"),
        "reste_a_facturer": row_data.get("reste_a_facturer"),
        "mensuel": {
            month: {
                "previsionnel": row_data.get(f"{month}_previsionnel"),
                "facture": row_data.get(f"{month}_facture"),
            }
            for month in MONTHS
        },
        "totaux": extract_totals(row_data),
        "raw": row_data,
    }


def extract_totals(row_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "previsionnel": row_data.get("total_previsionnel"),
        "facture": row_data.get("total_facture"),
    }


def classify_row(row_data: Dict[str, Any], current_affaire_id: Optional[str]) -> str:
    head_fields = [row_data.get("client"), row_data.get("affaire"), row_data.get("tag"), row_data.get("numero")]
    head_text = " ".join([str(v).lower() for v in head_fields if v not in (None, "")])

    if "total" in head_text:
        return "total_line"

    has_affaire = row_data.get("affaire") not in (None, "")
    has_numeric_payload = any(
        row_data.get(key) not in (None, "")
        for key in ["commande_ht", "reste_a_facturer", "total_previsionnel", "total_facture"]
    )

    if has_affaire:
        return "parent"

    if current_affaire_id and has_numeric_payload:
        return "mission"

    return "skip"


def read_row_map(ws: Any, row_index: int) -> Dict[str, Any]:
    values = {}
    for column_letter in COLUMN_MAPPING.keys():
        values[COLUMN_MAPPING[column_letter]] = ws[f"{column_letter}{row_index}"].value
    return values


def extract_row_data(ws: Any, row_index: int) -> Dict[str, Any]:
    row = {}
    for column_letter, field_name in COLUMN_MAPPING.items():
        row[field_name] = normalize_cell_value(ws[f"{column_letter}{row_index}"].value)
    return row


def normalize_cell_value(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return value


def is_row_empty(row_data: Dict[str, Any]) -> bool:
    return all(value in (None, "") for value in row_data.values())


app = FinanceASGIApp()
