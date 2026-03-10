import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from clients.boond_client import BoondApiError, BoondClient, BoondEndpoints
from services.boond_mapping_service import match_project_name, resolve_best_project_match


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def clean_number(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = clean_text(value).replace("\u202f", "").replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except Exception:
        return 0.0


class BoondImputationService:
    def __init__(self, client: BoondClient, cache_file: Path, hours_per_day: float = 8.0) -> None:
        self.client = client
        self.cache_file = Path(cache_file)
        self.hours_per_day = max(0.1, float(hours_per_day or 8.0))

    def load_cache(self, project: str) -> Optional[Dict[str, Any]]:
        if not self.cache_file.exists():
            return None
        try:
            payload = json.loads(self.cache_file.read_text(encoding="utf-8"))
            if payload.get("project") == project:
                return payload
        except Exception:
            return None
        return None

    def save_cache(self, payload: Dict[str, Any]) -> None:
        try:
            self.cache_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    @staticmethod
    def _find_first_text(obj: Any, keys: List[str]) -> str:
        if isinstance(obj, dict):
            for k in keys:
                if k in obj and clean_text(obj.get(k)):
                    return clean_text(obj.get(k))
            for v in obj.values():
                found = BoondImputationService._find_first_text(v, keys)
                if found:
                    return found
        elif isinstance(obj, list):
            for it in obj:
                found = BoondImputationService._find_first_text(it, keys)
                if found:
                    return found
        return ""

    @staticmethod
    def _find_first_number(obj: Any, keys: List[str]) -> float:
        if isinstance(obj, dict):
            for k in keys:
                if k in obj:
                    n = clean_number(obj.get(k))
                    if n != 0.0:
                        return n
            for v in obj.values():
                found = BoondImputationService._find_first_number(v, keys)
                if found != 0.0:
                    return found
        elif isinstance(obj, list):
            for it in obj:
                found = BoondImputationService._find_first_number(it, keys)
                if found != 0.0:
                    return found
        return 0.0

    @staticmethod
    def _extract_project_name(times_report: Dict[str, Any]) -> str:
        return BoondImputationService._find_first_text(times_report, ["projectName", "project", "mission", "affaire", "project_label", "title"])

    @staticmethod
    def _extract_date(times_report: Dict[str, Any]) -> str:
        return BoondImputationService._find_first_text(times_report, ["date", "workDate", "entryDate", "day", "timesheetDate"])

    def _extract_days(self, times_report: Dict[str, Any]) -> float:
        days = self._find_first_number(times_report, ["days", "day", "jours", "quantityDays"])
        if days > 0:
            return days
        hours = self._find_first_number(times_report, ["hours", "heures", "quantityHours"])
        if hours > 0:
            return hours / self.hours_per_day
        return 0.0

    @staticmethod
    def _month_from_date(date_str: str) -> str:
        txt = clean_text(date_str)
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
            try:
                dt = datetime.strptime(txt[:10], fmt)
                return f"{dt.year:04d}-{dt.month:02d}"
            except Exception:
                continue
        if len(txt) >= 7 and txt[4] == "-":
            return txt[:7]
        return ""

    def resolve_resource_daily_cost(self, administrative_payload: Dict[str, Any]) -> Tuple[float, Optional[str]]:
        """
        Résolution du coût journalier ressource.
        Priorité:
        1) coût journalier complet (costDay / dailyCost)
        2) fallback taux journalier exploitable (rate / tjm)
        3) sinon 0 + warning
        """
        c1 = self._find_first_number(administrative_payload, ["costDay", "dailyCost", "coutJournalier", "cout_journalier"])
        if c1 > 0:
            return c1, None
        c2 = self._find_first_number(administrative_payload, ["tjm", "rate", "dailyRate", "resourceDailyRate"])
        if c2 > 0:
            return c2, None
        return 0.0, "Coût journalier introuvable côté administratif ressource."

    @staticmethod
    def _resource_id(resource: Dict[str, Any]) -> str:
        return clean_text(resource.get("id") or resource.get("resourceId") or resource.get("_id"))

    @staticmethod
    def _resource_name(resource: Dict[str, Any]) -> str:
        return clean_text(resource.get("name") or resource.get("displayName") or resource.get("fullName") or resource.get("label"))

    def _all_resources(self) -> List[Dict[str, Any]]:
        return list(self.client.get_paginated(BoondEndpoints.RESOURCES, params={"maxResults": 100, "sort": "id", "order": "asc"}))

    def build_imputations(self, project: str) -> Dict[str, Any]:
        resources = self._all_resources()
        if not resources:
            raise BoondApiError("boond_resources_empty", "Aucune ressource BOOND disponible.", 404)

        # 1) Construire l'espace des projets candidats depuis les times-reports
        candidate_projects: List[str] = []
        per_resource_reports: Dict[str, List[Dict[str, Any]]] = {}
        for res in resources:
            rid = self._resource_id(res)
            if not rid:
                continue
            reports = list(self.client.get_paginated(BoondEndpoints.resource_times_reports(rid), params={"maxResults": 200, "sort": "date", "order": "asc"}))
            per_resource_reports[rid] = reports
            for row in reports:
                pname = self._extract_project_name(row)
                if pname:
                    candidate_projects.append(pname)

        matching = resolve_best_project_match(project, candidate_projects)
        if not matching.get("matched_project_name"):
            raise BoondApiError("boond_project_not_found", f"Projet BOOND introuvable pour: {project}", 404)
        matched_name = clean_text(matching.get("matched_project_name"))

        # 2) Récupérer administratif ressource + agréger par (resource_id, month)
        grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
        warnings: List[str] = []

        for res in resources:
            rid = self._resource_id(res)
            if not rid:
                continue
            rname = self._resource_name(res) or f"Ressource {rid}"

            try:
                administrative = self.client.get(BoondEndpoints.resource_administrative(rid))
            except BoondApiError as exc:
                if exc.reason == "boond_not_found":
                    administrative = {}
                    warnings.append(f"Données administratives introuvables pour ressource {rid}.")
                else:
                    raise

            cost_day, warn = self.resolve_resource_daily_cost(administrative if isinstance(administrative, dict) else {})
            if warn:
                warnings.append(f"{rname}: {warn}")

            reports = per_resource_reports.get(rid, [])
            for row in reports:
                pname = self._extract_project_name(row)
                if not pname or match_project_name(matched_name, pname) < 70:
                    continue
                month = self._month_from_date(self._extract_date(row))
                if not month:
                    continue
                days = self._extract_days(row)
                if days <= 0:
                    continue
                key = (rid, month)
                rec = grouped.setdefault(key, {
                    "resource_id": rid,
                    "resource": rname,
                    "month": month,
                    "days": 0.0,
                    "cost_day": cost_day,
                    "total_cost": 0.0,
                })
                rec["days"] += days
                rec["cost_day"] = cost_day
                rec["total_cost"] = rec["days"] * rec["cost_day"]

        imputations = []
        total_days = 0.0
        total_cost = 0.0
        for rec in grouped.values():
            rec["days"] = round(clean_number(rec.get("days")), 2)
            rec["cost_day"] = round(clean_number(rec.get("cost_day")), 2)
            rec["total_cost"] = round(clean_number(rec.get("total_cost")), 2)
            total_days += rec["days"]
            total_cost += rec["total_cost"]
            imputations.append(rec)

        imputations.sort(key=lambda x: (x.get("month", ""), x.get("resource", "")))

        response = {
            "project": project,
            "matching": {
                "input_project": project,
                "matched_project_name": matched_name,
                "confidence": matching.get("confidence", 0),
                "warning": bool(matching.get("warning", False)),
            },
            "source": "boond_api",
            "generated_at": now_iso(),
            "imputations": imputations,
            "totals": {
                "days": round(total_days, 2),
                "total_cost": round(total_cost, 2),
            },
        }
        if matching.get("warning_message"):
            response["matching"]["warning_message"] = matching.get("warning_message")
        if warnings:
            response["warnings"] = sorted(set(warnings))
        return response

    def get_imputations(self, project: str, refresh: bool = False) -> Dict[str, Any]:
        project = clean_text(project)
        if not project:
            raise BoondApiError("project_missing", "Le paramètre project est requis.", 400)

        if not self.client.enabled:
            raise BoondApiError("boond_not_configured", "BOOND non configuré: renseigner serveur/login/password.", 400)

        if not refresh:
            cached = self.load_cache(project)
            if cached:
                return cached

        payload = self.build_imputations(project)
        self.save_cache(payload)
        return payload
