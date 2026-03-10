import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, Optional


class BoondApiError(Exception):
    def __init__(self, reason: str, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.status_code = status_code


class BoondEndpoints:
    """Endpoints BOOND centralisés pour éviter de disperser les chemins API."""

    RESOURCES = "/resources"

    @staticmethod
    def resource_times_reports(resource_id: str) -> str:
        return f"/resources/{resource_id}/times-reports"

    @staticmethod
    def resource_administrative(resource_id: str) -> str:
        return f"/resources/{resource_id}/administrative"


class BoondClient:
    """
    Client BOOND API.

    Authentification:
    - Basic Auth: Authorization: Basic base64(login:password)

    Base URL:
    - https://{BOOND_API_SERVER}/api/{BOOND_API_VERSION}
    """

    def __init__(self, api_server: str, api_version: str, login: str, password: str, timeout: int = 30) -> None:
        self.api_server = (api_server or "").strip()
        self.api_version = (api_version or "1.0").strip()
        self.login = (login or "").strip()
        self.password = password or ""
        self.timeout = int(timeout or 30)

    @property
    def enabled(self) -> bool:
        return bool(self.api_server and self.login and self.password)

    @property
    def base_url(self) -> str:
        return f"https://{self.api_server}/api/{self.api_version}"

    def _auth_header(self) -> str:
        pair = f"{self.login}:{self.password}".encode("utf-8")
        token = base64.b64encode(pair).decode("ascii")
        return f"Basic {token}"

    def _get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if not self.enabled:
            raise BoondApiError("boond_not_configured", "Configuration BOOND incomplète.", status_code=400)

        query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v not in (None, "")})
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        req = urllib.request.Request(url)
        req.add_header("Authorization", self._auth_header())
        req.add_header("Accept", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
            return json.loads(raw)
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise BoondApiError("boond_auth_failed", "Authentification BOOND invalide ou accès API non autorisé.", 401)
            if exc.code == 403:
                raise BoondApiError("boond_forbidden", "Accès BOOND refusé pour cet utilisateur.", 403)
            if exc.code == 404:
                raise BoondApiError("boond_not_found", f"Endpoint BOOND introuvable: {path}", 404)
            if exc.code == 429:
                raise BoondApiError("boond_rate_limited", "Trop de requêtes vers BOOND (429).", 429)
            if exc.code >= 500:
                raise BoondApiError("boond_server_error", f"Erreur serveur BOOND ({exc.code}).", 502)
            raise BoondApiError("boond_http_error", f"Erreur HTTP BOOND ({exc.code}).", 502)
        except TimeoutError:
            raise BoondApiError("boond_timeout", "Timeout lors de l'appel BOOND.", 504)
        except Exception as exc:
            raise BoondApiError("boond_network_error", f"Erreur réseau BOOND: {exc}", 502)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._get_json(path, params=params)

    def get_paginated(self, path: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
        """
        Pagination standard avec:
        - page
        - maxResults
        - order
        - sort

        Arrêt auto si page vide ou nombre d'éléments < maxResults.
        """
        q = dict(params or {})
        page = int(q.pop("page", 1) or 1)
        max_results = int(q.pop("maxResults", 100) or 100)
        order = q.pop("order", "asc")
        sort = q.pop("sort", "id")

        while True:
            payload = self._get_json(path, params={**q, "page": page, "maxResults": max_results, "order": order, "sort": sort})
            rows = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(rows, list):
                rows = []
            for row in rows:
                if isinstance(row, dict):
                    yield row
            if not rows or len(rows) < max_results:
                break
            page += 1
