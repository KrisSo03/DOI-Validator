import time
from typing import Dict, Tuple
import requests


def validate_doi_http(
    doi: str,
    timeout: float,
    max_retries: int,
    cache: Dict[str, Dict],
) -> Tuple[str, bool, str, int, str, float]:
    """
    Returns: (doi, ok, category, status, message, response_time)
    category: valid | invalid | unknown
    """
    key = doi.lower()
    if key in cache:
        c = cache[key]
        return doi, c["ok"], c["category"], c["status"], c["message"], c["time"]

    url = f"https://doi.org/{doi}"
    headers = {
        "User-Agent": "Mozilla/5.0 (DOI Validator)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7,es;q=0.5",
    }

    start = time.time()

    def store(ok: bool, cat: str, status: int, msg: str):
        rt = time.time() - start
        cache[key] = {"ok": ok, "category": cat, "status": status, "message": msg, "time": rt}
        return doi, ok, cat, status, msg, rt

    for attempt in range(max_retries):
        try:
            # 1) HEAD primero (rápido)
            r = requests.head(url, headers=headers, allow_redirects=True, timeout=timeout)
            status = r.status_code

            # 2) Fallback a GET cuando HEAD es "sospechoso" o inconcluso
            #   - 405: método no permitido
            #   - 403: posible bloqueo
            #   - 404: a veces HEAD da 404 aunque GET resuelve (caso que te está pegando)
            #   - 400: puede ser comportamiento raro; intentamos GET para confirmar
            #   - >=500: error servidor
            if status in (405, 403, 404, 400) or status >= 500:
                r = requests.get(
                    url, headers=headers, allow_redirects=True, timeout=timeout, stream=True
                )
                status = r.status_code

            # 3) Interpretación final (ya con el status de la "mejor" prueba)
            if 200 <= status < 400:
                return store(True, "valid", status, f"✓ Resuelve (HTTP {status})")

            if status == 429:
                if attempt < max_retries - 1:
                    time.sleep((2 ** attempt) * 1.0)
                    continue
                return store(False, "unknown", status, "⚠️ Rate limit (HTTP 429)")

            if 500 <= status < 600:
                if attempt < max_retries - 1:
                    time.sleep((2 ** attempt) * 0.8)
                    continue
                return store(False, "unknown", status, f"⚠️ Error servidor (HTTP {status})")

            # 4) Mensajes más precisos para 404 / 400
            if status == 404:
                # OJO: ahora sí es 404 después de intentar GET también
                return store(False, "invalid", status, "✗ doi.org respondió 404 (no resolvió el DOI)")
            if status == 400:
                return store(False, "invalid", status, "✗ doi.org respondió 400 (Bad Request; posible DOI/formato inválido)")

            # 5) Otros casos
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) * 0.6)
                continue
            return store(False, "unknown", status, f"⚠️ Respuesta no concluyente (HTTP {status})")

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) * 1.0)
                continue
            return store(False, "unknown", 0, "⚠️ Timeout")
        except requests.exceptions.ConnectionError:
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) * 1.0)
                continue
            return store(False, "unknown", 0, "⚠️ Error de conexión")
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep((2 ** attempt) * 0.6)
                continue
            return store(False, "unknown", 0, f"⚠️ Error: {type(e).__name__}: {str(e)[:80]}")

    return store(False, "unknown", 0, "⚠️ Máximo de reintentos alcanzado")

