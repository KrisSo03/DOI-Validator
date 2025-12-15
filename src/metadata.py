from typing import Optional, Tuple
import re
import unicodedata
from difflib import SequenceMatcher

import requests


def _strip_accents(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_title(t: str) -> str:
    t = (t or "").strip()
    t = _strip_accents(t).lower()
    t = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", t)  # guiones unicode
    t = re.sub(r"[^a-z0-9\s\-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def title_match_score(bibliographic_title: Optional[str], crossref_title: Optional[str]) -> Optional[float]:
    """
    Retorna un score 0-1 (más alto = más parecido).
    Si falta alguno de los dos títulos, retorna None.
    """
    if not bibliographic_title or not crossref_title:
        return None

    a = normalize_title(bibliographic_title)
    b = normalize_title(crossref_title)
    if not a or not b:
        return None

    # Similaridad por secuencia (tolerante a pequeñas diferencias)
    seq = SequenceMatcher(None, a, b).ratio()

    # Jaccard por tokens (tolerante a reordenamientos)
    ta = set(a.split())
    tb = set(b.split())
    jacc = (len(ta & tb) / max(1, len(ta | tb))) if (ta or tb) else 0.0

    # Mezcla conservadora
    score = max(seq, jacc, (seq + jacc) / 2.0)
    return float(round(score, 4))


def title_match_label(score: Optional[float], threshold: float) -> str:
    if score is None:
        return "unknown"
    return "match" if score >= threshold else "mismatch"


def crossref_title_by_doi(doi: str, timeout: float = 15.0) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns: (title, container_or_publisher)
    """
    url = f"https://api.crossref.org/works/{doi}"
    headers = {"User-Agent": "doi-validator/1.0 (mailto:example@example.com)"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return None, None
        data = r.json().get("message", {}) or {}
        title_list = data.get("title") or []
        title = title_list[0].strip() if title_list else None
        container = (data.get("container-title") or [None])[0]
        publisher = data.get("publisher")
        return title, (container or publisher)
    except Exception:
        return None, None


def crossref_search_by_bibliographic(query: str, timeout: float = 15.0) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns: (title, doi, container_or_publisher)
    """
    q = (query or "").strip()
    if len(q) < 12:
        return None, None, None

    url = "https://api.crossref.org/works"
    headers = {"User-Agent": "doi-validator/1.0 (mailto:example@example.com)"}
    params = {"query.bibliographic": q, "rows": 1}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return None, None, None
        items = (r.json().get("message", {}) or {}).get("items", []) or []
        if not items:
            return None, None, None
        item = items[0]
        title_list = item.get("title") or []
        title = title_list[0].strip() if title_list else None
        doi = item.get("DOI")
        container = (item.get("container-title") or [None])[0]
        publisher = item.get("publisher")
        return title, doi, (container or publisher)
    except Exception:
        return None, None, None
