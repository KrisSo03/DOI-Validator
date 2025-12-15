from __future__ import annotations

import re
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional PDF extractor
try:
    import pdfplumber  # type: ignore
except Exception:  # pragma: no cover
    pdfplumber = None

try:
    from PyPDF2 import PdfReader  # type: ignore
except Exception as e:  # pragma: no cover
    PdfReader = None  # type: ignore

from src.pdf_extract import normalize_text
from src.references import slice_references_section, extract_reference_lines
from src.doi_extract import clean_doi, is_valid_doi_format

FIGSHARE_BASE = "https://api.figshare.com/v2"


# =========================================================
# Requests: sesión con reintentos (robusto)
# =========================================================
def session_with_retries(total: int = 6, backoff_factor: float = 0.5) -> requests.Session:
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    s = requests.Session()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


# =========================================================
# Figshare API
# =========================================================
def figshare_list_theses(limit: int = 50, timeout_sec: float = 30.0) -> List[Dict[str, Any]]:
    """Lista tesis (o artículos tipo tesis) desde Figshare con paginación.
    Intenta item_type=3 y luego 8 para compatibilidad.
    """
    s = session_with_retries()
    page_size = 50

    for item_type in (3, 8):
        out: List[Dict[str, Any]] = []
        page = 1
        while len(out) < int(limit):
            params = {
                "item_type": item_type,
                "page": page,
                "page_size": min(page_size, int(limit) - len(out)),
                "order": "published_date",
                "order_direction": "desc",
            }
            r = s.get(f"{FIGSHARE_BASE}/articles", params=params, timeout=float(timeout_sec))
            if r.status_code >= 400:
                break
            batch = r.json() or []
            if not isinstance(batch, list) or not batch:
                break
            out.extend(batch)
            page += 1

        if out:
            return out[: int(limit)]

    return []


def figshare_article_detail(article_id: int, timeout_sec: float = 30.0) -> Optional[Dict[str, Any]]:
    s = session_with_retries()
    try:
        r = s.get(f"{FIGSHARE_BASE}/articles/{int(article_id)}", timeout=float(timeout_sec))
        if r.status_code >= 400:
            return None
        data = r.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def figshare_extract_pdf_urls(detail: Dict[str, Any]) -> List[str]:
    files = (detail or {}).get("files", []) or []
    pdfs: List[str] = []
    for f in files:
        url = f.get("download_url")
        name = (f.get("name") or "").lower()
        mime = (f.get("mime_type") or "").lower()
        if url and (name.endswith(".pdf") or mime == "application/pdf"):
            pdfs.append(url)
    return pdfs


def figshare_download_pdf_bytes(url: str, timeout_sec: float = 60.0) -> bytes:
    s = session_with_retries()
    r = s.get(url, timeout=float(timeout_sec))
    r.raise_for_status()
    return r.content


# =========================================================
# PDF text extraction
# =========================================================
def extract_text_from_pdf_bytes(pdf_bytes: bytes, mode: str = "tail", max_pages_from_end: int = 10) -> str:
    """Extrae texto del PDF.
    mode: 'tail' (últimas N páginas) o 'full' (todo).
    Preferencia: pdfplumber si está disponible, si no PyPDF2.
    """
    if pdfplumber is not None:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            pages = pdf.pages
            chosen = pages if mode == "full" else (pages[-max_pages_from_end:] if len(pages) > max_pages_from_end else pages)
            parts: List[str] = []
            for p in chosen:
                parts.append(normalize_text(p.extract_text() or ""))
            return "\n".join(parts)

    if PdfReader is None:
        return ""

    reader = PdfReader(BytesIO(pdf_bytes))
    total = len(reader.pages)
    start = 0 if mode == "full" else max(0, total - int(max_pages_from_end))
    parts = []
    for i in range(start, total):
        try:
            parts.append(normalize_text(reader.pages[i].extract_text() or ""))
        except Exception:
            continue
    return "\n".join(parts)


# =========================================================
# DOI extraction robusta (saltos de línea / guiones)
# =========================================================
_DOI_REGEX_ROBUST = re.compile(r"(10\.\d{4,9}(?:\.\d+)*\s*/\s*[-._;()/:A-Z0-9]+)", flags=re.IGNORECASE)
_TRAILING_PUNCT = ".,;:)]}>\"'"

def _normalize_for_doi_harvest(t: str) -> str:
    if not t:
        return ""
    t = t.replace("\u00ad", "")  # soft hyphen
    t = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", t)  # une cortes con guion
    t = re.sub(r"(\S)\s*\n\s*(\S)", r"\1 \2", t)  # une saltos de línea
    t = re.sub(r"\s*/\s*", "/", t)  # normaliza slash
    return t

def extract_dois_robust(text: str, max_context: int = 60) -> List[Dict[str, Any]]:
    t = _normalize_for_doi_harvest(normalize_text(text or ""))
    out: List[Dict[str, Any]] = []
    for m in _DOI_REGEX_ROBUST.finditer(t):
        raw = (m.group(1) or "").strip().rstrip(_TRAILING_PUNCT)
        doi = clean_doi(raw).rstrip(_TRAILING_PUNCT)
        if not doi or not is_valid_doi_format(doi):
            continue
        start = max(0, m.start() - max_context)
        end = min(len(t), m.end() + max_context)
        ctx = t[start:end].replace("\n", " ").strip()
        out.append({"doi": doi, "raw": raw, "pattern": "Robusto", "position": m.start(), "context": ctx})

    out.sort(key=lambda x: x["position"])
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for d in out:
        k = d["doi"].lower()
        if k in seen:
            continue
        uniq.append(d)
        seen.add(k)
    return uniq


# =========================================================
# Helpers para referencias
# =========================================================
def find_reference_line_for_doi(doi: str, reference_lines: List[str]) -> Optional[str]:
    doi_norm = re.sub(r"\s+", "", (doi or "").lower())
    if not doi_norm:
        return None
    for ln in reference_lines or []:
        ln_norm = re.sub(r"\s+", "", (ln or "").lower())
        if doi_norm in ln_norm:
            return ln
    return None


def process_pdf_bytes_to_doi_rows(
    pdf_bytes: bytes,
    file_name: str,
    mode: str = "tail",
    max_pages_from_end: int = 10,
    prefer_refs_section: bool = True,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    base_text = extract_text_from_pdf_bytes(pdf_bytes, mode=mode, max_pages_from_end=max_pages_from_end)
    base_text = normalize_text(base_text or "")

    if prefer_refs_section:
        ref_text, _, _ = slice_references_section(base_text)
        text_for_dois = ref_text or base_text
    else:
        text_for_dois = base_text

    dois_info = extract_dois_robust(text_for_dois)
    reference_lines = extract_reference_lines(text_for_dois)

    for d in dois_info:
        d["file_name"] = file_name
        d["page"] = "N/A"
        d["reference_line"] = find_reference_line_for_doi(d["doi"], reference_lines) or ""

    return dois_info, reference_lines
