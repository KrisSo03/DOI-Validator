import re
from typing import Dict, List, Optional

from .pdf_extract import normalize_text

DOI_PATTERNS = [
    r"doi:\s*(10\.\d{4,9}(?:\.\d+)*\/(?:(?![\"&\'<>])\S)+)",
    r"https?://(?:dx\.)?doi\.org/(10\.\d{4,9}(?:\.\d+)*\/(?:(?![\"&\'<>])\S)+)",
    r"(?:^|[\s\(\[{,;:])(10\.\d{4,9}(?:\.\d+)*\/(?:(?![\"&\'<>])\S)+)",
    r"[\[\(\{](10\.\d{4,9}(?:\.\d+)*\/(?:(?![\"&\'<>])\S)+)[\]\)\}]",
    r"(?:DOI|doi|Doi)[\s:]+(10\.\d{4,9}(?:\.\d+)*\/(?:(?![\"&\'<>])\S)+)",
]


# =========================================================
# Normalización extra para DOIs "rotos" por PDF
# (guiones de partición, saltos de línea/página, cortes alrededor de '/')
# =========================================================
def normalize_text_for_doi_extraction(text: str) -> str:
    """
    Repara artefactos típicos de PDFs antes de extraer DOIs:
    - \f (salto de página)
    - cortes con guion + salto de línea: 9928-\n254 -> 9928254
    - cortes después de '/': 10.xxxx/\nxxxxx -> 10.xxxx/xxxxx
    - cortes antes de '/': 10.xxxx\n/xxxxx -> 10.xxxx/xxxxx
    """
    if not text:
        return ""

    t = text

    # Saltos de página (form feed)
    t = t.replace("\f", "\n")

    # 1) Unir palabras/DOIs partidos por guion de final de línea (hyphenation)
    # SOLO si el guion está justo antes de un salto (o espacios + salto).
    # Ej: "9928-\n254" o "9928- \n 254" => "9928254"
    t = re.sub(r"-\s*\n\s*", "", t)

    # 2) Unir cortes alrededor de "/" (muy común en DOIs)
    t = re.sub(r"/\s*\n\s*", "/", t)      # después de /
    t = re.sub(r"\s*\n\s*/", "/", t)      # antes de /

    return t


def clean_doi(doi: str) -> str:
    """
    Limpieza del DOI ya capturado:
    - normaliza entidades HTML
    - elimina espacios
    - elimina puntuación colgante
    Nota: La "reparación" de guiones por salto de línea se hace ANTES (en normalize_text_for_doi_extraction).
    """
    doi = normalize_text(doi)
    doi = re.sub(r"\s+", "", doi)
    doi = (
        doi.replace("&quot;", "")
        .replace("&#34;", "")
        .replace("&nbsp;", "")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
    )
    doi = re.sub(r"[.,;:)\]}\'\"]+$", "", doi)
    doi = re.sub(r"\.{2,}$", "", doi)
    return doi.strip()


def is_valid_doi_format(doi: str) -> bool:
    if not re.match(r"^10\.\d{4,9}(?:\.\d+)*\/.+$", doi):
        return False
    parts = doi.split("/", 1)
    if len(parts) < 2:
        return False
    suffix = parts[1].strip()
    if len(suffix) < 2:
        return False
    invalid_chars = ["<", ">", '"', "{", "}", "|", "\\", "^", "`", " "]
    return not any(c in doi for c in invalid_chars)


def extract_dois_from_text(text: str, max_context: int = 60) -> List[Dict]:
    """
    Extracts DOIs from text using multiple patterns.
    Returns list of dicts with: doi, raw, pattern, position, context

    FIX: Antes de normalize_text(), repara artefactos de PDFs que "rompen" DOIs.
    """
    # 1) Reparar texto crudo para DOIs partidos
    repaired = normalize_text_for_doi_extraction(text or "")

    # 2) Normalización general (la tuya)
    t = normalize_text(repaired)

    out: List[Dict] = []

    for idx, pat in enumerate(DOI_PATTERNS, start=1):
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            raw = m.group(1)
            doi = clean_doi(raw)
            if not doi or not is_valid_doi_format(doi):
                continue

            start = max(0, m.start() - max_context)
            end = min(len(t), m.end() + max_context)
            context = t[start:end].replace("\n", " ").strip()

            out.append(
                {"doi": doi, "raw": raw, "pattern": f"Patrón {idx}", "position": m.start(), "context": context}
            )

    out.sort(key=lambda x: x["position"])
    return out


def assign_page(dois_info: List[Dict], pages_text: List[str]) -> None:
    """
    FIX: Para evitar "N/A" cuando el DOI está partido en el texto de la página,
    se aplica la misma reparación (normalize_text_for_doi_extraction) por página
    y se compara en versión "compacta" (sin espacios).
    """
    for d in dois_info:
        d["page"] = "N/A"
        target = (d.get("doi") or "").lower()
        if not target:
            continue

        target_compact = re.sub(r"\s+", "", target)

        for pi, ptxt in enumerate(pages_text, 1):
            page_repaired = normalize_text_for_doi_extraction(ptxt or "")
            page_norm = normalize_text(page_repaired).lower()
            page_compact = re.sub(r"\s+", "", page_norm)

            if target_compact in page_compact:
                d["page"] = pi
                break


def find_reference_line_for_doi(doi: str, reference_lines: List[str]) -> Optional[str]:
    """
    Intenta ubicar la línea de referencia que contiene el DOI.

    FIX: además de compactar espacios, repara posibles artefactos de partición
    dentro de cada línea (por si extract_reference_lines devuelve fragmentos con \n).
    """
    doi_norm = re.sub(r"\s+", "", (doi or "").lower())
    if not doi_norm:
        return None

    for ln in reference_lines or []:
        ln_rep = normalize_text_for_doi_extraction(ln or "")
        ln_norm = re.sub(r"\s+", "", normalize_text(ln_rep).lower())
        if doi_norm in ln_norm:
            return normalize_text(ln_rep)
    return None

def find_reference_candidates_for_doi(doi: str, reference_lines: List[str]) -> List[tuple[int, str]]:
    """Devuelve TODOS los índices/líneas donde aparece el DOI (no solo la primera)."""
    doi_norm = re.sub(r"\s+", "", (doi or "").lower())
    if not doi_norm:
        return []
    out = []
    for i, ln in enumerate(reference_lines or []):
        ln_rep = normalize_text_for_doi_extraction(ln or "")
        ln_norm = re.sub(r"\s+", "", normalize_text(ln_rep).lower())
        if doi_norm in ln_norm:
            out.append((i, normalize_text(ln_rep)))
    return out


def build_reference_block_around_index(reference_lines: List[str], idx: int, before: int = 5, after: int = 1) -> str:
    """
    Construye un bloque tomando más líneas ANTES que DESPUÉS.
    Ideal cuando el DOI aparece al final de la referencia.
    """
    start = max(0, idx - before)
    end = min(len(reference_lines), idx + after + 1)
    block = " ".join(reference_lines[start:end])
    block = normalize_text_for_doi_extraction(block)
    block = normalize_text(block)
    block = re.sub(r"\s+", " ", block).strip()
    return block


def _is_plausible_title_for_selection(t: Optional[str]) -> bool:
    """Filtro ligero para evitar 'pp', 'vol', años, etc."""
    if not t:
        return False
    s = re.sub(r"\s+", " ", t).strip()
    low = s.lower()
    if len(s.split()) < 5:
        return False
    if re.search(r"\bpp\b|\bvol\b|\bno\b|\bissue\b|\bpages?\b", low) and len(s.split()) < 8:
        return False
    if re.fullmatch(r"(19|20)\d{2}([,;]\s*)?(pp\.?)?", low):
        return False
    return True


def pick_best_reference_block_for_doi(
    doi: str,
    reference_lines: List[str],
    crossref_title: str = "",
    before: int = 5,
    after: int = 1,
) -> tuple[str, Optional[str], float]:
    """
    Selecciona el mejor bloque de referencia para el DOI.
    Si hay título Crossref, usa ese título como ancla para escoger el bloque correcto.

    Returns: (best_block, best_bib_title, selection_score)
    """
    cands = find_reference_candidates_for_doi(doi, reference_lines)
    if not cands:
        return "", None, 0.0

    best_block = ""
    best_title = None
    best_score = 0.0

    cr = normalize_text(crossref_title or "")
    for idx, _ln in cands:
        block = build_reference_block_around_index(reference_lines, idx, before=before, after=after)
        bib_title = extract_bibliographic_title(block)

        if not _is_plausible_title_for_selection(bib_title):
            continue

        if cr:
            s = title_match_score(bib_title, cr) or 0.0
        else:
            # Sin Crossref: escoger el más “largo/estable” como heurística mínima
            s = min(1.0, len(bib_title) / 200.0)

        if s > best_score:
            best_score = s
            best_block = block
            best_title = bib_title

    # Si todo fue ruido, al menos devuelve el bloque simple del primer candidato
    if not best_block:
        idx0, _ = cands[0]
        best_block = build_reference_block_around_index(reference_lines, idx0, before=before, after=after)
        best_title = extract_bibliographic_title(best_block)
        best_score = 0.0

    return best_block, best_title, best_score


def extract_bibliographic_title(reference_line: Optional[str]) -> Optional[str]:
    """
    Heurística principal:
      - El título generalmente va después del ')' que cierra la fecha (p.ej. (2018) o (2018a) o (n.d.)).
    Fallback:
      - Si es una referencia web con [Organización] y (n.d.), el título suele venir antes del bracket.
    """
    if not reference_line:
        return None

    # FIX: repara cortes antes de normalizar, por si la referencia venía fragmentada
    ln = normalize_text(normalize_text_for_doi_extraction(reference_line))

    # Remover URLs y DOI del texto para que no contaminen la extracción
    ln = re.sub(r"https?://\S+", "", ln, flags=re.IGNORECASE)
    ln = re.sub(r"\bdoi:\s*\S+", "", ln, flags=re.IGNORECASE)
    ln = re.sub(r"\b10\.\d{4,9}(?:\.\d+)*\/\S+", "", ln, flags=re.IGNORECASE)
    ln = re.sub(r"\s+", " ", ln).strip()

    # Fallback web común: "Título [Organización]. (n.d.). Retrieved from ..."
    # En este caso, el título suele estar ANTES del bracket.
    if re.search(r"\(\s*n\.d\.\s*\)", ln, flags=re.IGNORECASE) and "[" in ln and "]" in ln:
        before_bracket = ln.split("[", 1)[0].strip()
        if 8 <= len(before_bracket) <= 300:
            return before_bracket

    # Regla principal: título inicia después de ')' del año / n.d.
    # Acepta: (2018), (2018a), (2023b), (n.d.)
    m = re.search(r"\(\s*((?:19|20)\d{2}[a-z]?|n\.d\.)\s*\)\s*", ln, flags=re.IGNORECASE)
    if m:
        rest = ln[m.end():].lstrip()

        # Saltar puntuación típica justo después del año: ").", ") ." etc.
        rest = re.sub(r"^[\.\:\;\-–—\s]+", "", rest).strip()

        # Cortes típicos: antes de "Retrieved from"
        rest = re.split(r"\bRetrieved\s+from\b", rest, maxsplit=1, flags=re.IGNORECASE)[0].strip()

        # Tomar hasta el primer punto "fuerte" (separador usual entre título y fuente)
        parts = [p.strip() for p in rest.split(".") if p.strip()]
        if parts:
            cand = parts[0].strip()
            if 8 <= len(cand) <= 300:
                return cand

        # Si no se pudo, devolver lo que haya (controlado)
        if 8 <= len(rest) <= 300:
            return rest

    # Fallback adicional (si no hay año en paréntesis):
    # Caso 1: “Título” entre comillas
    mq = re.search(r"[\"“”](.+?)[\"“”]", ln)
    if mq:
        cand = mq.group(1).strip()
        if len(cand) >= 8:
            return cand

    # Caso 2: fallback por segmentos (entre 1er y 2do punto)
    parts = [p.strip() for p in ln.split(".") if p.strip()]
    if len(parts) >= 3:
        cand = parts[1]
        if 8 <= len(cand) <= 300:
            return cand

    return None
