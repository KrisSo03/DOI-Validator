import re
from typing import Optional, Tuple, List
from .pdf_extract import normalize_text

REF_START = re.compile(
    r"""
^\s*
(?:\d+[\.\)]\s*)?
(?:references
|bibliography
|works\s+cited
|literature\s+cited
|referencias
|bibliograf[ií]a
|referencias\s+bibliogr[aá]ficas
|obras\s+citadas
|literatura\s+citada
)
\s*[:\-]?\s*$
""",
    re.IGNORECASE | re.VERBOSE,
)

REF_END = re.compile(
    r"""
^\s*
(?:\d+[\.\)]\s*)?
(?:appendix|ap[eé]ndice|annex|anexo
|acknowledg(e)?ments|agradecimientos
|supplementary|material\s+suplementario
|funding|financiamiento
|author\s+contributions|contribuci[oó]n\s+de\s+autores
|conflict\s+of\s+interest|conflicto\s+de\s+inter[eé]s
)
\s*[:\-]?\s*$
""",
    re.IGNORECASE | re.VERBOSE,
)


def slice_references_section(full_text: str, min_lines_after: int = 12) -> Tuple[str, Optional[int], Optional[int]]:
    lines = full_text.splitlines()
    start = None
    for i, line in enumerate(lines):
        if REF_START.match(line.strip()):
            start = i
            break
    if start is None:
        return full_text, None, None

    end = len(lines)
    for j in range(start + 1, len(lines)):
        if REF_END.match(lines[j].strip()):
            if (j - start) >= min_lines_after:
                end = j
                break

    ref_text = "\n".join(lines[start:end]).strip()
    if len(ref_text) < 250:
        return full_text, None, None
    return ref_text, start, end


def extract_reference_lines(ref_text: str) -> List[str]:
    lines = [ln.strip() for ln in normalize_text(ref_text).splitlines()]
    lines = [ln for ln in lines if len(ln) >= 35]
    lines = [ln for ln in lines if not REF_START.match(ln)]
    return lines
