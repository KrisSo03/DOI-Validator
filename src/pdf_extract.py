import unicodedata
from typing import List, Tuple
import pdfplumber


def normalize_text(t: str) -> str:
    t = unicodedata.normalize("NFKC", t or "")
    t = t.replace("\u00ad", "")  # soft hyphen
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    return t


def extract_text_pages(pdf_file) -> Tuple[List[str], str]:
    pages_text: List[str] = []
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            pages_text.append(normalize_text(page.extract_text() or ""))
    method = "pdfplumber"
    if len("".join(pages_text).strip()) < 120:
        method = "pdfplumber (texto limitado; posible PDF escaneado)"
    return pages_text, method
