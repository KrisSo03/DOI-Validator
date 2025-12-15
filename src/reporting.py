from datetime import datetime
from typing import List, Dict
import pandas as pd


def to_dataframe(rows: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if not df.empty:
        sort_cols = [c for c in ["Categoría", "Título match", "Código HTTP", "DOI"] if c in df.columns]
        if sort_cols:
            df.sort_values(by=sort_cols, inplace=True, ignore_index=True)
    return df


def make_txt_report(df: pd.DataFrame) -> str:
    total = len(df)
    valid_count = int((df["Categoría"] == "valid").sum()) if total and "Categoría" in df.columns else 0
    invalid_count = int((df["Categoría"] == "invalid").sum()) if total and "Categoría" in df.columns else 0
    unknown_count = int((df["Categoría"] == "unknown").sum()) if total and "Categoría" in df.columns else 0

    match_count = int((df["Título match"] == "match").sum()) if total and "Título match" in df.columns else 0
    mismatch_count = int((df["Título match"] == "mismatch").sum()) if total and "Título match" in df.columns else 0
    t_unknown_count = int((df["Título match"] == "unknown").sum()) if total and "Título match" in df.columns else 0

    lines = [
        "REPORTE DOI VALIDATOR",
        f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"Total: {total}",
        f"Válidos: {valid_count}",
        f"Inválidos: {invalid_count}",
        f"No verificables: {unknown_count}",
        "",
        f"Title match (Crossref vs Bibliografía) - match: {match_count} | mismatch: {mismatch_count} | unknown: {t_unknown_count}",
        "",
        "-" * 72,
    ]

    for _, r in df.iterrows():
        extra = ""
        if "Título (Crossref)" in df.columns and str(r.get("Título (Crossref)", "")).strip():
            extra += f" | Título(Crossref): {r.get('Título (Crossref)')}"
        if "Título (Bibliografía)" in df.columns and str(r.get("Título (Bibliografía)", "")).strip():
            extra += f" | Título(Biblio): {r.get('Título (Bibliografía)')}"
        if "Título match" in df.columns:
            extra += f" | TitleMatch={r.get('Título match')}"
        if "Score título" in df.columns and str(r.get("Score título", "")).strip():
            extra += f" | Score={r.get('Score título')}"

        lines.append(f"{r.get('Estado','')} | {r.get('DOI','')} | HTTP={r.get('Código HTTP','')} | {r.get('Mensaje','')}{extra}")

    return "\n".join(lines)
