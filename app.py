from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.doi_validate import validate_doi_http
from src.metadata import crossref_title_by_doi, title_match_score, title_match_label
from src.reporting import to_dataframe, make_txt_report
from src.doi_extract import clean_doi, is_valid_doi_format

# ---- Utilidades (Figshare + extracción robusta) ----
from documento import (
    figshare_list_theses,
    figshare_article_detail,
    figshare_extract_pdf_urls,
    figshare_download_pdf_bytes,
    process_pdf_bytes_to_doi_rows,
    extract_dois_robust,
)

# =========================
# CONFIG UI
# =========================
st.set_page_config(page_title="DOI Validator", page_icon="📚", layout="wide")

PALETTE = {
    "azul": "#0B1D51",
    "morado": "#725CAD",
    "celeste": "#8CCDEB",
    "crema": "#FFE3A9",
    "gris": "#6B7280",
    "naranja": "#FF8C42",  # Para sospechosos
    "amarillo": "#FFD93D",  # Para desconocidos
}
STATUS_COLORS = {
    "válido": PALETTE["morado"], 
    "inválido": PALETTE["azul"], 
    "sospechoso": PALETTE["naranja"],
    "desconocido": PALETTE["celeste"]
}
TITLE_MATCH_COLORS = {"coincide": PALETTE["morado"], "no_coincide": PALETTE["azul"], "desconocido": PALETTE["celeste"]}
PAGE_FONT = "Inter, Source Sans Pro, sans-serif"


def _safe_int(x) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def _apply_layout(fig: go.Figure, titulo: str = "", titulo_x: str = "", titulo_y: str = "", altura: int = 360):
    fig.update_layout(
        title=dict(text=titulo, x=0.0, xanchor="left", font=dict(size=18, family=PAGE_FONT)),
        height=altura,
        margin=dict(t=70, b=55, l=70, r=25),
        font=dict(family=PAGE_FONT, size=13),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(title="", orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0.0),
        uniformtext=dict(minsize=10, mode="hide"),
    )
    fig.update_xaxes(title_text=titulo_x, automargin=True, showgrid=True, gridcolor="rgba(0,0,0,0.08)", zeroline=False)
    fig.update_yaxes(title_text=titulo_y, automargin=True, showgrid=True, gridcolor="rgba(0,0,0,0.08)", zeroline=False)
    return fig


def _dedupe_dois(dois_info: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for d in sorted(dois_info, key=lambda x: x.get("position", 0)):
        k = (d.get("doi") or "").lower()
        if not k or k in seen:
            continue
        out.append(d)
        seen.add(k)
    return out


def _parse_pasted_dois(text: str) -> List[Dict[str, Any]]:
    # Extrae DOIs de cualquier pegado (líneas, URLs, texto)
    found = extract_dois_robust(text or "")
    rows = []
    for d in found:
        rows.append(
            {
                **d,
                "file_name": "Pegado",
                "page": "N/A",
                "reference_line": "",
                "bib_title": "",
            }
        )
    return rows


def extract_title_by_style(reference: str, style: str) -> str:
    """
    Extrae el título de una referencia bibliográfica según el estilo de citación.
    
    Estilos soportados:
    - APA 7: Título en cursiva después del año, antes del nombre de revista/libro
    - IEEE: Título entre comillas después de autores
    - MLA: Título en cursiva después de autores
    - Chicago: Título en cursiva después de autores y año
    - Vancouver: Título después de autores, termina en punto
    """
    if not reference or not reference.strip():
        return ""
    
    ref = reference.strip()
    
    # APA 7: Autor(es). (Año). Título en cursiva. Revista/Editorial.
    # Patrón: después de (año) buscar texto hasta punto o revista
    if style == "APA 7":
        # Buscar patrón (año). Título
        match = re.search(r'\((\d{4}[a-z]?)\)\.\s*(.+?)\.(?:\s+[A-Z]|\s+http|$)', ref, re.IGNORECASE)
        if match:
            title = match.group(2).strip()
            # Limpiar posibles URLs y DOIs del título
            title = re.sub(r'https?://\S+', '', title)
            title = re.sub(r'doi:\s*\S+', '', title, flags=re.IGNORECASE)
            return title.strip()
        
        # Patrón alternativo: buscar después de año hasta Vol., pp., o revista
        match = re.search(r'\((\d{4}[a-z]?)\)\.\s*(.+?)(?:\s+Vol\.|\s+pp\.|\s+\d+\(|\.|http)', ref, re.IGNORECASE)
        if match:
            title = match.group(2).strip()
            title = re.sub(r'https?://\S+', '', title)
            title = re.sub(r'doi:\s*\S+', '', title, flags=re.IGNORECASE)
            return title.strip()
    
    # IEEE: [#] Autor(es), "Título entre comillas," Revista, vol., no., pp., año.
    elif style == "IEEE":
        # Buscar texto entre comillas
        match = re.search(r'"([^"]+)"', ref)
        if match:
            return match.group(1).strip()
        
        # Si no hay comillas, buscar después de coma y antes de revista/vol
        match = re.search(r',\s+([^,]+?),\s+(?:vol\.|in\s+)', ref, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    # MLA: Autor(es). "Título." Revista, vol., no., año, pp.
    elif style == "MLA":
        # Buscar texto entre comillas
        match = re.search(r'"([^"]+)"', ref)
        if match:
            return match.group(1).strip()
        
        # Buscar título en cursiva (después de autores)
        match = re.search(r'(?:^|\.\s+)([A-Z][^.]+?)\.\s+[A-Z]', ref)
        if match:
            return match.group(1).strip()
    
    # Chicago: Autor(es). Año. Título. Editorial o Revista.
    elif style == "Chicago":
        # Buscar después de año hasta punto
        match = re.search(r'(\d{4})\.\s*(.+?)\.(?:\s+[A-Z]|$)', ref)
        if match:
            title = match.group(2).strip()
            title = re.sub(r'https?://\S+', '', title)
            title = re.sub(r'doi:\s*\S+', '', title, flags=re.IGNORECASE)
            return title.strip()
    
    # Vancouver: Autor(es). Título. Revista. Año;vol(no):pp.
    elif style == "Vancouver":
        # Buscar título entre primer y segundo punto después de autores
        parts = ref.split('.')
        if len(parts) >= 3:
            # Típicamente: [0]=autores, [1]=título, [2]=revista
            title = parts[1].strip()
            if title and not re.match(r'^\d{4}', title):  # No es un año
                title = re.sub(r'https?://\S+', '', title)
                title = re.sub(r'doi:\s*\S+', '', title, flags=re.IGNORECASE)
                return title.strip()
    
    # Auto (fallback): intentar detectar automáticamente
    else:
        # Intentar cada método en orden de probabilidad
        for style_attempt in ["APA 7", "IEEE", "MLA", "Vancouver", "Chicago"]:
            title = extract_title_by_style(reference, style_attempt)
            if title and len(title) > 10:  # Título razonable encontrado
                return title
    
    return ""


def _categorize_doi(category: str, http_status: Any) -> str:
    """
    Refina la categorización de DOIs para distinguir entre:
    - válido: DOI válido y accesible
    - inválido: DOI claramente inválido
    - sospechoso: DOI con problemas sospechosos (4xx, 5xx)
    - desconocido: No se pudo verificar
    """
    if category == "valid":
        return "válido"
    elif category == "invalid":
        return "inválido"
    elif category == "unknown":
        # Si tenemos código HTTP, es sospechoso, sino desconocido
        if http_status is not None and http_status != "N/A":
            try:
                status_code = int(http_status)
                # 4xx o 5xx = sospechoso
                if 400 <= status_code < 600:
                    return "sospechoso"
            except:
                pass
        return "desconocido"
    return category


# =========================
# Sidebar
# =========================
with st.sidebar:
    st.header("Parámetros")
    timeout = st.slider("Timeout (segundos)", min_value=3, max_value=40, value=15, step=1)
    max_retries = st.slider("Reintentos (doi.org)", min_value=0, max_value=5, value=2, step=1)
    workers = st.slider("Hilos (workers)", min_value=1, max_value=32, value=10, step=1)

    st.divider()
    st.subheader("📚 Estilo de referencias")
    citation_style = st.selectbox(
        "Formato de citas bibliográficas",
        options=["Auto (detectar)", "APA 7", "IEEE", "MLA", "Chicago", "Vancouver"],
        index=0,
        help="Selecciona el estilo de citación usado en los documentos para mejorar la extracción de títulos"
    )
    
    st.divider()
    include_crossref = st.checkbox("Consultar títulos por DOI (Crossref)", value=True)
    validate_title_match = st.checkbox("Validar match de título (Referencia vs Crossref)", value=True)
    title_threshold = st.slider("Umbral de match de título", min_value=0.5, max_value=0.95, value=0.78, step=0.01)

    st.divider()
    pdf_scope = st.radio("Extracción PDF", ["Últimas N páginas (recomendado)", "Todo el PDF (más lento)"], index=0)
    max_pages_from_end = st.slider("N páginas desde el final", 2, 40, 10, 1)
    prefer_refs_section = st.checkbox("Priorizar sección de referencias (si se detecta)", value=True)

st.title("📚 Validación DOI")
st.caption("Fuentes: múltiples PDFs, pegar DOIs, o Figshare (API). Validación con doi.org y (opcional) Crossref.")

# =========================
# 3 fuentes de entrada
# =========================
tabs_in = st.tabs(["📄 PDFs", "📋 Pegar DOIs", "🔗 Figshare"])

all_dois_info: List[Dict[str, Any]] = []
pdf_results: List[Dict[str, Any]] = []
docs_procesados = 0

# --- PDFs ---
with tabs_in[0]:
    uploaded_files = st.file_uploader("Sube uno o más PDFs", type=["pdf"], accept_multiple_files=True)
    if uploaded_files:
        st.write(f"**{len(uploaded_files)} archivo(s) cargado(s):**")
        for f in uploaded_files:
            st.write(f"- {f.name}")

# --- Pegar DOIs ---
with tabs_in[1]:
    pasted_text = st.text_area(
        "Pega DOIs (uno por línea) o URLs de doi.org. También puede ser texto con DOIs incrustados.",
        height=160,
        placeholder="10.1109/MIC.2022.3141559\nhttps://doi.org/10.1109/MS.2024.3392884",
    )

# --- Figshare ---
with tabs_in[2]:
    st.caption("Figshare: ingresa IDs manualmente o lista y selecciona tesis desde la API.")
    fig_mode = st.radio("Modo Figshare", ["Ingresar IDs", "Listar / Seleccionar"], horizontal=True)
    fig_ids: List[int] = []

    if fig_mode == "Ingresar IDs":
        ids_raw = st.text_area("IDs (uno por línea)", height=120, placeholder="1234567\n2345678")
        if ids_raw.strip():
            for ln in ids_raw.splitlines():
                ln = ln.strip()
                if ln.isdigit():
                    fig_ids.append(int(ln))
    else:
        col1, col2 = st.columns(2)
        with col1:
            fig_limit = st.number_input("Cantidad a listar", min_value=5, max_value=200, value=25, step=5)
        with col2:
            fig_take = st.number_input("Cantidad a procesar", min_value=1, max_value=int(fig_limit), value=min(5, int(fig_limit)), step=1)

        if st.button("🔎 Cargar lista desde Figshare"):
            with st.spinner("Consultando Figshare..."):
                st.session_state["figshare_summaries"] = figshare_list_theses(limit=int(fig_limit), timeout_sec=float(timeout))

        summaries = st.session_state.get("figshare_summaries") or []
        if summaries:
            options = {f"{s.get('title','(sin título)')} — id:{s.get('id')}": int(s.get("id")) for s in summaries if s.get("id")}
            selected = st.multiselect("Selecciona tesis", list(options.keys()), default=list(options.keys())[: int(fig_take)])
            fig_ids = [options[k] for k in selected]

# =========================
# Cache
# =========================
if "doi_cache" not in st.session_state:
    st.session_state["doi_cache"] = {}
if "crossref_cache" not in st.session_state:
    st.session_state["crossref_cache"] = {}

# =========================
# Ejecutar extracción + validación
# =========================
if st.button("🚀 Extraer y Validar", type="primary"):
    # --- A) extraer de PDFs ---
    pdf_mode = "full" if pdf_scope.startswith("Todo") else "tail"
    if uploaded_files:
        docs_procesados += len(uploaded_files)
        pdf_progress = st.progress(0)
        pdf_status = st.empty()
        for idx, uploaded_file in enumerate(uploaded_files, 1):
            pdf_status.text(f"Extrayendo DOIs de {uploaded_file.name} ({idx}/{len(uploaded_files)})...")
            pdf_bytes = uploaded_file.read()
            dois_info, ref_lines = process_pdf_bytes_to_doi_rows(
                pdf_bytes,
                file_name=uploaded_file.name,
                mode=pdf_mode,
                max_pages_from_end=int(max_pages_from_end),
                prefer_refs_section=bool(prefer_refs_section),
            )
            # enriquecer con bib title
            for d in dois_info:
                ref = d.get("reference_line") or ""
                d["bib_title"] = extract_title_by_style(ref, citation_style) or ""
            all_dois_info.extend(dois_info)
            pdf_progress.progress(idx / len(uploaded_files))
        pdf_progress.empty()
        pdf_status.empty()

    # --- B) extraer de pegado ---
    if pasted_text and pasted_text.strip():
        pasted_rows = _parse_pasted_dois(pasted_text)
        # Agregar títulos según estilo seleccionado
        for d in pasted_rows:
            ref = d.get("reference_line") or ""
            d["bib_title"] = extract_title_by_style(ref, citation_style) or ""
        all_dois_info.extend(pasted_rows)

    # --- C) extraer de Figshare ---
    if fig_ids:
        fig_prog = st.progress(0)
        fig_status = st.empty()
        for i, aid in enumerate(fig_ids, 1):
            fig_status.text(f"Figshare {i}/{len(fig_ids)}: id {aid}")
            detail = figshare_article_detail(aid, timeout_sec=float(timeout))
            if not detail:
                fig_prog.progress(i / len(fig_ids))
                continue
            pdf_urls = figshare_extract_pdf_urls(detail)
            if not pdf_urls:
                fig_prog.progress(i / len(fig_ids))
                continue
            # toma el primer PDF
            pdf_url = pdf_urls[0]
            try:
                pdf_bytes = figshare_download_pdf_bytes(pdf_url, timeout_sec=float(timeout))
                dois_info, ref_lines = process_pdf_bytes_to_doi_rows(
                    pdf_bytes,
                    file_name=(detail.get("title") or f"Figshare id:{aid}"),
                    mode=pdf_mode,
                    max_pages_from_end=int(max_pages_from_end),
                    prefer_refs_section=bool(prefer_refs_section),
                )
                for d in dois_info:
                    d["figshare_id"] = aid
                    d["figshare_url"] = detail.get("figshare_url") or ""
                    d["pdf_url"] = pdf_url
                    ref = d.get("reference_line") or ""
                    d["bib_title"] = extract_title_by_style(ref, citation_style) or ""
                all_dois_info.extend(dois_info)
            except Exception:
                pass
            fig_prog.progress(i / len(fig_ids))
        fig_prog.empty()
        fig_status.empty()

    unique_dois = _dedupe_dois(all_dois_info)

    st.write(f"DOIs únicos encontrados: **{len(unique_dois)}**")
    if not unique_dois:
        st.warning("No se encontraron DOIs en ninguna fuente.")
        st.stop()

    # --- Validación HTTP (doi.org) en paralelo ---
    progress = st.progress(0)
    status = st.empty()

    rows = []
    cache = st.session_state["doi_cache"]

    with ThreadPoolExecutor(max_workers=int(workers)) as ex:
        futs = []
        for d in unique_dois:
            futs.append(ex.submit(validate_doi_http, d["doi"], float(timeout), int(max_retries), cache))

        done = 0
        for fut in as_completed(futs):
            doi, ok, category, http_status, message, rt = fut.result()
            done += 1
            progress.progress(done / max(1, len(unique_dois)))
            status.text(f"Validando {done}/{len(unique_dois)} ...")

            # Categorización refinada
            refined_category = _categorize_doi(category, http_status)
            
            # Iconos por categoría
            icon_map = {
                "válido": "✅",
                "inválido": "❌",
                "sospechoso": "⚠️",
                "desconocido": "❓"
            }

            rows.append(
                {
                    "DOI": doi,
                    "URL": f"https://doi.org/{doi}",
                    "Categoría": refined_category,
                    "Estado": icon_map.get(refined_category, "❓"),
                    "Código HTTP": http_status if http_status is not None else "N/A",
                    "Mensaje": message,
                    "Tiempo (s)": round(float(rt or 0.0), 3),
                }
            )

    # reconectar info auxiliar
    by_doi = {d["doi"].lower(): d for d in unique_dois}
    for r in rows:
        d = by_doi.get(r["DOI"].lower(), {})
        r["Archivo"] = d.get("file_name", "N/A")
        r["Página"] = d.get("page", "N/A")
        r["Patrón"] = d.get("pattern", "")
        r["Contexto"] = d.get("context", "")
        r["Referencia (línea)"] = d.get("reference_line", "")
        r["Título (Bibliografía)"] = d.get("bib_title", "")
        r["Figshare ID"] = d.get("figshare_id", "")
        r["Figshare URL"] = d.get("figshare_url", "")
        r["PDF URL"] = d.get("pdf_url", "")

    # --- Crossref + match ---
    if include_crossref:
        status.text("Consultando títulos por DOI (Crossref)...")
        cr_cache = st.session_state["crossref_cache"]
        for i, r in enumerate(rows, start=1):
            doi = r["DOI"]
            key = doi.lower()
            if key in cr_cache:
                cr_title, cr_src = cr_cache[key]
            else:
                cr_title, cr_src = crossref_title_by_doi(doi, timeout=float(timeout))
                cr_cache[key] = (cr_title, cr_src)

            r["Título (Crossref)"] = cr_title or ""
            r["Fuente (Crossref)"] = cr_src or ""

            if validate_title_match:
                score = title_match_score(r.get("Título (Bibliografía)"), r.get("Título (Crossref)"))
                r["Score título"] = "" if score is None else score
                label = title_match_label(score, float(title_threshold))
                # Traducir etiquetas de match
                label_traduccion = {"match": "coincide", "mismatch": "no_coincide", "unknown": "desconocido"}
                r["Título match"] = label_traduccion.get(label, "desconocido")
            else:
                r["Score título"] = ""
                r["Título match"] = "desconocido"

            status.text(f"Crossref {i}/{len(rows)}")

    df = to_dataframe(rows)
    st.session_state["df"] = df
    st.session_state["docs_procesados"] = docs_procesados

    status.empty()
    progress.empty()
    st.success("Validación completada.")

df = st.session_state.get("df")
docs_procesados = st.session_state.get("docs_procesados", 0)

if df is None or df.empty:
    st.info("Carga DOIs (en alguna fuente) y haz clic en **Extraer y Validar**.")
    st.stop()

tabs = st.tabs(["📊 Dashboard", "📋 Resultados", "⬇️ Exportar"])

with tabs[0]:
    total_dois = len(df)
    valid_count = _safe_int((df["Categoría"] == "válido").sum()) if "Categoría" in df.columns else 0
    invalid_count = _safe_int((df["Categoría"] == "inválido").sum()) if "Categoría" in df.columns else 0
    suspicious_count = _safe_int((df["Categoría"] == "sospechoso").sum()) if "Categoría" in df.columns else 0
    unknown_count = _safe_int((df["Categoría"] == "desconocido").sum()) if "Categoría" in df.columns else 0
    pct_valid = round((valid_count / max(1, total_dois)) * 100, 1)

    # KPIs con las 4 categorías
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Documentos procesados", f"{docs_procesados}")
    c2.metric("DOIs analizados", f"{total_dois}")
    c3.metric("✅ Válidos", f"{valid_count}", f"{pct_valid}%")
    c4.metric("❌ Inválidos", f"{invalid_count}")
    c5.metric("⚠️ Sospechosos", f"{suspicious_count}")
    c6.metric("❓ Desconocidos", f"{unknown_count}")

    st.divider()

    st.subheader("Flujo de validación de DOI")
    sankey_fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="rgba(0,0,0,0.15)", width=1),
                    label=["DOIs analizados", "Válidos", "Inválidos", "Sospechosos", "Desconocidos"],
                    color=[
                        PALETTE["celeste"], 
                        STATUS_COLORS["válido"], 
                        STATUS_COLORS["inválido"], 
                        STATUS_COLORS["sospechoso"],
                        STATUS_COLORS["desconocido"]
                    ],
                ),
                link=dict(
                    source=[0, 0, 0, 0],
                    target=[1, 2, 3, 4],
                    value=[valid_count, invalid_count, suspicious_count, unknown_count],
                    color=[
                        "rgba(114,92,173,0.4)",   # válidos (morado)
                        "rgba(11,29,81,0.4)",     # inválidos (azul)
                        "rgba(255,140,66,0.4)",   # sospechosos (naranja)
                        "rgba(140,205,235,0.4)"   # desconocidos (celeste)
                    ],
                ),
            )
        ]
    )
    sankey_fig = _apply_layout(sankey_fig, titulo="DOIs analizados → Resultado de validación (doi.org)", altura=380)
    st.plotly_chart(sankey_fig, use_container_width=True)

    st.divider()
    st.subheader("Distribución por categoría de validación")
    donut = go.Figure(
        data=[
            go.Pie(
                labels=["Válidos", "Inválidos", "Sospechosos", "Desconocidos"],
                values=[valid_count, invalid_count, suspicious_count, unknown_count],
                hole=0.5,
                marker=dict(colors=[
                    STATUS_COLORS["válido"], 
                    STATUS_COLORS["inválido"], 
                    STATUS_COLORS["sospechoso"],
                    STATUS_COLORS["desconocido"]
                ]),
                textinfo="percent+label",
                textposition="inside",
                sort=False,
            )
        ]
    )
    donut = _apply_layout(donut, titulo="Distribución de resultados", altura=400)
    donut.update_layout(showlegend=True)
    st.plotly_chart(donut, use_container_width=True)

    if "Archivo" in df.columns:
        st.divider()
        st.subheader("DOIs por archivo/fuente")
        file_counts = df["Archivo"].value_counts().reset_index()
        file_counts.columns = ["Archivo", "Cantidad"]
        fig = go.Figure(data=[go.Bar(
            x=file_counts["Archivo"], 
            y=file_counts["Cantidad"], 
            text=file_counts["Cantidad"], 
            textposition="outside",
            marker=dict(color=PALETTE["morado"])
        )])
        fig = _apply_layout(fig, titulo="Cantidad de DOIs por archivo", titulo_x="Archivo", titulo_y="Cantidad", altura=360)
        st.plotly_chart(fig, use_container_width=True)

    if "Título match" in df.columns:
        st.divider()
        st.subheader("Coincidencia de título (Bibliografía vs Crossref)")
        tm = df["Título match"].astype(str).value_counts().reset_index()
        tm.columns = ["Título match", "Cantidad"]
        tm_fig = go.Figure()
        for _, row in tm.iterrows():
            lbl = row["Título match"]
            tm_fig.add_trace(go.Bar(
                x=[lbl], 
                y=[row["Cantidad"]], 
                text=[row["Cantidad"]], 
                textposition="outside", 
                marker=dict(color=TITLE_MATCH_COLORS.get(lbl, PALETTE["celeste"])), 
                showlegend=False
            ))
        tm_fig = _apply_layout(tm_fig, titulo="Resultado de match de título", titulo_x="Resultado", titulo_y="Cantidad", altura=320)
        st.plotly_chart(tm_fig, use_container_width=True)

with tabs[1]:
    st.subheader("Tabla de resultados")
    
    # Filtros por categoría
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        cat_filter = st.multiselect(
            "Filtrar por categoría:",
            options=["válido", "inválido", "sospechoso", "desconocido"],
            default=["válido", "inválido", "sospechoso", "desconocido"],
            format_func=lambda x: {
                "válido": "✅ Válidos",
                "inválido": "❌ Inválidos", 
                "sospechoso": "⚠️ Sospechosos",
                "desconocido": "❓ Desconocidos"
            }.get(x, x)
        )
    
    # Aplicar filtro
    df_filtered = df[df["Categoría"].isin(cat_filter)] if cat_filter else df
    
    show_cols = [
        "Estado","DOI","Archivo","Código HTTP","Categoría","Página",
        "Título (Bibliografía)","Título (Crossref)","Título match","Score título","Fuente (Crossref)",
        "Mensaje","Tiempo (s)","URL","Figshare ID","Figshare URL","PDF URL"
    ]
    show_cols = [c for c in show_cols if c in df_filtered.columns]
    st.dataframe(df_filtered[show_cols], use_container_width=True, height=560,
                 column_config={
                     "URL": st.column_config.LinkColumn("Enlace"),
                     "Figshare URL": st.column_config.LinkColumn("Figshare") if "Figshare URL" in show_cols else None,
                     "PDF URL": st.column_config.LinkColumn("PDF") if "PDF URL" in show_cols else None,
                 })
    
    st.caption(f"Mostrando {len(df_filtered)} de {len(df)} DOIs")

with tabs[2]:
    st.subheader("Exportar")
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Descargar CSV", data=csv_bytes, file_name="resultados_doi.csv", mime="text/csv")
    txt = make_txt_report(df)
    st.download_button("⬇️ Descargar TXT", data=txt.encode("utf-8"), file_name="reporte_doi.txt", mime="text/plain")