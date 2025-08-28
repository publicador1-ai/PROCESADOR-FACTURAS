import os
import re
import io
import json
import logging
from decimal import Decimal, ROUND_HALF_UP, getcontext
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import functions_framework
from cloudevents.http import CloudEvent

from google.cloud import storage
from google.cloud import documentai_v1 as documentai
import google.auth
from googleapiclient.discovery import build
from pypdf import PdfReader

# --- Configuración Decimal para cálculos financieros ---
getcontext().prec = 28
TWOPLACES = Decimal("0.01")

# --- Entorno ---
PROJECT_ID = os.environ.get("PROJECT_ID")
DOCAI_LOCATION = os.environ.get("DOCAI_LOCATION", "us")

INPUT_BUCKET = os.environ.get("INPUT_BUCKET")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET")

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")

SAMS_PROCESSOR_ID = os.environ.get("SAMS_PROCESSOR_ID")
SAMS_PROCESSOR_VERSION_ID = os.environ.get("SAMS_PROCESSOR_VERSION_ID")

CITY_PROCESSOR_ID = os.environ.get("CITY_PROCESSOR_ID")
CITY_PROCESSOR_VERSION_ID = os.environ.get("CITY_PROCESSOR_VERSION_ID")

# --- Clientes perezosos (se crean al primer uso) ---
_storage_client: Optional[storage.Client] = None
_docai_client: Optional[documentai.DocumentProcessorServiceClient] = None
_sheets_service = None

def storage_client() -> storage.Client:
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client(project=PROJECT_ID)
    return _storage_client

def docai_client() -> documentai.DocumentProcessorServiceClient:
    global _docai_client
    if _docai_client is None:
        _docai_client = documentai.DocumentProcessorServiceClient()
    return _docai_client

def sheets_service():
    global _sheets_service
    if _sheets_service is None:
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
        _sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _sheets_service

# -------------------- Utilidades --------------------

def log(event: Dict[str, Any]):
    """Registra eventos JSON en los logs."""
    logging.info(json.dumps(event, ensure_ascii=False))

def download_bytes(bucket_name: str, blob_name: str) -> bytes:
    """Descarga un blob de GCS en memoria."""
    return storage_client().bucket(bucket_name).blob(blob_name).download_as_bytes()

def read_first_page_text(pdf_bytes: bytes) -> str:
    """Extrae texto de la primera página de un PDF."""
    reader = PdfReader(io.BytesIO(pdf_bytes))
    if not reader.pages:
        return ""
    return reader.pages[0].extract_text() or ""

def detect_provider(text: str) -> str:
    """Detecta el proveedor en base a texto del PDF (Sam’s / City / Unknown)."""
    t = text.upper()
    if "NUEVA WAL MART DE MEXICO" in t or "SAM'S CLUB" in t or "SAMS CLUB" in t:
        return "SAMS"
    if "TIENDAS SORIANA" in t or "CITY CLUB" in t:
        return "CITY"
    return "UNKNOWN"

def processor_version_path_for(provider: str) -> str:
    """Construye el path de la versión de Document AI según el proveedor."""
    if provider == "SAMS":
        return docai_client().processor_version_path(
            PROJECT_ID, DOCAI_LOCATION, SAMS_PROCESSOR_ID, SAMS_PROCESSOR_VERSION_ID
        )
    if provider == "CITY":
        return docai_client().processor_version_path(
            PROJECT_ID, DOCAI_LOCATION, CITY_PROCESSOR_ID, CITY_PROCESSOR_VERSION_ID
        )
    raise ValueError("Proveedor desconocido")

def parse_decimal(s: Optional[str]) -> Decimal:
    """Convierte cadenas con símbolos a Decimal (ej. '$23,959.03')."""
    if not s:
        return Decimal("0")
    s = s.replace("$", "").replace(",", "").replace("\xa0", " ").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return Decimal(m.group(0)) if m else Decimal("0")

def fmt_money(d: Decimal) -> str:
    """Formatea un Decimal a string con dos decimales y separadores de miles."""
    return f"{d.quantize(TWOPLACES):,.2f}"

def fmt_date_ddmmyyyy(s: str) -> str:
    """Convierte fechas variadas a formato DD/MM/YYYY."""
    s = (s or "").strip()
    if not s:
        return ""
    # Formatos comunes
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%d/%m/%Y")
        except Exception:
            pass
    # Patrón DD/MM/YY
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", s)
    if m:
        d, mth, y = m.groups()
        y = y if len(y) == 4 else ("20" + y.zfill(2))
        try:
            dt = datetime(int(y), int(mth), int(d))
            return dt.strftime("%d/%m/%Y")
        except Exception:
            pass
    return s

def extract_percent(text: str, label: str) -> Optional[Decimal]:
    """Extrae porcentajes de IVA o IEPS del texto (ej. 'IVA 16%' → 0.16)."""
    if not text:
        return None
    pattern = rf"{label}\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*%"
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        return Decimal(m.group(1)) / Decimal(100)
    return None

def sam_cost_unit_custom(amount: Decimal, discount: Decimal, qty: Decimal,
                         iva_pct: Optional[Decimal], ieps_pct: Optional[Decimal]) -> Tuple[Decimal, Decimal, str, str]:
    """
    Fórmula: (importe - descuento)/cantidad * (1+IVA) * (1+IEPS)
    Devuelve (unit_net, line_net, iva_label, ieps_label)
    """
    if qty <= 0:
        return (Decimal("0"), Decimal("0"), "No Aplicable", "No Aplicable")

    base_unit = (amount - discount) / qty

    iva_factor = (Decimal("1") + iva_pct) if iva_pct is not None else Decimal("1")
    ieps_factor = (Decimal("1") + ieps_pct) if ieps_pct is not None else Decimal("1")

    unit_net = base_unit * iva_factor * ieps_factor
    line_net = unit_net * qty

    iva_label = f"Aplicable {int(iva_pct*100)}%" if iva_pct is not None else "No Aplicable"
    if ieps_pct is not None:
        ieps_percent = ieps_pct * Decimal(100)
        ieps_str = f"{ieps_percent.normalize()}"
        if "." in ieps_str:
            try:
                ieps_str = f"{Decimal(ieps_str).quantize(Decimal('0.1'))}"
            except Exception:
                pass
        ieps_label = f"Aplicable {ieps_str}%"
    else:
        ieps_label = "No Aplicable"

    return (unit_net, line_net, iva_label, ieps_label)

def unique_key_sam(item: Dict[str, Any]) -> Tuple:
    """Crea clave única para evitar duplicados en Sam’s."""
    return (
        item.get("CODIGO_DE_PRODUCTO") or "",
        item.get("DESCRIPCION_PRODUCTO") or "",
        str(item.get("CANTIDAD_PRODUCTO") or ""),
        str(item.get("COSTO_TOTAL_POR_PRODUCTO") or ""),
    )

def unique_key_city(item: Dict[str, Any]) -> Tuple:
    """Crea clave única para evitar duplicados en City."""
    return (
        item.get("product_code") or "",
        item.get("description") or "",
        str(item.get("quantity") or ""),
        str(item.get("amount") or ""),
    )

def get_mapping_sheet() -> Dict[str, str]:
    """
    Lee la pestaña PRODUCTOS (A:D):
    A = SKUInterno, D = Código Proveedor.
    Devuelve dict { codigo_proveedor: SKUInterno }.
    """
    values = sheets_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range="PRODUCTOS!A:D"
    ).execute().get("values", [])
    mapping: Dict[str, str] = {}
    for row in values[1:]:
        sku = (row[0].strip() if len(row) > 0 else "")
        code = (row[3].strip() if len(row) > 3 else "")
        if code:
            mapping[code] = sku or code
    return mapping

def sku_from_mapping(mapping: Dict[str, str], code: str) -> str:
    """Devuelve SKU interno según el código del proveedor o el mismo código si no existe mapeo."""
    return mapping.get(code, code)

def append_rows(rows: List[List[Any]]):
    """Escribe filas en la pestaña ENTRADAS de la hoja de cálculo."""
    if not rows:
        return
    body = {"values": rows}
    sheets_service().spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="ENTRADAS!A:J",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()

def move_blob(src_bucket: str, blob_name: str, dst_bucket: str):
    """
    Mueve un blob de un bucket a otro. Copia y luego borra el archivo original.
    """
    sc = storage_client()
    source_bucket = sc.bucket(src_bucket)
    dest_bucket_obj = sc.bucket(dst_bucket)
    src_blob = source_bucket.blob(blob_name)
    # Copiar
    source_bucket.copy_blob(src_blob, dest_bucket_obj, blob_name)
    # Borrar original
    src_blob.delete()

# ---------- Funciones DocAI ----------

def process_with_docai(provider: str, pdf_bytes: bytes) -> documentai.Document:
    """
    Procesa un PDF con el procesador de Document AI apropiado.
    """
    name = processor_version_path_for(provider)
    log({"step": "DOCAI_VERSION", "version": name})
    req = documentai.ProcessRequest(
        name=name,
        raw_document=documentai.RawDocument(content=pdf_bytes, mime_type="application/pdf")
    )
    result = docai_client().process_document(request=req)
    return result.document

def extract_sams(doc: documentai.Document) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """
    Devuelve (header, items) para Sam’s:
    header: { FECHA_FACTURA, NUMERO_FACTURA }
    items: lista de dicts con campos esperados.
    """
    header: Dict[str, str] = {}
    items: List[Dict[str, str]] = []
    for ent in doc.entities or []:
        t = ent.type_.upper()
        if t in ("FECHA_FACTURA", "NUMERO_FACTURA"):
            header[t] = (ent.mention_text or "").strip()
        elif t == "PRODUCTO":
            props = {p.type_.upper(): (p.mention_text or "") for p in ent.properties or []}
            items.append(props)
    return header, items

def extract_city(doc: documentai.Document) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """
    Devuelve (header, items) para City Club.
    """
    header: Dict[str, str] = {}
    items: List[Dict[str, str]] = []
    for ent in doc.entities or []:
        t = ent.type_.lower()
        if t in ("invoice_date", "invoice_id"):
            header[t] = (ent.mention_text or "").strip()
        elif t == "line_item":
            props = {p.type_.lower(): (p.mention_text or "") for p in ent.properties or []}
            items.append(props)
    return header, items

# ---------- Handler CloudEvent (GCS) ----------

@functions_framework.cloud_event
def procesar_facturas(event: CloudEvent):
    """
    Función que maneja eventos de Cloud Storage (archivo PDF subido).
    """
    try:
        data = event.data or {}
        bucket = data.get("bucket")
        name = data.get("name")

        log({"step": "EVENT_RECEIVED", "bucket": bucket, "name": name})

        if not bucket or not name:
            log({"step": "UNHANDLED_ERROR", "err": "Evento sin bucket/nombre"})
            return

        # 1) Descarga el PDF (prueba primero entrada, luego procesadas)
        pdf_bytes = None
        src_bucket_used = None
        for candidate in (INPUT_BUCKET, PROCESSED_BUCKET):
            try:
                pdf_bytes = download_bytes(candidate, name)
                src_bucket_used = candidate
                break
            except Exception:
                continue
        if not pdf_bytes:
            log({"step": "ERROR_NO_SOURCE", "msg": "Archivo no encontrado en entrada/procesadas"})
            return

        log({"step": "DOWNLOAD_OK", "bytes": len(pdf_bytes)})

        # 2) Detectar proveedor
        first_page_text = read_first_page_text(pdf_bytes)
        provider = detect_provider(first_page_text)
        log({"step": "PROVIDER_DETECTED", "provider": provider})

        if provider == "UNKNOWN":
            # Si no se reconoce el proveedor, mueve el PDF a procesadas y termina.
            if src_bucket_used != PROCESSED_BUCKET:
                try:
                    move_blob(src_bucket_used, name, PROCESSED_BUCKET)
                    log({"step": "MOVED_OK", "dest": PROCESSED_BUCKET, "name": name})
                except Exception as e:
                    log({"step": "MOVE_ERROR", "err": str(e)})
            return

        # 3) Procesar con Document AI
        doc = process_with_docai(provider, pdf_bytes)
        log({"step": "DOCAI_PROCESS_OK", "pages": len(doc.pages or [])})

        # 4) Extraer datos
        if provider == "SAMS":
            header, items = extract_sams(doc)
        else:
            header, items = extract_city(doc)

        invoice_num = header.get("NUMERO_FACTURA") or header.get("invoice_id") or ""
        invoice_date_str = header.get("FECHA_FACTURA") or header.get("invoice_date") or ""
        invoice_date = fmt_date_ddmmyyyy(invoice_date_str)

        # 5) Obtener mapeo SKU
        mapping = get_mapping_sheet()

        # 6) Construir filas para Sheets
        out_rows: List[List[Any]] = []
        seen = set()

        if provider == "SAMS":
            # Procesamiento y deduplicación para Sam’s
            for it in items:
                k = unique_key_sam(it)
                if k in seen:
                    continue
                seen.add(k)

                qty = parse_decimal(it.get("CANTIDAD_PRODUCTO"))
                code = (it.get("CODIGO_DE_PRODUCTO") or "").strip()
                desc = (it.get("DESCRIPCION_PRODUCTO") or "").strip()
                importe_bruto = parse_decimal(it.get("COSTO_TOTAL_POR_PRODUCTO"))
                descuento = parse_decimal(it.get("DESCUENTO"))
                iva_text = it.get("IVA") or ""
                iva_pct = extract_percent(iva_text, "IVA")
                ieps_pct = extract_percent(iva_text, "IEPS")

                # Último recurso: si no aparece la etiqueta, lee los dos primeros porcentajes.
                if iva_pct is None or ieps_pct is None:
                    nums = re.findall(r"(\d+(?:\.\d+)?)\s*%", iva_text)
                    if iva_pct is None and nums:
                        iva_pct = Decimal(nums[0]) / Decimal(100)
                    if ieps_pct is None and len(nums) > 1:
                        ieps_pct = Decimal(nums[1]) / Decimal(100)

                unit_net, line_net, iva_label, ieps_label = sam_cost_unit_custom(
                    importe_bruto, descuento, qty, iva_pct, ieps_pct
                )

                sku = sku_from_mapping(mapping, code)

                out_rows.append([
                    sku,
                    desc,
                    str(int(qty) if qty != Decimal("0") else 0),
                    fmt_money(unit_net),
                    fmt_money(line_net),
                    iva_label,
                    ieps_label,
                    invoice_date,
                    invoice_num,
                    "Sam´s Club",
                ])
        else:
            # Procesamiento y deduplicación para City Club
            for it in items:
                k = unique_key_city(it)
                if k in seen:
                    continue
                seen.add(k)

                qty = parse_decimal(it.get("quantity"))
                code = (it.get("product_code") or "").strip()
                desc = (it.get("description") or "").strip()
                amount_gross = parse_decimal(it.get("amount"))
                total_amount = parse_decimal(it.get("total_amount"))
                vat_amount = parse_decimal(it.get("vat"))
                ieps_amount = parse_decimal(it.get("ieps"))

                unit_net = (total_amount / qty) if qty > 0 else Decimal("0")
                iva_label = "Aplicable 16%" if vat_amount > 0 else "No Aplicable"
                ieps_pct_calc = (ieps_amount / amount_gross) if amount_gross > 0 else Decimal("0")
                ieps_percent = ieps_pct_calc * Decimal(100)
                ieps_label = "No Aplicable"
                if ieps_percent > 0:
                    ieps_str = f"{ieps_percent.quantize(Decimal('0.1'))}".rstrip("0").rstrip(".")
                    ieps_label = f"Aplicable {ieps_str}%"

                sku = sku_from_mapping(mapping, code)

                out_rows.append([
                    sku,
                    desc,
                    str(int(qty) if qty != Decimal("0") else 0),
                    fmt_money(unit_net),
                    fmt_money(total_amount),
                    iva_label,
                    ieps_label,
                    invoice_date,
                    invoice_num,
                    "City Club",
                ])

        # 7) Registrar filas en Sheets
        if out_rows:
            append_rows(out_rows)
            log({"step": "SHEETS_APPEND_OK", "rows": len(out_rows)})

        # 8) Mover PDF a procesados
        if src_bucket_used and src_bucket_used != PROCESSED_BUCKET:
            try:
                move_blob(src_bucket_used, name, PROCESSED_BUCKET)
                log({"step": "MOVED_OK", "dest": PROCESSED_BUCKET, "name": name})
            except Exception as e:
                log({"step": "MOVE_ERROR", "err": str(e)})

    except Exception as e:
        log({"step": "UNHANDLED_ERROR", "err": str(e)})
