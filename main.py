# main.py
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
    logging.info(json.dumps(event, ensure_ascii=False))

def download_bytes(bucket_name: str, blob_name: str) -> bytes:
    return storage_client().bucket(bucket_name).blob(blob_name).download_as_bytes()

def read_first_page_text(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    if not reader.pages:
        return ""
    return reader.pages[0].extract_text() or ""

def detect_provider(text: str) -> str:
    t = text.upper()
    if "NUEVA WAL MART DE MEXICO" in t or "SAM'S CLUB" in t or "SAMS CLUB" in t:
        return "SAMS"
    if "TIENDAS SORIANA" in t or "CITY CLUB" in t:
        return "CITY"
    return "UNKNOWN"

def processor_version_path_for(provider: str) -> str:
    if provider == "SAMS":
        return docai_client().processor_version_path(
            PROJECT_ID, DOCAI_LOCATION, SAMS_PROCESSOR_ID, SAMS_PROCESSOR_VERSION_ID
        )
    if provider == "CITY":
        # Para versión preentrenada, también se usa processorVersion si te dieron ID; si no, usa processor()
        # Aquí usaremos directamente la versión como nos la diste (foundation 1.5)
        return docai_client().processor_version_path(
            PROJECT_ID, DOCAI_LOCATION, CITY_PROCESSOR_ID, CITY_PROCESSOR_VERSION_ID
        )
    raise ValueError("Proveedor desconocido para construir processorVersion")

def parse_decimal(s: Optional[str]) -> Decimal:
    if not s:
        return Decimal("0")
    # Elimina símbolos y separadores comunes
    s = s.replace("$", "").replace(",", "").replace("\xa0", " ").strip()
    # Extrae primer número válido (incluye negativos y decimales)
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return Decimal(m.group(0)) if m else Decimal("0")

def parse_int(s: Optional[str]) -> int:
    d = parse_decimal(s)
    # Redondeo hacia 0 para cantidades enteras
    return int(d.to_integral_value(rounding=ROUND_HALF_UP))

def fmt_money(d: Decimal) -> str:
    return f"{d.quantize(TWOPLACES):,.2f}"

def fmt_date_ddmmyyyy(s: str) -> str:
    """
    Recibe fecha en casi cualquier formato (incluye ISO, DD/MM, etc.) y devuelve DD/MM/YYYY.
    """
    s = (s or "").strip()
    if not s:
        return ""
    # Intenta formatos comunes antes de usar parser pesado
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%d/%m/%Y")
        except Exception:
            pass
    # Patrón suelto de DD/MM/YY(YY)
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b", s)
    if m:
        d, mth, y = m.groups()
        y = y if len(y) == 4 else ("20" + y.zfill(2))
        try:
            dt = datetime(int(y), int(mth), int(d))
            return dt.strftime("%d/%m/%Y")
        except Exception:
            pass
    # Último recurso: deja como está
    return s

def extract_percent(text: str, label: str) -> Optional[Decimal]:
    """
    Busca algo como 'IVA 16%' o 'IEPS 53%' (con tolerancia a espacios).
    """
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
    Tu fórmula: (importe - descuento)/cantidad * (1+IVA) * (1+IEPS)
    Devuelve (unit_net, line_net, iva_label, ieps_label)
    """
    if qty <= 0:
        return (Decimal("0"), Decimal("0"), "No Aplicable", "No Aplicable")

    base_unit = (amount - discount) / qty

    iva_factor = (Decimal("1") + iva_pct) if iva_pct is not None else Decimal("1")
    ieps_factor = (Decimal("1") + ieps_pct) if ieps_pct is not None else Decimal("1")

    unit_net = (base_unit * iva_factor * ieps_factor)
    line_net = unit_net * qty

    iva_label = f"Aplicable {int((iva_pct or Decimal('0'))*100)}%" if iva_pct else "No Aplicable"
    # IEPS con un decimal si aplica (ej. 8.5%). Si entero, sin decimal.
    if ieps_pct is not None:
        ieps_percent = (ieps_pct * 100)
        ieps_str = f"{ieps_percent.normalize()}"  # limpia ceros
        # normaliza formato “X.X%”
        if "." in ieps_str:
            # limita a 1 decimal visual si quedó muy largo
            try:
                ieps_str = f"{Decimal(ieps_str).quantize(Decimal('0.1'))}"
            except Exception:
                pass
        ieps_label = f"Aplicable {ieps_str}%"
    else:
        ieps_label = "No Aplicable"

    return (unit_net, line_net, iva_label, ieps_label)

def unique_key_sam(item: Dict[str, Any]) -> Tuple:
    return (
        item.get("CODIGO_DE_PRODUCTO") or "",
        item.get("DESCRIPCION_PRODUCTO") or "",
        str(item.get("CANTIDAD_PRODUCTO") or ""),
        str(item.get("COSTO_TOTAL_POR_PRODUCTO") or ""),
    )

def unique_key_city(item: Dict[str, Any]) -> Tuple:
    return (
        item.get("product_code") or "",
        item.get("description") or "",
        str(item.get("quantity") or ""),
        str(item.get("amount") or ""),
    )

def get_mapping_sheet() -> Dict[str, str]:
    """
    Lee pestaña PRODUCTOS (A:D):
    A=SKUInterno, D=Codigo Proveedor
    Devuelve dict { codigo_proveedor: SKUInterno }
    """
    values = sheets_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range="PRODUCTOS!A:D"
    ).execute().get("values", [])
    mapping: Dict[str, str] = {}
    for row in values[1:]:  # salta encabezado
        sku = (row[0].strip() if len(row) > 0 else "")
        code = (row[3].strip() if len(row) > 3 else "")
        if code:
            mapping[code] = sku or code
    return mapping

def sku_from_mapping(mapping: Dict[str, str], code: str) -> str:
    return mapping.get(code, code)

def append_rows(rows: List[List[Any]]):
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
    sc = storage_client()
    source_bucket = sc.bucket(src_bucket)
    dest_bucket = sc.bucket(dst_bucket)
    blob = source_bucket.blob(blob_name)
    # Copia y luego borra
    source_bucket.copy_blob(blob, dest_bucket, blob_name)
    blob.delete()

# ----------------- DocAI parsing helpers -----------------

def process_with_docai(provider: str, pdf_bytes: bytes) -> documentai.ProcessResponse:
    name = processor_version_path_for(provider)
    log({"step": "DOCAI_VERSION", "version": name})
    request = documentai.ProcessRequest(
        name=name,
        raw_document=documentai.RawDocument(
            content=pdf_bytes, mime_type="application/pdf"
        ),
    )
    return docai_client().process_document(request)

def get_all_entities(doc: documentai.Document) -> List[documentai.Document.Entity]:
    # Entidades al nivel documento
    return list(doc.entities or [])

def children_map(e: documentai.Document.Entity) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for p in (e.properties or []):
        out[p.type_.upper()] = (p.mention_text or "").strip()
    return out

def extract_sams(doc: documentai.Document) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    """
    Regresa (header, items) para Sam’s:
    header: { FECHA_FACTURA, NUMERO_FACTURA }
    items: lista de dict con campos esperados por tu esquema custom
    """
    header: Dict[str, str] = {}
    items: List[Dict[str, str]] = []

    for ent in get_all_entities(doc):
        t = ent.type_.upper()
        if t in ("FECHA_FACTURA", "NUMERO_FACTURA"):
            header[t] = (ent.mention_text or "").strip()
        elif t == "PRODUCTO":
            row = children_map(ent)
            items.append(row)
        # Algunas versiones ponen IVA/IEPS al nivel de la línea (propiedad de PRODUCTO).
        # Si viniera a nivel documento, no lo usamos aquí.

    return header, items

def extract_city(doc: documentai.Document) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    header: Dict[str, str] = {}
    items: List[Dict[str, str]] = []

    for ent in get_all_entities(doc):
        t = ent.type_.lower()
        if t in ("invoice_date", "invoice_id"):
            header[t] = (ent.mention_text or "").strip()
        elif t == "line_item":
            row = {}
            for p in (ent.properties or []):
                row[p.type_.lower()] = (p.mention_text or "").strip()
            items.append(row)
    return header, items

# ----------------- Handler CloudEvent -----------------

@functions_framework.cloud_event
def procesar_facturas(event: CloudEvent):
    """
    Maneja eventos de Cloud Storage (Eventarc) para PDF subido.
    """
    try:
        data = event.data or {}
        bucket = data.get("bucket") or data.get("bucketName")
        name = data.get("name") or data.get("objectId")

        log({"step": "EVENT_RECEIVED", "bucket": bucket, "name": name})

        if not bucket or not name:
            log({"step": "UNHANDLED_ERROR", "err": "Evento sin bucket/name"})
            return

        # 1) Descarga PDF (si ya fue movido por un intento anterior, intenta buscar en procesadas)
        pdf_bytes: Optional[bytes] = None
        src_bucket_used = None
        for candidate in (INPUT_BUCKET, PROCESSED_BUCKET, bucket):
            try:
                pdf_bytes = download_bytes(candidate, name)
                src_bucket_used = candidate
                break
            except Exception:
                continue

        if not pdf_bytes:
            log({"step": "ERROR_NO_SOURCE", "msg": "No está ni en entrada ni en procesados"})
            return

        log({"step": "DOWNLOAD_OK", "bytes": len(pdf_bytes)})

        # 2) Detecta proveedor leyendo la 1a página
        first_text = read_first_page_text(pdf_bytes)
        provider = detect_provider(first_text)
        log({"step": "PROVIDER_DETECTED", "provider": provider})

        if provider == "UNKNOWN":
            # No procesa, solo mueve a procesadas si viene de entrada
            if src_bucket_used and src_bucket_used != PROCESSED_BUCKET:
                try:
                    move_blob(src_bucket_used, name, PROCESSED_BUCKET)
                    log({"step": "MOVED_OK", "dest": PROCESSED_BUCKET, "name": name})
                except Exception as e:
                    log({"step": "MOVE_ERROR", "err": str(e)})
            return

        # 3) Llama a Document AI
        resp = process_with_docai(provider, pdf_bytes)
        doc = resp.document
        pages = len(doc.pages or [])
        log({"step": "DOCAI_PROCESS_OK", "pages": pages})

        # 4) Extrae datos por proveedor
        if provider == "SAMS":
            header, items = extract_sams(doc)
        else:
            header, items = extract_city(doc)

        invoice_num = header.get("NUMERO_FACTURA") or header.get("invoice_id") or ""
        invoice_date = fmt_date_ddmmyyyy(header.get("FECHA_FACTURA") or header.get("invoice_date") or "")
        log({"step": "PARSE_OK", "invoice": invoice_num[:20], "items": len(items)})

        # 5) Construye mapping de SKU
        mapping = get_mapping_sheet()

        # 6) Formateo de filas para Sheets
        out_rows: List[List[Any]] = []
        seen = set()

        if provider == "SAMS":
            # Dedup y cálculo con tu fórmula
            for it in items:
                k = unique_key_sam(it)
                if k in seen:
                    continue
                seen.add(k)

                qty = Decimal(it.get("CANTIDAD_PRODUCTO") or "0")
                code = (it.get("CODIGO_DE_PRODUCTO") or "").strip()
                desc = (it.get("DESCRIPCION_PRODUCTO") or "").strip()
                gross = parse_decimal(it.get("COSTO_TOTAL_POR_PRODUCTO"))
                discount = parse_decimal(it.get("DESCUENTO"))

                # IVA/IEPS vienen en el bloque 'IVA' (texto crudo con porcentajes)
                iva_block = it.get("IVA") or ""
                iva_pct = extract_percent(iva_block, "IVA")
                ieps_pct = extract_percent(iva_block, "IEPS")

                unit_net, line_net, iva_label, ieps_label = sam_cost_unit_custom(
                    gross, discount, qty, iva_pct, ieps_pct
                )

                sku = sku_from_mapping(mapping, code)

                out_rows.append([
                    sku,                              # SKU
                    desc,                             # Descripción
                    str(qty),                         # Unidades
                    fmt_money(unit_net),              # Costo por unidad neta
                    fmt_money(line_net),              # Costo total
                    iva_label,                        # Producto con IVA
                    ieps_label,                       # Producto con IEPS
                    invoice_date,                     # Fecha
                    invoice_num,                      # Factura proveedor
                    "Sam´s Club"                      # Proveedor
                ])
        else:
            # CITY CLUB según especificación
            for it in items:
                k = unique_key_city(it)
                if k in seen:
                    continue
                seen.add(k)

                qty = Decimal(it.get("quantity") or "0")
                code = (it.get("product_code") or "").strip()
                desc = (it.get("description") or "").strip()
                amount_gross = parse_decimal(it.get("amount"))
                total_amount = parse_decimal(it.get("total_amount"))  # neto de la línea
                vat_amount = parse_decimal(it.get("vat"))
                ieps_amount = parse_decimal(it.get("ieps"))

                unit_net = (total_amount / qty) if qty > 0 else Decimal("0")
                iva_label = "Aplicable 16%" if vat_amount > 0 else "No Aplicable"

                ieps_pct = (ieps_amount / amount_gross) if amount_gross > 0 else Decimal("0")
                ieps_label = f"Aplicable {(ieps_pct*100).quantize(Decimal('0.1'))}%" if ieps_amount > 0 else "No Aplicable"

                sku = sku_from_mapping(mapping, code)

                out_rows.append([
                    sku,
                    desc,
                    str(qty),
                    fmt_money(unit_net),
                    fmt_money(total_amount),
                    iva_label,
                    ieps_label,
                    invoice_date,
                    invoice_num,
                    "City Club"
                ])

        # 7) Escribe a Sheets
        if out_rows:
            append_rows(out_rows)
            log({"step": "SHEETS_APPEND_OK", "rows": len(out_rows)})

        # 8) Mueve PDF a procesadas (si venía de entrada)
        if src_bucket_used and src_bucket_used != PROCESSED_BUCKET:
            try:
                move_blob(src_bucket_used, name, PROCESSED_BUCKET)
                log({"step": "MOVED_OK", "dest": PROCESSED_BUCKET, "name": name})
            except Exception as e:
                log({"step": "MOVE_ERROR", "err": str(e)})

    except Exception as e:
        log({"step": "UNHANDLED_ERROR", "err": str(e)})

