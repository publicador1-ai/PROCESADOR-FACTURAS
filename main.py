# main.py
import os
import json
import re
import logging
from decimal import Decimal, getcontext, ROUND_HALF_UP
from datetime import datetime

from pypdf import PdfReader
from google.cloud import storage
from google.cloud import documentai_v1 as documentai
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import functions_framework  # para CloudEvent/Gen2

# ---------- Configuración ----------
PROJECT_ID = os.getenv("PROJECT_ID")
DOCAI_LOCATION = os.getenv("DOCAI_LOCATION", "us")

INPUT_BUCKET = os.getenv("INPUT_BUCKET")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET")

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

SAMS_PROCESSOR_ID = os.getenv("SAMS_PROCESSOR_ID")
SAMS_PROCESSOR_VERSION_ID = os.getenv("SAMS_PROCESSOR_VERSION_ID")

CITY_PROCESSOR_ID = os.getenv("CITY_PROCESSOR_ID")
CITY_PROCESSOR_VERSION_ID = os.getenv("CITY_PROCESSOR_VERSION_ID")

# Precisión y redondeo financiero
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP

storage_client = storage.Client()

# Sheets API con credenciales por defecto del entorno (service account de ejecución)
def _sheets():
    creds, _ = google.auth.default(scopes=[
        "https://www.googleapis.com/auth/spreadsheets"
    ])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def log_dict(**kw):
    logging.info(json.dumps(kw, ensure_ascii=False))

# ---------- Utilidades numéricas y de texto ----------

def D(x) -> Decimal:
    """Convierte a Decimal limpiando comas y espacios."""
    if x is None:
        return Decimal("0")
    if isinstance(x, (int, float, Decimal)):
        return Decimal(str(x))
    s = str(x).strip().replace(",", "")
    # Quita símbolos de moneda si los hubiera
    s = re.sub(r"[^\d.\-]", "", s)
    return Decimal(s) if s else Decimal("0")

SPANISH_MONTHS = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, "MAYO": 5, "JUNIO": 6,
    "JULIO": 7, "AGOSTO": 8, "SEPTIEMBRE": 9, "SETIEMBRE": 9, "OCTUBRE": 10,
    "NOVIEMBRE": 11, "DICIEMBRE": 12
}

def format_date_ddmmyyyy(s: str) -> str:
    """Intenta normalizar fechas variadas al formato DD/MM/YYYY."""
    if not s:
        return ""
    t = s.strip()
    # 1) DD/MM/YYYY o DD-MM-YYYY
    m = re.match(r"^\s*(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})\s*$", t)
    if m:
        d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{d:02d}/{mth:02d}/{y}"

    # 2) YYYY-MM-DD
    m = re.match(r"^\s*(\d{4})\-(\d{1,2})\-(\d{1,2})\s*$", t)
    if m:
        y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{d:02d}/{mth:02d}/{y}"

    # 3) “27 de MAYO del 2025”, “27 de Mayo de 2025”
    m = re.match(r"^\s*(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚÜÑ]+)\s+(?:de|del)\s+(\d{4})", t, flags=re.IGNORECASE)
    if m:
        d = int(m.group(1))
        mon = m.group(2).upper()
        mon = (mon
               .replace("Á","A").replace("É","E").replace("Í","I")
               .replace("Ó","O").replace("Ú","U").replace("Ü","U"))
        mth = SPANISH_MONTHS.get(mon, 0)
        y = int(m.group(3))
        if 1 <= mth <= 12:
            return f"{d:02d}/{mth:02d}/{y}"

    # Fallback: intenta parsear ISO sin dependencias
    try:
        dt = datetime.fromisoformat(t)
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return t  # deja como vino

def label_pct(p: Decimal, places=2) -> str:
    if p is None or p == Decimal("0"):
        return "No Aplicable"
    q = (p * Decimal("100")).quantize(Decimal("1.00"))
    if q == q.to_integral():  # entero
        return f"Aplicable {int(q)}%"
    # 1 decimal si cabe, si no 2
    q1 = (p * Decimal("100")).quantize(Decimal("1.0"))
    return f"Aplicable {q1.normalize()}%"

def extract_sams_percentages(block_text: str):
    """
    Del bloque de texto 'IVA' de Sam's, extrae IVA% e IEPS% como Decimals (ej. 0.16, 0.53).
    Acepta formatos como 'IVA 16.000000 %', 'IEPS 53 %', etc.
    """
    if not block_text:
        return Decimal("0"), Decimal("0")
    t = block_text.upper()
    # Quitar caracteres raros
    t = t.replace(",", ".")
    # IVA
    iva = Decimal("0")
    m = re.search(r"IVA[^\d]*(\d+(?:\.\d+)?)\s*%", t)
    if m:
        iva = D(m.group(1)) / Decimal("100")
    # IEPS
    ieps = Decimal("0")
    m = re.search(r"IEPS[^\d]*(\d+(?:\.\d+)?)\s*%", t)
    if m:
        ieps = D(m.group(1)) / Decimal("100")
    return iva, ieps

# ---------- GCS helpers ----------

def download_bytes(bucket: str, name: str) -> bytes:
    """Descarga el PDF. Si no está en entrada, prueba procesadas para evitar 404 por reintentos."""
    try:
        blob = storage_client.bucket(bucket).blob(name)
        return blob.download_as_bytes()
    except Exception as e:
        # prueba en procesadas
        try:
            blob = storage_client.bucket(PROCESSED_BUCKET).blob(name)
            return blob.download_as_bytes()
        except Exception:
            raise

def move_to_processed(name: str):
    """Copia objeto a bucket de procesadas y borra el de entrada."""
    src_bucket = storage_client.bucket(INPUT_BUCKET)
    dst_bucket = storage_client.bucket(PROCESSED_BUCKET)
    src_blob = src_bucket.blob(name)
    # Copiar al mismo nombre
    src_bucket.copy_blob(src_blob, dst_bucket, name)
    # Borrar del bucket de entrada
    src_blob.delete()
    log_dict(step="MOVED_OK", dest=PROCESSED_BUCKET, name=name)

# ---------- Detección de proveedor ----------

def detect_provider_from_pdf(pdf_bytes: bytes) -> str:
    text = ""
    try:
        reader = PdfReader(io=pdf_bytes)
    except TypeError:
        # PdfReader espera un stream; usa un wrapper
        import io as _io
        reader = PdfReader(_io.BytesIO(pdf_bytes))
    page0 = reader.pages[0]
    text = page0.extract_text() or ""
    t = text.upper()
    if "NUEVA WAL MART DE MEXICO" in t or "SAM'S CLUB" in t or "SAMS CLUB" in t:
        return "SAMS"
    if "TIENDAS SORIANA" in t or "CITY CLUB" in t:
        return "CITY"
    return "UNKNOWN"

# ---------- Document AI ----------

def process_with_docai(pdf_bytes: bytes, provider: str):
    client = documentai.DocumentProcessorServiceClient()
    if provider == "SAMS":
        name = f"projects/{PROJECT_ID}/locations/{DOCAI_LOCATION}/processors/{SAMS_PROCESSOR_ID}/processorVersions/{SAMS_PROCESSOR_VERSION_ID}"
    elif provider == "CITY":
        name = f"projects/{PROJECT_ID}/locations/{DOCAI_LOCATION}/processors/{CITY_PROCESSOR_ID}/processorVersions/{CITY_PROCESSOR_VERSION_ID}"
    else:
        raise RuntimeError("Proveedor desconocido para DocAI")

    log_dict(step="DOCAI_VERSION", version=name)

    raw_document = documentai.RawDocument(content=pdf_bytes, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    result = client.process_document(request=request)
    return result.document

# ---------- Parseo de resultados DocAI ----------

def _norm_type(t: str) -> str:
    t = (t or "").upper()
    t = (t
         .replace("Á","A").replace("É","E").replace("Í","I")
         .replace("Ó","O").replace("Ú","U").replace("Ü","U")
         .replace("’","'"))
    return t

def parse_sams(doc: documentai.Document):
    """Devuelve (header, items) para Sam's.
       header: {'invoice_date': 'DD/MM/YYYY', 'invoice_id': '...'}
       items: lista de dicts con keys: code, desc, qty, amount, discount, iva_pct, ieps_pct
    """
    header = {"invoice_date": "", "invoice_id": ""}
    items = []

    # Header
    for e in doc.entities:
        t = _norm_type(e.type_)
        mt = (e.mention_text or "").strip()
        if t == "FECHA_FACTURA" and not header["invoice_date"]:
            header["invoice_date"] = format_date_ddmmyyyy(mt)
        elif t == "NUMERO_FACTURA" and not header["invoice_id"]:
            header["invoice_id"] = mt

    # Items
    for e in doc.entities:
        if _norm_type(e.type_) != "PRODUCTO":
            continue
        props = { _norm_type(p.type_): (p.mention_text or "") for p in e.properties }

        qty = D(props.get("CANTIDAD_PRODUCTO"))
        code = props.get("CODIGO_DE_PRODUCTO", "").strip()
        desc = props.get("DESCRIPCION_PRODUCTO", "").strip()

        amount = D(props.get("COSTO_TOTAL_POR_PRODUCTO"))  # importe bruto de la línea
        discount = D(props.get("DESCUENTO"))
        tax_block = props.get("IVA", "")  # aquí viene el bloque de texto con IVA/IEPS
        iva_pct, ieps_pct = extract_sams_percentages(tax_block)

        items.append({
            "code": code,
            "desc": desc,
            "qty": qty,
            "amount": amount,
            "discount": discount,
            "iva_pct": iva_pct,
            "ieps_pct": ieps_pct
        })

    return header, items

def parse_city(doc: documentai.Document):
    """Devuelve (header, items) para City Club."""
    header = {"invoice_date": "", "invoice_id": ""}
    items = []

    for e in doc.entities:
        t = _norm_type(e.type_)
        mt = (e.mention_text or "").strip()
        if t == "INVOICE_DATE" and not header["invoice_date"]:
            header["invoice_date"] = format_date_ddmmyyyy(mt)
        elif t == "INVOICE_ID" and not header["invoice_id"]:
            header["invoice_id"] = mt

    for e in doc.entities:
        if _norm_type(e.type_) != "LINE_ITEM":
            continue
        props = { _norm_type(p.type_): (p.mention_text or "") for p in e.properties }
        qty = D(props.get("QUANTITY"))
        code = props.get("PRODUCT_CODE", "").strip()
        desc = props.get("DESCRIPTION", "").strip()

        amount = D(props.get("AMOUNT"))           # bruto
        total_amount = D(props.get("TOTAL_AMOUNT"))  # neto de línea (incl impuestos)
        vat_amt = D(props.get("VAT"))
        ieps_amt = D(props.get("IEPS"))

        # IVA: si vat > 0, fijo 16%
        iva_pct = Decimal("0.16") if vat_amt > 0 else Decimal("0")
        # IEPS%: ieps / amount
        ieps_pct = (ieps_amt / amount) if amount > 0 else Decimal("0")

        items.append({
            "code": code, "desc": desc, "qty": qty,
            "amount": amount, "total_amount": total_amount,
            "iva_pct": iva_pct, "ieps_pct": ieps_pct
        })

    return header, items

# ---------- Mapeo SKU en Sheets ----------

def get_provider_code_to_sku():
    """
    Lee la pestaña PRODUCTOS (A: SKUInterno, D: Codigo Proveedor) y construye un dict {CodigoProveedor: SKUInterno}.
    Columna C (Categoría) se ignora.
    """
    try:
        service = _sheets()
        rng = "PRODUCTOS!A:D"
        resp = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=rng).execute()
        rows = resp.get("values", [])
        mapping = {}
        for r in rows[1:]:  # salta encabezado
            sku = (r[0] if len(r) > 0 else "").strip()
            prov_code = (r[3] if len(r) > 3 else "").strip()
            if prov_code:
                mapping[prov_code] = sku or prov_code
        return mapping
    except HttpError as e:
        log_dict(step="MAPPING_ERROR", err=str(e))
        return {}

# ---------- Escritura en Sheets ----------

def append_rows(rows):
    """
    Escribe en la pestaña ENTRADAS con el orden exacto:
    [SKU, Descripción, Unidades, Costo por unidad neta, Costo total,
     Producto con IVA, Producto con IEPS, Fecha, Factura proveedor, Proveedor]
    """
    service = _sheets()
    body = {"values": rows}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="ENTRADAS!A:J",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()
    log_dict(step="SHEETS_APPEND_OK", rows=len(rows))

# ---------- Handler CloudEvent (GCS finalize) ----------

@functions_framework.cloud_event
def procesar_facturas(cloud_event):
    try:
        data = cloud_event.data or {}
        bucket = data.get("bucket")
        name = data.get("name")
        log_dict(step="EVENT_RECEIVED", bucket=bucket, name=name)

        if not bucket or not name:
            log_dict(step="UNHANDLED_ERROR", err="Evento sin bucket o nombre")
            return ("", 204)

        # Descarga PDF (si no está en entrada, intenta en procesadas)
        pdf_bytes = download_bytes(bucket, name)
        log_dict(step="DOWNLOAD_OK", bytes=len(pdf_bytes))

        # Detecta proveedor por PDF
        provider = detect_provider_from_pdf(pdf_bytes)
        log_dict(step="PROVIDER_DETECTED", provider=provider)

        if provider == "UNKNOWN":
            # Mueve directo a procesadas y sale
            move_to_processed(name)
            return ("", 204)

        # Document AI
        doc = process_with_docai(pdf_bytes, provider)
        pages = len(doc.pages) if getattr(doc, "pages", None) else 0
        log_dict(step="DOCAI_PROCESS_OK", pages=pages)

        # Parse y cálculos
        if provider == "SAMS":
            header, items = parse_sams(doc)
        else:
            header, items = parse_city(doc)

        # Deduplicación de items (por code+desc+qty+amount)
        seen = set()
        unique = []
        for it in items:
            key = (it.get("code",""), it.get("desc",""),
                   str(it.get("qty","")), str(it.get("amount","")),
                   str(it.get("discount","")))
            if key in seen:
                continue
            seen.add(key)
            unique.append(it)
        items = unique

        log_dict(step="PARSE_OK", invoice=header.get("invoice_id",""), items=len(items))

        # Mapeo de SKU
        code2sku = get_provider_code_to_sku()

        # Construir filas a insertar
        rows = []
        invoice_date = header.get("invoice_date","")
        invoice_id = header.get("invoice_id","")
        prov_label = "Sam´s Club" if provider == "SAMS" else "City Club"

        for it in items:
            code = (it.get("code") or "").strip()
            desc = it.get("desc") or ""
            qty = D(it.get("qty"))

            if provider == "SAMS":
                amount = D(it.get("amount"))      # importe bruto de línea
                discount = D(it.get("discount"))  # descuento de línea
                iva_pct = it.get("iva_pct") or Decimal("0")
                ieps_pct = it.get("ieps_pct") or Decimal("0")

                # Tu fórmula: (importe - descuento)/cantidad * (1+IVA) * (1+IEPS)
                base_unit = (amount - discount) / qty if qty > 0 else Decimal("0")
                unit_net = base_unit * (Decimal("1")+iva_pct) * (Decimal("1")+ieps_pct)
                line_net = (unit_net * qty)

                iva_label = label_pct(iva_pct)
                ieps_label = label_pct(ieps_pct)

            else:  # CITY
                total_amount = D(it.get("total_amount"))
                amount = D(it.get("amount"))
                iva_pct = it.get("iva_pct") or Decimal("0")
                ieps_pct = it.get("ieps_pct") or Decimal("0")

                unit_net = (total_amount / qty) if qty > 0 else Decimal("0")
                line_net = total_amount

                iva_label = label_pct(iva_pct)
                ieps_label = label_pct(ieps_pct)

            # SKU por mapeo; si no existe, usar el propio code
            sku = code2sku.get(code, code if code else "")

            # Formateos
            def fmt2(x: Decimal) -> str:
                return f"{x.quantize(Decimal('1.00'))}"

            rows.append([
                sku,                 # SKU
                desc,                # Descripción
                f"{qty.normalize()}",# Unidades
                fmt2(unit_net),      # Costo por unidad neta
                fmt2(line_net),      # Costo total
                iva_label,           # Producto con IVA
                ieps_label,          # Producto con IEPS
                invoice_date,        # Fecha DD/MM/YYYY
                invoice_id,          # Factura proveedor
                prov_label           # Proveedor
            ])

        if rows:
            append_rows(rows)

        # Mover PDF a procesadas (si vino de entrada)
        if bucket == INPUT_BUCKET:
            try:
                move_to_processed(name)
            except Exception as e:
                # Si ya no está en entrada (por reintento), no romper
                log_dict(step="MOVE_WARN", err=str(e))

        return ("", 204)

    except Exception as e:
        log_dict(step="UNHANDLED_ERROR", err=str(e))
        # No lanzar excepción para que Eventarc no reintente indefinidamente
        return ("", 204)


FU


