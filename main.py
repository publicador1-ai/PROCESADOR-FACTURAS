import os
import json
import re
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from datetime import datetime
from dateutil import parser as dateparser

from google.cloud import storage
from google.cloud import documentai
from googleapiclient.discovery import build
from google.oauth2 import service_account
from pypdf import PdfReader
from flask import Request

# ========= Config =========
PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("DOCAI_LOCATION", "us")

INPUT_BUCKET = os.getenv("INPUT_BUCKET")               # facturas-entrada-xyz
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET")       # facturas-procesadas-xyz
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

SAMS_PROCESSOR_ID = os.getenv("SAMS_PROCESSOR_ID")
SAMS_PROCESSOR_VERSION_ID = os.getenv("SAMS_PROCESSOR_VERSION_ID")

CITY_PROCESSOR_ID = os.getenv("CITY_PROCESSOR_ID")
CITY_PROCESSOR_VERSION_ID = os.getenv("CITY_PROCESSOR_VERSION_ID")

# Hojas
SHEET_MAP = "PRODUCTOS"
SHEET_OUT = "ENTRADAS"

# Clientes
storage_client = storage.Client()
sheets_service = build("sheets", "v4")
docai_client = documentai.DocumentProcessorServiceClient()


# ========= Utilidades de números/fechas =========

def D(x) -> Decimal:
    if x is None:
        return Decimal("0")
    if isinstance(x, (int, float, Decimal)):
        return Decimal(str(x))
    s = str(x)
    # Eliminar moneda y separadores de miles
    s = re.sub(r"[^\d,.\-]", "", s)
    # Si hay comas y puntos, asumir coma miles y punto decimal
    if s.count(",") > 0 and s.count(".") > 0:
        s = s.replace(",", "")
    else:
        # Si solo hay comas, tratarlas como decimal
        if s.count(",") == 1 and s.count(".") == 0:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")


def money2(x: Decimal) -> str:
    return f"${x.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,}".replace(",", ",")


def fmt2(x: Decimal) -> str:
    return f"{x.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}"


def porcentaje_label(pct: Decimal) -> str:
    if pct is None or pct == 0:
        return "No Aplicable"
    # sin decimales si es entero, 1 decimal si .X
    q = pct.quantize(Decimal("0.1")) if (pct % 1) != 0 else pct.quantize(Decimal("1"))
    # quitar posibles .0
    s = str(q).rstrip("0").rstrip(".")
    return f"Aplicable {s}%"


def parse_date_any(value: str) -> str:
    """
    Devuelve DD/MM/YYYY. Soporta:
    - 2025-05-27
    - 27/5/2025 o 27/05/2025
    - 25 de Mayo del 2025 (meses en español)
    """
    if not value:
        return ""
    meses = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
        "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
        "noviembre": 11, "diciembre": 12
    }
    s = value.strip()

    # Intento directo con dateutil (maneja la mayoría de formatos ISO/numéricos)
    try:
        dt = dateparser.parse(s, dayfirst=True, fuzzy=True)
        return f"{dt.day:02d}/{dt.month:02d}/{dt.year}"
    except Exception:
        pass

    # 25 de Mayo del 2025
    m = re.search(r"(\d{1,2})\s+de\s+([A-Za-záéíóúü]+)\s+(?:del|de)\s+(\d{4})", s, flags=re.I)
    if m:
        d = int(m.group(1))
        mes = m.group(2).lower()
        y = int(m.group(3))
        mnum = meses.get(mes, 1)
        return f"{d:02d}/{mnum:02d}/{y}"

    # como último recurso, intenta solo números
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", s)
    if m:
        d = int(m.group(1))
        m_ = int(m.group(2))
        y_ = int(m.group(3))
        if y_ < 100:
            y_ += 2000
        return f"{d:02d}/{m_:02d}/{y_}"

    return s  # devolver crudo si no se pudo


# ========= GCS helpers =========

def download_bytes(bucket_name: str, blob_name: str) -> bytes:
    blob = storage_client.bucket(bucket_name).blob(blob_name)
    return blob.download_as_bytes()

def upload_bytes(bucket_name: str, blob_name: str, data: bytes):
    bucket = storage_client.bucket(bucket_name)
    bucket.blob(blob_name).upload_from_string(data, content_type="application/pdf")

def move_between_buckets(src_bucket: str, blob_name: str, dest_bucket: str):
    source_bucket = storage_client.bucket(src_bucket)
    dest_bucket_obj = storage_client.bucket(dest_bucket)
    src_blob = source_bucket.blob(blob_name)
    storage_client.copy_blob(src_blob, dest_bucket_obj, blob_name)
    src_blob.delete()


# ========= Provider detection por texto de PDF =========

def detect_provider(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(io_bytes := pdf_bytes)
    except Exception:
        # pypdf exige un fichero; solución: usar BytesIO
        from io import BytesIO
        reader = PdfReader(BytesIO(pdf_bytes))

    first_page = reader.pages[0]
    text = first_page.extract_text() or ""
    t = text.upper()

    if ("NUEVA WAL MART DE MEXICO" in t) or ("SAM'S CLUB" in t) or ("SAM´S CLUB" in t) or ("SAM´S" in t) or ("SAMS CLUB" in t):
        return "SAMS"
    if ("TIENDAS SORIANA" in t) or ("CITY CLUB" in t):
        return "CITY"
    return "UNKNOWN"


# ========= Document AI =========

def processor_version_path_for(provider: str) -> str:
    if provider == "SAMS":
        return docai_client.processor_version_path(
            PROJECT_ID, LOCATION, SAMS_PROCESSOR_ID, SAMS_PROCESSOR_VERSION_ID
        )
    elif provider == "CITY":
        return docai_client.processor_version_path(
            PROJECT_ID, LOCATION, CITY_PROCESSOR_ID, CITY_PROCESSOR_VERSION_ID
        )
    else:
        return None


def process_with_docai(pdf_bytes: bytes, provider: str) -> documentai.Document:
    version = processor_version_path_for(provider)
    print(json.dumps({"step": "DOCAI_VERSION", "version": version}))
    if not version:
        raise RuntimeError("Processor version path no definido (revisa variables de entorno).")

    raw_document = documentai.RawDocument(content=pdf_bytes, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=version, raw_document=raw_document)
    result = docai_client.process_document(request=request)
    return result.document


# ========= Parsing de entidades =========

def get_prop(entity, name: str):
    if not getattr(entity, "properties", None):
        return None
    for p in entity.properties:
        if p.type_.strip().upper() == name.strip().upper():
            return p
    return None

def safe_text(entity) -> str:
    if not entity:
        return ""
    return (entity.mention_text or "").strip()

def extract_tax_pcts_sams(iva_block: str):
    """
    Busca 'IVA ... 16%' y 'IEPS ... 53%' en el bloque.
    Devuelve (iva_pct Decimal, ieps_pct Decimal)
    """
    text = (iva_block or "").upper().replace(",", ".")
    iva_pct = Decimal("0")
    ieps_pct = Decimal("0")

    m_iva = re.search(r"IVA[^%]*?(\d+(?:\.\d+)?)\s*%", text, flags=re.I)
    if m_iva:
        iva_pct = D(m_iva.group(1))

    m_ieps = re.search(r"IEPS[^%]*?(\d+(?:\.\d+)?)\s*%", text, flags=re.I)
    if m_ieps:
        ieps_pct = D(m_ieps.group(1))

    # fallback: si no encontró etiquetas, tomar los dos primeros % del bloque
    if iva_pct == 0 and ieps_pct == 0:
        nums = re.findall(r"(\d+(?:\.\d+)?)\s*%", text)
        if nums:
            iva_pct = D(nums[0])
            if len(nums) > 1:
                ieps_pct = D(nums[1])

    return iva_pct, ieps_pct


def parse_sams(doc: documentai.Document):
    """
    Lee los 'PRODUCTO' y subcampos personalizados en español.
    """
    items_out = []
    fecha_factura = ""
    num_factura = ""

    for ent in doc.entities:
        t = ent.type_.upper()
        if t == "FECHA_FACTURA":
            fecha_factura = safe_text(ent)
        elif t == "NUMERO_FACTURA":
            num_factura = safe_text(ent)

    for ent in doc.entities:
        if ent.type_.upper() != "PRODUCTO":
            continue

        qty = D(safe_text(get_prop(ent, "CANTIDAD_PRODUCTO")))
        qty = int(qty) if qty else 0

        codigo = safe_text(get_prop(ent, "CODIGO_DE_PRODUCTO"))
        descripcion = safe_text(get_prop(ent, "DESCRIPCION_PRODUCTO"))

        unit_base = D(safe_text(get_prop(ent, "COSTO_UNITARIO_PRODUCTO")))
        bruto_linea = D(safe_text(get_prop(ent, "COSTO_TOTAL_POR_PRODUCTO")))
        descuento_total = D(safe_text(get_prop(ent, "DESCUENTO")))

        iva_block = safe_text(get_prop(ent, "IVA"))
        iva_pct, ieps_pct = extract_tax_pcts_sams(iva_block)

        # Si no se tiene unitario base, derivarlo desde bruto
        if unit_base == 0 and (qty or 0) > 0:
            unit_base = (bruto_linea / D(qty))

        # === Cálculo según tu regla ===
        unit_net = (unit_base - (descuento_total / D(max(qty, 1)))) \
                   * (Decimal("1") + iva_pct / Decimal("100")) \
                   * (Decimal("1") + ieps_pct / Decimal("100"))
        line_net = unit_net * D(qty)

        items_out.append({
            "codigo": codigo,
            "descripcion": descripcion,
            "qty": qty,
            "unit_net": unit_net,
            "line_net": line_net,
            "iva_pct": iva_pct,
            "ieps_pct": ieps_pct,
            "fecha": fecha_factura,
            "factura": num_factura,
            "proveedor": "Sam´s Club",
        })

    return items_out, fecha_factura, num_factura


def parse_city(doc: documentai.Document):
    """
    Modelo pre-entrenado (inglés): line_item/quantity/product_code/description/amount/total_amount/vat/ieps
    """
    items_out = []
    fecha_factura = ""
    num_factura = ""

    for ent in doc.entities:
        t = ent.type_.lower()
        if t == "invoice_date":
            fecha_factura = safe_text(ent)
        elif t == "invoice_id":
            num_factura = safe_text(ent)

    for ent in doc.entities:
        if ent.type_.lower() != "line_item":
            continue

        qty = D(safe_text(get_prop(ent, "quantity")))
        qty = int(qty) if qty else 0
        codigo = safe_text(get_prop(ent, "product_code"))
        descripcion = safe_text(get_prop(ent, "description"))

        amount = D(safe_text(get_prop(ent, "amount")))              # Importe bruto línea
        total_amount = D(safe_text(get_prop(ent, "total_amount")))  # Neto línea
        vat_amount = D(safe_text(get_prop(ent, "vat")))
        ieps_amount = D(safe_text(get_prop(ent, "ieps")))

        # Porcentajes
        iva_pct = Decimal("16") if vat_amount > 0 else Decimal("0")
        ieps_pct = (ieps_amount / amount * 100) if amount > 0 else Decimal("0")

        unit_net = (total_amount / D(max(qty, 1))) if qty else total_amount
        line_net = total_amount

        items_out.append({
            "codigo": codigo,
            "descripcion": descripcion,
            "qty": qty,
            "unit_net": unit_net,
            "line_net": line_net,
            "iva_pct": iva_pct,
            "ieps_pct": ieps_pct,
            "fecha": fecha_factura,
            "factura": num_factura,
            "proveedor": "City Club",
        })

    return items_out, fecha_factura, num_factura


# ========= Google Sheets =========

def load_mapping() -> dict:
    """
    Lee hoja PRODUCTOS:
    Col A: SKUInterno
    Col D: Codigo Proveedor
    """
    rng = f"{SHEET_MAP}!A:D"
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=rng
    ).execute()
    values = resp.get("values", [])
    mapping = {}
    # Saltar header; buscar filas con A y D
    for row in values[1:]:
        sku = row[0].strip() if len(row) > 0 else ""
        prov = row[3].strip() if len(row) > 3 else ""
        if prov:
            mapping[prov] = sku or prov
    return mapping


def append_entries(rows: list):
    """
    Escribe en 'ENTRADAS' en el orden exacto:
    SKU | Descripción | Unidades | Costo por unidad neta | Costo total |
    Producto con IVA | Producto con IEPS | Fecha | Factura proveedor | Proveedor
    """
    rng = f"{SHEET_OUT}!A:J"
    body = {"values": rows}
    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=rng,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()


# ========= Handler =========

def procesar_facturas(request: Request):
    try:
        # CloudEvents (gen2) llegan por POST con JSON
        envelope = request.get_json(silent=True) or {}
        bucket = envelope.get("bucket")
        name = envelope.get("name")
        print(json.dumps({"step": "EVENT_RECEIVED", "bucket": bucket, "name": name}))

        if not name:
            return ("", 204)

        # El archivo podría ya no estar en entrada si hubo reintento: revisa ambos
        pdf_bytes = None
        src_bucket = None

        try:
            pdf_bytes = download_bytes(INPUT_BUCKET, name)
            src_bucket = INPUT_BUCKET
            print(json.dumps({"step": "DOWNLOAD_OK", "bytes": len(pdf_bytes)}))
        except Exception:
            try:
                pdf_bytes = download_bytes(PROCESSED_BUCKET, name)
                src_bucket = PROCESSED_BUCKET
                print(json.dumps({"step": "DOWNLOAD_OK_FROM_PROCESSED", "bytes": len(pdf_bytes)}))
            except Exception:
                print(json.dumps({"step": "ERROR_NO_SOURCE", "msg": "No está ni en entrada ni en procesados"}))
                return ("", 204)

        # Detectar proveedor por texto
        provider = detect_provider(pdf_bytes)
        print(json.dumps({"step": "PROVIDER_DETECTED", "provider": provider}))

        if provider == "UNKNOWN":
            # Mueve directo a procesadas y termina
            if src_bucket != PROCESSED_BUCKET:
                move_between_buckets(src_bucket, name, PROCESSED_BUCKET)
                print(json.dumps({"step": "MOVED_OK", "dest": PROCESSED_BUCKET, "name": name}))
            return ("", 204)

        # Procesar con Document AI
        doc = process_with_docai(pdf_bytes, provider)
        print(json.dumps({"step": "DOCAI_PROCESS_OK", "pages": len(doc.pages)}))

        # Parsear entidades y cálculos
        if provider == "SAMS":
            items, fecha_raw, factura = parse_sams(doc)
        else:
            items, fecha_raw, factura = parse_city(doc)

        print(json.dumps({"step": "PARSE_OK", "invoice": factura, "items": len(items)}))

        # Mapeo de SKU por código de proveedor
        mapping = load_mapping()

        # Armar filas para Sheets
        fecha_fmt = parse_date_any(fecha_raw)
        rows = []
        for it in items:
            codigo = it["codigo"] or ""
            sku = mapping.get(codigo, codigo)

            iva_lbl = porcentaje_label(it["iva_pct"])
            ieps_lbl = porcentaje_label(it["ieps_pct"])

            rows.append([
                sku,                                    # SKU
                it["descripcion"],                      # Descripción
                it["qty"],                              # Unidades
                fmt2(it["unit_net"]),                   # Costo por unidad neta (2 dec)
                fmt2(it["line_net"]),                   # Costo total
                iva_lbl,                                # Producto con IVA
                ieps_lbl,                               # Producto con IEPS
                fecha_fmt,                              # Fecha DD/MM/YYYY
                it["factura"] or factura,               # Factura proveedor
                it["proveedor"],                        # Proveedor
            ])

        # Evitar duplicados por reintentos: si ya movimos antes, igual escribimos,
        # pero aquí puedes filtrar por (factura+codigo) si lo deseas. Por ahora, lo mantenemos simple.

        append_entries(rows)
        print(json.dumps({"step": "SHEETS_APPEND_OK", "rows": len(rows)}))

        # Mover PDF a procesadas si estaba en entrada
        if src_bucket != PROCESSED_BUCKET:
            move_between_buckets(src_bucket, name, PROCESSED_BUCKET)
            print(json.dumps({"step": "MOVED_OK", "dest": PROCESSED_BUCKET, "name": name}))

        return ("", 204)

    except Exception as e:
        print(json.dumps({"step": "UNHANDLED_ERROR", "err": str(e)}))
        # Devuelve 204 para no reintentar en bucle; los logs ya muestran el error
        return ("", 204)

# trigger buildLOL

