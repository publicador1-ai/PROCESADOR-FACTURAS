import os
import re
import io
import json
import logging
from decimal import Decimal, getcontext
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import functions_framework
from cloudevents.http import CloudEvent

from google.cloud import storage
from google.cloud import documentai_v1 as documentai
import google.auth
from googleapiclient.discovery import build
from pypdf import PdfReader

# --- Decimals para cálculos financieros ---
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

# --- Clientes perezosos ---
_storage_client: Optional[storage.Client] = None
_docai_client: Optional[documentai.DocumentProcessorServiceClient] = None
_sheets: Optional[Any] = None


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
    global _sheets
    if _sheets is None:
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
        _sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _sheets


# ---------- Utilidades ----------
def log(event: Dict[str, Any]):
    logging.info(json.dumps(event, ensure_ascii=False))


def download_bytes(bucket_name: str, blob_name: str) -> bytes:
    return storage_client().bucket(bucket_name).blob(blob_name).download_as_bytes()


def read_first_page_text(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        if not reader.pages:
            return ""
        return reader.pages[0].extract_text() or ""
    except Exception:
        return ""


def detect_provider(text: str) -> str:
    t = (text or "").upper()
    if "SAM'S CLUB" in t or "SAMS CLUB" in t or "NUEVA WAL MART DE MEXICO" in t:
        return "SAMS"
    if "CITY CLUB" in t or "TIENDAS SORIANA" in t:
        return "CITY"
    return "UNKNOWN"


def processor_version_path_for(provider: str) -> str:
    if provider == "SAMS":
        return docai_client().processor_version_path(
            PROJECT_ID, DOCAI_LOCATION, SAMS_PROCESSOR_ID, SAMS_PROCESSOR_VERSION_ID
        )
    if provider == "CITY":
        return docai_client().processor_version_path(
            PROJECT_ID, DOCAI_LOCATION, CITY_PROCESSOR_ID, CITY_PROCESSOR_VERSION_ID
        )
    raise ValueError("Proveedor desconocido")


SPANISH_MONTHS = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, "MAYO": 5, "JUNIO": 6,
    "JULIO": 7, "AGOSTO": 8, "SEPTIEMBRE": 9, "SETIEMBRE": 9, "OCTUBRE": 10,
    "NOVIEMBRE": 11, "DICIEMBRE": 12,
}


def fmt_date_ddmmyyyy(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    # 1) formatos típicos
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except Exception:
            pass
    # 2) "5 de Mayo del 2025" / "31 de agosto de 2025"
    m = re.search(r"(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚáéíóú]+)\s+(?:de|del)\s+(\d{4})", s, re.IGNORECASE)
    if m:
        d = int(m.group(1))
        mon = SPANISH_MONTHS.get(m.group(2).upper(), None)
        y = int(m.group(3))
        if mon:
            try:
                return datetime(y, mon, d).strftime("%d/%m/%Y")
            except Exception:
                pass
    # 3) fallback
    return s


def parse_decimal(s: Optional[str]) -> Decimal:
    if not s:
        return Decimal("0")
    s = s.replace("$", "").replace(",", "").replace("\xa0", " ").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return Decimal(m.group(0)) if m else Decimal("0")


def fmt_money(d: Decimal) -> str:
    return f"{d.quantize(TWOPLACES):,.2f}"


def percent_from_text(text: Optional[str]) -> Optional[Decimal]:
    """
    Extrae porcentaje como fracción (0.16) desde un bloque como:
    'Impuesto: 002-IVA, Tipo factor: Tasa, Tasa o Cuota: 16.000000%'
    Si no hay porcentaje, devuelve None.
    """
    if not text:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if not m:
        return None
    return Decimal(m.group(1)) / Decimal(100)


def normalize_code(code: str) -> str:
    """Normaliza código de proveedor para comparación robusta."""
    s = (code or "").strip().upper().replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    no_spaces = re.sub(r"\s+", "", s)
    only_alnum = re.sub(r"[^A-Z0-9]", "", s)
    return only_alnum or no_spaces or s


def build_products_mapping() -> Dict[str, Tuple[str, str]]:
    """
    Lee PRODUCTOS!A:D
    A = SKUInterno, B = DescripcionInterna, D = CodigoProveedor
    Devuelve dict por múltiples claves normalizadas del código de proveedor:
      clave -> (sku_interno, descripcion_interna)
    """
    values = sheets_service().spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range="PRODUCTOS!A:D"
    ).execute().get("values", [])
    mapping: Dict[str, Tuple[str, str]] = {}
    for row in values[1:]:
        sku = (row[0].strip() if len(row) > 0 else "")
        desc = (row[1].strip() if len(row) > 1 else "")
        supplier_code = (row[3].strip() if len(row) > 3 else "")
        if not supplier_code:
            continue
        # claves robustas
        raw = supplier_code.strip().upper().replace("\n", " ")
        raw = re.sub(r"\s+", " ", raw)
        no_spaces = re.sub(r"\s+", "", raw)
        only_alnum = re.sub(r"[^A-Z0-9]", "", raw)

        for k in {raw, no_spaces, only_alnum}:
            if k:
                mapping[k] = (sku or supplier_code, desc)
    return mapping


def sku_and_desc_from_mapping(mapping: Dict[str, Tuple[str, str]], supplier_code: str) -> Tuple[str, str]:
    key = normalize_code(supplier_code)
    return mapping.get(key, (supplier_code, ""))


def move_blob(src_bucket: str, blob_name: str, dst_bucket: str):
    sc = storage_client()
    source_bucket = sc.bucket(src_bucket)
    dest_bucket = sc.bucket(dst_bucket)
    blob = source_bucket.blob(blob_name)
    source_bucket.copy_blob(blob, dest_bucket, blob_name)
    blob.delete()


# ---------- DocAI helpers ----------
def process_with_docai(provider: str, pdf_bytes: bytes) -> documentai.ProcessResponse:
    name = processor_version_path_for(provider)
    log({"step": "DOCAI_VERSION", "version": name})
    request = documentai.ProcessRequest(
        name=name,
        raw_document=documentai.RawDocument(content=pdf_bytes, mime_type="application/pdf"),
    )
    return docai_client().process_document(request)


def get_all_entities(doc: documentai.Document):
    return list(doc.entities or [])


def _props_to_dict(e: documentai.Document.Entity, upper: bool = True) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for p in (e.properties or []):
        key = p.type_.upper() if upper else p.type_.lower()
        out[key] = (p.mention_text or "").strip()
    return out


def extract_sams(doc: documentai.Document) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    header: Dict[str, str] = {}
    items: List[Dict[str, str]] = []
    for ent in get_all_entities(doc):
        t = ent.type_.upper()
        if t in ("FECHA_FACTURA", "NUMERO_FACTURA"):
            header[t] = (ent.mention_text or "").strip()
        elif t == "PRODUCTO":
            row = _props_to_dict(ent, upper=True)
            # Algunos diseños traen impuestos como propiedades separadas:
            # IVA / IEPS con bloques de texto -> se quedan en row["IVA"], row["IEPS"] si existen.
            items.append(row)
    return header, items


def extract_city(doc: documentai.Document) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
    header: Dict[str, str] = {}
    items: List[Dict[str, str]] = []
    for ent in get_all_entities(doc):
        t = ent.type_.lower()
        if t in ("invoice_date", "invoice_id"):
            header[t] = (ent.mention_text or "").strip()
        elif t == "line_item":
            row = _props_to_dict(ent, upper=False)
            items.append(row)
    return header, items


def compute_unit_and_labels(amount: Decimal, discount: Decimal, qty: Decimal,
                            iva_pct: Optional[Decimal], ieps_pct: Optional[Decimal]) -> Tuple[Decimal, Decimal, str, str]:
    """
    (importe - descuento)/cantidad * (1+IVA) * (1+IEPS)
    IVA 0% -> 'No Aplicable'
    IEPS None -> 'No Aplicable'
    """
    if qty <= 0:
        return (Decimal("0"), Decimal("0"), "No Aplicable", "No Aplicable")

    base_unit = (amount - discount) / qty

    iva_value = iva_pct if iva_pct is not None else Decimal("0")
    ieps_value = ieps_pct if ieps_pct is not None else Decimal("0")

    unit_net = base_unit * (Decimal("1") + iva_value) * (Decimal("1") + ieps_value)
    line_net = (amount - discount) * (Decimal("1") + iva_value) * (Decimal("1") + ieps_value)

    iva_label = "No Aplicable" if iva_value == 0 else f"Aplicable {int((iva_value*100).quantize(Decimal('1')))}%"
    if ieps_pct is None:
        ieps_label = "No Aplicable"
    else:
        ieps_percent = (ieps_value * 100)
        # mostrar sin ceros extra (1 decimal si hace falta)
        s = f"{ieps_percent.quantize(Decimal('0.1')).normalize()}"
        if s.endswith(".0"):
            s = s[:-2]
        ieps_label = f"Aplicable {s}%"

    return (unit_net, line_net, iva_label, ieps_label)


def dedupe_key_sams(it: Dict[str, Any], iva_pct: Optional[Decimal], ieps_pct: Optional[Decimal]) -> Tuple:
    amt = parse_decimal(it.get("COSTO_TOTAL_POR_PRODUCTO"))
    qty = parse_decimal(it.get("CANTIDAD_PRODUCTO"))
    code = (it.get("CODIGO_DE_PRODUCTO") or "").strip().upper()
    desc = (it.get("DESCRIPCION_PRODUCTO") or "").strip().upper()
    iva = str(iva_pct or Decimal("0"))
    ieps = str(ieps_pct) if ieps_pct is not None else "None"
    return (normalize_code(code), desc, str(qty), str(amt.quantize(TWOPLACES)), iva, ieps)


def dedupe_key_city(it: Dict[str, Any]) -> Tuple:
    amt = parse_decimal(it.get("amount"))
    qty = parse_decimal(it.get("quantity"))
    code = (it.get("product_code") or "").strip().upper()
    desc = (it.get("description") or "").strip().upper()
    return (normalize_code(code), desc, str(qty), str(amt.quantize(TWOPLACES)))


# ---------- Handler ----------
@functions_framework.cloud_event
def procesar_facturas(event: CloudEvent):
    try:
        data = event.data or {}
        bucket = data.get("bucket") or data.get("bucketName")
        name = data.get("name") or data.get("objectId")
        log({"step": "EVENT_RECEIVED", "bucket": bucket, "name": name})

        if not bucket or not name:
            log({"step": "UNHANDLED_ERROR", "err": "Evento sin bucket o nombre de archivo"})
            return

        # Descarga (busca primero en INPUT, luego PROCESSED por si ya fue movido)
        pdf_bytes = None
        src_bucket_used = None
        for candidate in (INPUT_BUCKET, PROCESSED_BUCKET, bucket):
            try:
                pdf_bytes = download_bytes(candidate, name)
                src_bucket_used = candidate
                break
            except Exception:
                continue
        if not pdf_bytes:
            log({"step": "ERROR_NO_SOURCE", "msg": "Archivo no encontrado ni en entrada ni en procesados"})
            return
        log({"step": "DOWNLOAD_OK", "bytes": len(pdf_bytes)})

        # Provider
        provider = detect_provider(read_first_page_text(pdf_bytes))
        log({"step": "PROVIDER_DETECTED", "provider": provider})

        if provider == "UNKNOWN":
            if src_bucket_used and src_bucket_used != PROCESSED_BUCKET:
                try:
                    move_blob(src_bucket_used, name, PROCESSED_BUCKET)
                    log({"step": "MOVED_OK", "dest": PROCESSED_BUCKET, "name": name})
                except Exception as e:
                    log({"step": "MOVE_ERROR", "err": str(e)})
            return

        # DocAI
        resp = process_with_docai(provider, pdf_bytes)
        doc = resp.document
        log({"step": "DOCAI_PROCESS_OK", "pages": len(doc.pages or [])})

        # Extrae
        if provider == "SAMS":
            header, items = extract_sams(doc)
            invoice_num = header.get("NUMERO_FACTURA", "")
            invoice_date = fmt_date_ddmmyyyy(header.get("FECHA_FACTURA", ""))
        else:
            header, items = extract_city(doc)
            invoice_num = header.get("invoice_id", "")
            invoice_date = fmt_date_ddmmyyyy(header.get("invoice_date", ""))

        log({"step": "PARSE_OK", "invoice": invoice_num[:20], "items": len(items)})

        # Mapping productos
        mapping = build_products_mapping()

        out_rows: List[List[Any]] = []
        seen = set()

        if provider == "SAMS":
            for it in items:
                # IVA / IEPS desde los bloques de texto del modelo
                iva_pct = percent_from_text(it.get("IVA"))
                ieps_pct = percent_from_text(it.get("IEPS"))

                k = dedupe_key_sams(it, iva_pct, ieps_pct)
                if k in seen:
                    continue
                seen.add(k)

                qty = parse_decimal(it.get("CANTIDAD_PRODUCTO"))
                gross = parse_decimal(it.get("COSTO_TOTAL_POR_PRODUCTO"))
                discount = parse_decimal(it.get("DESCUENTO"))
                code = (it.get("CODIGO_DE_PRODUCTO") or "").strip()
                desc_doc = (it.get("DESCRIPCION_PRODUCTO") or "").strip()

                unit_net, line_net, iva_label, ieps_label = compute_unit_and_labels(
                    gross, discount, qty, iva_pct, ieps_pct
                )

                sku, desc_from_products = sku_and_desc_from_mapping(mapping, code)
                # Descripción: si hay mapeo usa PRODUCTOS!B, si no, la de DocAI
                final_desc = desc_from_products or desc_doc

                out_rows.append([
                    sku,
                    final_desc,
                    f"{qty.quantize(Decimal('1'))}",
                    fmt_money(unit_net),
                    fmt_money(line_net),
                    iva_label,
                    ieps_label,
                    invoice_date,            # lo mandamos ya formateado DD/MM/AAAA
                    invoice_num,
                    "Sam´s Club"
                ])
        else:
            # CITY
            for it in items:
                k = dedupe_key_city(it)
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

                # unit_net preferimos del total neto de línea
                unit_net = (total_amount / qty) if qty > 0 else Decimal("0")

                # Etiquetas
                if vat_amount > 0:
                    base_for_vat = (amount_gross + ieps_amount) if amount_gross > 0 else Decimal("0")
                    vat_pct = (vat_amount / base_for_vat) if base_for_vat > 0 else Decimal("0")
                    vat_percent = (vat_pct * 100)
                    iva_label = f"Aplicable {vat_percent.quantize(Decimal('0.1')).normalize()}%"
                elif vat_amount == 0 and amount_gross > 0:
                    iva_label = "No Aplicable"
                else:
                    iva_label = "No Aplicable"

                if ieps_amount > 0 and amount_gross > 0:
                    ieps_pct_calc = (ieps_amount / amount_gross)
                    ieps_percent = (ieps_pct_calc * 100)
                    ieps_label = f"Aplicable {ieps_percent.quantize(Decimal('0.1')).normalize()}%"
                else:
                    ieps_label = "No Aplicable"

                sku, desc_from_products = sku_and_desc_from_mapping(mapping, code)
                final_desc = desc_from_products or desc

                out_rows.append([
                    sku,
                    final_desc,
                    f"{qty.quantize(Decimal('1'))}",
                    fmt_money(unit_net),
                    fmt_money(total_amount),
                    iva_label,
                    ieps_label,
                    invoice_date,
                    invoice_num,
                    "City Club"
                ])

        if out_rows:
            sheets_service().spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range="ENTRADAS!A:J",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": out_rows},
            ).execute()
            log({"step": "SHEETS_APPEND_OK", "rows": len(out_rows)})

        if src_bucket_used and src_bucket_used != PROCESSED_BUCKET:
            try:
                move_blob(src_bucket_used, name, PROCESSED_BUCKET)
                log({"step": "MOVED_OK", "dest": PROCESSED_BUCKET, "name": name})
            except Exception as e:
                log({"step": "MOVE_ERROR", "err": str(e)})

    except Exception as e:
        log({"step": "UNHANDLED_ERROR", "err": str(e)})
