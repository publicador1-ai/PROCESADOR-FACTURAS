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

INPUT_BUCKET = "facturas-entrada-xyz"
PROCESSED_BUCKET = "facturas-procesadas-xyz"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")

SAMS_PROCESSOR_ID = os.environ.get("SAMS_PROCESSOR_ID")
SAMS_PROCESSOR_VERSION_ID = os.environ.get("SAMS_PROCESSOR_VERSION_ID")

CITY_PROCESSOR_ID = os.environ.get("CITY_PROCESSOR_ID")
CITY_PROCESSOR_VERSION_ID = os.environ.get("CITY_PROCESSOR_VERSION_ID")

# --- Clientes ---
storage_client = storage.Client(project=PROJECT_ID)
docai_client = documentai.DocumentProcessorServiceClient()
creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)

# -------------------- Utilidades --------------------

def log(event: Dict[str, Any]):
    """Registra eventos JSON en los logs."""
    logging.info(json.dumps(event, ensure_ascii=False))

def download_bytes(bucket_name: str, blob_name: str) -> bytes:
    """Descarga un blob de GCS en memoria."""
    return storage_client.bucket(bucket_name).blob(blob_name).download_as_bytes()

def read_first_page_text(pdf_bytes: bytes) -> str:
    """Extrae texto de la primera página de un PDF."""
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return reader.pages[0].extract_text() or "" if reader.pages else ""

def detect_provider(text: str) -> str:
    """Detecta el proveedor en base a texto del PDF."""
    t = text.upper()
    if "NUEVA WAL MART DE MEXICO" in t or "SAM'S CLUB" in t:
        return "Sam´s Club"
    if "TIENDAS SORIANA" in t or "CITY CLUB" in t:
        return "City Club"
    return "DESCONOCIDO"

def parse_decimal(s: Optional[str]) -> Decimal:
    """Convierte cadenas con símbolos a Decimal."""
    if not s: return Decimal("0")
    s = s.replace("$", "").replace(",", "").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return Decimal(m.group(0)) if m else Decimal("0")

def formatear_porcentaje(porcentaje_num: float) -> str:
    """Formatea un número de porcentaje."""
    if not porcentaje_num or porcentaje_num == 0:
        return "No Aplicable"
    if porcentaje_num == int(porcentaje_num):
        return f"Aplicable {int(porcentaje_num)}%"
    return f"Aplicable {porcentaje_num:.1f}%"

def get_mapping_sheet() -> Dict[str, str]:
    """Lee la pestaña PRODUCTOS y devuelve un mapa de codigo_proveedor -> SKUInterno."""
    values = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range="PRODUCTOS!A:D"
    ).execute().get("values", [])
    return {row[3].strip(): row[0].strip() for row in values[1:] if len(row) > 3 and row[3] and row[0]}

def append_rows_to_sheet(rows: List[List[Any]]):
    """Escribe filas en la pestaña ENTRADAS."""
    if not rows: return
    body = {"values": rows}
    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="ENTRADAS!A1",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()

def move_blob(src_bucket_name: str, blob_name: str, dst_bucket_name: str):
    """Mueve un blob de un bucket a otro."""
    source_bucket = storage_client.bucket(src_bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(dst_bucket_name)
    source_bucket.copy_blob(source_blob, destination_bucket, blob_name)
    source_blob.delete()
    log({"step": "MOVE_SUCCESS", "file": blob_name, "destination": dst_bucket_name})

def formatear_fecha(fecha_texto: str) -> str:
    """Convierte varios formatos de fecha a DD/MM/YYYY."""
    if not fecha_texto: return ''
    try:
        meses = {'ENERO': '01', 'FEBRERO': '02', 'MARZO': '03', 'ABRIL': '04', 'MAYO': '05', 'JUNIO': '06', 'JULIO': '07', 'AGOSTO': '08', 'SEPTIEMBRE': '09', 'OCTUBRE': '10', 'NOVIEMBRE': '11', 'DICIEMBRE': '12'}
        partes = fecha_texto.upper().replace('DEL ', '').split()
        if len(partes) >= 4 and partes[1] == 'DE' and partes[2] in meses:
            fecha_str = f"{partes[0].zfill(2)}/{meses[partes[2]]}/{partes[3]}"
            return datetime.strptime(fecha_str, '%d/%m/%Y').strftime('%d/%m/%Y')
    except (ValueError, IndexError): pass
    try: 
        return datetime.strptime(fecha_texto.split(' ')[0], '%Y-%m-%d').strftime('%d/%m/%Y')
    except ValueError: pass
    return fecha_texto

### CORRECCIÓN ###: Nuevas funciones más robustas para extraer datos de SAMS
def extraer_porcentaje_sams(texto_completo: str, tipo_impuesto: str) -> float:
    if not texto_completo: return 0.0
    pattern = re.compile(f"Impuesto: \\d+-{tipo_impuesto}.*?Tasa o Cuota: (\\d+\\.?\\d*)%", re.IGNORECASE | re.DOTALL)
    match = pattern.search(texto_completo)
    return float(match.group(1)) if match else 0.0

def extraer_importe_sams(texto_completo: str, tipo_impuesto: str) -> Decimal:
    if not texto_completo: return Decimal('0.0')
    pattern = re.compile(f"Impuesto: \\d+-{tipo_impuesto}.*?Importe: ([\\d,]+\\.\\d+)", re.IGNORECASE | re.DOTALL)
    match = pattern.search(texto_completo)
    return parse_decimal(match.group(1)) if match else Decimal('0.0')

# ---------- Handler CloudEvent (GCS) ----------

@functions_framework.cloud_event
def procesar_facturas(event: CloudEvent):
    data = event.data or {}
    bucket = data.get("bucket")
    name = data.get("name")

    if not bucket or not name:
        log({"step": "EVENT_REJECTED", "reason": "Evento sin bucket o nombre"})
        return
    
    log({"step": "EVENT_RECEIVED", "bucket": bucket, "name": name})

    try:
        pdf_bytes = download_bytes(bucket, name)
        log({"step": "DOWNLOAD_SUCCESS", "file": name})
        
        provider = detect_provider(read_first_page_text(pdf_bytes))
        log({"step": "PROVIDER_DETECTED", "provider": provider})

        if provider == "DESCONOCIDO":
            log({"step": "PROVIDER_UNKNOWN", "file": name})
            return # El archivo se moverá en el bloque 'finally'

        # --- Procesamiento Principal ---
        processor_id = SAMS_PROCESSOR_ID if provider == "Sam´s Club" else CITY_PROCESSOR_ID
        version_id = SAMS_PROCESSOR_VERSION_ID if provider == "Sam´s Club" else CITY_PROCESSOR_VERSION_ID
        processor_name = docai_client.processor_version_path(PROJECT_ID, DOCAI_LOCATION, processor_id, version_id)

        request = documentai.ProcessRequest(name=processor_name, raw_document=documentai.RawDocument(content=pdf_bytes, mime_type='application/pdf'))
        document = docai_client.process_document(request=request).document
        
        product_map = get_mapping_sheet()
        
        rows_to_add = []
        line_item_label = "PRODUCTO" if provider == "Sam´s Club" else "line_item"
        line_items = [entity for entity in document.entities if entity.type_ == line_item_label]
        
        fecha_bruta = next((entity.mention_text for entity in document.entities if entity.type_ in ['FECHA_FACTURA', 'invoice_date']), '')
        fecha_formateada = formatear_fecha(fecha_bruta)
        numero_factura = next((entity.mention_text for entity in document.entities if entity.type_ in ['NUMERO_FACTURA', 'invoice_id']), '')

        for item in line_items:
            item_details = {prop.type_: prop.mention_text for prop in item.properties}
            
            if provider == "Sam´s Club":
                unidades = parse_decimal(item_details.get('CANTIDAD_PRODUCTO', '0'))
                importe_bruto = parse_decimal(item_details.get('COSTO_TOTAL_POR_PRODUCTO', '0'))
                descuento = parse_decimal(item_details.get('DESCUENTO', '0'))
                texto_impuestos = item_details.get('IVA', '') + " " + item_details.get('IEPS', '')

                ### CORRECCIÓN ###: Lógica de cálculo directa con montos, no porcentajes
                importe_iva = extraer_importe_sams(texto_impuestos, "IVA")
                importe_ieps = extraer_importe_sams(texto_impuestos, "IEPS")
                costo_total_neto = importe_bruto - descuento + importe_iva + importe_ieps
                
                # Para el reporte, se extraen los porcentajes por separado
                iva_pct = extraer_porcentaje_sams(texto_impuestos, "IVA")
                ieps_pct = extraer_porcentaje_sams(texto_impuestos, "IEPS")
                valor_iva = formatear_porcentaje(iva_pct)
                valor_ieps = formatear_porcentaje(ieps_pct)
                
                codigo_producto = item_details.get('CODIGO_DE_PRODUCTO', '')
                descripcion = item_details.get('DESCRIPCION_PRODUCTO', '')

            else: # City Club
                unidades = parse_decimal(item_details.get('quantity', '0'))
                costo_total_neto = parse_decimal(item_details.get('total_amount', '0'))
                
                iva_monto = parse_decimal(item_details.get('vat', '0'))
                ieps_monto = parse_decimal(item_details.get('ieps', '0'))
                importe_bruto = parse_decimal(item_details.get('amount', '0'))

                iva_pct = 16.0 if iva_monto > 0 else 0.0
                ieps_pct = float((ieps_monto / importe_bruto) * 100) if ieps_monto > 0 and importe_bruto > 0 else 0.0
                
                valor_iva = formatear_porcentaje(iva_pct)
                valor_ieps = formatear_porcentaje(ieps_pct)

                codigo_producto = item_details.get('product_code', '')
                descripcion = item_details.get('description', '')

            costo_unitario_neto = (costo_total_neto / unidades) if unidades > 0 else Decimal('0.0')
            sku = product_map.get(codigo_producto, codigo_producto)
            
            ### CORRECCIÓN ###: Orden de columnas ajustado a 10
            new_row = [
                sku, 
                descripcion, 
                str(unidades.to_integral_value(rounding=ROUND_HALF_UP)),
                str(costo_unitario_neto.quantize(TWOPLACES, rounding=ROUND_HALF_UP)),
                str(costo_total_neto.quantize(TWOPLACES, rounding=ROUND_HALF_UP)),
                valor_iva, 
                valor_ieps, 
                fecha_formateada, 
                numero_factura, 
                provider
            ]
            rows_to_add.append(new_row)

        if rows_to_add:
            append_rows_to_sheet(rows_to_add)
            log({"step": "SHEETS_APPEND_SUCCESS", "rows_added": len(rows_to_add)})

    except Exception as e:
        log({"step": "PROCESSING_ERROR", "file": name, "error": str(e)})
    
    ### CORRECCIÓN ###: El movimiento del archivo se hace al final en un bloque 'finally'
    finally:
        try:
            move_blob(bucket, name, PROCESSED_BUCKET)
        except Exception as e:
            # Este error puede ocurrir si el archivo ya fue movido por una ejecución anterior
            log({"step": "MOVE_FILE_ERROR", "file": name, "error": str(e)})
