import functions_framework
import re
import io
from pypdf import PdfReader
from google.cloud import documentai
from google.cloud import storage
from google.api_core.client_options import ClientOptions
from googleapiclient.discovery import build
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
import logging

# =====================================================================================
# === CONFIGURACIÓN GLOBAL ===
# =====================================================================================
PROJECT_ID = 'silver-argon-461815-d7'
LOCATION = 'us'
SPREADSHEET_ID = '1u4llynfMnPZUqqNskuCzkQhC1XwI5n0_dL8ozLhvSl4'
GCS_PROCESSED_BUCKET = 'facturas-procesadas-xyz'

# IDs de los Procesadores y Versiones
PROCESSOR_ID_SAMS = '46bf76b2d9ec6795'
PROCESSOR_VERSION_SAMS = '5e846c053f59be04'
PROCESSOR_ID_CITYCLUB = 'f6ea58d6735bbf51'
PROCESSOR_VERSION_CITYCLUB = 'pretrained-foundation-model-v1.5-2025-05-05'

# Hojas de Google Sheets
SHEET_ENTRADAS = 'ENTRADAS'
SHEET_PRODUCTOS = 'PRODUCTOS'
# =====================================================================================

# --- Clientes de Google ---
storage_client = storage.Client()
sheets_service = build('sheets', 'v4')
docai_client = documentai.DocumentProcessorServiceClient(
    client_options=ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
)

# --- Funciones de Ayuda ---

def identificar_proveedor(pdf_bytes):
    try:
        pdf_file = io.BytesIO(pdf_bytes)
        reader = PdfReader(pdf_file)
        first_page_text = reader.pages[0].extract_text().upper()
        if "NUEVA WAL MART DE MEXICO" in first_page_text or "SAM'S CLUB" in first_page_text:
            return "Sam´s Club"
        elif "TIENDAS SORIANA" in first_page_text or "CITY CLUB" in first_page_text:
            return "City Club"
        return "DESCONOCIDO"
    except Exception as e:
        logging.error(f"Error al espiar el PDF: {e}")
        return "DESCONOCIDO"

def get_product_map():
    try:
        range_name = f"'{SHEET_PRODUCTOS}'!A:D"
        result = sheets_service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
        values = result.get('values', [])
        if len(values) < 2: return {}
        product_data = values[1:]
        return {row[3].strip(): row[0].strip() for row in product_data if len(row) > 3 and row[3] and row[0]}
    except Exception as e:
        logging.error(f"ERROR CRÍTICO al leer la hoja '{SHEET_PRODUCTOS}': {e}")
        return None

def text_to_decimal(text):
    if not text: return Decimal('0.0')
    clean_text = re.sub(r'[$,\s]', '', text)
    try:
        return Decimal(clean_text)
    except Exception:
        return Decimal('0.0')

def formatear_porcentaje(porcentaje_num):
    if porcentaje_num is None or porcentaje_num == 0:
        return "No Aplicable"
    if porcentaje_num == int(porcentaje_num):
        return f"Aplicable {int(porcentaje_num)}%"
    return f"Aplicable {porcentaje_num:.1f}%"

def extraer_porcentaje_sams(texto_completo, tipo_impuesto):
    if not texto_completo: return 0.0
    pattern = re.compile(f"Impuesto: \\d+-{tipo_impuesto}.*?Tasa o Cuota: (\\d+\\.?\\d*)%", re.IGNORECASE | re.DOTALL)
    match = pattern.search(texto_completo)
    return float(match.group(1)) if match else 0.0

def extraer_importe_sams(texto_completo, tipo_impuesto):
    if not texto_completo: return Decimal('0.0')
    pattern = re.compile(f"Impuesto: \\d+-{tipo_impuesto}.*?Importe: ([\\d,]+\\.\\d+)", re.IGNORECASE | re.DOTALL)
    match = pattern.search(texto_completo)
    return text_to_decimal(match.group(1)) if match else Decimal('0.0')

def formatear_fecha(fecha_texto):
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

# =====================================================================================
# === FUNCIÓN PRINCIPAL ===
# =====================================================================================
@functions_framework.cloud_event
def process_invoice(cloud_event):
    data = cloud_event.data
    bucket = data.get("bucket")
    name = data.get("name")

    if not bucket or not name:
        logging.warning("Evento sin bucket o nombre, ignorando.")
        return

    logging.info(f"Archivo detectado: {name} en bucket {bucket}")

    try:
        pdf_bytes = download_bytes(bucket, name)
        
        proveedor_final = identificar_proveedor(pdf_bytes)
        logging.info(f"Proveedor identificado como: {proveedor_final}")

        if proveedor_final == "DESCONOCIDO":
            logging.warning(f"Proveedor no reconocido para el archivo {name}")
        else:
            # --- PROCESAMIENTO PRINCIPAL ---
            if proveedor_final == "Sam´s Club":
                processor_name = docai_client.processor_version_path(PROJECT_ID, LOCATION, PROCESSOR_ID_SAMS, PROCESSOR_VERSION_SAMS)
            else: # City Club
                processor_name = docai_client.processor_version_path(PROJECT_ID, LOCATION, PROCESSOR_ID_CITYCLUB, PROCESSOR_VERSION_CITYCLUB)

            request = documentai.ProcessRequest(name=processor_name, raw_document=documentai.RawDocument(content=pdf_bytes, mime_type='application/pdf'))
            document = docai_client.process_document(request=request).document
            
            product_map = get_product_map()
            if product_map is None: raise Exception("Fallo al cargar el mapa de productos desde Sheets.")

            rows_to_add = []
            line_item_label = "PRODUCTO" if proveedor_final == "Sam´s Club" else "line_item"
            line_items = [entity for entity in document.entities if entity.type_ == line_item_label]
            
            fecha_bruta = next((entity.mention_text for entity in document.entities if entity.type_ in ['FECHA_FACTURA', 'invoice_date']), '')
            fecha_formateada = formatear_fecha(fecha_bruta)
            numero_factura = next((entity.mention_text for entity in document.entities if entity.type_ in ['NUMERO_FACTURA', 'invoice_id']), '')

            for item in line_items:
                item_details = {prop.type_: prop.mention_text for prop in item.properties}
                
                if proveedor_final == "Sam´s Club":
                    unidades = text_to_decimal(item_details.get('CANTIDAD_PRODUCTO', '0'))
                    importe_bruto = text_to_decimal(item_details.get('COSTO_TOTAL_POR_PRODUCTO', '0'))
                    descuento = text_to_decimal(item_details.get('DESCUENTO', '0'))
                    texto_impuestos = item_details.get('IVA', '') + " " + item_details.get('IEPS', '')

                    importe_iva = extraer_importe_sams(texto_impuestos, "IVA")
                    importe_ieps = extraer_importe_sams(texto_impuestos, "IEPS")
                    
                    costo_total_neto = importe_bruto - descuento + importe_iva + importe_ieps
                    
                    iva_pct = extraer_porcentaje_sams(texto_impuestos, "IVA")
                    ieps_pct = extraer_porcentaje_sams(texto_impuestos, "IEPS")
                    valor_iva = formatear_porcentaje(iva_pct)
                    valor_ieps = formatear_porcentaje(ieps_pct)

                    codigo_producto = item_details.get('CODIGO_DE_PRODUCTO', '')
                    descripcion = item_details.get('DESCRIPCION_PRODUCTO', '')

                else: # City Club
                    unidades = text_to_decimal(item_details.get('quantity', '0'))
                    costo_total_neto = text_to_decimal(item_details.get('total_amount', '0'))

                    iva_monto = text_to_decimal(item_details.get('vat', '0'))
                    ieps_monto = text_to_decimal(item_details.get('ieps', '0'))
                    importe_bruto = text_to_decimal(item_details.get('amount', '0'))

                    iva_pct = 16.0 if iva_monto > 0 else 0.0
                    ieps_pct = float((ieps_monto / importe_bruto) * 100) if ieps_monto > 0 and importe_bruto > 0 else 0.0
                    
                    valor_iva = formatear_porcentaje(iva_pct)
                    valor_ieps = formatear_porcentaje(ieps_pct)

                    codigo_producto = item_details.get('product_code', '')
                    descripcion = item_details.get('description', '')

                costo_unitario_neto = (costo_total_neto / unidades) if unidades > 0 else Decimal('0.0')
                sku = product_map.get(codigo_producto, codigo_producto)
                
                unidades_str = str(unidades.to_integral_value(rounding=ROUND_HALF_UP))
                costo_unitario_neto_str = str(costo_unitario_neto.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                costo_total_neto_str = str(costo_total_neto.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                
                new_row = [ sku, descripcion, unidades_str, costo_unitario_neto_str, costo_total_neto_str, valor_iva, valor_ieps, fecha_formateada, numero_factura, proveedor_final ]
                rows_to_add.append(new_row)

            if rows_to_add:
                append_rows_to_sheet(rows_to_add)
                logging.info(f"Se agregaron {len(rows_to_add)} filas a la hoja ENTRADAS.")

    except Exception as e:
        logging.error(f"Error catastrófico en el procesamiento de {name}: {e}", exc_info=True)
    
    finally:
        try:
            move_blob(bucket, name, PROCESSED_BUCKET)
        except Exception as e:
            logging.error(f"Error CRÍTICO al mover el archivo {name}: {e}")
