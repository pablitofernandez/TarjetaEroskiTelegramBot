import pandas as pd
import smtplib
from email.message import EmailMessage
import os
import hashlib
import logging
from pathlib import Path
import sqlite3
from datetime import datetime

logger = logging.getLogger("ExcelProcessorLogic")

SMTP_SERVER = os.environ.get('SMTP_SERVER')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
EMAIL_SENDER = os.environ.get('EMAIL_SENDER')
EMAIL_LOGIN = os.environ.get('EMAIL_LOGIN')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')
EMAIL_RECEIVER = os.environ.get('EMAIL_RECEIVER')
EMAIL_HEADER_IMAGE = os.environ.get('EMAIL_HEADER_IMAGE')
DB_FILE_PATH = Path(os.environ.get('DB_FILE', '/app/state/transactions.db'))
EXCEL_SHEET_NAME = os.environ.get('EXCEL_SHEET_NAME', 0)
try:
    EXCEL_SHEET_NAME = int(EXCEL_SHEET_NAME)
except ValueError:
    pass
EXCEL_SKIP_ROWS = int(os.environ.get('EXCEL_SKIP_ROWS', 0))
COL_DATE_EXCEL = os.environ.get('COL_DATE_EXCEL', 'Fecha')
COL_DESC_EXCEL = os.environ.get('COL_DESC_EXCEL', 'Descripción')
COL_AMOUNT_EXCEL = os.environ.get('COL_AMOUNT_EXCEL', 'Importe')
COL_BANK_ID_EXCEL = os.environ.get('COL_BANK_ID_EXCEL', None)
COL_DB_ID = "transaction_id"
COL_DB_DATE = "transaction_date"
COL_DB_DESC = "description"
COL_DB_AMOUNT = "amount"
COL_DB_CATEGORY = "category"
COL_DB_PROCESSED_AT = "processed_at"

def get_transaction_id(row, date_col, desc_col, amount_col, bank_id_col):
    """Crea un ID único para la transacción."""
    if bank_id_col and bank_id_col in row and pd.notna(row[bank_id_col]):
        return str(row[bank_id_col]).strip()
    else:
        try:
            if date_col not in row or desc_col not in row or amount_col not in row:
                 logger.error(f"Faltan columnas clave ({date_col}, {desc_col}, {amount_col}) en la fila para generar ID: {row}")
                 return None
            date_str = str(row[date_col]).strip()
            desc_str = str(row[desc_col]).strip()
            amount_str = str(row[amount_col]).strip()
            data_str = f"{date_str}_{desc_str}_{amount_str}"
            return hashlib.md5(data_str.encode()).hexdigest()
        except KeyError as e:
            logger.error(f"Error de clave al generar ID para la fila: {row}. Falta la columna: {e}")
            return None

def init_db(db_path: Path):
    """Inicializa la BD y crea la tabla si no existe."""
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS transactions (
            {COL_DB_ID} TEXT PRIMARY KEY NOT NULL,
            {COL_DB_DATE} TEXT NOT NULL,
            {COL_DB_DESC} TEXT NOT NULL,
            {COL_DB_AMOUNT} REAL NOT NULL,
            {COL_DB_CATEGORY} TEXT,
            {COL_DB_PROCESSED_AT} TEXT NOT NULL
        )
        ''')
        conn.commit()
        conn.close()
        logger.info(f"Base de datos inicializada/verificada en {db_path}")
    except sqlite3.Error as e:
        logger.error(f"Error al inicializar la base de datos {db_path}: {e}")
        raise # Relanzar para que la API lo capture

def check_if_id_exists(db_path: Path, transaction_id: str) -> bool:
    """Verifica si un transaction_id ya existe en la BD."""
    exists = False
    try:
        # Usar timeout en conexión por si la BD está bloqueada brevemente
        conn = sqlite3.connect(db_path, timeout=10.0)
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM transactions WHERE {COL_DB_ID} = ?", (transaction_id,))
        result = cursor.fetchone()
        if result:
            exists = True
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"Error al consultar la base de datos para ID {transaction_id}: {e}")
        raise # Relanzar para que la API lo capture
    return exists

def insert_transaction(db_path: Path, tx_data: dict):
    """Inserta una nueva transacción en la BD. Devuelve True si insertó, False si error/duplicado."""
    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        cursor = conn.cursor()
        cursor.execute(f'''
        INSERT INTO transactions (
            {COL_DB_ID}, {COL_DB_DATE}, {COL_DB_DESC}, {COL_DB_AMOUNT}, {COL_DB_PROCESSED_AT}, {COL_DB_CATEGORY}
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            tx_data[COL_DB_ID],
            tx_data[COL_DB_DATE],
            tx_data[COL_DB_DESC],
            tx_data[COL_DB_AMOUNT],
            tx_data[COL_DB_PROCESSED_AT],
            tx_data.get(COL_DB_CATEGORY)
        ))
        conn.commit()
        conn.close()
        logger.debug(f"Transacción insertada: {tx_data[COL_DB_ID]}")
        return True
    except sqlite3.IntegrityError:
         logger.warning(f"Intento de insertar ID duplicado (ignorado): {tx_data[COL_DB_ID]}")
         return False # No fue una inserción nueva, pero no es un error fatal
    except sqlite3.Error as e:
        logger.error(f"Error al insertar transacción {tx_data[COL_DB_ID]} en la BD: {e}")
        # No relanzamos aquí, simplemente devolvemos False para contarlo como fallo
        return False

def send_email(subject: str, transactions: list, filename: str):
    """Envía el correo electrónico con formato HTML y cabecera de imagen."""
    if not all([SMTP_SERVER, EMAIL_LOGIN, EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER]):
        logger.error("Faltan variables de entorno para enviar correo. No se enviará.")
        return False

    header_image_url = EMAIL_HEADER_IMAGE

    # --- Construcción del HTML ---
    # Estilos CSS inline
    body_style = "font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333; margin: 0; padding: 0; background-color: #f8f9fa;"
    container_style = "max-width: 600px; margin: 20px auto; padding: 0; border: 1px solid #ddd; border-radius: 5px; background-color: #ffffff; overflow: hidden;" # overflow:hidden para bordes redondeados con imagen
    # Estilo para el contenedor de la imagen, centrado
    image_container_style = "text-align: center; margin-bottom: 20px; background-color: #ffffff;" # Fondo blanco por si la imagen tarda
    # Estilo para la imagen: display block y márgenes auto para centrar, max-width para responsive
    image_style = f"display: block; margin: 0 auto; max-width: 100%; height: auto; border: 0;" # height:auto para mantener proporción
    content_padding_style = "padding: 0 20px 20px 20px;" # Padding para el contenido debajo de la imagen
    header_style = "font-size: 20px; font-weight: bold; color: #0056b3; margin-bottom: 15px; text-align: center;"
    table_style = "width: 100%; border-collapse: collapse; margin-top: 15px;"
    th_style = "border: 1px solid #ddd; padding: 8px 12px; background-color: #f2f2f2; text-align: left; font-weight: bold;"
    td_style = "border: 1px solid #ddd; padding: 8px 12px; text-align: left;"
    td_amount_style = f"{td_style} text-align: right; font-weight: bold;"
    footer_style = "margin-top: 20px; padding: 10px; font-size: 12px; color: #888; text-align: center; background-color: #f2f2f2;"

    # Generar las filas de la tabla dinámicamente
    transaction_rows_html = ""
    for tx in transactions:
        escaped_desc = tx['descripcion']
        transaction_rows_html += f"""
        <tr>
            <td style="{td_style}">{tx['fecha']}</td>
            <td style="{td_style}">{escaped_desc}</td>
            <td style="{td_amount_style}">{tx['monto']} €</td>
        </tr>
        """

    # Plantilla HTML completa con la imagen
    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{subject}</title>
    </head>
    <body style="{body_style}">
        <div style="{container_style}">
            <!-- Contenedor de la Imagen -->
            <div style="{image_container_style}">
                <img src="{header_image_url}" alt="Cabecera Movimientos" width="100%" height="auto" style="{image_style}">
            </div>

            <!-- Contenido Principal con Padding -->
            <div style="{content_padding_style}">
                <p>Se han detectado <strong>{len(transactions)}</strong> nuevos movimientos en el archivo: <strong>{filename}</strong></p>

                <table style="{table_style}">
                    <thead>
                        <tr>
                            <th style="{th_style}">Fecha</th>
                            <th style="{th_style}">Descripción</th>
                            <th style="{th_style} text-align: right;">Importe</th>
                        </tr>
                    </thead>
                    <tbody>
                        {transaction_rows_html}
                    </tbody>
                </table>
            </div>

            <!-- Pie de Página -->
            <div style="{footer_style}">
                Este es un correo generado automáticamente.
            </div>
        </div>
    </body>
    </html>
    """

    # --- Creación y Envío del Mensaje ---
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER

    # Texto plano como fallback (sin imagen)
    plain_text_content = f"Se han detectado {len(transactions)} nuevos movimientos del archivo '{filename}':\n\n"
    for tx in transactions:
        plain_text_content += f"- Fecha: {tx['fecha']}, Desc: {tx['descripcion']}, Importe: {tx['monto']}\n"
    plain_text_content += "\nEste es un correo generado automáticamente."

    msg.set_content(plain_text_content)
    msg.add_alternative(html_content, subtype='html') # Versión HTML

    # Envío
    try:
        logger.info(f"Conectando a SMTP {SMTP_SERVER}:{SMTP_PORT}...")
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_LOGIN, EMAIL_PASSWORD)
        logger.info("Enviando email (HTML con imagen)...")
        server.send_message(msg)
        server.quit()
        logger.info("Email enviado correctamente.")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error("Error de autenticación SMTP.")
        return False
    except Exception as e:
        logger.error(f"Error al enviar email: {e}", exc_info=False)
        return False

def process_excel_file(filepath: Path, original_filename: str) -> dict:
    """
    Procesa un archivo Excel, lo guarda en la BD y devuelve un resumen.
    """
    logger.info(f"Iniciando lógica de procesamiento para: {original_filename} (desde {filepath})")
    try: init_db(DB_FILE_PATH)
    except Exception as e:
        logger.critical(f"Fallo crítico al inicializar la BD para {original_filename}: {e}", exc_info=True)
        return {'status': 'error', 'message': 'Error crítico inicializando la base de datos.', 'new_count': 0, 'failed_rows': 0}

    df = None
    try:
        # ... (Lectura de Excel y validaciones) ...
        df = pd.read_excel(filepath, sheet_name=EXCEL_SHEET_NAME, skiprows=EXCEL_SKIP_ROWS,
                           engine='openpyxl' if str(filepath).lower().endswith('.xlsx') else 'xlrd')
        logger.info(f"Archivo Excel '{original_filename}' leído. {len(df)} filas encontradas.")
        essential_cols_excel = [COL_DATE_EXCEL, COL_DESC_EXCEL, COL_AMOUNT_EXCEL]
        if COL_BANK_ID_EXCEL: essential_cols_excel.append(COL_BANK_ID_EXCEL)
        required_cols_excel = {COL_DATE_EXCEL, COL_DESC_EXCEL, COL_AMOUNT_EXCEL}
        missing_cols = required_cols_excel - set(df.columns)
        if missing_cols:
            msg = f"Faltan columnas requeridas en '{original_filename}': {missing_cols}."
            logger.error(msg)
            return {'status': 'error', 'message': msg, 'new_count': 0, 'failed_rows': 0}
        df.dropna(subset=essential_cols_excel, how='all', inplace=True)
        logger.info(f"Filas después de eliminar vacíos: {len(df)}")
        if df.empty:
            logger.info(f"'{original_filename}' vacío o sin datos relevantes.")
            return {'status': 'warning', 'message': f"El archivo '{original_filename}' está vacío o no tiene datos válidos.", 'new_count': 0, 'failed_rows': 0}

    except FileNotFoundError:
        logger.error(f"Archivo temporal no encontrado: {filepath}")
        return {'status': 'error', 'message': 'Error interno: Archivo temp no encontrado.', 'new_count': 0, 'failed_rows': 0}
    except ImportError as e:
        logger.error(f"Error importación librería Excel: {e}")
        return {'status': 'error', 'message': "Error config interna: Falta librería Excel.", 'new_count': 0, 'failed_rows': 0}
    except Exception as e:
        logger.error(f"Error al leer/limpiar '{original_filename}': {e}", exc_info=True)
        return {'status': 'error', 'message': f"Error al leer Excel '{original_filename}'. ¿Formato correcto?", 'new_count': 0, 'failed_rows': 0}

    new_transactions_count = 0
    processed_rows_count = 0
    failed_rows_count = 0
    new_transactions_details_for_email = []
    current_time_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for index, row in df.iterrows():
        processed_rows_count += 1
        tx_id = get_transaction_id(row, COL_DATE_EXCEL, COL_DESC_EXCEL, COL_AMOUNT_EXCEL, COL_BANK_ID_EXCEL)
        if tx_id is None:
            logger.warning(f"No se pudo generar ID fila {index + EXCEL_SKIP_ROWS + 2} en '{original_filename}'.")
            failed_rows_count += 1
            continue
        try:
            if not check_if_id_exists(DB_FILE_PATH, tx_id):
                fecha_dt = pd.to_datetime(row[COL_DATE_EXCEL], errors='coerce')
                if pd.isna(fecha_dt):
                    logger.warning(f"Fecha inválida fila {index + EXCEL_SKIP_ROWS + 2} en '{original_filename}'.")
                    failed_rows_count += 1; continue
                fecha_db = fecha_dt.strftime('%Y-%m-%d')
                fecha_email = fecha_dt.strftime('%d/%m/%Y') # Formato para email
                desc = str(row[COL_DESC_EXCEL]).strip() if pd.notna(row[COL_DESC_EXCEL]) else 'N/A'
                try:
                    monto = float(row[COL_AMOUNT_EXCEL])
                    # Formato específico para email (con comas y 2 decimales)
                    monto_email = f"{monto:,.2f}"
                except (ValueError, TypeError):
                    logger.warning(f"Importe inválido fila {index + EXCEL_SKIP_ROWS + 2} en '{original_filename}'.")
                    failed_rows_count += 1; continue

                tx_data = { # Datos para BD
                    COL_DB_ID: tx_id, COL_DB_DATE: fecha_db, COL_DB_DESC: desc,
                    COL_DB_AMOUNT: monto, COL_DB_PROCESSED_AT: current_time_iso,
                    COL_DB_CATEGORY: None
                }
                if insert_transaction(DB_FILE_PATH, tx_data):
                    new_transactions_count += 1
                    # Guardar datos formateados para el email
                    new_transactions_details_for_email.append({
                        'fecha': fecha_email,
                        'descripcion': desc,
                        'monto': monto_email
                    })
                else:
                    if not check_if_id_exists(DB_FILE_PATH, tx_id):
                         failed_rows_count += 1
        except KeyError as e:
            logger.error(f"Error clave fila {index + EXCEL_SKIP_ROWS + 2} en '{original_filename}': {e}")
            failed_rows_count += 1
        except Exception as e:
            logger.error(f"Error inesperado fila {index + EXCEL_SKIP_ROWS + 2} en '{original_filename}': {e}", exc_info=True)
            failed_rows_count += 1

    logger.info(f"Procesamiento lógico '{original_filename}' completado. Nuevas: {new_transactions_count}. Fallidas: {failed_rows_count}.")

    final_status = 'success'
    final_message = f"Procesado '{original_filename}': {new_transactions_count} nuevas transacciones añadidas."
    if failed_rows_count > 0:
         final_message += f" ({failed_rows_count} filas con errores/omitidas)."
         final_status = 'warning'
    if new_transactions_count == 0 and failed_rows_count == 0 and processed_rows_count > 0:
        final_message = f"Procesado '{original_filename}': No se encontraron nuevas transacciones (ya registradas)."

    # Enviar email si hubo transacciones nuevas
    if new_transactions_count > 0:
        email_subject = f"Invernalia Banking: nuevos movimientos bancarios detectados ({new_transactions_count})"
        # Llamar a send_email con la lista de detalles y el nombre del archivo
        if send_email(email_subject, new_transactions_details_for_email, original_filename):
            logger.info(f"Email notificación para '{original_filename}' enviado.")
        else:
            logger.error(f"Fallo al enviar email notificación para '{original_filename}'.")

    return {
        'status': final_status,
        'message': final_message,
        'new_count': new_transactions_count,
        'failed_rows': failed_rows_count
    }