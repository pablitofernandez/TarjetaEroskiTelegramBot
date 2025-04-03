import pandas as pd
import smtplib
from email.message import EmailMessage
import os
import hashlib
import logging
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta # Añadido timedelta
import difflib # Añadido difflib

logger = logging.getLogger("ExcelProcessorLogic")

# --- Configuración desde Variables de Entorno ---
SMTP_SERVER = os.environ.get('SMTP_SERVER')
SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
EMAIL_SENDER = os.environ.get('EMAIL_SENDER')
EMAIL_LOGIN = os.environ.get('EMAIL_LOGIN')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')
EMAIL_RECEIVER = os.environ.get('EMAIL_RECEIVER')
EMAIL_HEADER_IMAGE = os.environ.get('EMAIL_HEADER_IMAGE')
DB_FILE_PATH = Path(os.environ.get('DB_FILE', '/app/state/transactions.db'))
EXCEL_SHEET_NAME = os.environ.get('EXCEL_SHEET_NAME', 0)
try: EXCEL_SHEET_NAME = int(EXCEL_SHEET_NAME)
except ValueError: pass
EXCEL_SKIP_ROWS = int(os.environ.get('EXCEL_SKIP_ROWS', 0))
COL_DATE_EXCEL = os.environ.get('COL_DATE_EXCEL', 'Fecha')
COL_DESC_EXCEL = os.environ.get('COL_DESC_EXCEL', 'Descripción')
COL_AMOUNT_EXCEL = os.environ.get('COL_AMOUNT_EXCEL', 'Importe')
COL_BANK_ID_EXCEL = os.environ.get('COL_BANK_ID_EXCEL', None)
# --- Nuevas Variables para Duplicados ---
SIMILARITY_THRESHOLD = float(os.environ.get('SIMILARITY_THRESHOLD', 0.6)) # Umbral más bajo
DATE_WINDOW_DAYS = int(os.environ.get('DATE_WINDOW_DAYS', 3))

# Nombres de Columnas BD (Mantenidos por consistencia)
COL_DB_ID = "transaction_id"
COL_DB_DATE = "transaction_date"
COL_DB_DESC = "description"
COL_DB_AMOUNT = "amount"
COL_DB_CATEGORY = "category"
COL_DB_PROCESSED_AT = "processed_at"
# COL_DB_CARD_NUMBER = "card_number" # Si añades número de tarjeta

# --- Funciones de Base de Datos (init_db, check_if_id_exists, insert_transaction sin cambios) ---
def get_transaction_id(row, date_col, desc_col, amount_col, bank_id_col):
    """Crea un ID único para la transacción."""
    if bank_id_col and bank_id_col in row and pd.notna(row[bank_id_col]):
        return str(row[bank_id_col]).strip()
    else:
        try:
            if date_col not in row or desc_col not in row or amount_col not in row:
                 logger.error(f"Faltan columnas clave ({date_col}, {desc_col}, {amount_col}) en la fila para generar ID: {row}")
                 return None
            # Asegurarse que los componentes del ID son consistentes
            date_str = str(pd.to_datetime(row[date_col], errors='coerce').date()) # Usar solo fecha normalizada
            desc_str = str(row[desc_col]).strip().lower() # Normalizar descripción para ID
            amount_str = f"{float(row[amount_col]):.2f}" # Normalizar importe para ID
            data_str = f"{date_str}_{desc_str}_{amount_str}"
            return hashlib.md5(data_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error al generar ID para la fila: {row}. Error: {e}")
            return None

def init_db(db_path: Path):
    """Inicializa la BD y crea la tabla si no existe."""
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Considera añadir COL_DB_CARD_NUMBER si lo usas
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
        # Crear índices puede mejorar rendimiento de búsquedas
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_date ON transactions ({COL_DB_DATE});")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_amount ON transactions ({COL_DB_AMOUNT});")
        conn.commit()
        conn.close()
        logger.info(f"Base de datos inicializada/verificada en {db_path}")
    except sqlite3.Error as e:
        logger.error(f"Error al inicializar la base de datos {db_path}: {e}")
        raise

def check_if_id_exists(db_path: Path, transaction_id: str) -> bool:
    """Verifica si un transaction_id ya existe en la BD."""
    if not transaction_id: return False # No buscar si el ID es nulo
    exists = False
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM transactions WHERE {COL_DB_ID} = ?", (transaction_id,))
        result = cursor.fetchone()
        if result:
            exists = True
    except sqlite3.Error as e:
        logger.error(f"Error al consultar la base de datos para ID {transaction_id}: {e}")
        raise
    finally:
        if conn: conn.close()
    return exists

def insert_transaction(db_path: Path, tx_data: dict) -> bool:
    """Inserta una nueva transacción en la BD. Devuelve True si insertó, False si error/duplicado."""
    # Añade validación básica de datos entrantes
    required_keys = {COL_DB_ID, COL_DB_DATE, COL_DB_DESC, COL_DB_AMOUNT, COL_DB_PROCESSED_AT}
    if not required_keys.issubset(tx_data.keys()):
        logger.error(f"Datos incompletos para insertar: Faltan {required_keys - tx_data.keys()} en {tx_data.get(COL_DB_ID, 'ID Desconocido')}")
        return False

    conn = None
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
            tx_data.get(COL_DB_CATEGORY) # Usar get para categoría opcional
        ))
        conn.commit()
        logger.debug(f"Transacción insertada: {tx_data[COL_DB_ID]}")
        return True
    except sqlite3.IntegrityError:
         logger.warning(f"Intento de insertar ID duplicado (ignorado): {tx_data[COL_DB_ID]}")
         # Aunque check_if_id_exists debería prevenir esto, puede ocurrir por concurrencia
         return False
    except sqlite3.Error as e:
        logger.error(f"Error BD al insertar transacción {tx_data.get(COL_DB_ID, 'ID Desconocido')}: {e}")
        return False
    finally:
        if conn: conn.close()


# --- NUEVA FUNCIÓN find_potential_duplicate (MODIFICADA) ---
def find_potential_duplicate(db_path: Path, amount: float, date_obj: datetime, description: str) -> dict | None:
    """
    Busca en la BD una transacción potencialmente duplicada basada en importe,
    ventana de tiempo y similitud de descripción.
    DEVUELVE: Detalles de la transacción existente si encuentra coincidencia, sino None.
    """
    logger.debug(f"Buscando duplicado cercano para: Monto={amount}, Fecha={date_obj.strftime('%Y-%m-%d')}, Desc='{description[:30]}...'")
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row # Para acceder por nombre de columna
        cursor = conn.cursor()

        # Calcular ventana de tiempo
        date_start = (date_obj - timedelta(days=DATE_WINDOW_DAYS)).strftime('%Y-%m-%d')
        date_end = (date_obj + timedelta(days=DATE_WINDOW_DAYS)).strftime('%Y-%m-%d')

        query = f"""
        SELECT {COL_DB_ID}, {COL_DB_DATE}, {COL_DB_DESC}, {COL_DB_AMOUNT}
        FROM transactions
        WHERE {COL_DB_AMOUNT} BETWEEN ? AND ? -- Pequeño margen para flotantes
        AND {COL_DB_DATE} BETWEEN ? AND ?
        ORDER BY {COL_DB_DATE} DESC -- Priorizar más recientes
        LIMIT 10 -- Limitar candidatos a revisar
        """
        # Añadir pequeño margen al importe por posibles errores de redondeo
        amount_margin = 0.01
        params = [amount - amount_margin, amount + amount_margin, date_start, date_end]

        cursor.execute(query, params)
        potential_matches = cursor.fetchall()

        if not potential_matches:
            logger.debug("No se encontraron candidatos por importe y fecha.")
            return None

        normalized_new_desc = ' '.join(description.lower().split())

        best_match = None
        highest_similarity = 0.0

        for row in potential_matches:
            existing_desc = row[COL_DB_DESC]
            normalized_existing_desc = ' '.join(existing_desc.lower().split())

            similarity = difflib.SequenceMatcher(None, normalized_new_desc, normalized_existing_desc).ratio()
            logger.debug(f"Comparando con ID {row[COL_DB_ID]}: Similitud={similarity:.2f} (Umbral={SIMILARITY_THRESHOLD})")

            if similarity >= SIMILARITY_THRESHOLD and similarity > highest_similarity:
                 highest_similarity = similarity
                 best_match = dict(row) # Convertir sqlite3.Row a dict

        if best_match:
             logger.info(f"Potencial duplicado encontrado para '{description[:30]}...' (Monto: {amount}). Similar a transacción existente {best_match[COL_DB_ID]} (Similitud: {highest_similarity:.2f}). Pidiendo confirmación.")
             return best_match # Devolver detalles del match encontrado

        logger.debug("Ningún candidato superó el umbral de similitud.")
        return None

    except sqlite3.Error as e:
        logger.error(f"Error BD buscando duplicado cercano para monto {amount}: {e}")
        return None # En caso de error, no bloquear, asumir que no es duplicado
    except Exception as e:
        logger.error(f"Error inesperado buscando duplicado cercano: {e}", exc_info=True)
        return None
    finally:
        if conn: conn.close()

# --- Función send_email (sin cambios significativos, se mantiene igual) ---
def send_email(subject: str, transactions: list, filename: str):
    # ... (código de send_email se mantiene igual que antes) ...
    if not all([SMTP_SERVER, EMAIL_LOGIN, EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER]):
        logger.error("Faltan variables de entorno para enviar correo. No se enviará.")
        return False
    # ... resto del código de send_email ...
    # Asegúrate que los datos en 'transactions' para el email siguen siendo correctos
    # (fecha_email, desc, monto_email)
    header_image_url = EMAIL_HEADER_IMAGE
    body_style = "font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333; margin: 0; padding: 0; background-color: #f8f9fa;"
    container_style = "max-width: 600px; margin: 20px auto; padding: 0; border: 1px solid #ddd; border-radius: 5px; background-color: #ffffff; overflow: hidden;"
    image_container_style = "text-align: center; margin-bottom: 20px; background-color: #ffffff;"
    image_style = f"display: block; margin: 0 auto; max-width: 100%; height: auto; border: 0;"
    content_padding_style = "padding: 0 20px 20px 20px;"
    header_style = "font-size: 20px; font-weight: bold; color: #0056b3; margin-bottom: 15px; text-align: center;"
    table_style = "width: 100%; border-collapse: collapse; margin-top: 15px;"
    th_style = "border: 1px solid #ddd; padding: 8px 12px; background-color: #f2f2f2; text-align: left; font-weight: bold;"
    td_style = "border: 1px solid #ddd; padding: 8px 12px; text-align: left;"
    td_amount_style = f"{td_style} text-align: right; font-weight: bold;"
    footer_style = "margin-top: 20px; padding: 10px; font-size: 12px; color: #888; text-align: center; background-color: #f2f2f2;"

    transaction_rows_html = ""
    for tx in transactions: # Asegúrate que tx tiene 'fecha', 'descripcion', 'monto'
        escaped_desc = tx.get('descripcion', 'N/A')
        transaction_rows_html += f"""
        <tr>
            <td style="{td_style}">{tx.get('fecha', 'N/A')}</td>
            <td style="{td_style}">{escaped_desc}</td>
            <td style="{td_amount_style}">{tx.get('monto', 'N/A')} €</td>
        </tr>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head> <meta charset="UTF-8"> <title>{subject}</title> </head>
    <body style="{body_style}">
        <div style="{container_style}">
            <div style="{image_container_style}"><img src="{header_image_url}" alt="Cabecera" width="100%" height="auto" style="{image_style}"></div>
            <div style="{content_padding_style}">
                <p>Se han detectado <strong>{len(transactions)}</strong> nuevos movimientos confirmados del archivo: <strong>{filename}</strong></p>
                <table style="{table_style}">
                    <thead><tr>
                        <th style="{th_style}">Fecha</th>
                        <th style="{th_style}">Descripción</th>
                        <th style="{th_style} text-align: right;">Importe</th>
                    </tr></thead>
                    <tbody>{transaction_rows_html}</tbody>
                </table>
            </div>
            <div style="{footer_style}">Correo generado automáticamente.</div>
        </div>
    </body>
    </html>
    """

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    plain_text_content = f"Se han detectado {len(transactions)} nuevos movimientos confirmados del archivo '{filename}':\n\n"
    for tx in transactions:
        plain_text_content += f"- Fecha: {tx.get('fecha', 'N/A')}, Desc: {tx.get('descripcion', 'N/A')}, Importe: {tx.get('monto', 'N/A')}\n"
    msg.set_content(plain_text_content)
    msg.add_alternative(html_content, subtype='html')

    try:
        logger.info(f"Conectando a SMTP {SMTP_SERVER}:{SMTP_PORT}...")
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_LOGIN, EMAIL_PASSWORD)
        logger.info("Enviando email...")
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


# --- Función Principal de Procesamiento (MODIFICADA SIGNIFICATIVAMENTE) ---
def process_excel_file(filepath: Path, original_filename: str) -> dict:
    """
    Procesa un archivo Excel. Identifica nuevas transacciones, duplicados exactos,
    y posibles duplicados que requieren confirmación del usuario.
    Devuelve un diccionario con el resumen y la lista de pendientes si los hay.
    """
    logger.info(f"Iniciando lógica de procesamiento para: {original_filename} (desde {filepath})")
    start_time = datetime.now()

    try: init_db(DB_FILE_PATH)
    except Exception as e:
        logger.critical(f"Fallo crítico al inicializar la BD para {original_filename}: {e}", exc_info=True)
        return {'status': 'error', 'message': 'Error crítico inicializando la base de datos.', 'new_count_inserted': 0, 'failed_rows': 0, 'pending_confirmation': []}

    df = None
    try:
        # Lectura de Excel
        df = pd.read_excel(filepath, sheet_name=EXCEL_SHEET_NAME, skiprows=EXCEL_SKIP_ROWS,
                           engine='openpyxl' if str(filepath).lower().endswith('.xlsx') else 'xlrd')
        logger.info(f"Archivo Excel '{original_filename}' leído. {len(df)} filas encontradas (antes de limpiar).")

        # Validación de columnas
        required_cols_excel = {COL_DATE_EXCEL, COL_DESC_EXCEL, COL_AMOUNT_EXCEL}
        essential_cols_excel = list(required_cols_excel)
        if COL_BANK_ID_EXCEL: essential_cols_excel.append(COL_BANK_ID_EXCEL)

        missing_cols = required_cols_excel - set(df.columns)
        if missing_cols:
            msg = f"Faltan columnas requeridas en '{original_filename}': {missing_cols}."
            logger.error(msg)
            return {'status': 'error', 'message': msg, 'new_count_inserted': 0, 'failed_rows': len(df), 'pending_confirmation': []}

        # Limpieza básica
        df.dropna(subset=[COL_DATE_EXCEL, COL_AMOUNT_EXCEL], how='any', inplace=True) # Fecha e importe son cruciales
        logger.info(f"Filas después de eliminar vacíos en Fecha/Importe: {len(df)}")
        if df.empty:
            msg = f"'{original_filename}' vacío o sin datos válidos tras limpieza inicial."
            logger.info(msg)
            return {'status': 'warning', 'message': msg, 'new_count_inserted': 0, 'failed_rows': 0, 'pending_confirmation': []}

    except FileNotFoundError:
        logger.error(f"Archivo temporal no encontrado: {filepath}")
        return {'status': 'error', 'message': 'Error interno: Archivo temp no encontrado.', 'new_count_inserted': 0, 'failed_rows': 0, 'pending_confirmation': []}
    except ImportError as e:
         logger.error(f"Error importación librería Excel: {e}")
         return {'status': 'error', 'message': f"Error config interna: Falta librería Excel ({'openpyxl' if '.xlsx' in str(filepath).lower() else 'xlrd'}).", 'new_count_inserted': 0, 'failed_rows': 0, 'pending_confirmation': []}
    except Exception as e:
        logger.error(f"Error al leer/limpiar '{original_filename}': {e}", exc_info=True)
        return {'status': 'error', 'message': f"Error al leer Excel '{original_filename}'. ¿Formato correcto?", 'new_count_inserted': 0, 'failed_rows': len(df) if df is not None else 0, 'pending_confirmation': []}

    # --- Contadores e inicialización de listas ---
    new_transactions_inserted_count = 0
    processed_rows_count = 0
    failed_rows_count = 0
    exact_duplicates_skipped_count = 0
    pending_confirmation_list = []
    inserted_transactions_for_email = [] # Solo las insertadas directamente

    # --- Bucle de procesamiento de filas ---
    for index, row in df.iterrows():
        processed_rows_count += 1
        row_number = index + EXCEL_SKIP_ROWS + 2 # Para logs más claros

        # 1. Parsear datos y validar fila individual
        try:
            fecha_dt = pd.to_datetime(row[COL_DATE_EXCEL], errors='coerce', dayfirst=True) # <-- AÑADIR dayfirst=True
            if pd.isna(fecha_dt):
                logger.warning(f"Fecha inválida omitiendo fila {row_number} en '{original_filename}': {row[COL_DATE_EXCEL]}")
                failed_rows_count += 1; continue
            fecha_db = fecha_dt.strftime('%Y-%m-%d')
            fecha_display = fecha_dt.strftime('%d/%m/%Y') # Para mostrar al usuario

            desc = str(row.get(COL_DESC_EXCEL, '')).strip()
            if not desc: desc = "Sin Descripción"

            monto = round(float(row[COL_AMOUNT_EXCEL]), 2) # Redondear a 2 decimales siempre
            monto_display = f"{monto:,.2f}" # Formato es_ES para mostrar

        except (ValueError, TypeError, KeyError) as e:
            logger.warning(f"Dato inválido (Importe?) omitiendo fila {row_number} en '{original_filename}': {e}")
            failed_rows_count += 1; continue
        except Exception as e:
            logger.error(f"Error inesperado parseando fila {row_number} en '{original_filename}': {e}", exc_info=True)
            failed_rows_count += 1; continue

        # 2. Generar ID único para esta transacción (potencial)
        tx_id = get_transaction_id(row, COL_DATE_EXCEL, COL_DESC_EXCEL, COL_AMOUNT_EXCEL, COL_BANK_ID_EXCEL)
        if tx_id is None:
            logger.warning(f"No se pudo generar ID, omitiendo fila {row_number} en '{original_filename}'.")
            failed_rows_count += 1
            continue

        # --- Lógica de Duplicados ---
        try:
            # 3. Comprobar duplicado EXACTO primero
            if check_if_id_exists(DB_FILE_PATH, tx_id):
                logger.debug(f"Duplicado exacto (ID: {tx_id}) encontrado. Omitiendo fila {row_number}.")
                exact_duplicates_skipped_count += 1
                continue

            # 4. Si no es exacto, buscar duplicado CERCANO
            existing_match = find_potential_duplicate(DB_FILE_PATH, monto, fecha_dt, desc)

            if existing_match:
                # 5. Si se encuentra coincidencia cercana -> Añadir a PENDIENTES
                logger.info(f"Fila {row_number} marcada para confirmación (similar a {existing_match[COL_DB_ID]}).")
                pending_confirmation_list.append({
                    'new_transaction': {
                        'potential_id': tx_id, # El ID que tendría si se inserta
                        'date_db': fecha_db,
                        'date_display': fecha_display,
                        'desc': desc,
                        'amount': monto,
                        'amount_display': monto_display,
                        # Incluir cualquier otro dato necesario para la inserción posterior
                    },
                    'existing_match': {
                        'id': existing_match[COL_DB_ID],
                        'date_display': datetime.strptime(existing_match[COL_DB_DATE], '%Y-%m-%d').strftime('%d/%m/%Y'),
                        'desc': existing_match[COL_DB_DESC],
                        'amount_display': f"{existing_match[COL_DB_AMOUNT]:,.2f}"
                    }
                })
                continue # Pasar a la siguiente fila, no insertar ahora

            # 6. Si no es duplicado exacto NI cercano -> INSERTAR
            logger.debug(f"Fila {row_number} (ID: {tx_id}) no es duplicado. Insertando...")
            current_time_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            tx_data = {
                COL_DB_ID: tx_id,
                COL_DB_DATE: fecha_db,
                COL_DB_DESC: desc,
                COL_DB_AMOUNT: monto,
                COL_DB_PROCESSED_AT: current_time_iso,
                COL_DB_CATEGORY: None # O lógica de categoría si la tienes
            }
            if insert_transaction(DB_FILE_PATH, tx_data):
                new_transactions_inserted_count += 1
                # Añadir a la lista para el email SOLO si se insertó directamente
                inserted_transactions_for_email.append({
                    'fecha': fecha_display,
                    'descripcion': desc,
                    'monto': monto_display
                })
            else:
                # Si falla la inserción aquí, podría ser un error BD o duplicado exacto por concurrencia
                logger.error(f"Fallo al insertar transacción (ya validada como nueva): {tx_id}. Contando como fallo.")
                failed_rows_count += 1

        except Exception as e:
            logger.error(f"Error inesperado procesando lógica de duplicados/inserción fila {row_number}: {e}", exc_info=True)
            failed_rows_count += 1

    # --- Finalización del Procesamiento ---
    processing_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"Procesamiento lógico de '{original_filename}' completado en {processing_time:.2f}s. "
                f"Insertadas: {new_transactions_inserted_count}. "
                f"Pendientes: {len(pending_confirmation_list)}. "
                f"Duplicados exactos: {exact_duplicates_skipped_count}. "
                f"Fallidas: {failed_rows_count}.")

    # Determinar estado final y mensaje
    final_status = 'success'
    final_message_parts = [f"'{original_filename}' procesado."]
    if new_transactions_inserted_count > 0:
        final_message_parts.append(f"{new_transactions_inserted_count} nuevas insertadas.")
    if len(pending_confirmation_list) > 0:
        final_message_parts.append(f"{len(pending_confirmation_list)} requieren confirmación.")
        final_status = 'confirmation_required' # Prioridad si hay pendientes
    if exact_duplicates_skipped_count > 0:
        final_message_parts.append(f"{exact_duplicates_skipped_count} duplicados exactos omitidos.")
    if failed_rows_count > 0:
        final_message_parts.append(f"{failed_rows_count} filas con errores.")
        # Si solo hubo errores o pendientes+errores, el estado debe reflejarlo
        if final_status != 'confirmation_required': final_status = 'warning'

    if final_status == 'success' and new_transactions_inserted_count == 0 and len(pending_confirmation_list) == 0 and processed_rows_count > 0:
         final_message = f"'{original_filename}' procesado: No se encontraron transacciones nuevas ni dudosas (ya registradas o vacías)."
    else:
         final_message = " ".join(final_message_parts)

    # Enviar email SOLO si hubo inserciones DIRECTAS
    if inserted_transactions_for_email:
        email_subject = f"Invernalia Banking: {len(inserted_transactions_for_email)} nuevos movimientos confirmados"
        if send_email(email_subject, inserted_transactions_for_email, original_filename):
            logger.info(f"Email de notificación para '{original_filename}' enviado (solo directas).")
        else:
            logger.error(f"Fallo al enviar email de notificación para '{original_filename}'.")

    # Devolver el resultado estructurado
    return {
        'status': final_status,
        'message': final_message,
        'new_count_inserted': new_transactions_inserted_count,
        'pending_count': len(pending_confirmation_list),
        'duplicates_skipped': exact_duplicates_skipped_count,
        'failed_rows': failed_rows_count,
        'pending_confirmation': pending_confirmation_list # La lista de pendientes
    }