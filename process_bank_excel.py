import pandas as pd
import smtplib
from email.message import EmailMessage
import os
import hashlib
import logging
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta
import difflib
from typing import Optional # Necesario para Python 3.9 o para claridad en 3.10+

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
SIMILARITY_THRESHOLD = float(os.environ.get('SIMILARITY_THRESHOLD', 0.6))
DATE_WINDOW_DAYS = int(os.environ.get('DATE_WINDOW_DAYS', 3))

# Nombres de Columnas BD
COL_DB_ID = "transaction_id"
COL_DB_DATE = "transaction_date"
COL_DB_DESC = "description"
COL_DB_AMOUNT = "amount"
COL_DB_CATEGORY = "category"
COL_DB_PROCESSED_AT = "processed_at"

# --- Funciones de Base de Datos ---
def init_db(db_path: Path):
    """Inicializa la BD y crea las tablas (transactions, discarded_log) si no existen."""
    conn = None
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Tabla principal
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
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_date ON transactions ({COL_DB_DATE});")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_amount ON transactions ({COL_DB_AMOUNT});")

        # Tabla para log de descartados
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS discarded_log (
            discarded_tx_id TEXT PRIMARY KEY NOT NULL,
            existing_tx_id TEXT NOT NULL,
            discard_timestamp TEXT NOT NULL
        )
        ''')

        conn.commit()
        logger.info(f"Base de datos inicializada/verificada en {db_path} (Tablas: transactions, discarded_log)")
    except sqlite3.Error as e:
        logger.error(f"Error al inicializar la base de datos {db_path}: {e}")
        raise
    finally:
        if conn: conn.close()

def get_transaction_id(row, date_col, desc_col, amount_col, bank_id_col):
    """Crea un ID único para la transacción, normalizando los datos."""
    if bank_id_col and bank_id_col in row and pd.notna(row[bank_id_col]):
        return str(row[bank_id_col]).strip()
    else:
        try:
            # Validar columnas necesarias
            if date_col not in row or desc_col not in row or amount_col not in row:
                 logger.error(f"Faltan cols ({date_col}, {desc_col}, {amount_col}) para generar ID: {row.to_dict()}")
                 return None
            # Normalizar datos para el ID
            date_obj = pd.to_datetime(row[date_col], errors='coerce', dayfirst=True)
            if pd.isna(date_obj):
                logger.error(f"Fecha inválida '{row[date_col]}' para generar ID.")
                return None
            date_str = date_obj.strftime('%Y-%m-%d') # Formato ISO consistente

            desc_str = str(row[desc_col]).strip().lower() # Minúsculas y sin espacios extra

            amount_val = float(row[amount_col])
            amount_str = f"{amount_val:.2f}" # Formato consistente con 2 decimales

            data_str = f"{date_str}_{desc_str}_{amount_str}"
            return hashlib.md5(data_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error generando ID para fila: {row.to_dict()}. Error: {e}")
            return None

def check_if_id_exists(db_path: Path, transaction_id: str) -> bool:
    """Verifica si un transaction_id ya existe en la tabla 'transactions'."""
    if not transaction_id: return False
    exists = False
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM transactions WHERE {COL_DB_ID} = ?", (transaction_id,))
        exists = cursor.fetchone() is not None
    except sqlite3.Error as e:
        logger.error(f"Error BD consultando existencia ID {transaction_id}: {e}")
        # No relanzar, podría detener el proceso. Asumir que no existe.
    finally:
        if conn: conn.close()
    return exists

def check_if_previously_discarded(db_path: Path, transaction_id: str) -> bool:
    """Verifica si un transaction_id ya fue descartado previamente (tabla 'discarded_log')."""
    if not transaction_id: return False
    discarded = False
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM discarded_log WHERE discarded_tx_id = ?", (transaction_id,))
        discarded = cursor.fetchone() is not None
        if discarded: logger.debug(f"Transacción {transaction_id} encontrada en log de descartados.")
    except sqlite3.Error as e:
        logger.error(f"Error BD consultando discarded_log para ID {transaction_id}: {e}")
    finally:
        if conn: conn.close()
    return discarded

def insert_transaction(db_path: Path, tx_data: dict) -> bool:
    """Inserta una nueva transacción en la tabla 'transactions'."""
    required_keys = {COL_DB_ID, COL_DB_DATE, COL_DB_DESC, COL_DB_AMOUNT, COL_DB_PROCESSED_AT}
    if not required_keys.issubset(tx_data.keys()):
        logger.error(f"Datos incompletos para insertar: Faltan {required_keys - tx_data.keys()} en ID {tx_data.get(COL_DB_ID, '???')}")
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
            tx_data[COL_DB_ID], tx_data[COL_DB_DATE], tx_data[COL_DB_DESC],
            tx_data[COL_DB_AMOUNT], tx_data[COL_DB_PROCESSED_AT], tx_data.get(COL_DB_CATEGORY)
        ))
        conn.commit()
        logger.debug(f"Transacción insertada: {tx_data[COL_DB_ID]}")
        return True
    except sqlite3.IntegrityError:
         logger.warning(f"Intento de insertar ID duplicado (ignorado): {tx_data[COL_DB_ID]}")
         return False # No es error fatal si ya existe
    except sqlite3.Error as e:
        logger.error(f"Error BD al insertar ID {tx_data.get(COL_DB_ID, '???')}: {e}")
        return False
    finally:
        if conn: conn.close()

def log_discarded_duplicate(db_path: Path, discarded_id: str, existing_id: str):
    """Registra una transacción descartada en la tabla 'discarded_log'."""
    if not discarded_id or not existing_id:
        logger.warning("Intento de loggear descarte con IDs faltantes.")
        return False
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        cursor = conn.cursor()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Usar INSERT OR IGNORE para evitar error si ya existe (p.ej., doble click en bot)
        cursor.execute(
            "INSERT OR IGNORE INTO discarded_log (discarded_tx_id, existing_tx_id, discard_timestamp) VALUES (?, ?, ?)",
            (discarded_id, existing_id, timestamp)
        )
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"Transacción {discarded_id} registrada como descartada (similar a {existing_id}).")
        else:
            logger.debug(f"Transacción {discarded_id} ya estaba en log de descartados.")
        return True
    except sqlite3.Error as e:
        logger.error(f"Error BD registrando descarte ID {discarded_id}: {e}")
        return False
    finally:
        if conn: conn.close()

def find_potential_duplicate(db_path: Path, amount: float, date_obj: datetime, description: str) -> Optional[dict]:
    """Busca en 'transactions' una potencial duplicada. Devuelve detalles del match o None."""
    logger.debug(f"Buscando duplicado cercano: Monto={amount}, Fecha={date_obj.strftime('%Y-%m-%d')}, Desc='{description[:30]}...'")
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        date_start = (date_obj - timedelta(days=DATE_WINDOW_DAYS)).strftime('%Y-%m-%d')
        date_end = (date_obj + timedelta(days=DATE_WINDOW_DAYS)).strftime('%Y-%m-%d')
        amount_margin = 0.01 # Margen para flotantes
        query = f"""
        SELECT {COL_DB_ID}, {COL_DB_DATE}, {COL_DB_DESC}, {COL_DB_AMOUNT} FROM transactions
        WHERE {COL_DB_AMOUNT} BETWEEN ? AND ? AND {COL_DB_DATE} BETWEEN ? AND ?
        ORDER BY ABS(JULIANDAY({COL_DB_DATE}) - JULIANDAY(?)) -- Priorizar fecha más cercana
        LIMIT 10 -- Limitar candidatos
        """
        params = [amount - amount_margin, amount + amount_margin, date_start, date_end, date_obj.strftime('%Y-%m-%d')]
        cursor.execute(query, params)
        potential_matches = cursor.fetchall()

        if not potential_matches: return None

        normalized_new_desc = ' '.join(description.lower().split())
        best_match = None
        highest_similarity = 0.0

        for row in potential_matches:
            normalized_existing_desc = ' '.join(row[COL_DB_DESC].lower().split())
            similarity = difflib.SequenceMatcher(None, normalized_new_desc, normalized_existing_desc).ratio()
            logger.debug(f"Comparando con ID {row[COL_DB_ID]} ({row[COL_DB_DATE]}): Sim={similarity:.4f} (Umbral={SIMILARITY_THRESHOLD})")
            if similarity >= SIMILARITY_THRESHOLD and similarity > highest_similarity:
                 highest_similarity = similarity
                 best_match = dict(row)

        if best_match:
             logger.info(f"Potencial duplicado encontrado para '{description[:30]}...' (Monto: {amount}). Similar a {best_match[COL_DB_ID]} (Sim: {highest_similarity:.2f}).")
             return best_match
        else:
            logger.debug("Ningún candidato superó el umbral de similitud.")
            return None

    except sqlite3.Error as e:
        logger.error(f"Error BD buscando duplicado cercano monto {amount}: {e}")
        return None
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


# --- Función Principal de Procesamiento ---
def process_excel_file(filepath: Path, original_filename: str) -> dict:
    """Procesa Excel, identifica nuevas, duplicados exactos, descartados previos y pendientes."""
    logger.info(f"Iniciando lógica: {original_filename}")
    start_time = datetime.now()
    try: init_db(DB_FILE_PATH)
    except Exception as e:
        logger.critical(f"Fallo crítico init DB: {e}", exc_info=True)
        return {'status': 'error', 'message': 'Error crítico DB.', 'new_count_inserted': 0, 'pending_count': 0, 'duplicates_skipped': 0, 'previously_discarded_skipped': 0, 'failed_rows': 0, 'pending_confirmation': []}

    df = None
    try: # Lectura y validación inicial
        df = pd.read_excel(filepath, sheet_name=EXCEL_SHEET_NAME, skiprows=EXCEL_SKIP_ROWS,
                           engine='openpyxl' if str(filepath).lower().endswith('.xlsx') else 'xlrd')
        logger.info(f"'{original_filename}' leído: {len(df)} filas.")
        required_cols = {COL_DATE_EXCEL, COL_DESC_EXCEL, COL_AMOUNT_EXCEL}
        missing_cols = required_cols - set(df.columns)
        if missing_cols: raise ValueError(f"Faltan columnas: {missing_cols}")
        df.dropna(subset=[COL_DATE_EXCEL, COL_AMOUNT_EXCEL], how='any', inplace=True)
        if df.empty: return {'status': 'warning', 'message': f"'{original_filename}' vacío tras limpieza.", 'new_count_inserted': 0, 'pending_count': 0, 'duplicates_skipped': 0, 'previously_discarded_skipped': 0, 'failed_rows': 0, 'pending_confirmation': []}
        logger.info(f"Filas tras limpieza: {len(df)}")
    except Exception as e: # Captura errores de lectura, formato, etc.
        logger.error(f"Error leyendo/validando '{original_filename}': {e}", exc_info=True)
        msg = f"Error leer/validar '{original_filename}'. ¿Formato/Columnas correctas?"
        if isinstance(e, ImportError): msg = f"Falta librería Excel: {'openpyxl' if '.xlsx' in str(filepath).lower() else 'xlrd'}"
        if isinstance(e, FileNotFoundError): msg = "Error interno: Archivo temp no encontrado."
        return {'status': 'error', 'message': msg, 'new_count_inserted': 0, 'pending_count': 0, 'duplicates_skipped': 0, 'previously_discarded_skipped': 0, 'failed_rows': len(df) if df is not None else 0, 'pending_confirmation': []}

    # Contadores e inicialización
    inserted_count, failed_count, exact_dup_count, discarded_prev_count = 0, 0, 0, 0
    pending_list = []
    email_list = []

    # Bucle Principal
    for index, row in df.iterrows():
        row_num = index + EXCEL_SKIP_ROWS + 2
        try: # Parseo de fila individual
            fecha_dt = pd.to_datetime(row[COL_DATE_EXCEL], errors='raise', dayfirst=True)
            fecha_db = fecha_dt.strftime('%Y-%m-%d')
            fecha_display = fecha_dt.strftime('%d/%m/%Y')
            desc = str(row.get(COL_DESC_EXCEL, '')).strip() or "Sin Descripción"
            monto = round(float(row[COL_AMOUNT_EXCEL]), 2)
            monto_display = f"{monto:,.2f}"
        except Exception as e:
            logger.warning(f"Error parseando fila {row_num}: {e}. Omitiendo.")
            failed_count += 1; continue

        tx_id = get_transaction_id(row, COL_DATE_EXCEL, COL_DESC_EXCEL, COL_AMOUNT_EXCEL, COL_BANK_ID_EXCEL)
        if not tx_id: failed_count += 1; continue

        try: # Lógica de duplicados/inserción
            if check_if_id_exists(DB_FILE_PATH, tx_id):
                exact_dup_count += 1; continue
            if check_if_previously_discarded(DB_FILE_PATH, tx_id):
                discarded_prev_count += 1; continue

            existing_match = find_potential_duplicate(DB_FILE_PATH, monto, fecha_dt, desc)
            if existing_match:
                pending_list.append({
                    'new_transaction': { 'potential_id': tx_id, 'date_db': fecha_db, 'date_display': fecha_display, 'desc': desc, 'amount': monto, 'amount_display': monto_display },
                    'existing_match': { 'id': existing_match[COL_DB_ID], 'date_display': datetime.strptime(existing_match[COL_DB_DATE], '%Y-%m-%d').strftime('%d/%m/%Y'), 'desc': existing_match[COL_DB_DESC], 'amount_display': f"{existing_match[COL_DB_AMOUNT]:,.2f}" }
                })
                continue

            # Insertar
            tx_data = { COL_DB_ID: tx_id, COL_DB_DATE: fecha_db, COL_DB_DESC: desc, COL_DB_AMOUNT: monto, COL_DB_PROCESSED_AT: datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COL_DB_CATEGORY: None }
            if insert_transaction(DB_FILE_PATH, tx_data):
                inserted_count += 1
                email_list.append({'fecha': fecha_display, 'descripcion': desc, 'monto': monto_display})
            else: failed_count += 1 # Error en inserción (raro si comprobaciones previas OK)

        except Exception as e:
            logger.error(f"Error inesperado procesando fila {row_num}: {e}", exc_info=True)
            failed_count += 1

    # Finalización y Resumen
    proc_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"Procesado '{original_filename}' en {proc_time:.2f}s. "
                f"Ins:{inserted_count}, Pend:{len(pending_list)}, Dup:{exact_dup_count}, Desc:{discarded_prev_count}, Fail:{failed_count}")

    status = 'success'
    msg_parts = [f"'{original_filename}' procesado."]
    if inserted_count: msg_parts.append(f"{inserted_count} insertadas.")
    if len(pending_list): msg_parts.append(f"{len(pending_list)} pendientes."); status = 'confirmation_required'
    if exact_dup_count: msg_parts.append(f"{exact_dup_count} duplicados exactos.")
    if discarded_prev_count: msg_parts.append(f"{discarded_prev_count} descartados previos.")
    if failed_count: msg_parts.append(f"{failed_count} errores."); status = 'warning' if status == 'success' else status

    if status == 'success' and not inserted_count and not len(pending_list):
        message = f"'{original_filename}': No se encontraron transacciones nuevas ni dudosas."
    else: message = " ".join(msg_parts)

    if email_list:
        subject = f"Nuevos movimientos ({len(email_list)}) - {original_filename}"
        send_email(subject, email_list, original_filename)

    return {
        'status': status, 'message': message, 'new_count_inserted': inserted_count,
        'pending_count': len(pending_list), 'duplicates_skipped': exact_dup_count,
        'previously_discarded_skipped': discarded_prev_count, 'failed_rows': failed_count,
        'pending_confirmation': pending_list
    }