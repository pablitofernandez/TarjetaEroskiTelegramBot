import os
import sqlite3
from flask import Flask, request, jsonify
from pathlib import Path
import logging
import tempfile
from werkzeug.utils import secure_filename
from datetime import datetime
from typing import Optional # Importar Optional si usas type hints

# Importar funciones y constantes necesarias de la lógica
from process_bank_excel import (
    process_excel_file, insert_transaction, check_if_id_exists,
    log_discarded_duplicate, # Importar la nueva función
    DB_FILE_PATH, COL_DB_ID, COL_DB_DATE, COL_DB_DESC, COL_DB_AMOUNT,
    COL_DB_PROCESSED_AT, COL_DB_CATEGORY
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("APIServer")

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 # 16 MB max upload

# --- Funciones DB ---
def get_db_connection():
    """Establece conexión con la BD SQLite."""
    try:
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False, timeout=10.0)
        conn.row_factory = sqlite3.Row # Acceso a columnas por nombre
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error conectando a DB {DB_FILE_PATH}: {e}")
        return None

# --- Endpoints ---
@app.route('/api/last_transactions', methods=['GET'])
def get_last_transactions():
    """Devuelve las últimas N transacciones."""
    logger.info(f"GET /api/last_transactions from {request.remote_addr}")
    try: count = int(request.args.get('count', 5)); assert 1 <= count <= 100
    except: count = 5

    conn = get_db_connection()
    if not conn: return jsonify({"error": "DB Connection Error"}), 500
    transactions = []
    try:
        cursor = conn.cursor()
        query = f"SELECT {COL_DB_DATE}, {COL_DB_DESC}, {COL_DB_AMOUNT} FROM transactions ORDER BY {COL_DB_DATE} DESC, {COL_DB_PROCESSED_AT} DESC LIMIT ?"
        cursor.execute(query, (count,))
        transactions = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Error BD en last_transactions: {e}")
        return jsonify({"error": "Error consultando DB"}), 500
    finally:
        if conn: conn.close()
    return jsonify(transactions)

@app.route('/api/process_excel', methods=['POST'])
def handle_process_excel():
    """Recibe, guarda temporalmente y procesa un archivo Excel."""
    logger.info(f"POST /api/process_excel from {request.remote_addr}")
    if 'excel_file' not in request.files: return jsonify({"status": "error", "message": "'excel_file' no encontrado."}), 400
    file = request.files['excel_file']
    if not file.filename: return jsonify({"status": "error", "message": "Nombre archivo vacío."}), 400

    original_filename = secure_filename(file.filename)
    allowed = {'.xlsx', '.xls'}; file_ext = Path(original_filename).suffix.lower()
    if file_ext not in allowed: return jsonify({"status": "error", "message": f"Tipo no permitido: {file_ext}. Solo {allowed}"}), 400

    try:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_filepath = Path(temp_dir_name) / original_filename
            file.save(temp_filepath)
            logger.info(f"Archivo temporal: {temp_filepath}")
            result = process_excel_file(temp_filepath, original_filename) # Llamada a la lógica

            status = result.get('status', 'error')
            http_status = 200 # Default OK para success, warning, confirmation_required
            if status == 'error': http_status = 500
            if "Faltan columnas" in result.get('message', '') or "leer Excel" in result.get('message', ''): http_status = 400

            logger.info(f"Resultado para {original_filename}: Status API='{status}', HTTP={http_status}")
            return jsonify(result), http_status
    except Exception as e:
        logger.error(f"Error inesperado en /api/process_excel para {original_filename}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Error interno servidor."}), 500

@app.route('/api/confirm_transaction', methods=['POST'])
def handle_confirm_transaction():
    """Recibe la decisión del usuario (insert/discard) y actúa en consecuencia."""
    logger.info(f"POST /api/confirm_transaction from {request.remote_addr}")
    try:
        data = request.get_json()
        if not data: return jsonify({"status": "error", "message": "Request body vacío/inválido."}), 400

        tx_id = data.get('transaction_id') # ID de la transacción nueva/pendiente
        action = data.get('action')
        details = data.get('details') # Detalles de la tx nueva (para 'insert')
        existing_tx_id = data.get('existing_tx_id') # ID de la tx existente (para 'discard')

        logger.info(f"Confirmación: ID={tx_id}, Acción={action}, ExistingID={existing_tx_id}")

        if not tx_id or action not in ['insert', 'discard']:
            return jsonify({"status": "error", "message": "Datos confirmación inválidos (tx_id/action)."}), 400
        if action == 'insert' and not details:
            return jsonify({"status": "error", "message": "Faltan 'details' para 'insert'."}), 400
        if action == 'discard' and not existing_tx_id:
            return jsonify({"status": "error", "message": "Falta 'existing_tx_id' para 'discard'."}), 400

        if action == 'insert':
            # Revalidar detalles básicos
            required_keys = {'date_db', 'desc', 'amount'}
            if not required_keys.issubset(details.keys()):
                 return jsonify({"status": "error", "message": "Datos 'details' incompletos."}), 400
            # Re-chequear existencia por si acaso (concurrencia)
            if check_if_id_exists(DB_FILE_PATH, tx_id):
                 logger.warning(f"Confirmación 'insert' para {tx_id}, pero ya existe. Ignorando.")
                 return jsonify({"status": "ok", "message": f"Transacción {tx_id[:8]}... ya existía."}), 200
            # Construir datos y intentar insertar
            tx_data = {
                COL_DB_ID: tx_id, COL_DB_DATE: details['date_db'], COL_DB_DESC: details['desc'],
                COL_DB_AMOUNT: details['amount'], COL_DB_PROCESSED_AT: datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                COL_DB_CATEGORY: details.get(COL_DB_CATEGORY)
            }
            if insert_transaction(DB_FILE_PATH, tx_data):
                logger.info(f"Transacción {tx_id} insertada por confirmación.")
                # Considerar enviar email de confirmados individuales aquí?
                return jsonify({"status": "ok", "message": f"Transacción {tx_id[:8]}... insertada."}), 200
            else:
                logger.error(f"Error insertando {tx_id} tras confirmación.")
                return jsonify({"status": "error", "message": f"Error BD insertando {tx_id[:8]}..."}), 500

        elif action == 'discard':
            # Registrar el descarte en la tabla 'discarded_log'
            if log_discarded_duplicate(DB_FILE_PATH, tx_id, existing_tx_id):
                logger.info(f"Transacción {tx_id} registrada en discarded_log.")
            else:
                logger.error(f"Fallo registrando descarte de {tx_id} en discarded_log.")
                # No devolver error al bot, la decisión de descartar ya está tomada
            return jsonify({"status": "ok", "message": f"Transacción {tx_id[:8]}... descartada."}), 200

    except Exception as e:
        logger.error(f"Error inesperado en /api/confirm_transaction: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Error interno servidor."}), 500

@app.route('/health', methods=['GET'])
def health_check():
     """Verifica la conectividad con la base de datos."""
     logger.debug(f"GET /health from {request.remote_addr}")
     conn = None
     try:
          conn = get_db_connection(); conn.close()
          return jsonify({"status": "ok", "db_connection": True}), 200
     except Exception as e:
          logger.warning(f"Health check falló: {e}")
          return jsonify({"status": "error", "db_connection": False, "message": str(e)}), 503

# --- Inicio App ---
if __name__ == "__main__":
    api_port = int(os.environ.get('API_PORT', 5001))
    logger.info(f"Iniciando API Server en 0.0.0.0:{api_port}")
    app.run(host='0.0.0.0', port=api_port, debug=False) # debug=False para producción