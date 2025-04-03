import os
import sqlite3
from flask import Flask, request, jsonify
from pathlib import Path
import logging
import tempfile
from werkzeug.utils import secure_filename
from datetime import datetime # Importar datetime

# Importar funciones necesarias del módulo de lógica
from process_bank_excel import process_excel_file, insert_transaction, check_if_id_exists, DB_FILE_PATH, COL_DB_ID # Añadir DB_FILE_PATH, COL_DB_ID

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("APIServer")

# --- Constantes y Configuración (igual que antes) ---
# DB_FILE_PATH ahora se importa
COL_DB_DATE = "transaction_date"
COL_DB_DESC = "description"
COL_DB_AMOUNT = "amount"
COL_DB_PROCESSED_AT = "processed_at"
COL_DB_CATEGORY = "category" # Añadido por si insert_transaction lo necesita

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

# --- Funciones DB (get_db_connection igual) ---
def get_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False) # check_same_thread=False requiere cuidado
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error al conectar a la base de datos {DB_FILE_PATH}: {e}")
        return None

# --- Endpoints Existentes (get_last_transactions, handle_process_excel, health_check sin cambios funcionales clave) ---
@app.route('/api/last_transactions', methods=['GET'])
def get_last_transactions():
    # ... (sin cambios) ...
    logger.info(f"Request GET /api/last_transactions desde {request.remote_addr}")
    try: count = int(request.args.get('count', 5)); assert 1 <= count <= 100
    except: count = 5

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Error al conectar a la base de datos"}), 500

    transactions = []
    try:
        cursor = conn.cursor()
        query = f"SELECT {COL_DB_DATE}, {COL_DB_DESC}, {COL_DB_AMOUNT} FROM transactions ORDER BY {COL_DB_DATE} DESC, {COL_DB_PROCESSED_AT} DESC LIMIT ?"
        cursor.execute(query, (count,))
        transactions = [dict(row) for row in cursor.fetchall()]
        logger.info(f"Devolviendo {len(transactions)} transacciones.")
    except sqlite3.Error as e:
        logger.error(f"Error BD en last_transactions: {e}")
        return jsonify({"error": "Error al consultar las transacciones"}), 500
    finally:
        if conn: conn.close()
    return jsonify(transactions)


@app.route('/api/process_excel', methods=['POST'])
def handle_process_excel():
    # ... (validaciones iniciales de archivo igual) ...
    logger.info(f"Request POST /api/process_excel desde {request.remote_addr}")
    if 'excel_file' not in request.files: return jsonify({"status": "error", "message": "No se encontró el archivo ('excel_file')."}), 400
    file = request.files['excel_file']
    if file.filename == '': return jsonify({"status": "error", "message": "Nombre de archivo vacío."}), 400

    original_filename = secure_filename(file.filename)
    logger.info(f"Archivo recibido: {original_filename}")
    allowed_extensions = {'.xlsx', '.xls'}; file_ext = Path(original_filename).suffix.lower()
    if file_ext not in allowed_extensions:
         logger.warning(f"Extensión no permitida: {file_ext}")
         return jsonify({"status": "error", "message": f"Tipo archivo no permitido: {file_ext}. Solo {allowed_extensions}"}), 400

    temp_dir = None
    try:
        with tempfile.TemporaryDirectory() as temp_dir_name:
            temp_dir = Path(temp_dir_name) # Para logging
            temp_filepath = temp_dir / original_filename
            file.save(temp_filepath)
            logger.info(f"Archivo guardado temporalmente en: {temp_filepath}")

            # Llamada a la lógica de procesamiento (ahora devuelve estructura diferente)
            result = process_excel_file(temp_filepath, original_filename)

            # Determinar HTTP status basado en el nuevo 'status'
            http_status = 500 # Default a error
            api_status = result.get('status', 'error')

            if api_status == 'success': http_status = 200
            elif api_status == 'warning': http_status = 200
            elif api_status == 'confirmation_required': http_status = 200 # OK, pero requiere acción
            elif api_status == 'error':
                # Mantener lógica de 400 para errores de formato/columnas si es posible
                if "Faltan columnas" in result.get('message', '') or "leer Excel" in result.get('message', ''):
                     http_status = 400
                else: # Otros errores son 500
                     http_status = 500

            logger.info(f"Procesamiento para {original_filename} completado con status API: '{api_status}'. Devolviendo HTTP {http_status}.")
            # Devolver el resultado completo, incluyendo 'pending_confirmation' si existe
            return jsonify(result), http_status

    except Exception as e:
        logger.error(f"Error inesperado en endpoint /api/process_excel para {original_filename}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Error interno inesperado en el servidor."}), 500
    # No necesitamos finally para temp_dir.cleanup() si usamos 'with'

@app.route('/health', methods=['GET'])
def health_check():
    # ... (sin cambios) ...
     logger.debug(f"Health check solicitado desde {request.remote_addr}")
     conn = None
     try:
          conn = get_db_connection(); conn.close()
          return jsonify({"status": "ok", "db_connection": True}), 200
     except Exception as e:
          logger.warning(f"Health check falló al verificar DB: {e}")
          # Devolver 503 Service Unavailable si la BD falla
          return jsonify({"status": "error", "db_connection": False, "message": str(e)}), 503

# --- NUEVO ENDPOINT PARA CONFIRMACIÓN ---
@app.route('/api/confirm_transaction', methods=['POST'])
def handle_confirm_transaction():
    """Recibe la decisión del usuario (vía bot) para una transacción pendiente."""
    logger.info(f"Request POST /api/confirm_transaction desde {request.remote_addr}")

    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "Request body vacío o no es JSON."}), 400

        tx_id = data.get('transaction_id')
        action = data.get('action')
        details = data.get('details') # Detalles de la transacción NUEVA

        logger.info(f"Confirmación recibida: ID={tx_id}, Acción={action}")

        # Validaciones
        if not tx_id or not action or action not in ['insert', 'discard']:
            logger.warning(f"Datos de confirmación inválidos: {data}")
            return jsonify({"status": "error", "message": "Faltan 'transaction_id' o 'action' (insert/discard) inválida."}), 400
        if action == 'insert' and not details:
            logger.warning(f"Faltan 'details' para acción 'insert': {data}")
            return jsonify({"status": "error", "message": "Faltan 'details' para la acción 'insert'."}), 400

        # Procesar acción
        if action == 'insert':
            # Reconstruir el diccionario de datos necesario para insert_transaction
            # Usar los 'details' enviados por el bot
            required_keys = {'date_db', 'desc', 'amount'}
            if not required_keys.issubset(details.keys()):
                 logger.error(f"Faltan detalles clave en 'details' para insertar {tx_id}")
                 return jsonify({"status": "error", "message": "Datos incompletos en 'details' para insertar."}), 400

            # Comprobar de nuevo si ya existe (por si acaso hubo concurrencia o doble click)
            if check_if_id_exists(DB_FILE_PATH, tx_id):
                 logger.warning(f"Confirmación 'insert' para {tx_id}, pero ya existe. Ignorando.")
                 return jsonify({"status": "ok", "message": f"Transacción {tx_id} ya existía, no se re-insertó."}), 200

            tx_data = {
                COL_DB_ID: tx_id,
                COL_DB_DATE: details['date_db'],
                COL_DB_DESC: details['desc'],
                COL_DB_AMOUNT: details['amount'],
                COL_DB_PROCESSED_AT: datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # Usar hora actual de confirmación
                COL_DB_CATEGORY: details.get(COL_DB_CATEGORY) # Incluir si existe en details
            }

            if insert_transaction(DB_FILE_PATH, tx_data):
                logger.info(f"Transacción {tx_id} insertada por confirmación del usuario.")
                # Considerar enviar email aquí para las confirmadas? O al final?
                return jsonify({"status": "ok", "message": f"Transacción {tx_id} insertada."}), 200
            else:
                logger.error(f"Error al insertar transacción {tx_id} tras confirmación.")
                return jsonify({"status": "error", "message": f"Error al insertar transacción {tx_id} en BD."}), 500

        elif action == 'discard':
            logger.info(f"Transacción {tx_id} descartada por confirmación del usuario.")
            # Opcional: Guardar log de descartados en otro sitio si se necesita auditoría
            return jsonify({"status": "ok", "message": f"Transacción {tx_id} descartada."}), 200

    except Exception as e:
        logger.error(f"Error inesperado en /api/confirm_transaction: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Error interno inesperado en el servidor."}), 500


# --- Inicio de la Aplicación ---
if __name__ == "__main__":
    api_port = int(os.environ.get('API_PORT', 5001))
    logger.info(f"Iniciando servidor API Flask en 0.0.0.0:{api_port}")
    # debug=False es importante para producción
    app.run(host='0.0.0.0', port=api_port, debug=False)