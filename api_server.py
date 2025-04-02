import os
import sqlite3
from flask import Flask, request, jsonify
from pathlib import Path
import logging
import tempfile
from werkzeug.utils import secure_filename


from process_bank_excel import process_excel_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("APIServer")

DB_FILE_PATH = Path(os.environ.get('DB_FILE', '/app/state/transactions.db'))
COL_DB_DATE = "transaction_date"
COL_DB_DESC = "description"
COL_DB_AMOUNT = "amount"
COL_DB_PROCESSED_AT = "processed_at"

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

def get_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error al conectar a la base de datos {DB_FILE_PATH}: {e}")
        return None

@app.route('/api/last_transactions', methods=['GET'])
def get_last_transactions():
    logger.info(f"Request GET /api/last_transactions desde {request.remote_addr}")
    try:
        count = int(request.args.get('count', 5))
        if count <= 0 or count > 100: count = 5
    except ValueError:
        count = 5

    conn = get_db_connection()
    if not conn: return jsonify({"error": "Error al conectar a la base de datos"}), 500

    transactions = []
    try:
        cursor = conn.cursor()
        query = f"""
        SELECT {COL_DB_DATE}, {COL_DB_DESC}, {COL_DB_AMOUNT} FROM transactions
        ORDER BY {COL_DB_DATE} DESC, {COL_DB_PROCESSED_AT} DESC LIMIT ?
        """
        cursor.execute(query, (count,))
        rows = cursor.fetchall()
        transactions = [dict(row) for row in rows]
        logger.info(f"Devolviendo {len(transactions)} transacciones.")
    except sqlite3.Error as e:
        logger.error(f"Error al consultar BD para last_transactions: {e}")
        conn.close()
        return jsonify({"error": "Error al consultar las transacciones"}), 500
    finally:
        if conn: conn.close()
    return jsonify(transactions)

@app.route('/api/process_excel', methods=['POST'])
def handle_process_excel():
    logger.info(f"Request POST /api/process_excel desde {request.remote_addr}")

    if 'excel_file' not in request.files:
        logger.warning("Request a /api/process_excel sin 'excel_file'.")
        return jsonify({"status": "error", "message": "No se encontró el archivo ('excel_file') en la solicitud."}), 400

    file = request.files['excel_file']

    if file.filename == '':
        logger.warning("Request a /api/process_excel con nombre de archivo vacío.")
        return jsonify({"status": "error", "message": "Nombre de archivo vacío."}), 400

    original_filename = secure_filename(file.filename)
    logger.info(f"Archivo recibido: {original_filename}")

    allowed_extensions = {'.xlsx', '.xls'}
    file_ext = Path(original_filename).suffix.lower()
    if file_ext not in allowed_extensions:
         logger.warning(f"Extensión no permitida: {file_ext} para archivo {original_filename}")
         return jsonify({"status": "error", "message": f"Tipo de archivo no permitido: {file_ext}. Solo se aceptan {allowed_extensions}"}), 400

    temp_dir = None
    temp_filepath = None
    try:
        temp_dir = tempfile.TemporaryDirectory()
        temp_filepath = Path(temp_dir.name) / original_filename
        file.save(temp_filepath)
        logger.info(f"Archivo guardado temporalmente en: {temp_filepath}")

        result = process_excel_file(temp_filepath, original_filename)

        http_status = 500
        if result.get('status') == 'success':
            http_status = 200
        elif result.get('status') == 'warning':
            http_status = 200
        elif result.get('status') == 'error':
            if "Faltan columnas" in result.get('message', '') or "leer el archivo" in result.get('message', ''):
                 http_status = 400
            else:
                 http_status = 500

        logger.info(f"Procesamiento para {original_filename} completado con status: {result.get('status')}. Devolviendo HTTP {http_status}.")
        return jsonify(result), http_status

    except Exception as e:
        logger.error(f"Error inesperado en endpoint /api/process_excel para {original_filename}: {e}", exc_info=True)
        return jsonify({"status": "error", "message": "Error interno inesperado en el servidor."}), 500
    finally:
        if temp_dir:
            try:
                temp_dir.cleanup()
                logger.info(f"Directorio temporal {temp_dir.name} limpiado.")
            except Exception as e:
                logger.error(f"Error limpiando directorio temporal {temp_dir.name}: {e}")


@app.route('/health', methods=['GET'])
def health_check():
     logger.debug(f"Health check solicitado desde {request.remote_addr}")
     conn = None
     try:
          conn = get_db_connection()
          if conn:
               conn.close()
               return jsonify({"status": "ok", "db_connection": True}), 200
          else:
               return jsonify({"status": "error", "db_connection": False, "message":"Cannot connect to DB"}), 503
     except Exception as e:
          logger.warning(f"Health check falló al verificar DB: {e}")
          return jsonify({"status": "error", "db_connection": False, "message": str(e)}), 503


if __name__ == "__main__":
    api_port = int(os.environ.get('API_PORT', 5001))
    logger.info(f"Iniciando servidor API Flask en 0.0.0.0:{api_port}")
    app.run(host='0.0.0.0', port=api_port, debug=False)