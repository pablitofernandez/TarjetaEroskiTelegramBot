import logging
import os
from pathlib import Path
import requests
import locale
from datetime import datetime
import io

from telegram import Update, Document
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes

BOT_TOKEN = os.environ.get("BOT_TOKEN")
try:
    ALLOWED_USER_ID = int(os.environ.get("ALLOWED_USER_ID"))
except (TypeError, ValueError):
    logging.critical("Error: ALLOWED_USER_ID no definido o inválido.")
    exit(1)
PROCESSOR_API_URL = os.environ.get('PROCESSOR_API_URL', 'http://bank-processor:5001/api')

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger("TelegramBotListener")
try:
    locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
    logger.info("Locale 'es_ES.UTF-8' configurado.")
except locale.Error:
    logger.warning("Locale 'es_ES.UTF-8' no encontrado. Usando fallback 'C.UTF-8'.")
    try: locale.setlocale(locale.LC_ALL, 'C.UTF-8')
    except locale.Error: logger.error("No se pudo configurar ningún locale.")

# --- Manejador de Documentos ---
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Descarga archivo Excel a memoria y lo envía a la API de procesamiento."""
    chat_id = update.message.chat_id
    user = update.message.from_user
    logger.info(f"Documento recibido de User ID: {user.id} en Chat ID: {chat_id}")

    # 1. Validaciones (Usuario, Documento, Extensión)
    if user.id != ALLOWED_USER_ID:
        logger.warning(f"Documento ignorado del usuario no autorizado: {user.id}")
        await update.message.reply_text("Lo siento, no estoy autorizado a procesar archivos de ti.")
        return
    if not update.message.document:
         logger.warning("El mensaje no contiene un documento.")
         return # No responder nada
    document: Document = update.message.document
    file_name = document.file_name
    logger.info(f"Documento recibido: {file_name} (MIME type: {document.mime_type})")
    allowed_extensions = ('.xlsx', '.xls')
    if not file_name.lower().endswith(allowed_extensions):
        logger.warning(f"Tipo de archivo no permitido: {file_name}")
        await update.message.reply_text(f"Lo siento, solo puedo procesar archivos Excel ({', '.join(allowed_extensions)}).")
        return

    # 2. Descargar archivo a memoria (BytesIO)
    try:
        # Mensaje inicial mientras se descarga y procesa
        await update.message.reply_text(f"Recibido '{file_name}'. Procesando...")

        file_id = document.file_id
        new_file = await context.bot.get_file(file_id)

        file_content_stream = io.BytesIO()
        await new_file.download_to_memory(file_content_stream)
        file_content_stream.seek(0) # Rebobinar el stream para leerlo
        logger.info(f"Archivo '{file_name}' descargado a memoria ({file_content_stream.getbuffer().nbytes} bytes).")

    except Exception as e:
        logger.error(f"Error al descargar el archivo '{file_name}' a memoria: {e}", exc_info=True)
        await update.message.reply_text(f"Ocurrió un error al descargar tu archivo '{file_name}'. Por favor, inténtalo de nuevo.")
        return # Salir si la descarga falla

    # 3. Enviar archivo a la API del procesador
    api_endpoint = f"{PROCESSOR_API_URL}/process_excel"
    files_to_send = {'excel_file': (file_name, file_content_stream, document.mime_type)}
    feedback_message = f"❓ Hubo un problema procesando '{file_name}'. Inténtalo de nuevo o revisa los logs." # Mensaje por defecto

    try:
        logger.info(f"Enviando '{file_name}' a la API en {api_endpoint}...")
        # Aumentar timeout si los archivos pueden ser grandes o el procesamiento lento
        response = requests.post(api_endpoint, files=files_to_send, timeout=120) # Timeout de 2 minutos

        # Chequear errores HTTP (4xx, 5xx)
        response.raise_for_status()

        # Procesar respuesta JSON exitosa (HTTP 200)
        result = response.json()
        logger.info(f"Respuesta recibida de la API para '{file_name}': {result}")

        # Construir mensaje de feedback basado en la respuesta de la API
        status = result.get('status', 'error')
        message_from_api = result.get('message', 'Sin detalles.')

        if status == 'success':
            feedback_message = f"✅ {message_from_api}"
        elif status == 'warning':
             feedback_message = f"⚠️ {message_from_api}"
        else: # status == 'error'
             feedback_message = f"❌ Error procesando '{file_name}': {message_from_api}"

    except requests.exceptions.Timeout:
        logger.error(f"Timeout al enviar/procesar archivo '{file_name}' en la API ({api_endpoint}).")
        feedback_message = f"⏳ El procesamiento de '{file_name}' tardó demasiado. Puede que se haya completado parcialmente. Revisa con /last o contacta al admin."
    except requests.exceptions.RequestException as e:
        logger.error(f"Error de conexión/HTTP al enviar '{file_name}' a la API: {e}", exc_info=False)
        # Intentar obtener más detalles del error de la respuesta si existe
        error_detail = ""
        if e.response is not None:
            try:
                error_data = e.response.json()
                error_detail = error_data.get("message", f"(Código: {e.response.status_code})")
            except ValueError: # Si la respuesta no es JSON
                error_detail = f"(Código: {e.response.status_code})"
        else:
             error_detail = "(Error de conexión)"

        feedback_message = f"❌ No se pudo enviar '{file_name}' para procesar: {error_detail}"
    except Exception as e:
         # Capturar cualquier otro error inesperado
         logger.error(f"Error inesperado durante el envío o manejo de respuesta para '{file_name}': {e}", exc_info=True)
         feedback_message = f"🆘 Error inesperado procesando tu solicitud para '{file_name}'. Contacta al administrador."
    finally:
         # Limpiar el stream de memoria
         file_content_stream.close()

    # 4. Enviar feedback final al usuario
    await update.message.reply_text(feedback_message)


# --- Manejador /last ---
async def show_last_transactions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.message.from_user
    chat_id = update.message.chat_id
    logger.info(f"Comando /last recibido de User ID: {user.id} en Chat ID: {chat_id}. Args: {context.args}")

    if user.id != ALLOWED_USER_ID:
        logger.warning(f"/last ignorado del usuario no autorizado: {user.id}")
        await update.message.reply_text("Lo siento, no tienes permiso para usar este comando.")
        return

    count = 5
    if context.args:
        try:
            count = int(context.args[0])
            if count <= 0 or count > 50:
                await update.message.reply_text("Por favor, introduce un número entre 1 y 50.")
                return
        except (ValueError, IndexError):
            await update.message.reply_text("Uso: /last [número] (ej. /last 10). Usando 5 por defecto.")
            count = 5
    logger.info(f"Solicitando los últimos {count} movimientos a la API.")

    api_endpoint = f"{PROCESSOR_API_URL}/last_transactions"
    try:
        response = requests.get(api_endpoint, params={'count': count}, timeout=15)
        response.raise_for_status()
        transactions = response.json()

        if not transactions:
            await update.message.reply_text("No se encontraron transacciones recientes.")
            return

        message_lines = [f"📊 Últimos {len(transactions)} movimientos:"]
        for tx in transactions:
            try:
                fecha = datetime.strptime(tx.get('transaction_date', ''), '%Y-%m-%d').strftime('%d/%m/%Y')
            except ValueError: fecha = tx.get('transaction_date', 'Fecha Inv.')
            try:
                monto = float(tx.get('amount', 0.0))
                importe_formateado = locale.currency(monto, grouping=True, symbol=True)
            except (ValueError, TypeError): importe_formateado = f"{tx.get('amount', 'Importe Inv.')}"
            desc = tx.get('description', 'Sin desc.')
            message_lines.append(f"- {fecha}: {desc} ({importe_formateado})")

        final_message = "\n".join(message_lines)
        if len(final_message) > 4096: final_message = final_message[:4090] + "\n(...)"
        await update.message.reply_text(final_message)

    except requests.exceptions.ConnectionError:
        logger.error(f"Error de conexión en /last a {api_endpoint}")
        await update.message.reply_text("❌ No se pudo conectar con el servicio de procesamiento.")
    except requests.exceptions.Timeout:
        logger.error(f"Timeout en /last esperando a {api_endpoint}")
        await update.message.reply_text("❌ El servicio de procesamiento tardó demasiado en responder.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error en /last solicitando a {api_endpoint}: {e}")
        error_detail = f"(Código: {e.response.status_code})" if e.response else ""
        await update.message.reply_text(f"❌ Ocurrió un error al obtener los datos {error_detail}.")
    except Exception as e:
         logger.error(f"Error inesperado en /last: {e}", exc_info=True)
         await update.message.reply_text("❌ Ocurrió un error inesperado procesando /last.")


def main() -> None:
    if not BOT_TOKEN:
        logger.critical("Error: Falta BOT_TOKEN.")
        return
    logger.info("Iniciando el bot...")
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(MessageHandler(filters.Document.ALL & filters.User(user_id=ALLOWED_USER_ID), handle_document))
    application.add_handler(CommandHandler("last", show_last_transactions, filters=filters.User(user_id=ALLOWED_USER_ID)))
    logger.info("Bot iniciado y escuchando...")
    application.run_polling()
    logger.info("Bot detenido.")

if __name__ == "__main__":
    main()