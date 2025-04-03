import logging
import os
import requests
import locale
from datetime import datetime
import io
import json # Para codificar/decodificar datos en callback
import html

# Importaciones de Telegram
from telegram import Update, Document, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, MessageHandler, CommandHandler, filters, ContextTypes,
    CallbackQueryHandler # Importante para los botones
)
from telegram.constants import ParseMode # Para formato Markdown/HTML si se usa

# --- Configuración (igual que antes) ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
try: ALLOWED_USER_ID = int(os.environ.get("ALLOWED_USER_ID"))
except: logging.critical("Error: ALLOWED_USER_ID inválido."); exit(1)
PROCESSOR_API_URL = os.environ.get('PROCESSOR_API_URL', 'http://bank-processor:5001/api')

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING) # Silenciar logs verbosos de httpx
logger = logging.getLogger("TelegramBotListener")

# Configurar locale para formato de moneda
try: locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
except locale.Error: logger.warning("Locale 'es_ES.UTF-8' no encontrado, usando fallback."); locale.setlocale(locale.LC_ALL, '') # Usar default del sistema

# --- Constantes para Callback Data ---
CALLBACK_PREFIX_CONFIRM = "confirm"

# --- Manejador de Documentos (MODIFICADO) ---
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.message.chat_id
    user = update.message.from_user
    logger.info(f"Documento recibido de User ID: {user.id} en Chat ID: {chat_id}")

    # 1. Validaciones (igual que antes)
    if user.id != ALLOWED_USER_ID: await update.message.reply_text("No autorizado."); return
    if not update.message.document: return
    document: Document = update.message.document
    file_name = document.file_name or "archivo_sin_nombre.bin" # Fallback nombre
    logger.info(f"Documento: {file_name} (MIME: {document.mime_type})")
    allowed_extensions = ('.xlsx', '.xls')
    if not file_name.lower().endswith(allowed_extensions):
        await update.message.reply_text(f"Solo proceso archivos Excel ({', '.join(allowed_extensions)})."); return

    # 2. Descargar archivo a memoria (igual que antes)
    processing_message = await update.message.reply_text(f"Recibido '{file_name}'. Descargando y enviando para procesar...")
    file_content_stream = io.BytesIO()
    try:
        file_id = document.file_id
        new_file = await context.bot.get_file(file_id)
        await new_file.download_to_memory(file_content_stream)
        file_content_stream.seek(0)
        logger.info(f"'{file_name}' descargado ({file_content_stream.getbuffer().nbytes} bytes).")
    except Exception as e:
        logger.error(f"Error descargando '{file_name}': {e}", exc_info=True)
        await processing_message.edit_text(f"Error al descargar tu archivo '{file_name}'. Inténtalo de nuevo.")
        return

    # 3. Enviar archivo a la API (igual que antes, pero manejo de respuesta cambia)
    api_endpoint = f"{PROCESSOR_API_URL}/process_excel"
    files_to_send = {'excel_file': (file_name, file_content_stream, document.mime_type)}
    api_result = None
    http_status_code = None

    try:
        logger.info(f"Enviando '{file_name}' a {api_endpoint}...")
        response = requests.post(api_endpoint, files=files_to_send, timeout=180) # Timeout más largo
        http_status_code = response.status_code
        response.raise_for_status() # Lanza excepción para 4xx/5xx
        api_result = response.json()
        logger.info(f"Respuesta API para '{file_name}' (HTTP {http_status_code}): {api_result}")

    except requests.exceptions.Timeout:
        logger.error(f"Timeout enviando/procesando '{file_name}' en API.")
        await processing_message.edit_text(f"⏳ El procesamiento de '{file_name}' tardó demasiado. Revisa con /last o logs.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error conexión/HTTP ({http_status_code}) enviando '{file_name}': {e}", exc_info=False)
        error_detail = f"(Código: {http_status_code})" if http_status_code else "(Error de conexión)"
        if e.response is not None:
             try: error_detail = e.response.json().get("message", error_detail)
             except: pass # Ignorar si la respuesta de error no es JSON
        await processing_message.edit_text(f"❌ No se pudo enviar '{file_name}': {error_detail}")
    except Exception as e:
         logger.error(f"Error inesperado enviando/procesando '{file_name}': {e}", exc_info=True)
         await processing_message.edit_text(f"🆘 Error inesperado procesando '{file_name}'.")
    finally:
         file_content_stream.close() # Siempre cerrar el stream

    # --- 4. Procesar Respuesta de la API y Enviar Feedback/Preguntas ---
    if not api_result: return # Salir si no hubo respuesta válida de la API

    status = api_result.get('status', 'error')
    message_from_api = api_result.get('message', 'Sin detalles.')

    if status == 'error':
        await processing_message.edit_text(f"❌ Error procesando '{file_name}': {message_from_api}")
    elif status == 'warning':
        await processing_message.edit_text(f"⚠️ {message_from_api}")
        # Podrías añadir un /last aquí si quieres mostrar las insertadas
    elif status == 'success':
         await processing_message.edit_text(f"✅ {message_from_api}")
         # Podrías añadir un /last aquí
    elif status == 'confirmation_required':
        await processing_message.edit_text(f"⏳ {message_from_api} \nAhora necesito tu ayuda para confirmar los posibles duplicados...")

        pending_list = api_result.get('pending_confirmation', [])
        if not pending_list:
             logger.error("API devolvió 'confirmation_required' pero lista 'pending_confirmation' vacía.")
             await update.message.reply_text("Hubo un problema interno al procesar las confirmaciones.")
             return

        # --- Guardar estado pendiente en chat_data ---
        # Es más seguro guardar en user_data si varios usuarios pudieran usar el bot (aunque aquí está restringido)
        # Usaremos chat_data por simplicidad aquí asumiendo un solo usuario permitido
        if 'pending_confirmation_details' not in context.chat_data:
            context.chat_data['pending_confirmation_details'] = {}

        # --- Enviar preguntas individuales ---
        for pending_item in pending_list:
            new_tx = pending_item.get('new_transaction', {})
            existing_tx = pending_item.get('existing_match', {})
            potential_id = new_tx.get('potential_id')

            if not potential_id or not new_tx or not existing_tx:
                 logger.warning(f"Item pendiente inválido: {pending_item}")
                 continue

            # Guardar los detalles necesarios para la inserción posterior
            context.chat_data['pending_confirmation_details'][potential_id] = new_tx

            # Formatear mensaje de pregunta
            try:
                 amount_new_num = new_tx.get('amount', 0.0)
                 amount_old_num = float(existing_tx.get('amount_display', "0.0").replace(',','.'))
                 amount_new = locale.currency(amount_new_num, grouping=True, symbol='€')
                 amount_old = locale.currency(amount_old_num, grouping=True, symbol='€')
            except:
                 amount_new = f"{new_tx.get('amount_display', 'N/A')} €"
                 amount_old = f"{existing_tx.get('amount_display', 'N/A')} €"

            # --- Escapar datos dinámicos para HTML ---
            safe_new_date = html.escape(new_tx.get('date_display', 'N/A'))
            safe_new_desc = html.escape(new_tx.get('desc', 'N/A'))
            safe_new_amount = html.escape(amount_new) # El formato de moneda podría tener caracteres especiales
            safe_potential_id_short = html.escape(potential_id[:8])

            safe_existing_id_short = html.escape(existing_tx.get('id', 'N/A')[:8])
            safe_existing_date = html.escape(existing_tx.get('date_display', 'N/A'))
            safe_existing_desc = html.escape(existing_tx.get('desc', 'N/A'))
            safe_existing_amount = html.escape(amount_old)

            # --- Construir texto con formato HTML ---
            question_text = (
                f"🤔 <b>Posible Duplicado</b>\n\n"
                f"<b>Nueva Transacción:</b>\n"
                f"  Fecha: {safe_new_date}\n"
                f"  Desc: <code>{safe_new_desc}</code>\n"
                f"  Importe: <b>{safe_new_amount}</b>\n"
                f"  <i>(ID Potencial: <code>{safe_potential_id_short}...</code>)</i>\n\n" # Usamos <i> para cursiva
                f"<b>Similar a Existente:</b>\n"
                f"  ID: <code>{safe_existing_id_short}...</code>\n"
                f"  Fecha: {safe_existing_date}\n"
                f"  Desc: <code>{safe_existing_desc}</code>\n"
                f"  Importe: <b>{safe_existing_amount}</b>\n\n"
                f"➡️ <b>¿La NUEVA es un duplicado de la existente?</b>"
            )

            # Crear botones inline
            keyboard = [
                [
                    InlineKeyboardButton("Sí (Descartar Nueva)", callback_data=f"{CALLBACK_PREFIX_CONFIRM}:discard:{potential_id}"),
                    InlineKeyboardButton("No (Guardar Nueva)", callback_data=f"{CALLBACK_PREFIX_CONFIRM}:insert:{potential_id}"),
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            # Enviar pregunta
            await update.message.reply_text(
                question_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML # <-- CAMBIO IMPORTANTE
            )

# --- NUEVO Manejador de Callback Query para Confirmaciones ---
async def handle_confirmation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja la respuesta del usuario a los botones de confirmación."""
    query = update.callback_query
    await query.answer() # Obligatorio responder al callback

    callback_data_str = query.data
    logger.info(f"Callback query recibido: {callback_data_str}")

    try:
        parts = callback_data_str.split(':')
        if len(parts) != 3 or parts[0] != CALLBACK_PREFIX_CONFIRM:
             logger.warning(f"Callback data inválida: {callback_data_str}")
             await query.edit_message_text(text=f"Error: Callback inválido.", reply_markup=None)
             return

        action = parts[1] # 'insert' o 'discard'
        potential_id = parts[2]

        # Recuperar detalles guardados
        pending_details = context.chat_data.get('pending_confirmation_details', {}).get(potential_id)

        if not pending_details:
            logger.error(f"No se encontraron detalles pendientes para ID {potential_id} en chat_data. ¿Bot reiniciado?")
            await query.edit_message_text(text=f"🤔 No encuentro los detalles para confirmar {potential_id[:8]}... "
                                                "Puede que el bot se haya reiniciado. Intenta subir el archivo de nuevo.", reply_markup=None)
            return

        # Llamar a la API de confirmación
        api_endpoint = f"{PROCESSOR_API_URL}/confirm_transaction"
        payload = {
            "transaction_id": potential_id,
            "action": action,
            "details": pending_details # Enviar los detalles completos para 'insert'
        }
        confirmation_result_text = f"Error al contactar API para {action} {potential_id[:8]}..."

        try:
            logger.info(f"Enviando confirmación a {api_endpoint}: {payload}")
            response = requests.post(api_endpoint, json=payload, timeout=30)
            response.raise_for_status()
            api_response = response.json()
            logger.info(f"Respuesta de confirmación API: {api_response}")

            if api_response.get("status") == "ok":
                 if action == "insert": confirmation_result_text = f"✅ ¡Guardado! Transacción {potential_id[:8]}... insertada."
                 else: confirmation_result_text = f"🗑️ ¡Descartado! Transacción {potential_id[:8]}... ignorada."
                 # Limpiar estado una vez procesado
                 if potential_id in context.chat_data.get('pending_confirmation_details', {}):
                     del context.chat_data['pending_confirmation_details'][potential_id]
                     logger.debug(f"Detalles pendientes para {potential_id} eliminados de chat_data.")
            else:
                 confirmation_result_text = f"⚠️ La API devolvió un problema para {action} {potential_id[:8]}: {api_response.get('message', 'Error desconocido')}"

        except requests.exceptions.RequestException as e:
            logger.error(f"Error de red/HTTP confirmando {potential_id}: {e}")
            confirmation_result_text = f"❌ Error de red al confirmar {action} para {potential_id[:8]}."
        except Exception as e:
             logger.error(f"Error inesperado confirmando {potential_id}: {e}", exc_info=True)
             confirmation_result_text = f"🆘 Error inesperado al confirmar {action} para {potential_id[:8]}."

        # Editar el mensaje original de la pregunta con el resultado
        await query.edit_message_text(
            text=confirmation_result_text,
            reply_markup=None,
            parse_mode=ParseMode.HTML # <-- CAMBIAR A HTML
        )

    except Exception as e:
        logger.error(f"Error procesando callback query {callback_data_str}: {e}", exc_info=True)
        try:
            # Intentar informar al usuario en el chat si falla la edición
            await context.bot.send_message(chat_id=query.message.chat_id, text="Ocurrió un error procesando tu respuesta.")
        except: pass # Evitar errores anidados

# --- Manejador /last (sin cambios funcionales, pero mejor formato) ---
async def show_last_transactions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.message.from_user
    if user.id != ALLOWED_USER_ID: await update.message.reply_text("No autorizado."); return

    try: count = int(context.args[0]) if context.args else 5; assert 1 <= count <= 50
    except: await update.message.reply_text("Uso: /last [1-50]. Usando 5."); count = 5
    logger.info(f"User {user.id} solicitó /last {count}")

    api_endpoint = f"{PROCESSOR_API_URL}/last_transactions"
    try:
        response = requests.get(api_endpoint, params={'count': count}, timeout=15)
        response.raise_for_status()
        transactions = response.json()

        if not transactions: await update.message.reply_text("No hay transacciones recientes."); return

        message_lines = [f"📊 *Últimos {len(transactions)} movimientos:*"]
        for tx in transactions:
            try: fecha = datetime.strptime(tx['transaction_date'], '%Y-%m-%d').strftime('%d/%m/%y') # Formato corto
            except: fecha = tx.get('transaction_date', '??/??/??')
            try: importe = locale.currency(float(tx['amount']), grouping=True, symbol='€')
            except: importe = f"{tx.get('amount', '?')} €"
            desc = tx.get('description', 'N/A').replace('_', r'\_').replace('*', r'\*').replace('[', r'\[').replace('`', r'\`') # Escapar Markdown
            desc_short = desc[:50] + '...' if len(desc) > 50 else desc # Acortar descripciones largas
            message_lines.append(f"`{fecha}` | {importe} | `{desc_short}`")

        final_message = "\n".join(message_lines)
        await update.message.reply_text(final_message, parse_mode=ParseMode.MARKDOWN_V2)

    except requests.exceptions.RequestException as e:
        logger.error(f"Error API en /last: {e}")
        status = f" (HTTP {e.response.status_code})" if e.response else " (Red)"
        await update.message.reply_text(f"❌ Error obteniendo datos{status}.")
    except Exception as e:
         logger.error(f"Error inesperado en /last: {e}", exc_info=True)
         await update.message.reply_text("❌ Error inesperado procesando /last.")


# --- Función Principal del Bot (MODIFICADA para añadir Callback Handler) ---
def main() -> None:
    if not BOT_TOKEN: logger.critical("Error: Falta BOT_TOKEN."); return
    if not ALLOWED_USER_ID: logger.critical("Error: Falta ALLOWED_USER_ID."); return # Doble check

    logger.info("Iniciando el bot...")
    # Habilitar persistencia podría ser útil para chat_data, pero requiere configuración adicional
    application = Application.builder().token(BOT_TOKEN).build()

    # Filtro combinado para usuario y tipo de mensaje
    user_filter = filters.User(user_id=ALLOWED_USER_ID)

    # Manejadores
    application.add_handler(MessageHandler(filters.Document.ALL & user_filter, handle_document))
    application.add_handler(CommandHandler("last", show_last_transactions, filters=user_filter))
    # --- Añadir el manejador para los botones inline ---
    application.add_handler(CallbackQueryHandler(handle_confirmation_callback, pattern=f"^{CALLBACK_PREFIX_CONFIRM}:"))

    logger.info(f"Bot iniciado. Escuchando solo para User ID: {ALLOWED_USER_ID}...")
    application.run_polling()
    logger.info("Bot detenido.")

if __name__ == "__main__":
    main()