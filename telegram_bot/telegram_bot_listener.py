import logging
import os
import requests
import locale
from datetime import datetime
import io
import json
import html # Para escapar HTML

# Importaciones de Telegram
from telegram import Update, Document, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, MessageHandler, CommandHandler, filters, ContextTypes,
    CallbackQueryHandler, PersistenceInput # Añadir PersistenceInput si usas persistencia
)
# from telegram.ext import DictPersistence # Ejemplo de persistencia simple
from telegram.constants import ParseMode

# --- Configuración ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
try: ALLOWED_USER_ID = int(os.environ.get("ALLOWED_USER_ID"))
except: logging.critical("Error: ALLOWED_USER_ID inválido."); exit(1)
PROCESSOR_API_URL = os.environ.get('PROCESSOR_API_URL', 'http://bank-processor:5001/api')

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("TelegramBotListener")

try: locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
except locale.Error: logger.warning("Locale 'es_ES.UTF-8' no disponible."); locale.setlocale(locale.LC_ALL, '')

# --- Constantes ---
CALLBACK_PREFIX_CONFIRM = "confirm"
# Clave para guardar datos pendientes (usar user_data si hubiera múltiples usuarios)
PENDING_DATA_KEY = 'pending_confirmation_details'

# --- Manejador de Documentos ---
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.message.chat_id
    user = update.message.from_user
    logger.info(f"Documento de User ID: {user.id}")

    if user.id != ALLOWED_USER_ID: await update.message.reply_text("No autorizado."); return
    if not update.message.document: return
    document = update.message.document
    file_name = document.file_name or "archivo.bin"
    logger.info(f"Documento: {file_name} (MIME: {document.mime_type})")
    allowed_ext = ('.xlsx', '.xls')
    if not file_name.lower().endswith(allowed_ext):
        await update.message.reply_text(f"Solo proceso Excel ({', '.join(allowed_ext)})."); return

    processing_msg = await update.message.reply_text(f"Recibido '{file_name}'. Procesando...")
    file_stream = io.BytesIO()
    try:
        tg_file = await context.bot.get_file(document.file_id)
        await tg_file.download_to_memory(file_stream)
        file_stream.seek(0)
        logger.info(f"'{file_name}' descargado ({file_stream.getbuffer().nbytes} bytes).")
    except Exception as e:
        logger.error(f"Error descargando '{file_name}': {e}", exc_info=True)
        await processing_msg.edit_text(f"Error descargando '{file_name}'.")
        return

    api_result = None; http_status = None
    try:
        logger.info(f"Enviando '{file_name}' a {PROCESSOR_API_URL}/process_excel...")
        response = requests.post(f"{PROCESSOR_API_URL}/process_excel",
                                 files={'excel_file': (file_name, file_stream, document.mime_type)},
                                 timeout=180)
        http_status = response.status_code
        response.raise_for_status()
        api_result = response.json()
        logger.info(f"Respuesta API para '{file_name}' (HTTP {http_status}): {api_result}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error API/Red ({http_status}) para '{file_name}': {e}", exc_info=False)
        detail = f"(HTTP {http_status})" if http_status else "(Red)"
        if e.response:
            try: detail = e.response.json().get("message", detail)
            except: pass
        await processing_msg.edit_text(f"❌ Error conectando con procesador {detail}.")
    except Exception as e:
         logger.error(f"Error inesperado enviando/procesando '{file_name}': {e}", exc_info=True)
         await processing_msg.edit_text(f"🆘 Error inesperado procesando '{file_name}'.")
    finally:
         file_stream.close()

    if not api_result: return

    status = api_result.get('status', 'error')
    message = api_result.get('message', 'Sin detalles.')

    if status == 'error': await processing_msg.edit_text(f"❌ Error: {message}")
    elif status == 'warning': await processing_msg.edit_text(f"⚠️ {message}")
    elif status == 'success': await processing_msg.edit_text(f"✅ {message}")
    elif status == 'confirmation_required':
        await processing_msg.edit_text(f"⏳ {message}\nNecesito tu ayuda para confirmar posibles duplicados...")
        pending_list = api_result.get('pending_confirmation', [])
        if not pending_list:
             logger.error("API status 'confirmation_required' sin lista 'pending_confirmation'.")
             await update.message.reply_text("⚠️ Hubo un problema interno (lista pendientes vacía).")
             return

        # Limpiar/inicializar datos pendientes en context
        context.chat_data[PENDING_DATA_KEY] = {}

        for item in pending_list:
            new_tx = item.get('new_transaction')
            existing_tx = item.get('existing_match')
            potential_id = new_tx.get('potential_id') if new_tx else None
            existing_id = existing_tx.get('id') if existing_tx else None

            if not potential_id or not new_tx or not existing_tx or not existing_id:
                 logger.warning(f"Item pendiente inválido: {item}")
                 continue

            # Guardar datos completos para el callback
            context.chat_data[PENDING_DATA_KEY][potential_id] = {'new': new_tx, 'existing': existing_tx}

            # Formatear mensaje HTML
            try:
                amt_new = locale.currency(new_tx.get('amount', 0.0), grouping=True, symbol='€')
                amt_old = locale.currency(float(existing_tx.get('amount_display', "0.0").replace(',','.')), grouping=True, symbol='€')
            except: amt_new, amt_old = f"{new_tx.get('amount_display','?')} €", f"{existing_tx.get('amount_display','?')} €"

            safe_new_date = html.escape(new_tx.get('date_display', 'N/A'))
            safe_new_desc = html.escape(new_tx.get('desc', 'N/A'))
            safe_new_amt = html.escape(amt_new)
            safe_pot_id = html.escape(potential_id[:8])
            safe_ex_id = html.escape(existing_id[:8])
            safe_ex_date = html.escape(existing_tx.get('date_display', 'N/A'))
            safe_ex_desc = html.escape(existing_tx.get('desc', 'N/A'))
            safe_ex_amt = html.escape(amt_old)

            q_text = (
                f"🤔 <b>Posible Duplicado</b>\n\n"
                f"<b>Nueva Tx:</b>\n"
                f"  Fecha: {safe_new_date} | Importe: <b>{safe_new_amt}</b>\n"
                f"  Desc: <code>{safe_new_desc}</code>\n"
                f"  <i>(ID Potencial: <code>{safe_pot_id}...</code>)</i>\n\n"
                f"<b>Similar a Existente:</b>\n"
                f"  ID: <code>{safe_ex_id}...</code> | Fecha: {safe_ex_date}\n"
                f"  Desc: <code>{safe_ex_desc}</code>\n"
                f"  Importe: <b>{safe_ex_amt}</b>\n\n"
                f"➡️ <b>¿La NUEVA es un duplicado?</b>"
            )
            keyboard = [[
                InlineKeyboardButton("Sí (Descartar Nueva)", callback_data=f"{CALLBACK_PREFIX_CONFIRM}:discard:{potential_id}"),
                InlineKeyboardButton("No (Guardar Nueva)", callback_data=f"{CALLBACK_PREFIX_CONFIRM}:insert:{potential_id}"),
            ]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(q_text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

# --- Manejador de Callback Query ---
async def handle_confirmation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja la respuesta a los botones de confirmación."""
    query = update.callback_query
    await query.answer() # Obligatorio

    callback_data = query.data
    logger.info(f"Callback query: {callback_data}")
    try:
        prefix, action, potential_id = callback_data.split(':')
        if prefix != CALLBACK_PREFIX_CONFIRM or action not in ['insert', 'discard']: raise ValueError("Formato callback inválido")
    except ValueError as e:
        logger.warning(f"Callback data inválida: {callback_data} ({e})")
        await query.edit_message_text(text="Error: Callback inválido.", reply_markup=None)
        return

    # Recuperar datos pendientes asociados a este ID
    pending_data = context.chat_data.get(PENDING_DATA_KEY, {}).get(potential_id)
    if not pending_data or 'new' not in pending_data or 'existing' not in pending_data:
        logger.error(f"No se encontraron datos pendientes para ID {potential_id} en chat_data.")
        await query.edit_message_text(text=f"🤔 No encuentro detalles para {potential_id[:8]}... ¿Bot reiniciado? Intenta subir archivo de nuevo.", reply_markup=None)
        return

    # Llamar a la API de confirmación
    api_endpoint = f"{PROCESSOR_API_URL}/confirm_transaction"
    payload = {
        "transaction_id": potential_id,
        "action": action,
        "details": pending_data['new'] if action == 'insert' else None,
        "existing_tx_id": pending_data['existing']['id'] if action == 'discard' else None
    }
    result_text = f"Error API confirmando {action} {potential_id[:8]}..."
    try:
        logger.info(f"Enviando confirmación a API: {payload}")
        response = requests.post(api_endpoint, json=payload, timeout=30)
        response.raise_for_status()
        api_response = response.json()
        logger.info(f"Respuesta confirmación API: {api_response}")
        if api_response.get("status") == "ok":
            result_text = api_response.get("message", f"Acción '{action}' completada.") # Usar mensaje de API
            # Limpiar estado si fue exitoso
            if potential_id in context.chat_data.get(PENDING_DATA_KEY, {}):
                del context.chat_data[PENDING_DATA_KEY][potential_id]
        else:
            result_text = f"⚠️ Problema API: {api_response.get('message', 'Error desconocido')}"
    except requests.exceptions.RequestException as e:
        logger.error(f"Error red/HTTP confirmando {potential_id}: {e}")
        result_text = f"❌ Error red confirmando {action} para {potential_id[:8]}."
    except Exception as e:
         logger.error(f"Error inesperado confirmando {potential_id}: {e}", exc_info=True)
         result_text = f"🆘 Error inesperado confirmando {action} para {potential_id[:8]}."

    # Editar mensaje original con el resultado (usando HTML)
    await query.edit_message_text(text=html.escape(result_text), reply_markup=None, parse_mode=ParseMode.HTML)

# --- Manejador /last ---
async def show_last_transactions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra las últimas N transacciones."""
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

        lines = [f"📊 <b>Últimos {len(transactions)} movimientos:</b>"]
        for tx in transactions:
            try: fecha = datetime.strptime(tx['transaction_date'], '%Y-%m-%d').strftime('%d/%m/%y')
            except: fecha = tx.get('transaction_date', '??/??/??')
            try: importe = locale.currency(float(tx['amount']), grouping=True, symbol='€')
            except: importe = f"{tx.get('amount', '?')} €"
            # Escapar para HTML
            desc = html.escape(tx.get('description', 'N/A'))
            desc_short = desc[:50] + '...' if len(desc) > 50 else desc
            lines.append(f"<code>{html.escape(fecha)}</code> | {html.escape(importe)} | <code>{desc_short}</code>")

        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
    except requests.exceptions.RequestException as e:
        status = f" (HTTP {e.response.status_code})" if e.response else " (Red)"
        logger.error(f"Error API en /last: {e}")
        await update.message.reply_text(f"❌ Error obteniendo datos{status}.")
    except Exception as e:
         logger.error(f"Error inesperado en /last: {e}", exc_info=True)
         await update.message.reply_text("❌ Error inesperado procesando /last.")

# --- Función Principal del Bot ---
def main() -> None:
    if not BOT_TOKEN: logger.critical("Error: Falta BOT_TOKEN."); return
    if not ALLOWED_USER_ID: logger.critical("Error: Falta ALLOWED_USER_ID."); return

    logger.info("Iniciando el bot...")
    # Opcional: Añadir persistencia para que chat_data sobreviva reinicios
    # persistence = DictPersistence()
    # application = Application.builder().token(BOT_TOKEN).persistence(persistence).build()
    application = Application.builder().token(BOT_TOKEN).build()

    user_filter = filters.User(user_id=ALLOWED_USER_ID)

    # Handlers
    application.add_handler(MessageHandler(filters.Document.ALL & user_filter, handle_document))
    application.add_handler(CommandHandler("last", show_last_transactions, filters=user_filter))
    application.add_handler(CallbackQueryHandler(handle_confirmation_callback, pattern=f"^{CALLBACK_PREFIX_CONFIRM}:"))

    logger.info(f"Bot iniciado. Escuchando para User ID: {ALLOWED_USER_ID}...")
    application.run_polling()
    logger.info("Bot detenido.")

if __name__ == "__main__":
    main()