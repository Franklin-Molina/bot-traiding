import asyncio
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.types import Message
from loguru import logger
from core.config import settings
from bot.handlers import router

class SecurityMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data):
        if event.from_user.id not in settings.telegram_ids_list:
            logger.warning(f"Intento de acceso no autorizado de ID: {event.from_user.id}")
            await event.answer("🚫 No tienes permiso para usar este bot.")
            return
        return await handler(event, data)

async def start_bot(alert_queue: asyncio.Queue):
    """
    Inicia el bot de Telegram y el despachador de alertas.
    """
    bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    
    # Registrar Middleware de Seguridad
    dp.message.middleware(SecurityMiddleware())
    
    # Registrar Handlers
    dp.include_router(router)
    
    logger.info("Bot de Telegram configurado y handlers registrados.")
    
    # Task para procesar alertas de la cola
    alert_task = asyncio.create_task(alert_dispatcher(bot, alert_queue))
    
    try:
        # Iniciar polling
        await dp.start_polling(bot)
    finally:
        alert_task.cancel()
        await bot.session.close()

async def alert_dispatcher(bot: Bot, alert_queue: asyncio.Queue):
    """
    Consume alertas de la cola y las envía a los IDs permitidos.
    """
    while True:
        message = await alert_queue.get()
        try:
            for user_id in settings.telegram_ids_list:
                await bot.send_message(user_id, message)
            alert_queue.task_done()
        except Exception as e:
            logger.error(f"Error enviando alerta: {e}")
        await asyncio.sleep(0.1) # Rate limiting básico


