import asyncio
from aiogram import Bot
from core.config import settings
from loguru import logger

async def test_telegram():
    logger.info("Iniciando prueba de conexión con Telegram...")
    bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
    
    try:
        me = await bot.get_me()
        logger.success(f"Bot conectado exitosamente: @{me.username}")
        
        test_message = "🤖 *Prueba de Conexión Maestro V2.0*\n\nEl bot de trading está en línea y listo para operar."
        
        for user_id in settings.telegram_ids_list:
            try:
                await bot.send_message(user_id, test_message, parse_mode="Markdown")
                logger.info(f"Mensaje enviado con éxito al ID: {user_id}")
            except Exception as e:
                logger.error(f"Error enviando mensaje al ID {user_id}: {e}")
                
    except Exception as e:
        logger.critical(f"Error de conexión con el bot: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(test_telegram())
