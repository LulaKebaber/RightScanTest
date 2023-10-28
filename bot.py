import asyncio
import sys
import logging
from aiogram import Bot, Dispatcher

import handlers
from config_reader import config

async def main() -> None:
    bot = Bot(config.bot_token.get_secret_value())
    dp = Dispatcher()

    dp.include_routers(
        handlers.start.router,
        handlers.salary_aggregation.router,
    )

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())