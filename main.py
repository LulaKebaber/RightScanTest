import asyncio
import logging
import sys
from os import getenv

from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold
from aiogram.filters import Command

import motor.motor_asyncio
from datetime import datetime

import pymongo
import re

from service import aggregate_data, group_data_by_day

# Создаем бота и диспетчера
API_TOKEN = '6876471990:AAGkJ9l7Xibbh2oVLOekDJys_XD6d0ESup0'
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

client = pymongo.MongoClient('mongodb+srv://Kebaber:admin@rightscancluster.yrubl8b.mongodb.net/?retryWrites=true&w=majority')
db = client['RightScanDataBase']
collection = db['RightScanCollection']

@dp.message(Command("a"))
async def aggregate(message: types.Message):
    # Извлекаем параметры из сообщения
    dates = re.findall(r'\{(\d{2}.\d{2}.\d{4})\}', message.text)

    dt_from = datetime.strptime(dates[0], "%d.%m.%Y")  # Начальная дата
    dt_upto = datetime.strptime(dates[1], "%d.%m.%Y")  # Конечная дата
    dt_from = dt_from.isoformat()
    dt_upto = dt_upto.isoformat()
    # Преобразуем даты в формат ISO
    
    group_type = re.search(r'единица группировки - \{(.+?)\}', message.text).group(1)
    group_type_dict = {
        "день": "day",
        "месяц": "month",
        "час": "hour"
    }
    group_type = group_type_dict.get(group_type.lower())
    

    # Получаем агрегированные данные
    result = aggregate_data(dt_from, dt_upto, group_type, collection)

    # Отправляем результат пользователю
    await message.answer(f"Результат агрегации: \n\nDataset: {result['dataset']} \nLabels: {result['labels']}")


@dp.message(Command("help"))
async def handle_help(message: types.Message):
    await message.answer("Help!")

async def main() -> None:
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
