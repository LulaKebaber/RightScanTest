from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
from utils.date_preparator import DatePreparator
from utils.aggregation import Aggregator

router = Router()


@router.message()
async def aggregate_data(message: Message):
    data = DatePreparator(message.text).json_reader()
    dates = data[0:2]
    group_type = data[2]
    
    data = Aggregator(dates, group_type).aggregate_data()
    await message.answer(str(data))