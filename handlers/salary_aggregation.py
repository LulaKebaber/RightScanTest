from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext

from utils.states import Form
from utils.date_preparator import DatePreparator
from utils.aggregation import Aggregator

router = Router()


# @router.message(message: Message)
# async def aggregate(message: Message, state: FSMContext):
#     await state.set_state(Form.message)
#     await message.answer('Введи месседж')


@router.message()
async def aggregate_data(message: Message):
    dates = DatePreparator(message.text).prepare_dates()
    group_type = DatePreparator(message.text).prepare_group_type()

    data = Aggregator(dates, group_type).aggregate_data()
    await message.answer(str(data))