import asyncio
import json
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.utils.markdown import hbold, hlink
from main import collect_data
import os
import time
from apscheduler.schedulers.asyncio import AsyncIOScheduler

bot = Bot(token=os.getenv("TOKEN"),
          default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
schedular = AsyncIOScheduler()

chat_id = os.getenv("CHAT")
async def update_new_ad():
    collect_data()

    with open('result.json', 'r', encoding="utf-8") as file:
        items = json.load(file)

    for index, item in enumerate(items):
        if item not in prev_items:
            card = f'{hlink(item.get("name"), item.get("url"))}\n' \
                   f'Цена: {hbold(item.get("price"))}'

            if index % 5 == 0:
                time.sleep(3)

            await bot.send_message(chat_id, card)
        else:
            prev_items = items



@dp.message(CommandStart())
async def get_ad(message: types.Message):
    await message.answer("hello")

async def main():
    schedular.add_job(update_new_ad, trigger='interval', minutes=5)
    schedular.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())