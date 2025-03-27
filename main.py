# ====================== ИМПОРТЫ И НАСТРОЙКИ ======================
import logging
from datetime import datetime
from io import BytesIO
from typing import List, Tuple, Dict

import aiosqlite
import matplotlib.pyplot as plt
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    ReplyKeyboardMarkup,
    KeyboardButton,
    Message
)
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters import Command, Text
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from openpyxl import Workbook

# region ====================== КОНФИГУРАЦИЯ ======================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = 'ВАШ_TELEGRAM_BOT_TOKEN'
ADMIN_IDS = [123456789]  # ID администраторов
CHANNEL_ID = '@ваш_канал'  # ID канала для проверки подписки
COMMENT_CHANNEL_ID = '@ваш_канал_с_комментариями'
LEADERBOARD_MESSAGE_ID = None  # Будет обновляться динамически

POINT_SYSTEM = {
    'subscription': 1,
    'referral': 2,
    'comment': 1,
    'book_purchase': 5,
    'book_creation': 7
}

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())
# endregion


# ====================== АСИНХРОННАЯ БАЗА ДАННЫХ ======================
async def init_db():
    """Инициализация базы данных при первом запуске"""
    async with aiosqlite.connect('ratings.db') as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS users
                         (user_id INTEGER PRIMARY KEY, 
                          username TEXT, 
                          full_name TEXT, 
                          score INTEGER DEFAULT 0,
                          referral_code TEXT,
                          referrals INTEGER DEFAULT 0)''')

        await db.execute('''CREATE TABLE IF NOT EXISTS actions
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER,
                          action_type TEXT,
                          points INTEGER,
                          timestamp DATETIME,
                          details TEXT)''')

        await db.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                         (user_id INTEGER PRIMARY KEY,
                          is_subscribed INTEGER DEFAULT 0,
                          last_check DATETIME)''')

        await db.execute('''CREATE TABLE IF NOT EXISTS notification_settings
                         (user_id INTEGER PRIMARY KEY,
                          weekly_notifications INTEGER DEFAULT 1)''')

        await db.execute('''CREATE TABLE IF NOT EXISTS user_positions
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER,
                          position INTEGER,
                          date DATETIME)''')

        await db.execute('''CREATE TABLE IF NOT EXISTS admin_messages
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          message_type TEXT,
                          chat_id INTEGER,
                          message_id INTEGER)''')

        await db.commit()


# ====================== ОСНОВНЫЕ ФУНКЦИИ ======================
async def check_subscription(user_id: int) -> bool:
    """Проверка подписки пользователя на канал"""
    try:
        chat_member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        subscribed = chat_member.status in ['member', 'administrator', 'creator']

        async with aiosqlite.connect('ratings.db') as db:
            await db.execute('''INSERT OR REPLACE INTO subscriptions 
                              (user_id, is_subscribed, last_check) 
                              VALUES (?, ?, ?)''',
                             (user_id, int(subscribed), datetime.now()))
            await db.commit()

        return subscribed
    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        return False


async def add_points(user_id: int, action_type: str, count: int = 1, details: str = None) -> bool:
    """Начисление баллов пользователю"""
    if action_type not in POINT_SYSTEM:
        logger.error(f"Invalid action type: {action_type}")
        return False

    points = POINT_SYSTEM[action_type] * count

    try:
        async with aiosqlite.connect('ratings.db') as db:
            # Обновляем баллы пользователя
            await db.execute('UPDATE users SET score = score + ? WHERE user_id = ?',
                             (points, user_id))

            # Записываем действие
            await db.execute('''INSERT INTO actions 
                             (user_id, action_type, points, timestamp, details) 
                             VALUES (?, ?, ?, ?, ?)''',
                             (user_id, action_type, points, datetime.now(), details))

            # Для рефералов обновляем счетчик
            if action_type == 'referral':
                await db.execute('UPDATE users SET referrals = referrals + 1 WHERE user_id = ?',
                                 (user_id,))

            # Обновляем позицию в рейтинге
            cursor = await db.execute('''SELECT COUNT(*) FROM users WHERE score > 
                                      (SELECT score FROM users WHERE user_id = ?)''',
                                      (user_id,))
            position = (await cursor.fetchone())[0] + 1
            await db.execute('INSERT INTO user_positions (user_id, position, date) VALUES (?, ?, ?)',
                             (user_id, position, datetime.now()))

            await db.commit()

        return True
    except Exception as e:
        logger.error(f"Error adding points: {e}")
        return False


async def get_user_position(user_id: int) -> int:
    """Получение текущей позиции пользователя в рейтинге"""
    async with aiosqlite.connect('ratings.db') as db:
        cursor = await db.execute('''SELECT COUNT(*) FROM users WHERE score > 
                                  (SELECT score FROM users WHERE user_id = ?)''',
                                  (user_id,))
        position = (await cursor.fetchone())[0] + 1
        return position


async def get_top_users(limit: int = 10) -> List[Tuple]:
    """Получение топ-N пользователей"""
    async with aiosqlite.connect('ratings.db') as db:
        cursor = await db.execute('''SELECT user_id, username, full_name, score 
                                 FROM users 
                                 ORDER BY score DESC 
                                 LIMIT ?''', (limit,))
        return await cursor.fetchall()


async def get_user_stats(user_id: int) -> Dict[str, int]:
    """Получение статистики пользователя"""
    async with aiosqlite.connect('ratings.db') as db:
        cursor = await db.execute('''SELECT action_type, SUM(points), COUNT(*) 
                                 FROM actions 
                                 WHERE user_id = ? 
                                 GROUP BY action_type''', (user_id,))
        stats = await cursor.fetchall()

        cursor = await db.execute('SELECT score FROM users WHERE user_id = ?', (user_id,))
        total_score = (await cursor.fetchone())[0] if cursor.rowcount > 0 else 0

        return {
            'stats': stats,
            'total_score': total_score,
            'position': await get_user_position(user_id)
        }


# ====================== КОМАНДЫ ПОЛЬЗОВАТЕЛЯ ======================
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """Обработка команды /start"""
    user = message.from_user

    # Регистрация пользователя в БД
    async with aiosqlite.connect('ratings.db') as db:
        await db.execute('''INSERT OR IGNORE INTO users 
                          (user_id, username, full_name, referral_code) 
                          VALUES (?, ?, ?, ?)''',
                         (user.id, user.username, user.full_name, f"ref_{user.id}"))
        await db.commit()

    # Обработка реферальной ссылки
    if len(message.get_args()) > 0 and message.get_args().startswith('ref_'):
        referrer_id = int(message.get_args().split('_')[1])
        if referrer_id != user.id:
            await add_points(referrer_id, 'referral', details=f"Привел пользователя {user.id}")

    # Проверка подписки и начисление баллов
    if await check_subscription(user.id):
        await add_points(user.id, 'subscription')

    # Отправка приветственного сообщения
    await message.answer(
        f"👋 Привет, {user.full_name}!\nДобро пожаловать в систему рейтинга!",
        reply_markup=await get_main_menu_keyboard()
    )


@dp.message_handler(commands=['stats'])
@dp.message_handler(Text(equals="📊 Моя статистика"))
async def cmd_stats(message: types.Message):
    """Показать статистику пользователя"""
    user_stats = await get_user_stats(message.from_user.id)

    action_descriptions = {
        'subscription': "Подписки",
        'referral': "Рефералы",
        'comment': "Комментарии",
        'book_purchase': "Покупки книг",
        'book_creation': "Совместные проекты"
    }

    message_text = f"📊 <b>Ваша статистика</b>\n\n"
    for stat in user_stats['stats']:
        message_text += f"▫️ {action_descriptions.get(stat[0], stat[0])}: {stat[2]} раз(а) = {stat[1]} баллов\n"

    message_text += f"\n<b>Итого:</b> {user_stats['total_score']} баллов\n"
    message_text += f"<b>Место в рейтинге:</b> {user_stats['position']}\n"

    # Генерация круговой диаграммы
    chart_image = await generate_pie_chart(message.from_user.id, user_stats['stats'])

    await message.answer_photo(
        photo=chart_image,
        caption=message_text,
        parse_mode='HTML',
        reply_markup=await get_main_menu_keyboard()
    )


@dp.message_handler(commands=['leaderboard'])
@dp.message_handler(Text(equals="🏆 Таблица лидеров"))
async def cmd_leaderboard(message: types.Message):
    """Показать таблицу лидеров"""
    top_users = await get_top_users(10)
    user_stats = await get_user_stats(message.from_user.id)

    message_text = "🏆 <b>Топ-10 участников</b>\n\n"
    for idx, user in enumerate(top_users, 1):
        highlight = " <<< ВЫ" if user[0] == message.from_user.id else ""
        name = user[2] if user[2] else f"@{user[1]}" if user[1] else f"ID:{user[0]}"
        message_text += f"{idx}. {name} - {user[3]} баллов{highlight}\n"

    if user_stats['position'] > 10:
        message_text += f"\n...\n{user_stats['position']}. Вы - {user_stats['total_score']} баллов\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📊 Моя статистика", callback_data='my_stats'))
    keyboard.add(InlineKeyboardButton("📥 Полный рейтинг (Excel)", callback_data='full_report'))

    await message.answer(
        text=message_text,
        parse_mode='HTML',
        reply_markup=keyboard
    )


# ====================== АДМИН-ПАНЕЛЬ ======================
@dp.message_handler(commands=['admin'])
async def cmd_admin(message: types.Message):
    """Панель администратора"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав доступа к этой команде.")
        return

    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("➕ Начислить баллы", callback_data='admin_add_points'),
        InlineKeyboardButton("📊 Обновить рейтинг", callback_data='admin_update_leaderboard'),
        InlineKeyboardButton("📝 Добавить покупку", callback_data='admin_add_purchase'),
        InlineKeyboardButton("📝 Добавить проект", callback_data='admin_add_project'),
        InlineKeyboardButton("📤 Экспорт в Excel", callback_data='admin_export_excel')
    )

    await message.answer(
        "🛠 <b>Панель администратора</b>",
        parse_mode='HTML',
        reply_markup=keyboard
    )


# ====================== ОБРАБОТЧИКИ INLINE КНОПОК ======================
@dp.callback_query_handler(lambda c: c.data == 'my_stats')
async def process_callback_my_stats(callback_query: types.CallbackQuery):
    """Обработка кнопки 'Моя статистика'"""
    await cmd_stats(Message(
        chat=callback_query.message.chat,
        from_user=callback_query.from_user,
        text="/stats"
    ))
    await bot.answer_callback_query(callback_query.id)


@dp.callback_query_handler(lambda c: c.data == 'full_report')
async def process_callback_full_report(callback_query: types.CallbackQuery):
    """Обработка кнопки 'Полный рейтинг'"""
    await generate_excel_report_cmd(Message(
        chat=callback_query.message.chat,
        from_user=callback_query.from_user,
        text="/report"
    ))
    await bot.answer_callback_query(callback_query.id)


# ====================== УТИЛИТЫ ======================
async def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура главного меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("📊 Моя статистика"), KeyboardButton("🏆 Таблица лидеров")],
            [KeyboardButton("📢 Реферальная система"), KeyboardButton("⚙️ Настройки")]
        ],
        resize_keyboard=True
    )


async def generate_pie_chart(user_id: int, stats: List[Tuple]) -> BytesIO:
    """Генерация круговой диаграммы статистики"""
    labels = []
    sizes = []

    action_names = {
        'subscription': "Подписки",
        'referral': "Рефералы",
        'comment': "Комментарии",
        'book_purchase': "Книги",
        'book_creation': "Проекты"
    }

    for stat in stats:
        labels.append(action_names.get(stat[0], stat[0]))
        sizes.append(stat[1])

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')
    ax.set_title("Распределение ваших баллов")

    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()

    return buf


async def generate_excel_report() -> BytesIO:
    """Генерация Excel-отчета"""
    async with aiosqlite.connect('ratings.db') as db:
        cursor = await db.execute('''SELECT user_id, username, full_name, score 
                                 FROM users 
                                 ORDER BY score DESC''')
        users = await cursor.fetchall()

        wb = Workbook()
        ws = wb.active
        ws.title = "Рейтинг участников"
        ws.append(["Место", "ID", "Username", "Имя", "Баллы", "Рефералов"])

        for idx, user in enumerate(users, 1):
            cursor = await db.execute('''SELECT COUNT(*) FROM actions 
                                     WHERE details LIKE ? AND action_type = 'referral' ''',
                                      (f'%{user[0]}%',))
            referrals = (await cursor.fetchone())[0]
            ws.append([idx, user[0], user[1], user[2], user[3], referrals])

        excel_file = BytesIO()
        wb.save(excel_file)
        excel_file.seek(0)

        return excel_file


async def generate_excel_report_cmd(message: types.Message):
    """Команда генерации Excel-отчета"""
    excel_file = await generate_excel_report()
    await message.answer_document(
        document=InputFile(excel_file, filename='Рейтинг_участников.xlsx'),
        caption="📊 Полный отчет по рейтингу участников"
    )


# ====================== ЗАПУСК БОТА ======================
async def on_startup(dp):
    """Действия при запуске бота"""
    await init_db()
    logger.info("Бот успешно запущен")


if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)