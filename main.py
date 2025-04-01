# ====================== ИМПОРТЫ И НАСТРОЙКИ ======================
import logging
from datetime import datetime
from io import BytesIO
from typing import List, Tuple, Dict, Optional

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
    Message,
    CallbackQuery
)
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Command, Text
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from openpyxl import Workbook

# region ====================== КОНФИГУРАЦИЯ ======================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = '7072278948:AAHULSz4lWo-FADGtYPvT8zvug3RpySHIFA'
ADMIN_IDS = [1605841515, 903355504]  # ID администраторов
CHANNEL_ID = -1002396542142  # ID канала для проверки подписки (число)
CHANNEL_USERNAME = "lit451"  # Юзернейм канала без @
CHANNEL_INVITE_LINK = f"https://t.me/{CHANNEL_USERNAME}"  # Ссылка для приглашения

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


# ====================== БАЗА ДАННЫХ ======================
async def init_db():
    """Инициализация базы данных"""
    async with aiosqlite.connect('ratings.db') as db:
        # Удаляем старые таблицы, если они есть
        await db.execute('DROP TABLE IF EXISTS users')
        await db.execute('DROP TABLE IF EXISTS actions')
        await db.execute('DROP TABLE IF EXISTS referrals')
        await db.execute('DROP TABLE IF EXISTS orders')
        await db.execute('DROP TABLE IF EXISTS subscriptions')
        await db.execute('DROP TABLE IF EXISTS notification_settings')

        # Создаем новые таблицы с правильной структурой
        await db.execute('''CREATE TABLE IF NOT EXISTS users
                         (user_id INTEGER PRIMARY KEY, 
                          username TEXT, 
                          full_name TEXT, 
                          score INTEGER DEFAULT 0,
                          referral_code TEXT UNIQUE,
                          referrals INTEGER DEFAULT 0,
                          is_subscribed INTEGER DEFAULT 0,
                          subscribed_at DATETIME)''')

        await db.execute('''CREATE TABLE IF NOT EXISTS actions
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER,
                          action_type TEXT,
                          points INTEGER,
                          timestamp DATETIME,
                          details TEXT)''')

        await db.execute('''CREATE TABLE IF NOT EXISTS referrals
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          referrer_id INTEGER,
                          referral_id INTEGER,
                          subscribed INTEGER DEFAULT 0,
                          timestamp DATETIME,
                          UNIQUE(referrer_id, referral_id))''')

        await db.execute('''CREATE TABLE IF NOT EXISTS orders
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          user_id INTEGER,
                          username TEXT,
                          books_purchased INTEGER DEFAULT 0,
                          books_created INTEGER DEFAULT 0,
                          timestamp DATETIME)''')

        await db.execute('''CREATE TABLE IF NOT EXISTS notification_settings
                         (user_id INTEGER PRIMARY KEY,
                          weekly_notifications INTEGER DEFAULT 1)''')

        await db.commit()


# ====================== ОСНОВНЫЕ ФУНКЦИИ ======================
async def check_subscription(user_id: int) -> bool:
    """Проверка подписки пользователя на канал"""
    try:
        chat_member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        subscribed = chat_member.status in ['member', 'administrator', 'creator']

        async with aiosqlite.connect('ratings.db') as db:
            # Получаем текущий статус подписки
            cursor = await db.execute('SELECT is_subscribed FROM users WHERE user_id = ?', (user_id,))
            current_status = (await cursor.fetchone())
            current_status = current_status[0] if current_status else 0

            # Если статус изменился на "подписан"
            if subscribed and not current_status:
                await db.execute('''UPDATE users SET is_subscribed = 1, subscribed_at = ?
                                 WHERE user_id = ?''', (datetime.now(), user_id))

                # Начисляем баллы за подписку, если еще не начисляли
                cursor = await db.execute('''SELECT COUNT(*) FROM actions 
                                         WHERE user_id = ? AND action_type = 'subscription' ''',
                                          (user_id,))
                if (await cursor.fetchone())[0] == 0:
                    await add_points(user_id, 'subscription')

                # Проверяем рефералов
                cursor = await db.execute('''SELECT id, referrer_id FROM referrals 
                                          WHERE referral_id = ? AND subscribed = 0''', (user_id,))
                referrals = await cursor.fetchall()

                for ref in referrals:
                    ref_id, referrer_id = ref
                    # Проверяем, подписан ли реферер
                    cursor = await db.execute('SELECT is_subscribed FROM users WHERE user_id = ?', (referrer_id,))
                    referrer_subscribed = (await cursor.fetchone())[0]

                    if referrer_subscribed:
                        await add_points(referrer_id, 'referral', details=f"Привел пользователя {user_id}")
                        await db.execute('UPDATE users SET referrals = referrals + 1 WHERE user_id = ?', (referrer_id,))

                    # Помечаем реферала как подписавшегося
                    await db.execute('''UPDATE referrals SET subscribed = 1 
                                     WHERE id = ?''', (ref_id,))

            # Если статус изменился на "не подписан"
            elif not subscribed and current_status:
                await db.execute('UPDATE users SET is_subscribed = 0 WHERE user_id = ?', (user_id,))

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

            await db.commit()

        return True
    except Exception as e:
        logger.error(f"Error adding points: {e}")
        return False


async def get_total_score() -> int:
    """Получение общего количества баллов всех пользователей"""
    async with aiosqlite.connect('ratings.db') as db:
        cursor = await db.execute('SELECT SUM(score) FROM users WHERE is_subscribed = 1')
        total = (await cursor.fetchone())[0]
        return total if total else 0


async def generate_pie_chart(user_id: int) -> BytesIO:
    """Генерация круговой диаграммы с долей баллов пользователя"""
    async with aiosqlite.connect('ratings.db') as db:
        # Получаем баллы пользователя
        cursor = await db.execute('SELECT score FROM users WHERE user_id = ?', (user_id,))
        user_score = (await cursor.fetchone())[0] if cursor else 0

        # Получаем общее количество баллов
        total_score = await get_total_score()
        other_score = total_score - user_score if total_score > user_score else 0

        # Если нет данных
        if total_score == 0:
            user_score = 1
            other_score = 1
            total_score = 2

    # Создаем диаграмму
    labels = ['Ваши баллы', 'Другие участники']
    sizes = [user_score, other_score]
    colors = ['#ff9999', '#66b3ff']

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')  # Круговая диаграмма
    ax.set_title(f"Ваши баллы: {user_score} из {total_score} ({user_score / total_score * 100:.1f}%)")

    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()

    return buf


async def get_user_position(user_id: int) -> int:
    """Получение позиции пользователя в рейтинге"""
    async with aiosqlite.connect('ratings.db') as db:
        cursor = await db.execute('''SELECT COUNT(*) FROM users 
                                  WHERE score > (SELECT score FROM users WHERE user_id = ?)
                                  AND is_subscribed = 1''',
                                  (user_id,))
        position = (await cursor.fetchone())[0] + 1
        return position


async def get_top_users(limit: int = 10) -> List[Tuple]:
    """Получение топ-N пользователей (только подписанных)"""
    async with aiosqlite.connect('ratings.db') as db:
        cursor = await db.execute('''SELECT user_id, username, full_name, score 
                                 FROM users 
                                 WHERE is_subscribed = 1
                                 ORDER BY score DESC 
                                 LIMIT ?''', (limit,))
        return await cursor.fetchall()


async def get_user_stats(user_id: int) -> Dict[str, int]:
    """Получение статистики пользователя"""
    async with aiosqlite.connect('ratings.db') as db:
        # Статистика по действиям
        cursor = await db.execute('''SELECT action_type, SUM(points), COUNT(*) 
                                 FROM actions 
                                 WHERE user_id = ? 
                                 GROUP BY action_type''', (user_id,))
        stats = await cursor.fetchall()

        # Общий счет
        cursor = await db.execute('SELECT score FROM users WHERE user_id = ?', (user_id,))
        total_score = (await cursor.fetchone())[0] if cursor else 0

        # Статистика по заказам
        cursor = await db.execute('''SELECT SUM(books_purchased), SUM(books_created) 
                                  FROM orders 
                                  WHERE user_id = ?''', (user_id,))
        order_stats = await cursor.fetchone()
        books_purchased = order_stats[0] if order_stats and order_stats[0] else 0
        books_created = order_stats[1] if order_stats and order_stats[1] else 0

        # Статус подписки
        cursor = await db.execute('SELECT is_subscribed FROM users WHERE user_id = ?', (user_id,))
        is_subscribed = (await cursor.fetchone())[0] if cursor else 0

        return {
            'stats': stats,
            'total_score': total_score,
            'position': await get_user_position(user_id) if is_subscribed else None,
            'books_purchased': books_purchased,
            'books_created': books_created,
            'is_subscribed': is_subscribed
        }


async def get_referral_info(user_id: int) -> Dict:
    """Получение информации о рефералах"""
    async with aiosqlite.connect('ratings.db') as db:
        # Получаем реферальный код
        cursor = await db.execute('SELECT referral_code FROM users WHERE user_id = ?', (user_id,))
        referral_code = (await cursor.fetchone())[0] if cursor else None

        # Количество рефералов
        cursor = await db.execute('SELECT COUNT(*) FROM referrals WHERE referrer_id = ? AND subscribed = 1', (user_id,))
        referrals = (await cursor.fetchone())[0] if cursor else 0

        # Баллы за рефералов
        cursor = await db.execute('''SELECT SUM(points) FROM actions 
                                  WHERE user_id = ? AND action_type = 'referral' ''',
                                  (user_id,))
        referral_points = (await cursor.fetchone())[0] if cursor else 0

        return {
            'referral_code': referral_code,
            'referrals': referrals,
            'referral_points': referral_points
        }


async def register_user(user_id: int, username: str, full_name: str):
    """Регистрация нового пользователя"""
    async with aiosqlite.connect('ratings.db') as db:
        referral_code = f"ref_{user_id}"
        await db.execute('''INSERT OR IGNORE INTO users 
                          (user_id, username, full_name, referral_code) 
                          VALUES (?, ?, ?, ?)''',
                         (user_id, username, full_name, referral_code))
        await db.commit()


async def process_referral(referral_id: int, referrer_id: int):
    """Обработка реферала"""
    if referrer_id == referral_id:
        return False

    async with aiosqlite.connect('ratings.db') as db:
        # Проверяем, не регистрировался ли уже этот реферал
        cursor = await db.execute('''SELECT COUNT(*) FROM referrals 
                                  WHERE referrer_id = ? AND referral_id = ?''',
                                  (referrer_id, referral_id))
        already_exists = (await cursor.fetchone())[0]

        if not already_exists:
            # Фиксируем реферала (подписка будет проверена позже)
            await db.execute('''INSERT INTO referrals 
                             (referrer_id, referral_id, timestamp) 
                             VALUES (?, ?, ?)''',
                             (referrer_id, referral_id, datetime.now()))
            await db.commit()
            return True
    return False


async def add_order(user_id: int, username: str, purchased: int = 0, created: int = 0):
    """Добавление заказа"""
    async with aiosqlite.connect('ratings.db') as db:
        await db.execute('''INSERT INTO orders 
                         (user_id, username, books_purchased, books_created, timestamp) 
                         VALUES (?, ?, ?, ?, ?)''',
                         (user_id, username, purchased, created, datetime.now()))
        await db.commit()

    if purchased > 0:
        await add_points(user_id, 'book_purchase', purchased)
    if created > 0:
        await add_points(user_id, 'book_creation', created)


async def update_all_subscribers():
    """Обновление статуса подписки для всех пользователей"""
    try:
        async with aiosqlite.connect('ratings.db') as db:
            # Получаем всех пользователей из базы
            cursor = await db.execute('SELECT user_id FROM users')
            users = await cursor.fetchall()

            for user in users:
                user_id = user[0]
                await check_subscription(user_id)

    except Exception as e:
        logger.error(f"Error updating subscribers: {e}")


# ====================== КОМАНДЫ ПОЛЬЗОВАТЕЛЯ ======================
async def sync_channel_subscribers():
    """Синхронизация всех подписчиков канала с базой данных"""
    try:
        async with aiosqlite.connect('ratings.db') as db:
            # Получаем всех подписчиков канала
            channel_members = []
            async for member in bot.get_chat_members(CHANNEL_ID):
                if member.user.is_bot:
                    continue
                channel_members.append(member.user)

            # Добавляем/обновляем подписчиков в базе
            for member in channel_members:
                # Проверяем, есть ли пользователь в базе
                cursor = await db.execute('SELECT 1 FROM users WHERE user_id = ?', (member.id,))
                exists = await cursor.fetchone()

                if not exists:
                    # Добавляем нового подписчика
                    referral_code = f"ref_{member.id}"
                    await db.execute('''INSERT INTO users 
                                      (user_id, username, full_name, referral_code, is_subscribed) 
                                      VALUES (?, ?, ?, ?, ?)''',
                                     (member.id, member.username, member.full_name, referral_code, 1))

                    # Начисляем баллы за подписку
                    await db.execute('''INSERT INTO actions 
                                      (user_id, action_type, points, timestamp) 
                                      VALUES (?, ?, ?, ?)''',
                                     (member.id, 'subscription', POINT_SYSTEM['subscription'], datetime.now()))

                    # Обновляем общий счёт
                    await db.execute('UPDATE users SET score = score + ? WHERE user_id = ?',
                                     (POINT_SYSTEM['subscription'], member.id))
                else:
                    # Обновляем статус подписки
                    await db.execute('UPDATE users SET is_subscribed = 1 WHERE user_id = ?', (member.id,))

            await db.commit()
    except Exception as e:
        logger.error(f"Error syncing channel subscribers: {e}")


async def check_referral(user_id: int, referrer_id: int):
    """Проверка и обработка реферала"""
    try:
        async with aiosqlite.connect('ratings.db') as db:
            # Проверяем, новый ли это реферал
            cursor = await db.execute('''SELECT 1 FROM referrals 
                                      WHERE referral_id = ? AND referrer_id = ?''',
                                      (user_id, referrer_id))
            if not await cursor.fetchone():
                # Фиксируем реферала
                await db.execute('''INSERT INTO referrals 
                                  (referrer_id, referral_id, timestamp) 
                                  VALUES (?, ?, ?)''',
                                 (referrer_id, user_id, datetime.now()))

                # Начисляем баллы рефереру
                await add_points(referrer_id, 'referral')

                await db.commit()
                return True
        return False
    except Exception as e:
        logger.error(f"Error processing referral: {e}")
        return False


@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """Обработка команды /start"""
    user = message.from_user

    # Регистрация пользователя
    await register_user(user.id, user.username, user.full_name)

    # Обработка реферальной ссылки
    if len(message.get_args()) > 0 and message.get_args().startswith('ref_'):
        referrer_id = int(message.get_args().split('_')[1])
        await check_referral(user.id, referrer_id)

    # Проверка подписки
    await check_subscription(user.id)

    # Приветственное сообщение
    await message.answer(
        f"👋 Привет, {user.full_name}!\n"
        "Добро пожаловать в систему рейтинга!",
        reply_markup=await get_main_menu_keyboard()
    )


@dp.message_handler(commands=['stats'])
@dp.message_handler(Text(equals="📊 Моя статистика"))
async def cmd_stats(message: types.Message):
    """Показать статистику пользователя"""
    user_stats = await get_user_stats(message.from_user.id)
    referral_info = await get_referral_info(message.from_user.id)

    action_descriptions = {
        'subscription': "Подписки",
        'referral': "Рефералы",
        'comment': "Комментарии",
        'book_purchase': "Покупки книг",
        'book_creation': "Создание книг"
    }

    message_text = f"📊 <b>Ваша статистика</b>\n\n"
    for stat in user_stats['stats']:
        message_text += f"▫️ {action_descriptions.get(stat[0], stat[0])}: {stat[2]} раз(а) = {stat[1]} баллов\n"

    message_text += f"\n📚 <b>Книги:</b>\n"
    message_text += f"▫️ Куплено: {user_stats['books_purchased']}\n"
    message_text += f"▫️ Создано: {user_stats['books_created']}\n"

    message_text += f"\n👥 <b>Рефералы:</b>\n"
    message_text += f"▫️ Приглашено: {referral_info['referrals']}\n"
    message_text += f"▫️ Заработано баллов: {referral_info['referral_points']}\n"

    message_text += f"\n<b>Итого:</b> {user_stats['total_score']} баллов\n"

    if user_stats['is_subscribed']:
        message_text += f"<b>Место в рейтинге:</b> {user_stats['position']}\n"
    else:
        message_text += "❌ Вы не подписаны на канал, поэтому не участвуете в рейтинге\n"

    message_text += f"<b>Подписка на канал:</b> {'✅' if user_stats['is_subscribed'] else '❌'}\n"

    # Генерируем диаграмму
    chart_image = await generate_pie_chart(message.from_user.id)

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
    # Синхронизируем подписчиков перед показом рейтинга
    await sync_channel_subscribers()

    top_users = await get_top_users(10)
    user_stats = await get_user_stats(message.from_user.id)

    message_text = "🏆 <b>Топ-10 участников</b>\n\n"
    for idx, user in enumerate(top_users, 1):
        highlight = " <b>◄ ВЫ</b>" if user[0] == message.from_user.id else ""
        name = user[2] if user[2] else f"@{user[1]}" if user[1] else f"ID:{user[0]}"
        message_text += f"{idx}. {name} - {user[3]} баллов{highlight}\n"

    if user_stats['position'] and user_stats['position'] > 10:
        message_text += f"\n...\n{user_stats['position']}. Вы - {user_stats['total_score']} баллов\n"
    elif not user_stats['is_subscribed']:
        message_text += "\n❌ Вы не подписаны на канал и не участвуете в рейтинге\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("📊 Моя статистика", callback_data='my_stats'))
    keyboard.add(InlineKeyboardButton("📥 Полный рейтинг (Excel)", callback_data='full_report'))

    await message.answer(
        text=message_text,
        parse_mode='HTML',
        reply_markup=keyboard
    )


@dp.message_handler(Text(equals="📢 Реферальная система"))
async def cmd_referral(message: types.Message):
    """Показать реферальную информацию"""
    referral_info = await get_referral_info(message.from_user.id)
    user_stats = await get_user_stats(message.from_user.id)

    if not user_stats['is_subscribed']:
        await message.answer(
            "❌ Чтобы участвовать в реферальной программе, вы должны быть подписаны на канал!\n"
            f"Подпишитесь: {CHANNEL_INVITE_LINK}",
            reply_markup=await get_main_menu_keyboard()
        )
        return

    await message.answer(
        f"📢 <b>Реферальная система</b>\n\n"
        f"Ваш реферальный код: <code>{referral_info['referral_code']}</code>\n"
        f"Приглашено пользователей: {referral_info['referrals']}\n"
        f"Заработано баллов: {referral_info['referral_points']}\n\n"
        f"<b>Как приглашать:</b>\n"
        f"1. Поделитесь этой ссылкой:\n"
        f"<code>{CHANNEL_INVITE_LINK}?start={referral_info['referral_code']}</code>\n"
        f"2. За каждого друга, который подпишется по вашей ссылке, вы получите {POINT_SYSTEM['referral']} балла\n"
        f"3. Ваш друг получит {POINT_SYSTEM['subscription']} балл за подписку",
        parse_mode='HTML',
        reply_markup=await get_main_menu_keyboard()
    )


@dp.message_handler(Text(equals="⚙️ Настройки"))
async def cmd_settings(message: types.Message):
    """Показать настройки"""
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🔔 Включить уведомления", callback_data='enable_notifications'))
    keyboard.add(InlineKeyboardButton("🔕 Выключить уведомления", callback_data='disable_notifications'))

    # Получаем текущий статус уведомлений
    async with aiosqlite.connect('ratings.db') as db:
        cursor = await db.execute('SELECT weekly_notifications FROM notification_settings WHERE user_id = ?',
                                  (message.from_user.id,))
        status = await cursor.fetchone()
        notifications_status = status[0] if status else 1

    await message.answer(
        f"⚙️ <b>Настройки</b>\n\n"
        f"Текущий статус уведомлений: {'🔔 Включены' if notifications_status else '🔕 Выключены'}",
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
        InlineKeyboardButton("📝 Добавить заказ", callback_data='admin_add_order'),
        InlineKeyboardButton("📤 Экспорт в Excel", callback_data='admin_export_excel')
    )

    await message.answer(
        "🛠 <b>Панель администратора</b>",
        parse_mode='HTML',
        reply_markup=keyboard
    )


# ====================== ОБРАБОТКА ЗАКАЗОВ ======================
@dp.callback_query_handler(lambda c: c.data == 'admin_add_order')
async def process_admin_add_order(callback_query: CallbackQuery):
    """Обработка добавления заказа"""
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(
        callback_query.from_user.id,
        "📝 <b>Добавление заказа</b>\n\n"
        "Отправьте данные в формате:\n"
        "<code>@username количество_купленных количество_созданных</code>\n\n"
        "Пример:\n"
        "<code>@user123 2 1</code>",
        parse_mode='HTML'
    )


@dp.message_handler(lambda message: message.text and message.text.split()[0].startswith('@')
                                    and len(message.text.split()) >= 3
                                    and message.from_user.id in ADMIN_IDS)
async def process_order_input(message: Message):
    """Обработка ввода данных заказа"""
    try:
        parts = message.text.split()
        username = parts[0][1:]  # Убираем @
        purchased = int(parts[1])
        created = int(parts[2])

        # Получаем user_id по username
        async with aiosqlite.connect('ratings.db') as db:
            cursor = await db.execute('SELECT user_id FROM users WHERE username = ?', (username,))
            user = await cursor.fetchone()

            if user:
                user_id = user[0]
                await add_order(user_id, username, purchased, created)
                await message.answer(f"✅ Заказ для @{username} успешно добавлен!\n"
                                     f"Куплено: {purchased} книг\n"
                                     f"Создано: {created} книг")
            else:
                await message.answer(f"❌ Пользователь @{username} не найден в базе")
    except Exception as e:
        await message.answer(f"❌ Ошибка обработки заказа: {e}")


# ====================== ОБРАБОТКА CALLBACK ======================
@dp.callback_query_handler(lambda c: c.data == 'my_stats')
async def process_callback_my_stats(callback_query: CallbackQuery):
    """Обработка кнопки 'Моя статистика'"""
    await cmd_stats(Message(
        chat=callback_query.message.chat,
        from_user=callback_query.from_user,
        text="/stats"
    ))
    await bot.answer_callback_query(callback_query.id)


@dp.callback_query_handler(lambda c: c.data == 'full_report')
async def process_callback_full_report(callback_query: CallbackQuery):
    """Обработка кнопки 'Полный рейтинг'"""
    excel_file = await generate_excel_report()
    await bot.send_document(
        chat_id=callback_query.message.chat.id,
        document=InputFile(excel_file, filename='Рейтинг_участников.xlsx'),
        caption="📊 Полный отчет по рейтингу участников"
    )
    await bot.answer_callback_query(callback_query.id)


@dp.callback_query_handler(lambda c: c.data == 'admin_export_excel')
async def process_admin_export_excel(callback_query: CallbackQuery):
    """Обработка экспорта в Excel"""
    excel_file = await generate_excel_report()
    await bot.send_document(
        chat_id=callback_query.message.chat.id,
        document=InputFile(excel_file, filename='Рейтинг_участников.xlsx'),
        caption="📊 Полный отчет по рейтингу участников"
    )
    await bot.answer_callback_query(callback_query.id)


@dp.callback_query_handler(lambda c: c.data in ['enable_notifications', 'disable_notifications'])
async def process_notification_settings(callback_query: CallbackQuery):
    """Обработка настроек уведомлений"""
    status = 1 if callback_query.data == 'enable_notifications' else 0

    async with aiosqlite.connect('ratings.db') as db:
        await db.execute('''INSERT OR REPLACE INTO notification_settings 
                          (user_id, weekly_notifications) 
                          VALUES (?, ?)''',
                         (callback_query.from_user.id, status))
        await db.commit()

    await bot.answer_callback_query(
        callback_query.id,
        text=f"Уведомления {'включены' if status else 'выключены'}",
        show_alert=True
    )
    await cmd_settings(Message(
        chat=callback_query.message.chat,
        from_user=callback_query.from_user,
        text="/settings"
    ))


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


async def generate_excel_report() -> BytesIO:
    """Генерация Excel-отчета"""
    async with aiosqlite.connect('ratings.db') as db:
        cursor = await db.execute('''SELECT user_id, username, full_name, score, referrals, is_subscribed
                                 FROM users 
                                 ORDER BY score DESC''')
        users = await cursor.fetchall()

        wb = Workbook()
        ws = wb.active
        ws.title = "Рейтинг участников"
        ws.append(["Место", "ID", "Username", "Имя", "Баллы", "Рефералов", "Подписка", "Куплено книг", "Создано книг"])

        for idx, user in enumerate(users, 1):
            cursor = await db.execute('''SELECT SUM(books_purchased), SUM(books_created) 
                                     FROM orders WHERE user_id = ?''', (user[0],))
            order_stats = await cursor.fetchone()
            purchased = order_stats[0] if order_stats and order_stats[0] else 0
            created = order_stats[1] if order_stats and order_stats[1] else 0

            ws.append([
                idx,
                user[0],
                user[1],
                user[2],
                user[3],
                user[4],
                "Да" if user[5] else "Нет",
                purchased,
                created
            ])

        excel_file = BytesIO()
        wb.save(excel_file)
        excel_file.seek(0)

        return excel_file


# ====================== ЗАПУСК БОТА ======================
async def on_startup(dp):
    """Действия при запуске бота"""
    await init_db()
    logger.info("Бот успешно запущен")


if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)