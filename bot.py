import asyncio
import calendar
import json
import logging
import os
import re
import secrets
from datetime import date, timedelta
from html import escape as h
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
POLIS_PRICE = int(os.environ.get("POLIS_PRICE", "2000"))
WELCOME_BONUS = int(os.environ.get("WELCOME_BONUS", "5000"))
KASPI_CARD_INFO = os.environ.get("KASPI_CARD_INFO", "").replace("\\n", "\n")

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from num2words import num2words

from db import (
    change_balance,
    ensure_user,
    get_balance,
    get_polis_by_trace_id,
    has_welcome_bonus,
    init_db,
    is_tos_accepted,
    list_recent_polises,
    list_recent_transactions,
    list_users,
    log_polis,
    set_tos_accepted,
)
from nsk import fetch_bonus_malus
from pdf import PdfError, generate_pdf

GENERATED_DIR = Path(__file__).parent / "generated"
GENERATED_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO)

COMPANIES = {"nomad": "🏢 Номад Транзит"}

PERIODS = {
    "10d": {"label": "10 дней", "days": 10},
    "15d": {"label": "15 дней", "days": 15},
    "1m":  {"label": "1 месяц",   "months": 1},
    "3m":  {"label": "3 месяца",   "months": 3},
    "6m":  {"label": "6 месяцев",   "months": 6},
    "1y":  {"label": "1 год",  "months": 12},
}

CLASS_OPTIONS = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "M", "M1", "M2", "A"]

STANDARD_DOGOVOR_NO = "0656T160437N"

DISCLAIMER = (
    "\n\n<i>⚠️ Полис выдан для личного использования держателем. "
    "Передача документа третьим лицам, перепродажа, изменение или иное "
    "использование не допускаются. Ответственность за нарушение указанных "
    "условий несёт пользователь самостоятельно.</i>"
)

TOS_TEXT = (
    "<b>📋 Пользовательское соглашение</b>\n\n"
    "Используя данный сервис, пользователь подтверждает согласие со следующими условиями:\n\n"
    "<b>1. Достоверность данных.</b>\n"
    "Пользователь самостоятельно вводит сведения для оформления полиса (ФИО, ИИН, "
    "данные ТС и т.п.) и несёт полную ответственность за их достоверность.\n\n"
    "<b>2. Назначение документа.</b>\n"
    "Полученный полис предназначен исключительно для личного использования держателем "
    "полиса. Передача документа третьим лицам, перепродажа, изменение или повторное "
    "использование не допускаются.\n\n"
    "<b>3. Журнал операций.</b>\n"
    "Сервис ведёт журнал всех созданных документов: Telegram ID пользователя, ФИО и "
    "ИИН застрахованных, сведения о ТС, дата и время. Эти данные могут быть переданы "
    "по запросу страховой компании или уполномоченных органов РК.\n\n"
    "<b>4. Запрет на противоправное использование.</b>\n"
    "Использование сервиса с целью введения третьих лиц в заблуждение, мошенничества "
    "или подделки документов запрещено и влечёт ответственность пользователя по "
    "законодательству Республики Казахстан.\n\n"
    "<b>5. Ограничение ответственности сервиса.</b>\n"
    "Сервис не несёт ответственности за действия пользователя, нарушающие условия "
    "настоящего соглашения, а также за последствия передачи документа третьим лицам.\n\n"
    "<b>6. Принятие условий.</b>\n"
    "Нажатие кнопки «✅ Согласен» означает полное и безоговорочное принятие настоящих условий."
)


def gen_trace_id() -> str:
    """8-char trace ID without confusing chars (no 0/O, 1/I)."""
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(8))


class Form(StatesGroup):
    people_count = State()
    company = State()
    period = State()
    sum_input = State()
    sum_confirm = State()
    dogovor_choice = State()
    dogovor_manual = State()
    p_iin_choice = State()
    p_iin_input = State()
    p_iin_confirm = State()
    p_iin_not_found = State()
    p_fio = State()
    p_klass = State()
    car_brand = State()
    car_number = State()
    vin = State()
    final_confirm = State()


def fmt_date(d: date) -> str:
    return d.strftime("%d.%m.%Y")


def add_months(d: date, months: int) -> date:
    total = d.month - 1 + months
    year = d.year + total // 12
    month = total % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def compute_period(period_key: str, dogovor_date: date) -> tuple[date, date]:
    cfg = PERIODS[period_key]
    date_from = dogovor_date + timedelta(days=1)
    if "days" in cfg:
        date_to = dogovor_date + timedelta(days=cfg["days"])
    else:
        date_to = add_months(dogovor_date, cfg["months"])
    return date_from, date_to


def format_amount_ru(n: int) -> str:
    words = num2words(n, lang="ru")
    return f"{n:,},00 {words} тенге 00 тиын"


dp = Dispatcher(storage=MemoryStorage())
bot = Bot(token=BOT_TOKEN)


def company_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label, callback_data=f"co:{key}")]
        for key, label in COMPANIES.items()
    ])


def period_keyboard() -> InlineKeyboardMarkup:
    keys = list(PERIODS.keys())
    rows = [
        [InlineKeyboardButton(text=PERIODS[k]["label"], callback_data=f"pe:{k}") for k in keys[i:i + 2]]
        for i in range(0, len(keys), 2)
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def sum_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Верно", callback_data="sum:yes")],
        [InlineKeyboardButton(text="✏️ Изменить", callback_data="sum:no")],
    ])


def dogovor_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📋 Стандартный ({STANDARD_DOGOVOR_NO})", callback_data="do:std")],
        [InlineKeyboardButton(text="✍️ Ввести вручную", callback_data="do:man")],
        [InlineKeyboardButton(text="⬜ Оставить пустым", callback_data="do:none")],
    ])


def iin_choice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇰🇿 Гражданин РК (укажу ИИН)", callback_data="iin:kz")],
        [InlineKeyboardButton(text="🌍 Иностранный гражданин (без ИИН)", callback_data="iin:foreign")],
    ])


def iin_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, верно", callback_data="ic:yes")],
        [InlineKeyboardButton(text="❌ Нет, введу вручную", callback_data="ic:no")],
    ])


def iin_not_found_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Повторить поиск", callback_data="nf:retry")],
        [InlineKeyboardButton(text="✏️ Изменить ИИН", callback_data="nf:change")],
        [InlineKeyboardButton(text="📝 Ввести ФИО вручную", callback_data="nf:fio")],
    ])


def klass_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(CLASS_OPTIONS), 5):
        rows.append([InlineKeyboardButton(text=c, callback_data=f"kl:{c}") for c in CLASS_OPTIONS[i:i + 5]])
    rows.append([InlineKeyboardButton(text="⬜ Оставить пустым", callback_data="kl:skip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def final_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить — создать документ", callback_data="fin:yes")],
        [InlineKeyboardButton(text="🔄 Начать заново", callback_data="fin:restart")],
    ])


def tos_short_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Согласен", callback_data="tos:yes")],
        [InlineKeyboardButton(text="📄 Подробнее", callback_data="tos:more")],
    ])


def tos_full_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Согласен — принимаю условия", callback_data="tos:yes")],
        [InlineKeyboardButton(text="❌ Не согласен", callback_data="tos:no")],
    ])


def tos_short_text() -> str:
    bonus_line = ""
    if WELCOME_BONUS > 0:
        bonus_line = (
            f"\n🎁 После принятия условий вы получите приветственный бонус "
            f"<b>+{fmt_money(WELCOME_BONUS)}</b> на счёт.\n"
        )
    return (
        "👋 <b>Добро пожаловать!</b>\n\n"
        "Это сервис для быстрого оформления страховых полисов.\n"
        + bonus_line +
        "\nНажмите <b>«✅ Согласен»</b> чтобы продолжить, "
        "или <b>«📄 Подробнее»</b> — чтобы прочитать условия использования."
    )


def people_count_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(n), callback_data=f"pc:{n}") for n in range(1, 6)],
    ])


def fmt_money(n: int) -> str:
    return f"{n:,} тг".replace(",", " ")


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


BTN_NEW_POLIS = "📄 Новый полис"
BTN_BALANCE = "💰 Баланс"
BTN_TOPUP = "💳 Пополнить"
BTN_HELP = "❓ Помощь"


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_NEW_POLIS)],
            [KeyboardButton(text=BTN_BALANCE), KeyboardButton(text=BTN_TOPUP)],
            [KeyboardButton(text=BTN_HELP)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


async def show_welcome(msg: Message, state: FSMContext):
    await state.clear()
    await ensure_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    if not await is_tos_accepted(msg.from_user.id):
        await msg.answer(tos_short_text(), parse_mode="HTML", reply_markup=tos_short_keyboard())
        return
    await _send_main_greeting(msg, msg.from_user.first_name, msg.from_user.id)


async def _send_main_greeting(message: Message, first_name: str | None, user_id: int):
    balance = await get_balance(user_id)
    polises_left = balance // POLIS_PRICE

    greeting = (
        f"Здравствуйте, <b>{h(first_name or 'клиент')}</b>!\n\n"
        f"💰 Ваш баланс: <b>{fmt_money(balance)}</b>\n"
        f"📄 1 полис = <b>{fmt_money(POLIS_PRICE)}</b> (хватит на {polises_left} полисов)\n\n"
    )
    if balance < POLIS_PRICE:
        greeting += "⚠️ Недостаточно средств на балансе. Используйте кнопку <b>💳 Пополнить</b> ниже.\n\n"
    greeting += "Выберите действие в меню или нажмите <b>Новый полис</b>."

    await message.answer(greeting, parse_mode="HTML", reply_markup=main_menu())


async def start_new_polis(msg: Message, state: FSMContext):
    await state.clear()
    await ensure_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    if not await is_tos_accepted(msg.from_user.id):
        await msg.answer(tos_short_text(), parse_mode="HTML", reply_markup=tos_short_keyboard())
        return
    if not is_admin(msg.from_user.id):
        balance = await get_balance(msg.from_user.id)
        if balance < POLIS_PRICE:
            await msg.answer(
                f"⚠️ Недостаточно средств на балансе.\n\n"
                f"Стоимость: <b>{fmt_money(POLIS_PRICE)}</b>\n"
                f"Ваш баланс: <b>{fmt_money(balance)}</b>\n\n"
                f"Для пополнения нажмите <b>💳 Пополнить</b>.",
                parse_mode="HTML",
                reply_markup=main_menu(),
            )
            return
    await msg.answer(
        "<b>Сколько застрахованных в полисе?</b>",
        parse_mode="HTML",
        reply_markup=people_count_keyboard(),
    )
    await state.set_state(Form.people_count)


DEFAULT_COMPANY = next(iter(COMPANIES))  # single-company mode: skip selection step


@dp.callback_query(Form.people_count, F.data.startswith("pc:"))
async def on_people_count(cb: CallbackQuery, state: FSMContext):
    n = int(cb.data.split(":", 1)[1])
    if not (1 <= n <= 5):
        await cb.answer("Ошибка", show_alert=True)
        return
    await state.update_data(
        people_count=n,
        persons=[],
        current_person=0,
        company=DEFAULT_COMPANY,
    )
    await cb.message.edit_text(f"Кол-во застрахованных: <b>{n}</b> ✅", parse_mode="HTML")
    await cb.message.answer(
        "<b>Срок действия полиса:</b>",
        parse_mode="HTML",
        reply_markup=period_keyboard(),
    )
    await state.set_state(Form.period)
    await cb.answer()


@dp.message(CommandStart())
async def start(msg: Message, state: FSMContext):
    await show_welcome(msg, state)


@dp.callback_query(F.data == "tos:more")
async def on_tos_more(cb: CallbackQuery):
    await cb.message.edit_text(
        TOS_TEXT,
        parse_mode="HTML",
        reply_markup=tos_full_keyboard(),
    )
    await cb.answer()


@dp.callback_query(F.data == "tos:yes")
async def on_tos_yes(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await ensure_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    await set_tos_accepted(cb.from_user.id)

    bonus_line = ""
    if WELCOME_BONUS > 0 and not await has_welcome_bonus(cb.from_user.id):
        await change_balance(cb.from_user.id, WELCOME_BONUS, "topup", meta="welcome bonus")
        bonus_line = (
            f"\n\n🎁 Приветственный бонус: <b>+{fmt_money(WELCOME_BONUS)}</b> "
            f"зачислен на ваш баланс."
        )

    await cb.message.edit_text(
        "✅ Пользовательское соглашение принято." + bonus_line,
        parse_mode="HTML",
    )
    await _send_main_greeting(cb.message, cb.from_user.first_name, cb.from_user.id)
    await cb.answer()


@dp.callback_query(F.data == "tos:no")
async def on_tos_no(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "Без принятия пользовательского соглашения сервис недоступен.\n"
        "Если передумаете — нажмите /start.",
        parse_mode="HTML",
    )
    await cb.answer()


@dp.message(F.text == BTN_NEW_POLIS)
async def on_btn_new_polis(msg: Message, state: FSMContext):
    await start_new_polis(msg, state)


@dp.message(F.text == BTN_BALANCE)
async def on_btn_balance(msg: Message, state: FSMContext):
    await state.clear()
    await cmd_balance(msg)


@dp.message(F.text == BTN_TOPUP)
async def on_btn_topup(msg: Message, state: FSMContext):
    await state.clear()
    await cmd_topup(msg)


@dp.message(F.text == BTN_HELP)
async def on_btn_help(msg: Message, state: FSMContext):
    await state.clear()
    await cmd_help(msg)


@dp.message(Command("cancel"))
async def cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Отменено. Чтобы начать заново — /start.")


@dp.message(Command("balance"))
async def cmd_balance(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name, WELCOME_BONUS)
    balance = await get_balance(msg.from_user.id)
    txs = await list_recent_transactions(msg.from_user.id, 5)

    text = f"💰 Ваш баланс: <b>{fmt_money(balance)}</b>\n"
    if txs:
        text += "\n<b>Последние операции:</b>\n"
        for amount, ttype, meta, created in txs:
            sign = "+" if amount > 0 else ""
            label = {"topup": "Пополнение", "polis": "Полис", "refund": "Возврат"}.get(ttype, ttype)
            date_str = str(created)[:16]
            text += f"  {date_str} — {label}: <b>{sign}{fmt_money(amount)}</b>\n"
    else:
        text += "\nОпераций пока нет."
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu())


@dp.message(Command("topup"))
async def cmd_topup(msg: Message):
    info = KASPI_CARD_INFO or "Свяжитесь с администратором."
    text = (
        "<b>Пополнение баланса:</b>\n\n"
        f"{h(info)}\n\n"
        f"Ваш ID: <code>{msg.from_user.id}</code>\n"
        f"Укажите этот ID при отправке чека."
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu())


@dp.message(Command("myid"))
async def cmd_myid(msg: Message):
    await msg.answer(
        f"Ваш Telegram ID: <code>{msg.from_user.id}</code>",
        parse_mode="HTML",
        reply_markup=main_menu(),
    )


@dp.message(Command("help"))
async def cmd_help(msg: Message):
    text = (
        "<b>Команды:</b>\n"
        "📄 Новый полис — создать новый полис\n"
        "💰 Баланс — проверить баланс\n"
        "💳 Пополнить — как пополнить баланс\n"
        "❓ Помощь — это сообщение\n\n"
        "/cancel — отменить процесс\n"
        "/myid — мой ID\n"
    )
    if is_admin(msg.from_user.id):
        text += (
            "\n<b>Админ-команды:</b>\n"
            "<code>/add_balance USER_ID СУММА</code> — пополнить баланс пользователя (пример: <code>/add_balance 123456 10000</code>)\n"
            "<code>/users</code> — список пользователей\n"
            "<code>/audit</code> — последние 20 созданных полисов\n"
            "<code>/log TRACE_ID</code> — детальный отчёт по ID полиса\n"
        )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu())


@dp.message(Command("add_balance"))
async def cmd_add_balance(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    parts = (msg.text or "").split()
    if len(parts) != 3 or not parts[1].lstrip("-").isdigit() or not parts[2].lstrip("-").isdigit():
        await msg.answer("Использование: <code>/add_balance USER_ID СУММА</code>", parse_mode="HTML")
        return
    target_id = int(parts[1])
    amount = int(parts[2])
    new_balance = await change_balance(target_id, amount, "topup", meta=f"by admin {msg.from_user.id}")
    if new_balance is None:
        await msg.answer("Ошибка: баланс уйдёт в минус.")
        return
    await msg.answer(
        f"✅ Пользователь <code>{target_id}</code>: {'+' if amount>=0 else ''}{fmt_money(amount)}\n"
        f"Новый баланс: <b>{fmt_money(new_balance)}</b>",
        parse_mode="HTML",
    )
    try:
        await bot.send_message(
            target_id,
            f"💰 Ваш баланс пополнен: <b>+{fmt_money(amount)}</b>\n"
            f"Новый баланс: <b>{fmt_money(new_balance)}</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass


@dp.message(Command("log"))
async def cmd_log(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    parts = (msg.text or "").split()
    if len(parts) != 2:
        await msg.answer("Использование: <code>/log TRACE_ID</code>", parse_mode="HTML")
        return
    trace_id = parts[1].upper()
    log = await get_polis_by_trace_id(trace_id)
    if not log:
        await msg.answer(f"Полис <code>{h(trace_id)}</code> не найден.", parse_mode="HTML")
        return
    persons = json.loads(log.get("persons_json") or "[]")
    persons_lines = []
    for i, p in enumerate(persons, 1):
        persons_lines.append(
            f"  {i}. {h(p.get('fio', ''))} | "
            f"ИИН: <code>{h(p.get('iin') or '(нет)')}</code> | "
            f"Кл: {h(p.get('klass') or '-')}"
        )
    persons_text = "\n".join(persons_lines) or "  (нет)"

    user_label = f"<code>{log['telegram_id']}</code>"
    if log.get('username'):
        user_label += f" (@{h(log['username'])})"
    if log.get('first_name'):
        user_label += f" — {h(log['first_name'])}"

    text = (
        f"<b>📋 Полис {h(log['trace_id'])}</b>\n\n"
        f"<b>Создал:</b> {user_label}\n"
        f"<b>Дата:</b> {log['created_at']}\n\n"
        f"<b>Договор №:</b> <code>{h(log.get('dogovor_no') or '(пусто)')}</code>\n"
        f"<b>Сумма:</b> {h(log.get('amount') or '')}\n"
        f"<b>Период:</b> {h(log.get('date_from') or '')} — {h(log.get('date_to') or '')}\n\n"
        f"<b>Авто:</b> {h(log.get('car_brand') or '')} | "
        f"<code>{h(log.get('car_number') or '')}</code> | "
        f"VIN: <code>{h(log.get('vin') or '')}</code>\n\n"
        f"<b>Застрахованные:</b>\n{persons_text}"
    )
    await msg.answer(text, parse_mode="HTML")


@dp.message(Command("audit"))
async def cmd_audit(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    rows = await list_recent_polises(20)
    if not rows:
        await msg.answer("Журнал пуст.")
        return
    lines = ["<b>Последние 20 полисов:</b>\n"]
    for r in rows:
        date_str = str(r['created_at'])[:16]
        user_label = f"<code>{r['telegram_id']}</code>"
        if r.get('username'):
            user_label += f" @{h(r['username'])}"
        elif r.get('first_name'):
            user_label += f" {h(r['first_name'])}"
        lines.append(
            f"<code>{r['trace_id']}</code> | {date_str} | {user_label} | "
            f"{h(r.get('car_number') or '-')}"
        )
    lines.append("\nДеталь: <code>/log TRACE_ID</code>")
    await msg.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("users"))
async def cmd_users(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    users = await list_users(50)
    if not users:
        await msg.answer("Пользователей нет.")
        return
    lines = ["<b>Пользователи (по балансу):</b>\n"]
    for tg_id, username, first_name, balance in users:
        name = f"@{username}" if username else (first_name or "?")
        lines.append(f"<code>{tg_id}</code> {h(name)} — <b>{fmt_money(balance)}</b>")
    await msg.answer("\n".join(lines), parse_mode="HTML")


@dp.callback_query(Form.company, F.data.startswith("co:"))
async def on_company(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":", 1)[1]
    if key not in COMPANIES:
        await cb.answer("Ошибка", show_alert=True)
        return
    await state.update_data(company=key)
    await cb.message.edit_text(f"Компания: <b>{h(COMPANIES[key])}</b> ✅", parse_mode="HTML")
    await cb.message.answer("<b>Срок действия полиса:</b>", parse_mode="HTML", reply_markup=period_keyboard())
    await state.set_state(Form.period)
    await cb.answer()


@dp.callback_query(Form.period, F.data.startswith("pe:"))
async def on_period(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":", 1)[1]
    if key not in PERIODS:
        await cb.answer("Ошибка", show_alert=True)
        return
    await state.update_data(period=key)
    await cb.message.edit_text(f"Срок: <b>{h(PERIODS[key]['label'])}</b> ✅", parse_mode="HTML")
    await cb.message.answer(
        "<b>Введите сумму страховки в тенге.</b>\n"
        "Например: <code>15000</code>",
        parse_mode="HTML",
    )
    await state.set_state(Form.sum_input)
    await cb.answer()


@dp.message(Form.sum_input, F.text)
async def on_sum_input(msg: Message, state: FSMContext):
    digits = re.sub(r"\D", "", msg.text)
    if not digits:
        await msg.answer("Введите число:")
        return
    n = int(digits)
    formatted = format_amount_ru(n)
    await state.update_data(amount=formatted)
    await msg.answer(
        f"Сумма будет указана так:\n\n<code>{h(formatted)}</code>\n\nВерно?",
        parse_mode="HTML",
        reply_markup=sum_confirm_keyboard(),
    )
    await state.set_state(Form.sum_confirm)


@dp.callback_query(Form.sum_confirm, F.data == "sum:no")
async def on_sum_no(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Введите сумму заново:")
    await state.set_state(Form.sum_input)
    await cb.answer()


@dp.callback_query(Form.sum_confirm, F.data == "sum:yes")
async def on_sum_yes(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("<b>Номер договора</b> — как заполнить?", parse_mode="HTML", reply_markup=dogovor_keyboard())
    await state.set_state(Form.dogovor_choice)
    await cb.answer()


@dp.callback_query(Form.dogovor_choice, F.data == "do:std")
async def on_dogovor_std(cb: CallbackQuery, state: FSMContext):
    await state.update_data(dogovor_no=STANDARD_DOGOVOR_NO)
    await cb.message.edit_text(f"Номер договора: <code>{STANDARD_DOGOVOR_NO}</code> ✅", parse_mode="HTML")
    await _ask_iin(cb.message, state)
    await cb.answer()


@dp.callback_query(Form.dogovor_choice, F.data == "do:none")
async def on_dogovor_none(cb: CallbackQuery, state: FSMContext):
    await state.update_data(dogovor_no="")
    await cb.message.edit_text("Номер договора: <i>пусто</i> ✅", parse_mode="HTML")
    await _ask_iin(cb.message, state)
    await cb.answer()


@dp.callback_query(Form.dogovor_choice, F.data == "do:man")
async def on_dogovor_manual(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("Введите номер договора:")
    await state.set_state(Form.dogovor_manual)
    await cb.answer()


@dp.message(Form.dogovor_manual, F.text)
async def on_dogovor_manual_input(msg: Message, state: FSMContext):
    await state.update_data(dogovor_no=msg.text.strip().upper())
    await _ask_iin(msg, state)


def _person_label(idx: int, total: int) -> str:
    return "Страхователь (1-й застрахованный)" if idx == 0 else f"Застрахованный №{idx + 1}"


def _ensure_person(persons: list, idx: int) -> list:
    persons = list(persons)
    while len(persons) <= idx:
        persons.append({})
    return persons


async def _ask_iin(msg: Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get("current_person", 0)
    n = data.get("people_count", 1)
    await msg.answer(
        f"<b>{_person_label(idx, n)} ({idx + 1}/{n}):</b>",
        parse_mode="HTML",
        reply_markup=iin_choice_keyboard(),
    )
    await state.set_state(Form.p_iin_choice)


@dp.callback_query(Form.p_iin_choice, F.data == "iin:kz")
async def on_p_iin_kz(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    idx = data.get("current_person", 0)
    n = data.get("people_count", 1)
    await cb.message.edit_text(
        f"{_person_label(idx, n)}: <b>Гражданин РК</b> ✅", parse_mode="HTML"
    )
    await cb.message.answer("<b>Введите ИИН</b>:", parse_mode="HTML")
    await state.set_state(Form.p_iin_input)
    await cb.answer()


@dp.callback_query(Form.p_iin_choice, F.data == "iin:foreign")
async def on_p_iin_foreign(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    idx = data.get("current_person", 0)
    n = data.get("people_count", 1)
    persons = _ensure_person(data.get("persons", []), idx)
    persons[idx] = {"fio": "", "iin": "", "klass": "", "foreign": True}
    await state.update_data(persons=persons)
    await cb.message.edit_text(
        f"{_person_label(idx, n)}: <b>Иностранный гражданин</b> ✅", parse_mode="HTML"
    )
    await cb.message.answer("<b>ФИО</b>:", parse_mode="HTML")
    await state.set_state(Form.p_fio)
    await cb.answer()


async def _do_iin_search(msg: Message, state: FSMContext, iin: str):
    data = await state.get_data()
    idx = data.get("current_person", 0)
    persons = _ensure_person(data.get("persons", []), idx)
    persons[idx] = {**persons[idx], "iin": iin, "foreign": False}
    await state.update_data(persons=persons)

    wait_msg = await msg.answer("🔎 Идёт поиск…")
    result = await fetch_bonus_malus(iin)
    try:
        await wait_msg.delete()
    except Exception:
        pass
    if result:
        await state.update_data(nsk_fio=result["full_name"], nsk_class=result["class"])
        await msg.answer(
            f"Найдено:\n\n"
            f"<b>ФИО:</b> {h(result['full_name'])}\n"
            f"<b>Класс:</b> {h(result['class'])}\n\n"
            f"Верно?",
            parse_mode="HTML",
            reply_markup=iin_confirm_keyboard(),
        )
        await state.set_state(Form.p_iin_confirm)
    else:
        await msg.answer(
            f"❌ По ИИН <code>{h(iin)}</code> ничего не найдено.\n\nКак продолжим?",
            parse_mode="HTML",
            reply_markup=iin_not_found_keyboard(),
        )
        await state.set_state(Form.p_iin_not_found)


@dp.message(Form.p_iin_input, F.text)
async def on_p_iin_input(msg: Message, state: FSMContext):
    iin = msg.text.strip()
    await _do_iin_search(msg, state, iin)


@dp.callback_query(Form.p_iin_not_found, F.data == "nf:retry")
async def on_iin_nf_retry(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    idx = data.get("current_person", 0)
    persons = data.get("persons", [])
    iin = persons[idx].get("iin", "") if idx < len(persons) else ""
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer()
    if not iin:
        await cb.message.answer("<b>Введите ИИН</b>:", parse_mode="HTML")
        await state.set_state(Form.p_iin_input)
        return
    await _do_iin_search(cb.message, state, iin)


@dp.callback_query(Form.p_iin_not_found, F.data == "nf:change")
async def on_iin_nf_change(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("<b>Введите ИИН заново</b>:", parse_mode="HTML")
    await state.set_state(Form.p_iin_input)
    await cb.answer()


@dp.callback_query(Form.p_iin_not_found, F.data == "nf:fio")
async def on_iin_nf_fio(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("<b>Введите ФИО вручную</b>:", parse_mode="HTML")
    await state.set_state(Form.p_fio)
    await cb.answer()


@dp.callback_query(Form.p_iin_confirm, F.data == "ic:yes")
async def on_p_iin_confirm_yes(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    idx = data.get("current_person", 0)
    persons = _ensure_person(data.get("persons", []), idx)
    persons[idx] = {
        **persons[idx],
        "fio": data.get("nsk_fio", "").upper(),
        "klass": data.get("nsk_class", "").upper(),
    }
    await state.update_data(persons=persons)
    await cb.message.edit_reply_markup(reply_markup=None)
    await _next_person_or_car(cb.message, state)
    await cb.answer()


@dp.callback_query(Form.p_iin_confirm, F.data == "ic:no")
async def on_p_iin_confirm_no(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("<b>Введите ФИО вручную</b>:", parse_mode="HTML")
    await state.set_state(Form.p_fio)
    await cb.answer()


@dp.message(Form.p_fio, F.text)
async def on_p_fio(msg: Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get("current_person", 0)
    persons = _ensure_person(data.get("persons", []), idx)
    persons[idx] = {**persons[idx], "fio": msg.text.strip().upper()}
    await state.update_data(persons=persons)
    if persons[idx].get("foreign"):
        await _next_person_or_car(msg, state)
    else:
        await msg.answer("<b>Класс</b>:", parse_mode="HTML", reply_markup=klass_keyboard())
        await state.set_state(Form.p_klass)


@dp.callback_query(Form.p_klass, F.data.startswith("kl:"))
async def on_p_klass_choice(cb: CallbackQuery, state: FSMContext):
    val = cb.data.split(":", 1)[1]
    klass = "" if val == "skip" else val
    data = await state.get_data()
    idx = data.get("current_person", 0)
    persons = _ensure_person(data.get("persons", []), idx)
    persons[idx] = {**persons[idx], "klass": klass}
    await state.update_data(persons=persons)
    display = klass if klass else "пусто"
    await cb.message.edit_text(f"Класс: <b>{h(display)}</b> ✅", parse_mode="HTML")
    await _next_person_or_car(cb.message, state)
    await cb.answer()


@dp.message(Form.p_klass, F.text)
async def on_p_klass_text(msg: Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get("current_person", 0)
    persons = _ensure_person(data.get("persons", []), idx)
    persons[idx] = {**persons[idx], "klass": msg.text.strip().upper()}
    await state.update_data(persons=persons)
    await _next_person_or_car(msg, state)


async def _next_person_or_car(msg: Message, state: FSMContext):
    data = await state.get_data()
    idx = data.get("current_person", 0)
    n = data.get("people_count", 1)
    if idx + 1 < n:
        await state.update_data(current_person=idx + 1)
        await _ask_iin(msg, state)
    else:
        await _ask_car_brand(msg, state)


async def _ask_car_brand(msg: Message, state: FSMContext):
    await msg.answer("<b>Марка и модель автомобиля</b>:", parse_mode="HTML")
    await state.set_state(Form.car_brand)


@dp.message(Form.car_brand, F.text)
async def on_brand(msg: Message, state: FSMContext):
    await state.update_data(car_brand=msg.text.strip().upper())
    await msg.answer("<b>Гос. номер</b>:", parse_mode="HTML")
    await state.set_state(Form.car_number)


@dp.message(Form.car_number, F.text)
async def on_car_number(msg: Message, state: FSMContext):
    await state.update_data(car_number=msg.text.strip().upper())
    await msg.answer("<b>VIN</b>:", parse_mode="HTML")
    await state.set_state(Form.vin)


@dp.message(Form.vin, F.text)
async def on_vin(msg: Message, state: FSMContext):
    await state.update_data(vin=msg.text.strip().upper())
    await _show_final_summary(msg, state)


async def _show_final_summary(msg: Message, state: FSMContext):
    data = await state.get_data()
    dogovor_date = date.today()
    date_from, date_to = compute_period(data["period"], dogovor_date)
    await state.update_data(
        dogovor_date=fmt_date(dogovor_date),
        date_from=fmt_date(date_from),
        date_to=fmt_date(date_to),
    )

    persons = data.get("persons", [])
    persons_lines = []
    for i, p in enumerate(persons):
        persons_lines.append(
            f"  <b>Застрахованный {i + 1}:</b> {h(p.get('fio', ''))}\n"
            f"     ИИН: <code>{h(p.get('iin') or '(пусто)')}</code> | "
            f"Класс: {h(p.get('klass') or '(пусто)')}"
        )
    persons_text = "\n".join(persons_lines)

    dogovor_display = data.get("dogovor_no") or "(пусто)"
    summary = (
        "<b>Все данные:</b>\n\n"
        f"<b>Компания:</b> {h(COMPANIES[data['company']])}\n"
        f"<b>Срок:</b> {h(PERIODS[data['period']]['label'])}\n"
        f"<b>Сумма:</b> {h(data['amount'])}\n"
        f"<b>Договор №:</b> <code>{h(dogovor_display)}</code>\n\n"
        f"<b>Застрахованные ({data.get('people_count', 1)}):</b>\n{persons_text}\n\n"
        f"<b>Авто:</b> {h(data['car_brand'])}\n"
        f"<b>Гос. номер:</b> <code>{h(data['car_number'])}</code>\n"
        f"<b>VIN:</b> <code>{h(data['vin'])}</code>\n"
        f"<b>Дата договора:</b> {fmt_date(dogovor_date)}\n"
        f"<b>Период действия:</b> {fmt_date(date_from)} — {fmt_date(date_to)}"
    )
    await msg.answer(summary, parse_mode="HTML", reply_markup=final_confirm_keyboard())
    await state.set_state(Form.final_confirm)


@dp.callback_query(Form.final_confirm, F.data == "fin:restart")
async def on_restart(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("Чтобы начать заново — /start.")
    await cb.answer()


@dp.callback_query(Form.final_confirm, F.data == "fin:yes")
async def on_final_yes(cb: CallbackQuery, state: FSMContext):
    user_id = cb.from_user.id
    admin = is_admin(user_id)
    new_balance = None
    if not admin:
        new_balance = await change_balance(user_id, -POLIS_PRICE, "polis", meta="reserve")
        if new_balance is None:
            current = await get_balance(user_id)
            await cb.answer()
            await cb.message.edit_reply_markup(reply_markup=None)
            await cb.message.answer(
                f"⚠️ Недостаточно средств на балансе.\n\n"
                f"Стоимость: <b>{fmt_money(POLIS_PRICE)}</b>\n"
                f"Ваш баланс: <b>{fmt_money(current)}</b>\n\n"
                f"Пополнить — /topup",
                parse_mode="HTML",
            )
            await state.clear()
            return

    await cb.answer("Документ создаётся…")
    await cb.message.edit_reply_markup(reply_markup=None)
    if admin:
        wait_msg = await cb.message.answer("⏳ Генерация PDF… (бесплатно, админ)")
    else:
        wait_msg = await cb.message.answer(
            f"⏳ Генерация PDF…\n"
            f"Списано с баланса: <b>−{fmt_money(POLIS_PRICE)}</b> (баланс: {fmt_money(new_balance)})",
            parse_mode="HTML",
        )

    data = await state.get_data()
    safe_no = re.sub(r"[^A-Za-z0-9_-]", "_", data.get("dogovor_no") or "empty")
    pdf_path = GENERATED_DIR / f"polis_{user_id}_{safe_no}.pdf"

    persons = data.get("persons", [])
    first_fio = (persons[0].get("fio", "") if persons else "")
    fio_parts = first_fio.split()
    name = fio_parts[1] if len(fio_parts) >= 2 else (fio_parts[0] if fio_parts else "")
    filename_display = f"страховка {name}.pdf" if name else "страховка.pdf"

    try:
        await generate_pdf(data, pdf_path)
    except PdfError as e:
        if not admin:
            refunded = await change_balance(user_id, POLIS_PRICE, "refund", meta=str(e))
            await wait_msg.edit_text(
                f"❌ Ошибка: {h(str(e))}\n\n"
                f"Баланс возвращён: <b>+{fmt_money(POLIS_PRICE)}</b> (баланс: {fmt_money(refunded or 0)})",
                parse_mode="HTML",
            )
        else:
            await wait_msg.edit_text(f"❌ Ошибка: {h(str(e))}", parse_mode="HTML")
        await state.clear()
        return
    except Exception as e:
        logging.exception("generate_pdf failed")
        if not admin:
            refunded = await change_balance(user_id, POLIS_PRICE, "refund", meta=f"unexpected: {e}")
            await wait_msg.edit_text(
                f"❌ Неизвестная ошибка: {h(str(e))}\n\n"
                f"Баланс возвращён: <b>+{fmt_money(POLIS_PRICE)}</b> (баланс: {fmt_money(refunded or 0)})",
                parse_mode="HTML",
            )
        else:
            await wait_msg.edit_text(f"❌ Неизвестная ошибка: {h(str(e))}", parse_mode="HTML")
        await state.clear()
        return

    try:
        await wait_msg.delete()
    except Exception:
        pass

    trace_id = gen_trace_id()
    try:
        await log_polis(
            trace_id, user_id, cb.from_user.username, cb.from_user.first_name, data
        )
    except Exception:
        logging.exception("log_polis failed")

    trace_line = f"\n🔖 ID документа: <code>{trace_id}</code>"
    if admin:
        caption = "✅ Полис готов!" + trace_line + DISCLAIMER
    else:
        final_balance = await get_balance(user_id)
        caption = (
            f"✅ Полис готов!\nОстаток баланса: <b>{fmt_money(final_balance)}</b>"
            + trace_line + DISCLAIMER
        )
    await cb.message.answer_document(
        FSInputFile(pdf_path, filename=filename_display),
        caption=caption,
        parse_mode="HTML",
    )
    await state.clear()
    await cb.message.answer("Для нового полиса — /start.")


async def setup_bot_commands():
    common = [
        BotCommand(command="start", description="Старт / новый полис"),
        BotCommand(command="balance", description="Проверить баланс"),
        BotCommand(command="topup", description="Пополнить баланс"),
        BotCommand(command="myid", description="Мой ID"),
        BotCommand(command="help", description="Помощь"),
        BotCommand(command="cancel", description="Отменить процесс"),
    ]
    await bot.set_my_commands(common, scope=BotCommandScopeDefault())

    admin = common + [
        BotCommand(command="add_balance", description="Пополнить баланс пользователя"),
        BotCommand(command="users", description="Список пользователей"),
        BotCommand(command="audit", description="Журнал последних полисов"),
        BotCommand(command="log", description="Детали полиса по ID"),
    ]
    for admin_id in ADMIN_IDS:
        try:
            await bot.set_my_commands(admin, scope=BotCommandScopeChat(chat_id=admin_id))
        except Exception as e:
            logging.warning(f"set_my_commands for admin {admin_id} failed: {e}")


async def main():
    await init_db()
    await setup_bot_commands()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
