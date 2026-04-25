import asyncio
import calendar
import logging
import os
import re
from datetime import date, timedelta
from html import escape as h
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
POLIS_PRICE = int(os.environ.get("POLIS_PRICE", "2000"))
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
    init_db,
    list_recent_transactions,
    list_users,
)
from nsk import fetch_bonus_malus
from pdf import PdfError, generate_pdf

GENERATED_DIR = Path(__file__).parent / "generated"
GENERATED_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO)

COMPANIES = {"nomad": "🏢 Номад Транзит"}

PERIODS = {
    "10d": {"label": "10 күн", "days": 10},
    "15d": {"label": "15 күн", "days": 15},
    "1m":  {"label": "1 ай",   "months": 1},
    "3m":  {"label": "3 ай",   "months": 3},
    "6m":  {"label": "6 ай",   "months": 6},
    "1y":  {"label": "1 жыл",  "months": 12},
}

CLASS_OPTIONS = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "M", "M1", "M2", "A"]

STANDARD_DOGOVOR_NO = "0656T160437N"


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
        [InlineKeyboardButton(text="✅ Дұрыс", callback_data="sum:yes")],
        [InlineKeyboardButton(text="✏️ Қайта жазу", callback_data="sum:no")],
    ])


def dogovor_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📋 Стандарт ({STANDARD_DOGOVOR_NO})", callback_data="do:std")],
        [InlineKeyboardButton(text="✍️ Қолмен жазу", callback_data="do:man")],
        [InlineKeyboardButton(text="⬜ Бос қалдыру", callback_data="do:none")],
    ])


def iin_choice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇰🇿 ҚР азаматы (ИИН жазамын)", callback_data="iin:kz")],
        [InlineKeyboardButton(text="🌍 Шетел азаматы (ИИН жоқ)", callback_data="iin:foreign")],
    ])


def iin_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Иә, дұрыс", callback_data="ic:yes")],
        [InlineKeyboardButton(text="❌ Жоқ, қолмен жазамын", callback_data="ic:no")],
    ])


def iin_not_found_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Қайта іздеу", callback_data="nf:retry")],
        [InlineKeyboardButton(text="✏️ ИИН өзгерту", callback_data="nf:change")],
        [InlineKeyboardButton(text="📝 ФИО өзім жазамын", callback_data="nf:fio")],
    ])


def klass_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(CLASS_OPTIONS), 5):
        rows.append([InlineKeyboardButton(text=c, callback_data=f"kl:{c}") for c in CLASS_OPTIONS[i:i + 5]])
    rows.append([InlineKeyboardButton(text="⬜ Бос қалдыру", callback_data="kl:skip")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def final_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Растау — құжат жасау", callback_data="fin:yes")],
        [InlineKeyboardButton(text="🔄 Қайта бастау", callback_data="fin:restart")],
    ])


def people_count_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(n), callback_data=f"pc:{n}") for n in range(1, 6)],
    ])


def fmt_money(n: int) -> str:
    return f"{n:,} тг".replace(",", " ")


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


BTN_NEW_POLIS = "📄 Жаңа полис"
BTN_BALANCE = "💰 Баланс"
BTN_TOPUP = "💳 Толтыру"
BTN_HELP = "❓ Көмек"


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
    balance = await get_balance(msg.from_user.id)
    polises_left = balance // POLIS_PRICE

    greeting = (
        f"Сәлем, <b>{h(msg.from_user.first_name or 'достым')}</b>!\n\n"
        f"💰 Балансыңыз: <b>{fmt_money(balance)}</b>\n"
        f"📄 1 полис = <b>{fmt_money(POLIS_PRICE)}</b> ({polises_left} полис жасауға жетеді)\n\n"
    )
    if balance < POLIS_PRICE:
        greeting += "⚠️ Балансыңыз жеткіліксіз. Төменгі батырма арқылы /topup — қалай толтыруға болады.\n\n"
    greeting += "Төменгі батырмалардан таңдаңыз немесе <b>Жаңа полис</b> басыңыз."

    await msg.answer(greeting, parse_mode="HTML", reply_markup=main_menu())


async def start_new_polis(msg: Message, state: FSMContext):
    await state.clear()
    await ensure_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    if not is_admin(msg.from_user.id):
        balance = await get_balance(msg.from_user.id)
        if balance < POLIS_PRICE:
            await msg.answer(
                f"⚠️ Балансыңыз жеткіліксіз.\n\n"
                f"Қажет: <b>{fmt_money(POLIS_PRICE)}</b>\n"
                f"Балансыңыз: <b>{fmt_money(balance)}</b>\n\n"
                f"Толтыру үшін <b>💳 Толтыру</b> батырмасын басыңыз.",
                parse_mode="HTML",
                reply_markup=main_menu(),
            )
            return
    await msg.answer(
        "<b>Полисте неше адам?</b>",
        parse_mode="HTML",
        reply_markup=people_count_keyboard(),
    )
    await state.set_state(Form.people_count)


@dp.callback_query(Form.people_count, F.data.startswith("pc:"))
async def on_people_count(cb: CallbackQuery, state: FSMContext):
    n = int(cb.data.split(":", 1)[1])
    if not (1 <= n <= 5):
        await cb.answer("Қате", show_alert=True)
        return
    await state.update_data(people_count=n, persons=[], current_person=0)
    await cb.message.edit_text(f"Адам саны: <b>{n}</b> ✅", parse_mode="HTML")
    await cb.message.answer(
        "<b>Қай компанияның полисін шығарамыз?</b>",
        parse_mode="HTML",
        reply_markup=company_keyboard(),
    )
    await state.set_state(Form.company)
    await cb.answer()


@dp.message(CommandStart())
async def start(msg: Message, state: FSMContext):
    await show_welcome(msg, state)


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
    await msg.answer("Тоқтатылды. Қайта бастау үшін /start.")


@dp.message(Command("balance"))
async def cmd_balance(msg: Message):
    await ensure_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    balance = await get_balance(msg.from_user.id)
    txs = await list_recent_transactions(msg.from_user.id, 5)

    text = f"💰 Балансыңыз: <b>{fmt_money(balance)}</b>\n"
    if txs:
        text += "\n<b>Соңғы операциялар:</b>\n"
        for amount, ttype, meta, created in txs:
            sign = "+" if amount > 0 else ""
            label = {"topup": "Толтыру", "polis": "Полис", "refund": "Қайтару"}.get(ttype, ttype)
            date_str = str(created)[:16]
            text += f"  {date_str} — {label}: <b>{sign}{fmt_money(amount)}</b>\n"
    else:
        text += "\nОперация әлі жоқ."
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu())


@dp.message(Command("topup"))
async def cmd_topup(msg: Message):
    info = KASPI_CARD_INFO or "Админмен байланысыңыз."
    text = (
        "<b>Баланс толтыру:</b>\n\n"
        f"{h(info)}\n\n"
        f"Сіздің ID: <code>{msg.from_user.id}</code>\n"
        f"Бұл ID-ді чек жіберген кезде бірге айтыңыз."
    )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu())


@dp.message(Command("myid"))
async def cmd_myid(msg: Message):
    await msg.answer(
        f"Сіздің Telegram ID: <code>{msg.from_user.id}</code>",
        parse_mode="HTML",
        reply_markup=main_menu(),
    )


@dp.message(Command("help"))
async def cmd_help(msg: Message):
    text = (
        "<b>Командалар:</b>\n"
        "📄 Жаңа полис — жаңа полис жасау\n"
        "💰 Баланс — балансты тексеру\n"
        "💳 Толтыру — қалай толтыру\n"
        "❓ Көмек — осы хабар\n\n"
        "/cancel — процесті тоқтату\n"
        "/myid — менің ID\n"
    )
    if is_admin(msg.from_user.id):
        text += (
            "\n<b>Админ командалары:</b>\n"
            "<code>/add_balance USER_ID SUMMA</code> — қосу (мыс: <code>/add_balance 123456 10000</code>)\n"
            "<code>/users</code> — пайдаланушылар тізімі\n"
        )
    await msg.answer(text, parse_mode="HTML", reply_markup=main_menu())


@dp.message(Command("add_balance"))
async def cmd_add_balance(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    parts = (msg.text or "").split()
    if len(parts) != 3 or not parts[1].lstrip("-").isdigit() or not parts[2].lstrip("-").isdigit():
        await msg.answer("Қолдану: <code>/add_balance USER_ID SUMMA</code>", parse_mode="HTML")
        return
    target_id = int(parts[1])
    amount = int(parts[2])
    new_balance = await change_balance(target_id, amount, "topup", meta=f"by admin {msg.from_user.id}")
    if new_balance is None:
        await msg.answer("Қате: баланс теріс болып кетеді.")
        return
    await msg.answer(
        f"✅ User <code>{target_id}</code>: {'+' if amount>=0 else ''}{fmt_money(amount)}\n"
        f"Жаңа баланс: <b>{fmt_money(new_balance)}</b>",
        parse_mode="HTML",
    )
    try:
        await bot.send_message(
            target_id,
            f"💰 Балансыңыз толтырылды: <b>+{fmt_money(amount)}</b>\n"
            f"Жаңа баланс: <b>{fmt_money(new_balance)}</b>",
            parse_mode="HTML",
        )
    except Exception:
        pass


@dp.message(Command("users"))
async def cmd_users(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    users = await list_users(50)
    if not users:
        await msg.answer("Пайдаланушы жоқ.")
        return
    lines = ["<b>Пайдаланушылар (баланс бойынша):</b>\n"]
    for tg_id, username, first_name, balance in users:
        name = f"@{username}" if username else (first_name or "?")
        lines.append(f"<code>{tg_id}</code> {h(name)} — <b>{fmt_money(balance)}</b>")
    await msg.answer("\n".join(lines), parse_mode="HTML")


@dp.callback_query(Form.company, F.data.startswith("co:"))
async def on_company(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":", 1)[1]
    if key not in COMPANIES:
        await cb.answer("Қате", show_alert=True)
        return
    await state.update_data(company=key)
    await cb.message.edit_text(f"Компания: <b>{h(COMPANIES[key])}</b> ✅", parse_mode="HTML")
    await cb.message.answer("<b>Полистің қолдану мерзімі:</b>", parse_mode="HTML", reply_markup=period_keyboard())
    await state.set_state(Form.period)
    await cb.answer()


@dp.callback_query(Form.period, F.data.startswith("pe:"))
async def on_period(cb: CallbackQuery, state: FSMContext):
    key = cb.data.split(":", 1)[1]
    if key not in PERIODS:
        await cb.answer("Қате", show_alert=True)
        return
    await state.update_data(period=key)
    await cb.message.edit_text(f"Период: <b>{h(PERIODS[key]['label'])}</b> ✅", parse_mode="HTML")
    await cb.message.answer(
        "<b>Сақтандыру сомасы</b> қанша теңге? Санмен жазыңыз:",
        parse_mode="HTML",
    )
    await state.set_state(Form.sum_input)
    await cb.answer()


@dp.message(Form.sum_input, F.text)
async def on_sum_input(msg: Message, state: FSMContext):
    digits = re.sub(r"\D", "", msg.text)
    if not digits:
        await msg.answer("Сан жазыңыз:")
        return
    n = int(digits)
    formatted = format_amount_ru(n)
    await state.update_data(amount=formatted)
    await msg.answer(
        f"Сома былай жазылады:\n\n<code>{h(formatted)}</code>\n\nДұрыс па?",
        parse_mode="HTML",
        reply_markup=sum_confirm_keyboard(),
    )
    await state.set_state(Form.sum_confirm)


@dp.callback_query(Form.sum_confirm, F.data == "sum:no")
async def on_sum_no(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Соманы қайта жазыңыз:")
    await state.set_state(Form.sum_input)
    await cb.answer()


@dp.callback_query(Form.sum_confirm, F.data == "sum:yes")
async def on_sum_yes(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("<b>Номер договора</b> қалай қоямыз?", parse_mode="HTML", reply_markup=dogovor_keyboard())
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
    await cb.message.edit_text("Номер договора: <i>бос</i> ✅", parse_mode="HTML")
    await _ask_iin(cb.message, state)
    await cb.answer()


@dp.callback_query(Form.dogovor_choice, F.data == "do:man")
async def on_dogovor_manual(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("Номер договораны жазыңыз:")
    await state.set_state(Form.dogovor_manual)
    await cb.answer()


@dp.message(Form.dogovor_manual, F.text)
async def on_dogovor_manual_input(msg: Message, state: FSMContext):
    await state.update_data(dogovor_no=msg.text.strip().upper())
    await _ask_iin(msg, state)


def _person_label(idx: int, total: int) -> str:
    return "Сақтанушы (1)" if idx == 0 else f"{idx + 1}-ші адам"


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
        f"{_person_label(idx, n)}: <b>ҚР азаматы</b> ✅", parse_mode="HTML"
    )
    await cb.message.answer("<b>ИИН жазыңыз</b>:", parse_mode="HTML")
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
        f"{_person_label(idx, n)}: <b>Шетел азаматы</b> ✅", parse_mode="HTML"
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

    wait_msg = await msg.answer("🔎 Іздеп жатырмын…")
    result = await fetch_bonus_malus(iin)
    try:
        await wait_msg.delete()
    except Exception:
        pass
    if result:
        await state.update_data(nsk_fio=result["full_name"], nsk_class=result["class"])
        await msg.answer(
            f"Таптым:\n\n"
            f"<b>ФИО:</b> {h(result['full_name'])}\n"
            f"<b>Класс:</b> {h(result['class'])}\n\n"
            f"Дұрыс па?",
            parse_mode="HTML",
            reply_markup=iin_confirm_keyboard(),
        )
        await state.set_state(Form.p_iin_confirm)
    else:
        await msg.answer(
            f"❌ ИИН <code>{h(iin)}</code> бойынша табылмады.\n\nҚалай жалғастырамыз?",
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
        await cb.message.answer("<b>ИИН жазыңыз</b>:", parse_mode="HTML")
        await state.set_state(Form.p_iin_input)
        return
    await _do_iin_search(cb.message, state, iin)


@dp.callback_query(Form.p_iin_not_found, F.data == "nf:change")
async def on_iin_nf_change(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("<b>ИИН-ді қайта жазыңыз</b>:", parse_mode="HTML")
    await state.set_state(Form.p_iin_input)
    await cb.answer()


@dp.callback_query(Form.p_iin_not_found, F.data == "nf:fio")
async def on_iin_nf_fio(cb: CallbackQuery, state: FSMContext):
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("<b>ФИО</b> өзіңіз жазыңыз:", parse_mode="HTML")
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
    await cb.message.answer("<b>ФИО</b> өзіңіз жазыңыз:", parse_mode="HTML")
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
    display = klass if klass else "бос"
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
    await msg.answer("<b>Машина маркасы мен моделі</b>:", parse_mode="HTML")
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
            f"  <b>Адам {i + 1}:</b> {h(p.get('fio', ''))}\n"
            f"     ИИН: <code>{h(p.get('iin') or '(бос)')}</code> | "
            f"Класс: {h(p.get('klass') or '(бос)')}"
        )
    persons_text = "\n".join(persons_lines)

    dogovor_display = data.get("dogovor_no") or "(бос)"
    summary = (
        "<b>Барлық мәліметтер:</b>\n\n"
        f"<b>Компания:</b> {h(COMPANIES[data['company']])}\n"
        f"<b>Период:</b> {h(PERIODS[data['period']]['label'])}\n"
        f"<b>Сомасы:</b> {h(data['amount'])}\n"
        f"<b>Договор №:</b> <code>{h(dogovor_display)}</code>\n\n"
        f"<b>Адамдар ({data.get('people_count', 1)}):</b>\n{persons_text}\n\n"
        f"<b>Машина:</b> {h(data['car_brand'])}\n"
        f"<b>Гос. номер:</b> <code>{h(data['car_number'])}</code>\n"
        f"<b>VIN:</b> <code>{h(data['vin'])}</code>\n"
        f"<b>Дата договора:</b> {fmt_date(dogovor_date)}\n"
        f"<b>Срок:</b> {fmt_date(date_from)} — {fmt_date(date_to)}"
    )
    await msg.answer(summary, parse_mode="HTML", reply_markup=final_confirm_keyboard())
    await state.set_state(Form.final_confirm)


@dp.callback_query(Form.final_confirm, F.data == "fin:restart")
async def on_restart(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.message.answer("Қайта бастау үшін /start басыңыз.")
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
                f"⚠️ Балансыңыз жеткіліксіз.\n\n"
                f"Қажет: <b>{fmt_money(POLIS_PRICE)}</b>\n"
                f"Балансыңыз: <b>{fmt_money(current)}</b>\n\n"
                f"Толтыру үшін — /topup",
                parse_mode="HTML",
            )
            await state.clear()
            return

    await cb.answer("Құжат жасалып жатыр…")
    await cb.message.edit_reply_markup(reply_markup=None)
    if admin:
        wait_msg = await cb.message.answer("⏳ PDF жасалып жатыр… (тегін, админ)")
    else:
        wait_msg = await cb.message.answer(
            f"⏳ PDF жасалып жатыр…\n"
            f"Баланстан ұсталды: <b>−{fmt_money(POLIS_PRICE)}</b> (баланс: {fmt_money(new_balance)})",
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
                f"❌ Қате: {h(str(e))}\n\n"
                f"Баланс қайтарылды: <b>+{fmt_money(POLIS_PRICE)}</b> (баланс: {fmt_money(refunded or 0)})",
                parse_mode="HTML",
            )
        else:
            await wait_msg.edit_text(f"❌ Қате: {h(str(e))}", parse_mode="HTML")
        await state.clear()
        return
    except Exception as e:
        logging.exception("generate_pdf failed")
        if not admin:
            refunded = await change_balance(user_id, POLIS_PRICE, "refund", meta=f"unexpected: {e}")
            await wait_msg.edit_text(
                f"❌ Белгісіз қате: {h(str(e))}\n\n"
                f"Баланс қайтарылды: <b>+{fmt_money(POLIS_PRICE)}</b> (баланс: {fmt_money(refunded or 0)})",
                parse_mode="HTML",
            )
        else:
            await wait_msg.edit_text(f"❌ Белгісіз қате: {h(str(e))}", parse_mode="HTML")
        await state.clear()
        return

    try:
        await wait_msg.delete()
    except Exception:
        pass
    if admin:
        caption = "✅ Полис дайын!"
    else:
        final_balance = await get_balance(user_id)
        caption = f"✅ Полис дайын!\nҚалған баланс: <b>{fmt_money(final_balance)}</b>"
    await cb.message.answer_document(
        FSInputFile(pdf_path, filename=filename_display),
        caption=caption,
        parse_mode="HTML",
    )
    await state.clear()
    await cb.message.answer("Жаңа полис үшін /start басыңыз.")


async def setup_bot_commands():
    common = [
        BotCommand(command="start", description="Бастау / жаңа полис"),
        BotCommand(command="balance", description="Балансты тексеру"),
        BotCommand(command="topup", description="Баланс толтыру"),
        BotCommand(command="myid", description="Менің ID"),
        BotCommand(command="help", description="Көмек"),
        BotCommand(command="cancel", description="Процесті тоқтату"),
    ]
    await bot.set_my_commands(common, scope=BotCommandScopeDefault())

    admin = common + [
        BotCommand(command="add_balance", description="Пайдаланушыға баланс қосу"),
        BotCommand(command="users", description="Пайдаланушылар тізімі"),
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
