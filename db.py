import os
from pathlib import Path

import aiosqlite

_DATA_DIR = Path(os.environ.get("DATA_DIR") or Path(__file__).parent)
_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = _DATA_DIR / "bot.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    telegram_id INTEGER PRIMARY KEY,
    username    TEXT,
    first_name  TEXT,
    balance     INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER NOT NULL,
    amount      INTEGER NOT NULL,
    type        TEXT NOT NULL,
    meta        TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(telegram_id, created_at DESC);

CREATE TABLE IF NOT EXISTS polis_log (
    trace_id     TEXT PRIMARY KEY,
    telegram_id  INTEGER NOT NULL,
    username     TEXT,
    first_name   TEXT,
    dogovor_no   TEXT,
    amount       TEXT,
    period_key   TEXT,
    dogovor_date TEXT,
    date_from    TEXT,
    date_to      TEXT,
    car_brand    TEXT,
    car_number   TEXT,
    vin          TEXT,
    persons_json TEXT,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_polis_user ON polis_log(telegram_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_polis_iin ON polis_log(persons_json);
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        # Migration: add tos_accepted_at to existing users tables
        try:
            await db.execute("ALTER TABLE users ADD COLUMN tos_accepted_at TIMESTAMP")
        except aiosqlite.OperationalError:
            pass  # column already exists
        await db.commit()


async def ensure_user(
    telegram_id: int,
    username: str | None,
    first_name: str | None,
) -> bool:
    """Create-or-update user. Returns True if user was newly created."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")
        async with db.execute(
            "SELECT 1 FROM users WHERE telegram_id = ?", (telegram_id,)
        ) as cur:
            existed = await cur.fetchone() is not None
        if existed:
            await db.execute(
                "UPDATE users SET username = ?, first_name = ? WHERE telegram_id = ?",
                (username, first_name, telegram_id),
            )
        else:
            await db.execute(
                "INSERT INTO users (telegram_id, username, first_name) VALUES (?, ?, ?)",
                (telegram_id, username, first_name),
            )
        await db.commit()
        return not existed


async def is_tos_accepted(telegram_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT tos_accepted_at FROM users WHERE telegram_id = ?", (telegram_id,)
        ) as cur:
            row = await cur.fetchone()
            return bool(row and row[0])


async def set_tos_accepted(telegram_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET tos_accepted_at = CURRENT_TIMESTAMP WHERE telegram_id = ?",
            (telegram_id,),
        )
        await db.commit()


async def has_welcome_bonus(telegram_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM transactions WHERE telegram_id = ? AND meta = 'welcome bonus' LIMIT 1",
            (telegram_id,),
        ) as cur:
            return await cur.fetchone() is not None


async def log_polis(
    trace_id: str,
    telegram_id: int,
    username: str | None,
    first_name: str | None,
    data: dict,
):
    import json
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO polis_log
            (trace_id, telegram_id, username, first_name, dogovor_no, amount,
             period_key, dogovor_date, date_from, date_to,
             car_brand, car_number, vin, persons_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                trace_id, telegram_id, username, first_name,
                data.get("dogovor_no", ""),
                data.get("amount", ""),
                data.get("period", ""),
                data.get("dogovor_date", ""),
                data.get("date_from", ""),
                data.get("date_to", ""),
                data.get("car_brand", ""),
                data.get("car_number", ""),
                data.get("vin", ""),
                json.dumps(data.get("persons", []), ensure_ascii=False),
            ),
        )
        await db.commit()


async def get_polis_by_trace_id(trace_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM polis_log WHERE trace_id = ?", (trace_id,)
        ) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def list_recent_polises(limit: int = 20) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT trace_id, telegram_id, username, first_name, dogovor_no, "
            "car_number, created_at FROM polis_log ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_balance(telegram_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT balance FROM users WHERE telegram_id = ?", (telegram_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def change_balance(telegram_id: int, delta: int, tx_type: str, meta: str = "") -> int | None:
    """Atomically change balance. Returns new balance, or None if would go negative."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")
        async with db.execute("SELECT balance FROM users WHERE telegram_id = ?", (telegram_id,)) as cur:
            row = await cur.fetchone()
        current = row[0] if row else 0
        new_balance = current + delta
        if new_balance < 0:
            await db.rollback()
            return None
        if row is None:
            await db.execute(
                "INSERT INTO users (telegram_id, balance) VALUES (?, ?)",
                (telegram_id, new_balance),
            )
        else:
            await db.execute(
                "UPDATE users SET balance = ? WHERE telegram_id = ?",
                (new_balance, telegram_id),
            )
        await db.execute(
            "INSERT INTO transactions (telegram_id, amount, type, meta) VALUES (?, ?, ?, ?)",
            (telegram_id, delta, tx_type, meta),
        )
        await db.commit()
        return new_balance


async def list_recent_transactions(telegram_id: int, limit: int = 5) -> list[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT amount, type, meta, created_at FROM transactions "
            "WHERE telegram_id = ? ORDER BY created_at DESC LIMIT ?",
            (telegram_id, limit),
        ) as cur:
            return await cur.fetchall()


async def list_users(limit: int = 50) -> list[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT telegram_id, username, first_name, balance FROM users "
            "ORDER BY balance DESC, created_at DESC LIMIT ?",
            (limit,),
        ) as cur:
            return await cur.fetchall()


async def user_exists(telegram_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM users WHERE telegram_id = ?", (telegram_id,)) as cur:
            return await cur.fetchone() is not None
