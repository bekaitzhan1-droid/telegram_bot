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
"""


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()


async def ensure_user(
    telegram_id: int,
    username: str | None,
    first_name: str | None,
    welcome_bonus: int = 0,
) -> bool:
    """Returns True if user was newly created (and credited with welcome_bonus, if any)."""
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
            initial_balance = max(0, welcome_bonus)
            await db.execute(
                "INSERT INTO users (telegram_id, username, first_name, balance) VALUES (?, ?, ?, ?)",
                (telegram_id, username, first_name, initial_balance),
            )
            if initial_balance > 0:
                await db.execute(
                    "INSERT INTO transactions (telegram_id, amount, type, meta) VALUES (?, ?, ?, ?)",
                    (telegram_id, initial_balance, "topup", "welcome bonus"),
                )
        await db.commit()
        return not existed


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
