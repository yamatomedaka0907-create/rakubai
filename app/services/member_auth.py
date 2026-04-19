from __future__ import annotations

import sqlite3
import re
import unicodedata
from pathlib import Path
from typing import Any


DB_PATH = Path("data/db/salon.db")


def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = _dict_factory
    return conn


def normalize_phone(phone: str | None) -> str:
    value = unicodedata.normalize("NFKC", phone or "")
    value = re.sub(r"[^0-9]", "", value)
    if value.startswith("81") and len(value) >= 11:
        value = "0" + value[2:]
    return value


def get_member_by_id(member_id: int) -> dict[str, Any] | None:
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT
                id,
                shop_id,
                name,
                phone,
                phone_normalized,
                email,
                email_reminder_enabled,
                created_at,
                updated_at
            FROM members
            WHERE id = ?
            LIMIT 1
            """,
            (member_id,),
        ).fetchone()
        return row
    finally:
        conn.close()


def get_member_by_phone(shop_id: str, phone: str) -> dict[str, Any] | None:
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT
                id,
                shop_id,
                name,
                phone,
                phone_normalized,
                email,
                password_hash,
                email_reminder_enabled,
                created_at,
                updated_at
            FROM members
            WHERE shop_id = ? AND phone_normalized = ?
            LIMIT 1
            """,
            (shop_id, normalize_phone(phone)),
        ).fetchone()
        return row
    finally:
        conn.close()


def get_logged_in_member(request, shop_id: str) -> dict[str, Any] | None:
    member_id = request.session.get("member_id")
    session_shop_id = request.session.get("member_shop_id")

    if not member_id:
        return None
    if session_shop_id and str(session_shop_id) != str(shop_id):
        return None

    try:
        member = get_member_by_id(int(member_id))
    except (TypeError, ValueError):
        return None

    if not member:
        request.session.pop("member_id", None)
        request.session.pop("member_shop_id", None)
        return None

    if str(member.get("shop_id") or "") != str(shop_id):
        return None

    return member


def login_member_session(request, member: dict[str, Any]) -> None:
    request.session["member_id"] = int(member["id"])
    request.session["member_shop_id"] = str(member["shop_id"])


def logout_member_session(request) -> None:
    request.session.pop("member_id", None)
    request.session.pop("member_shop_id", None)
