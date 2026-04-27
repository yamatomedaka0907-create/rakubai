from __future__ import annotations

from dotenv import load_dotenv

import calendar
import json
from datetime import date, datetime, timedelta, timezone, time as datetime_time
import os
import smtplib
import threading
import time
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
import secrets
import re
import unicodedata
import requests
from zoneinfo import ZoneInfo

load_dotenv()
load_dotenv(Path(__file__).resolve().with_name(".env"))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from app.db import (
    get_connection,
    init_db,
    get_shop,
    get_all_shops_for_platform,
    get_shop_management_data,
    get_plans,
    update_shop_basic_info,
    update_shop_staff_list,
    update_shop_subscription,
    create_shop_with_owner,
    create_shop_registration_verification,
    get_shop_registration_verification,
    verify_shop_registration_code,
    consume_shop_registration_verification,
    create_customer,
    create_reservation,
    find_customer,
    update_customer_contact,
    update_customer,
    delete_customer,
    get_customer_by_id,
    get_customer_notes,
    add_customer_note,
    delete_customer_note,
    get_customer_photos,
    add_customer_photo,
    delete_customer_photo,
    get_reservations,
    get_customers,
    get_member_customer_ids,
    update_reservation_status,
    authenticate_admin_user,
    get_admin_users,
    get_shop_subscription,
    get_child_shops,
    get_parent_shop,
    create_child_shop_under_parent,
    get_system_mail_settings,
    update_system_mail_settings,
    get_due_reservation_reminders,
    mark_reservation_reminder_sent,
    get_shop_homepage_settings,
    get_shop_homepage_sections,
    get_shop_homepage_by_public_path,
    create_member,
    authenticate_member,
    create_member_registration_verification,
    get_member_registration_verification,
    verify_member_registration_code,
    consume_member_registration_verification,
    get_member_by_id,
    get_member_reservations,
    get_member_all_reservations,
    get_member_by_customer_id,
    get_latest_chat_member_id,
    get_member_by_phone_normalized,
    get_member_linked_shops,
    deactivate_member,
    deactivate_members_by_phone,
    list_chat_messages,
    create_chat_message,
    mark_chat_messages_read_for_admin,
    mark_chat_messages_read_for_member,
    count_member_chat_messages_in_month,
    count_shop_chat_messages_in_month,
    get_admin_unread_chat_summary,
    get_member_unread_chat_summary,
    normalize_member_phone,
    create_audit_log,
    list_members_for_audit_api,
    list_audit_logs_for_api,
    get_shop_detail_for_audit_api,
    get_member_detail_for_audit_api,
    update_shop_for_audit_api,
    update_member_for_audit_api,
    force_cancel_shop_for_audit_api,
    force_cancel_member_for_audit_api,
    restore_shop_for_audit_api,
    restore_member_for_audit_api,
    ensure_line_settings_schema,
)
from app.runtime_data import (
    get_platform_admin,
    get_sample_categories,
    get_all_samples,
    get_sample,
)
from app.routers.admin import router as admin_router
from app.routers import admin_patch
from app.migrations.line_settings_migration import ensure_line_setting_columns


Path("data/uploads/shops").mkdir(parents=True, exist_ok=True)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="reservation-app-secret")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/uploads", StaticFiles(directory="data/uploads"), name="uploads")

templates = Jinja2Templates(directory="app/templates")
templates.env.globals["is_premium_subscription"] = lambda subscription: _is_premium_subscription(subscription)
PLATFORM_ADMIN = get_platform_admin()
JST = ZoneInfo("Asia/Tokyo")


def send_line_message(access_token: str, user_id: str, message: str) -> dict:
    access_token = str(access_token or "").strip()
    user_id = str(user_id or "").strip()
    message = str(message or "").strip()

    if not access_token:
        print("LINE send skipped: no access token")
        return {"ok": False, "reason": "no access token"}
    if not user_id:
        print("LINE send skipped: no user_id")
        return {"ok": False, "reason": "no user_id"}
    if not message:
        print("LINE send skipped: empty message")
        return {"ok": False, "reason": "empty message"}

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "to": user_id,
        "messages": [
            {
                "type": "text",
                "text": message,
            }
        ],
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        print("LINE send result:", response.status_code, response.text)
        return {
            "ok": 200 <= response.status_code < 300,
            "status_code": response.status_code,
            "response": response.text,
        }
    except Exception as exc:
        print("LINE send error:", repr(exc))
        return {"ok": False, "reason": str(exc)}


def send_line_payload(access_token: str, user_id: str, messages: list[dict]) -> dict:
    access_token = str(access_token or "").strip()
    user_id = str(user_id or "").strip()
    if not access_token:
        return {"ok": False, "reason": "no access token"}
    if not user_id:
        return {"ok": False, "reason": "no user_id"}
    if not messages:
        return {"ok": False, "reason": "no messages"}
    try:
        response = requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json={"to": user_id, "messages": messages[:5]},
            timeout=10,
        )
        print("LINE payload send result:", response.status_code, response.text)
        return {"ok": 200 <= response.status_code < 300, "status_code": response.status_code, "response": response.text}
    except Exception as exc:
        print("LINE payload send error:", repr(exc))
        return {"ok": False, "reason": str(exc)}


def build_line_quick_reply_text(text: str, labels: list[str]) -> dict:
    items = []
    for label in labels[:13]:
        clean = str(label or "").strip()
        if clean:
            items.append({"type": "action", "action": {"type": "message", "label": clean[:20], "text": clean}})
    msg = {"type": "text", "text": str(text or "")}
    if items:
        msg["quickReply"] = {"items": items}
    return msg


def ensure_line_booking_session_schema() -> None:
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS line_booking_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                line_user_id TEXT NOT NULL,
                step TEXT NOT NULL DEFAULT '',
                staff_id INTEGER,
                staff_name TEXT DEFAULT '',
                menu_id INTEGER,
                menu_name TEXT DEFAULT '',
                reservation_date TEXT DEFAULT '',
                start_time TEXT DEFAULT '',
                end_time TEXT DEFAULT '',
                data_json TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_line_booking_sessions_shop_user
            ON line_booking_sessions(shop_id, line_user_id)
        """)
        conn.commit()


def get_line_booking_session(shop_id: str, line_user_id: str) -> dict | None:
    ensure_line_booking_session_schema()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM line_booking_sessions WHERE shop_id = ? AND line_user_id = ? LIMIT 1",
            (str(shop_id or "").strip(), str(line_user_id or "").strip()),
        ).fetchone()
    return dict(row) if row else None


def save_line_booking_session(shop_id: str, line_user_id: str, **values) -> None:
    ensure_line_booking_session_schema()
    now_text = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    current = get_line_booking_session(shop_id, line_user_id) or {}
    fields = {
        "step": current.get("step") or "",
        "staff_id": current.get("staff_id"),
        "staff_name": current.get("staff_name") or "",
        "menu_id": current.get("menu_id"),
        "menu_name": current.get("menu_name") or "",
        "reservation_date": current.get("reservation_date") or "",
        "start_time": current.get("start_time") or "",
        "end_time": current.get("end_time") or "",
        "data_json": current.get("data_json") or "{}",
    }
    fields.update(values)
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO line_booking_sessions (
                shop_id, line_user_id, step, staff_id, staff_name, menu_id, menu_name,
                reservation_date, start_time, end_time, data_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(shop_id, line_user_id) DO UPDATE SET
                step = excluded.step,
                staff_id = excluded.staff_id,
                staff_name = excluded.staff_name,
                menu_id = excluded.menu_id,
                menu_name = excluded.menu_name,
                reservation_date = excluded.reservation_date,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                data_json = excluded.data_json,
                updated_at = excluded.updated_at
        """, (
            shop_id, line_user_id, fields["step"], fields["staff_id"], fields["staff_name"],
            fields["menu_id"], fields["menu_name"], fields["reservation_date"], fields["start_time"],
            fields["end_time"], fields["data_json"], now_text, now_text,
        ))
        conn.commit()


def clear_line_booking_session(shop_id: str, line_user_id: str) -> None:
    ensure_line_booking_session_schema()
    with get_connection() as conn:
        conn.execute("DELETE FROM line_booking_sessions WHERE shop_id = ? AND line_user_id = ?", (shop_id, line_user_id))
        conn.commit()


def _line_find_item_by_reply(items: list[dict], reply_text: str) -> dict | None:
    text = str(reply_text or "").strip()
    for item in items:
        item_id = str((item or {}).get("id") or "").strip()
        name = str((item or {}).get("name") or "").strip()
        if text in {name, item_id, f"{item_id}. {name}", f"{item_id} {name}"}:
            return item
    return None


def _line_selectable_menus(shop: dict, staff: dict | None = None) -> list[dict]:
    menus = list((shop or {}).get("menus") or [])
    ids = (staff or {}).get("menu_ids") or []
    try:
        ids = [int(x) for x in ids]
    except Exception:
        ids = []
    return [m for m in menus if not ids or int(m.get("id") or 0) in ids]


def build_line_datetime_options(shop_id: str, staff_id: int, duration: int, days: int = 7) -> list[dict]:
    duration = max(15, int(duration or 60))
    occupied = {
        (str(r.get("reservation_date") or ""), str(r.get("start_time") or "")[:5])
        for r in get_reservations(shop_id)
        if str(r.get("status") or "") == "予約済み" and int(r.get("staff_id") or 0) == int(staff_id or 0)
    }
    options = []
    now = datetime.now(JST)
    for day_offset in range(days):
        target = (now + timedelta(days=day_offset)).date()
        for hour in range(10, 19):
            for minute in (0, 30):
                start_dt = datetime.combine(target, datetime.min.time(), tzinfo=JST).replace(hour=hour, minute=minute)
                if start_dt <= now + timedelta(minutes=30):
                    continue
                end_dt = start_dt + timedelta(minutes=duration)
                if end_dt.hour > 19 or (end_dt.hour == 19 and end_dt.minute > 0):
                    continue
                date_text = target.isoformat()
                start_text = start_dt.strftime("%H:%M")
                if (date_text, start_text) in occupied:
                    continue
                options.append({"label": start_dt.strftime("%m/%d %H:%M"), "reservation_date": date_text, "start_time": start_text, "end_time": end_dt.strftime("%H:%M")})
                if len(options) >= 13:
                    return options
    return options


def handle_line_complete_booking_message(shop_id: str, user_id: str, message_text: str, access_token: str) -> dict:
    shop = get_shop(shop_id) or {}
    staff_list = list(shop.get("staff_list") or [])
    text = str(message_text or "").strip()
    normalized = text.replace(" ", "").replace("　", "").strip()

    if normalized in {"キャンセル", "取消", "中止", "やめる", "いいえ"}:
        clear_line_booking_session(shop_id, user_id)
        return send_line_payload(access_token, user_id, [{"type": "text", "text": "予約手続きをキャンセルしました。\nもう一度始める場合は「予約」と送信してください。"}])

    session = get_line_booking_session(shop_id, user_id)
    if "予約" in normalized or not session:
        if not staff_list:
            return send_line_payload(access_token, user_id, [{"type": "text", "text": "現在、選択できる担当者が登録されていません。"}])
        save_line_booking_session(shop_id, user_id, step="select_staff", staff_id=None, staff_name="", menu_id=None, menu_name="", reservation_date="", start_time="", end_time="")
        return send_line_payload(access_token, user_id, [build_line_quick_reply_text("担当者を選んでください。", [f"{st.get('id')}. {st.get('name')}" for st in staff_list if st.get("name")])])

    step = str(session.get("step") or "")
    if step == "select_staff":
        staff = _line_find_item_by_reply(staff_list, text)
        if not staff:
            return send_line_payload(access_token, user_id, [build_line_quick_reply_text("担当者を選んでください。", [f"{st.get('id')}. {st.get('name')}" for st in staff_list if st.get("name")])])
        menus = _line_selectable_menus(shop, staff)
        if not menus:
            clear_line_booking_session(shop_id, user_id)
            return send_line_payload(access_token, user_id, [{"type": "text", "text": "この担当者で選択できるメニューがありません。"}])
        save_line_booking_session(shop_id, user_id, step="select_menu", staff_id=int(staff.get("id") or 0), staff_name=str(staff.get("name") or ""))
        return send_line_payload(access_token, user_id, [build_line_quick_reply_text("メニューを選んでください。", [f"{m.get('id')}. {m.get('name')}" for m in menus if m.get("name")])])

    if step == "select_menu":
        staff = next((st for st in staff_list if int(st.get("id") or 0) == int(session.get("staff_id") or 0)), None)
        menus = _line_selectable_menus(shop, staff)
        menu = _line_find_item_by_reply(menus, text)
        if not menu:
            return send_line_payload(access_token, user_id, [build_line_quick_reply_text("メニューを選んでください。", [f"{m.get('id')}. {m.get('name')}" for m in menus if m.get("name")])])
        options = build_line_datetime_options(shop_id, int(session.get("staff_id") or 0), int(menu.get("duration") or 60))
        if not options:
            clear_line_booking_session(shop_id, user_id)
            return send_line_payload(access_token, user_id, [{"type": "text", "text": "現在、選択できる日時がありません。店舗へ直接お問い合わせください。"}])
        save_line_booking_session(shop_id, user_id, step="select_datetime", menu_id=int(menu.get("id") or 0), menu_name=str(menu.get("name") or ""), data_json=json.dumps({"datetime_options": options}, ensure_ascii=False))
        return send_line_payload(access_token, user_id, [build_line_quick_reply_text("日時を選んでください。", [opt["label"] for opt in options])])

    if step == "select_datetime":
        try:
            options = list(json.loads(session.get("data_json") or "{}").get("datetime_options") or [])
        except Exception:
            options = []
        selected = next((opt for opt in options if str(opt.get("label") or "") == text), None)
        if not selected:
            return send_line_payload(access_token, user_id, [build_line_quick_reply_text("日時を選んでください。", [str(opt.get("label") or "") for opt in options])])
        save_line_booking_session(shop_id, user_id, step="confirm", reservation_date=selected.get("reservation_date") or "", start_time=selected.get("start_time") or "", end_time=selected.get("end_time") or "")
        confirm = f"この内容で予約しますか？\n\n担当者：{session.get('staff_name')}\nメニュー：{session.get('menu_name')}\n日時：{selected.get('reservation_date')} {selected.get('start_time')}\n\n「はい」を選ぶと予約を確定します。"
        return send_line_payload(access_token, user_id, [build_line_quick_reply_text(confirm, ["はい", "いいえ"])])

    if step == "confirm":
        if normalized not in {"はい", "予約する", "確定", "お願いします"}:
            return send_line_payload(access_token, user_id, [build_line_quick_reply_text("予約する場合は「はい」を選んでください。", ["はい", "いいえ"])])
        customer_name = f"LINE予約 {user_id[-6:]}"
        customer = find_customer(shop_id, customer_name, "", "") or create_customer(shop_id, customer_name, "", "")
        reservation = create_reservation(
            shop_id, int(customer.get("id") or 0), customer_name, "", 0,
            int(session.get("staff_id") or 0), str(session.get("staff_name") or ""),
            int(session.get("menu_id") or 0), str(session.get("menu_name") or ""),
            0, 0, str(session.get("reservation_date") or ""), str(session.get("start_time") or ""),
            str(session.get("end_time") or ""), "予約済み", "line"
        )
        clear_line_booking_session(shop_id, user_id)
        done = f"予約が完了しました。\n\n担当者：{reservation.get('staff_name')}\nメニュー：{reservation.get('menu_name')}\n日時：{reservation.get('reservation_date')} {reservation.get('start_time')}"
        return send_line_payload(access_token, user_id, [{"type": "text", "text": done}])

    clear_line_booking_session(shop_id, user_id)
    return send_line_payload(access_token, user_id, [{"type": "text", "text": "もう一度「予約」と送信してください。"}])


@app.get("/features", response_class=HTMLResponse)
def features_page(request: Request):
    return templates.TemplateResponse("features.html", {"request": request})


@app.get("/features.html")
def features_html_redirect():
    return RedirectResponse(url="/features", status_code=302)



@app.get("/features/multi-device", response_class=HTMLResponse)
def feature_multi_device_page(request: Request):
    return templates.TemplateResponse("features/multi-device.html", {"request": request})


@app.get("/features/customer", response_class=HTMLResponse)
def feature_customer_page(request: Request):
    return templates.TemplateResponse("features/customer.html", {"request": request})


@app.get("/features/reservation", response_class=HTMLResponse)
def feature_reservation_page(request: Request):
    return templates.TemplateResponse("features/reservation.html", {"request": request})


@app.get("/features/analytics", response_class=HTMLResponse)
def feature_analytics_page(request: Request):
    return templates.TemplateResponse("features/analytics.html", {"request": request})


@app.get("/features/staff", response_class=HTMLResponse)
def feature_staff_page(request: Request):
    return templates.TemplateResponse("features/staff.html", {"request": request})


@app.get("/features/timeline", response_class=HTMLResponse)
def feature_timeline_page(request: Request):
    return templates.TemplateResponse("features/timeline.html", {"request": request})


@app.get("/features/store-info", response_class=HTMLResponse)
def feature_store_info_page(request: Request):
    return templates.TemplateResponse("features/store-info.html", {"request": request})


@app.get("/features/reservation-site", response_class=HTMLResponse)
def feature_reservation_site_page(request: Request):
    return templates.TemplateResponse("features/reservation-site.html", {"request": request})


@app.get("/features/line", response_class=HTMLResponse)
def feature_line_page(request: Request):
    return templates.TemplateResponse("features/line.html", {"request": request})


@app.get("/features/homepage", response_class=HTMLResponse)
def feature_homepage_page(request: Request):
    return templates.TemplateResponse("features/homepage.html", {"request": request})


@app.get("/features/remind", response_class=HTMLResponse)
def feature_remind_page(request: Request):
    return templates.TemplateResponse("features/remind.html", {"request": request})


@app.get("/features/membership", response_class=HTMLResponse)
def feature_membership_page(request: Request):
    return templates.TemplateResponse("features/membership.html", {"request": request})


@app.get("/features/photo", response_class=HTMLResponse)
def feature_photo_page(request: Request):
    return templates.TemplateResponse("features/photo.html", {"request": request})


@app.get("/features/store-management", response_class=HTMLResponse)
def feature_store_management_page(request: Request):
    return templates.TemplateResponse("features/store-management.html", {"request": request})


@app.get("/features/nomination", response_class=HTMLResponse)
def feature_nomination_page(request: Request):
    return templates.TemplateResponse("features/nomination.html", {"request": request})


@app.get("/features/chat", response_class=HTMLResponse)
def feature_chat_page(request: Request):
    return templates.TemplateResponse("features/chat.html", {"request": request})

@app.get("/line-test")
def line_test(shop_id: str = "yamato", user_id: str = ""):
    settings = get_shop_line_settings(shop_id)
    access_token = str(settings.get("line_channel_access_token") or "").strip()
    target_user_id = str(user_id or "").strip()

    if not target_user_id:
        recent_users = get_recent_line_webhook_users(shop_id, limit=1)
        if recent_users:
            target_user_id = str(recent_users[0].get("line_user_id") or "").strip()

    result = send_line_message(
        access_token=access_token,
        user_id=target_user_id,
        message="LINEテスト送信OKです。",
    )
    print("LINE test result:", result)
    return result



def build_line_reservation_url(shop_id: str, line_user_id: str) -> str:
    shop_id = str(shop_id or "").strip()
    line_user_id = str(line_user_id or "").strip()
    return f"https://www.rakubai.net/shop/{shop_id}?line_user_id={line_user_id}#reserve-form"



def send_line_reservation_button(access_token: str, user_id: str, shop_id: str) -> dict:
    access_token = str(access_token or "").strip()
    user_id = str(user_id or "").strip()
    shop_id = str(shop_id or "").strip()
    reserve_url = build_line_reservation_url(shop_id, user_id)

    if not access_token:
        print("LINE reservation button skipped: no access token")
        return {"ok": False, "reason": "no access token"}
    if not user_id:
        print("LINE reservation button skipped: no user_id")
        return {"ok": False, "reason": "no user_id"}
    if not shop_id:
        print("LINE reservation button skipped: no shop_id")
        return {"ok": False, "reason": "no shop_id"}

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "to": user_id,
        "messages": [
            {
                "type": "template",
                "altText": "予約ページを開く",
                "template": {
                    "type": "buttons",
                    "title": "ご予約はこちら",
                    "text": "下のボタンから予約ページを開いてください。",
                    "actions": [
                        {
                            "type": "uri",
                            "label": "予約する",
                            "uri": reserve_url,
                        }
                    ],
                },
            }
        ],
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        print("LINE reservation button result:", response.status_code, response.text)
        return {
            "ok": 200 <= response.status_code < 300,
            "status_code": response.status_code,
            "response": response.text,
            "reserve_url": reserve_url,
        }
    except Exception as exc:
        print("LINE reservation button error:", repr(exc))
        return {"ok": False, "reason": str(exc), "reserve_url": reserve_url}




@app.get("/line-reservation-button-test")
def line_reservation_button_test(shop_id: str = "yamato", user_id: str = ""):
    settings = get_shop_line_settings(shop_id)
    access_token = str(settings.get("line_channel_access_token") or "").strip()
    target_user_id = str(user_id or "").strip()

    if not target_user_id:
        recent_users = get_recent_line_webhook_users(shop_id, limit=1)
        if recent_users:
            target_user_id = str(recent_users[0].get("line_user_id") or "").strip()

    result = send_line_reservation_button(
        access_token=access_token,
        user_id=target_user_id,
        shop_id=shop_id,
    )
    print("LINE reservation button test result:", result)
    return result



def get_shop_line_settings(shop_id: str) -> dict:
    """店舗のLINE連携設定を取得します。

    既存DBではLINE用カラムが未作成の場合があるため、
    読み込み前に必要なカラムを自動追加します。
    """
    with get_connection() as conn:
        ensure_line_setting_columns(conn)
        row = conn.execute(
            """
            SELECT
                shop_id,
                line_mode,
                line_channel_access_token,
                line_channel_secret,
                line_liff_id,
                line_official_url,
                line_webhook_enabled
            FROM shops
            WHERE shop_id = ?
            LIMIT 1
            """,
            (str(shop_id or "").strip(),),
        ).fetchone()
    settings = dict(row) if row else {}
    settings["line_mode"] = normalize_line_mode(settings.get("line_mode"))
    return settings


def normalize_line_mode(value: str) -> str:
    """LINE連携モードを off / login / liff に正規化します。"""
    mode = str(value or "off").strip().lower()
    if mode in {"simple", "web", "login"}:
        return "login"
    if mode in {"perfect", "complete", "liff"}:
        return "liff"
    return "off"


def line_mode_label(mode: str) -> str:
    labels = {
        "off": "利用しない",
        "login": "簡単モード（Web予約＋LINE通知）",
        "liff": "LINE完結モード（LIFF）",
    }
    return labels.get(normalize_line_mode(mode), labels["off"])




def ensure_line_webhook_test_schema() -> None:
    """LINE Webhookで取得したテスト用user_idを保存するテーブルを用意します。"""
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS line_webhook_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                line_user_id TEXT NOT NULL,
                display_name TEXT DEFAULT '',
                event_type TEXT DEFAULT '',
                message_text TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_line_webhook_users_shop_user
            ON line_webhook_users(shop_id, line_user_id)
            """
        )
        conn.commit()


def save_line_webhook_user(shop_id: str, line_user_id: str, *, event_type: str = "", message_text: str = "") -> None:
    """Webhookで受け取ったLINE user_idを保存します。"""
    shop_id = str(shop_id or "").strip()
    line_user_id = str(line_user_id or "").strip()
    if not shop_id or not line_user_id:
        return

    ensure_line_webhook_test_schema()
    now_text = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO line_webhook_users (
                shop_id, line_user_id, event_type, message_text, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(shop_id, line_user_id) DO UPDATE SET
                event_type = excluded.event_type,
                message_text = excluded.message_text,
                updated_at = excluded.updated_at
            """,
            (shop_id, line_user_id, str(event_type or ""), str(message_text or ""), now_text, now_text),
        )
        conn.commit()


def get_recent_line_webhook_users(shop_id: str, limit: int = 5) -> list[dict]:
    """管理画面表示用に、直近で取得したLINE user_idを返します。"""
    shop_id = str(shop_id or "").strip()
    if not shop_id:
        return []

    ensure_line_webhook_test_schema()
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT line_user_id, event_type, message_text, created_at, updated_at
            FROM line_webhook_users
            WHERE shop_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (shop_id, int(limit or 5)),
        ).fetchall()
    return [dict(row) for row in rows]




def send_line_messages(access_token: str, user_id: str, messages: list[dict]) -> dict:
    access_token = str(access_token or "").strip()
    user_id = str(user_id or "").strip()
    if not access_token:
        return {"ok": False, "reason": "no access token"}
    if not user_id:
        return {"ok": False, "reason": "no user_id"}
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    try:
        response = requests.post(url, headers=headers, json={"to": user_id, "messages": messages[:5]}, timeout=10)
        print("LINE messages result:", response.status_code, response.text)
        return {"ok": 200 <= response.status_code < 300, "status_code": response.status_code, "response": response.text}
    except Exception as exc:
        print("LINE messages error:", repr(exc))
        return {"ok": False, "reason": str(exc)}


def _line_qr(label: str, data: str, display_text: str | None = None) -> dict:
    return {"type": "action", "action": {"type": "postback", "label": str(label)[:20], "data": str(data)[:300], "displayText": str(display_text or label)[:300]}}


def send_line_quick_reply(access_token: str, user_id: str, text: str, items: list[dict]) -> dict:
    message = {"type": "text", "text": str(text or "")[:5000]}
    if items:
        message["quickReply"] = {"items": items[:13]}
    return send_line_messages(access_token, user_id, [message])


def send_line_selection_buttons(access_token: str, user_id: str, text: str, labels: list[str]) -> dict:
    clean_labels = [str(label or "").strip() for label in labels if str(label or "").strip()]
    if not clean_labels:
        return send_line_message(access_token, user_id, text)

    contents = []
    for label in clean_labels[:13]:
        contents.append({
            "type": "button",
            "style": "primary",
            "height": "md",
            "action": {
                "type": "message",
                "label": label[:40],
                "text": label[:300],
            },
        })

    messages = [
        {"type": "text", "text": str(text or "")[:5000]},
        {
            "type": "flex",
            "altText": "選択してください",
            "contents": {
                "type": "bubble",
                "body": {
                    "type": "box",
                    "layout": "vertical",
                    "spacing": "md",
                    "contents": contents,
                },
            },
        },
    ]
    return send_line_messages(access_token, user_id, messages)


def ensure_line_reservation_session_schema() -> None:
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS line_reservation_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                line_user_id TEXT NOT NULL,
                step TEXT NOT NULL DEFAULT '',
                staff_id TEXT DEFAULT '', staff_name TEXT DEFAULT '',
                menu_id TEXT DEFAULT '', menu_name TEXT DEFAULT '',
                duration INTEGER DEFAULT 0, price INTEGER DEFAULT 0,
                reservation_date TEXT DEFAULT '', start_time TEXT DEFAULT '', end_time TEXT DEFAULT '',
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_line_reservation_sessions_shop_user ON line_reservation_sessions(shop_id, line_user_id)""")
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(line_reservation_sessions)").fetchall()}
        for column_name, column_type in {
            "customer_id": "INTEGER DEFAULT 0",
            "customer_name": "TEXT DEFAULT ''",
            "customer_phone": "TEXT DEFAULT ''",
            "slot_page": "INTEGER DEFAULT 0",
        }.items():
            if column_name not in columns:
                conn.execute(f"ALTER TABLE line_reservation_sessions ADD COLUMN {column_name} {column_type}")
        conn.commit()


def get_line_reservation_session(shop_id: str, line_user_id: str) -> dict | None:
    ensure_line_reservation_session_schema()
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM line_reservation_sessions WHERE shop_id = ? AND line_user_id = ? LIMIT 1", (str(shop_id or "").strip(), str(line_user_id or "").strip())).fetchone()
    return dict(row) if row else None


def upsert_line_reservation_session(shop_id: str, line_user_id: str, **values) -> dict | None:
    ensure_line_reservation_session_schema()
    now_text = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    current = get_line_reservation_session(shop_id, line_user_id) or {}
    data = {
        "step": current.get("step") or "",
        "staff_id": current.get("staff_id") or "",
        "staff_name": current.get("staff_name") or "",
        "menu_id": current.get("menu_id") or "",
        "menu_name": current.get("menu_name") or "",
        "duration": int(current.get("duration") or 0),
        "price": int(current.get("price") or 0),
        "reservation_date": current.get("reservation_date") or "",
        "start_time": current.get("start_time") or "",
        "end_time": current.get("end_time") or "",
        "customer_id": int(current.get("customer_id") or 0),
        "customer_name": current.get("customer_name") or "",
        "customer_phone": current.get("customer_phone") or "",
        "slot_page": int(current.get("slot_page") or 0),
    }
    data.update(values)
    with get_connection() as conn:
        conn.execute("""
            INSERT INTO line_reservation_sessions (shop_id,line_user_id,step,staff_id,staff_name,menu_id,menu_name,duration,price,reservation_date,start_time,end_time,customer_id,customer_name,customer_phone,slot_page,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(shop_id,line_user_id) DO UPDATE SET
                step=excluded.step, staff_id=excluded.staff_id, staff_name=excluded.staff_name,
                menu_id=excluded.menu_id, menu_name=excluded.menu_name, duration=excluded.duration, price=excluded.price,
                reservation_date=excluded.reservation_date, start_time=excluded.start_time, end_time=excluded.end_time,
                customer_id=excluded.customer_id, customer_name=excluded.customer_name, customer_phone=excluded.customer_phone,
                slot_page=excluded.slot_page,
                updated_at=excluded.updated_at
        """, (
            str(shop_id or "").strip(), str(line_user_id or "").strip(), str(data.get("step") or ""),
            str(data.get("staff_id") or ""), str(data.get("staff_name") or ""), str(data.get("menu_id") or ""),
            str(data.get("menu_name") or ""), int(data.get("duration") or 0), int(data.get("price") or 0),
            str(data.get("reservation_date") or ""), str(data.get("start_time") or ""), str(data.get("end_time") or ""),
            int(data.get("customer_id") or 0), str(data.get("customer_name") or ""), str(data.get("customer_phone") or ""),
            int(data.get("slot_page") or 0), now_text, now_text,
        ))
        conn.commit()
    return get_line_reservation_session(shop_id, line_user_id)


def clear_line_reservation_session(shop_id: str, line_user_id: str) -> None:
    ensure_line_reservation_session_schema()
    with get_connection() as conn:
        conn.execute("DELETE FROM line_reservation_sessions WHERE shop_id = ? AND line_user_id = ?", (str(shop_id or "").strip(), str(line_user_id or "").strip()))
        conn.commit()


def _line_staff_options(shop: dict) -> list[dict]:
    result = []
    for index, staff in enumerate((shop or {}).get("staff_list") or [], start=1):
        if isinstance(staff, dict):
            name = str(staff.get("name") or staff.get("staff_name") or "").strip()
            if name:
                result.append({"id": str(staff.get("id") or staff.get("staff_id") or index), "name": name})
    return result


def _line_menu_options(shop: dict) -> list[dict]:
    result = []
    for index, menu in enumerate((shop or {}).get("menus") or [], start=1):
        if isinstance(menu, dict):
            name = str(menu.get("name") or menu.get("menu_name") or "").strip()
            if name:
                result.append({"id": str(menu.get("id") or menu.get("menu_id") or index), "name": name, "duration": int(menu.get("duration") or menu.get("duration_minutes") or 60), "price": int(menu.get("price") or 0)})
    return result


def _line_find(options: list[dict], option_id: str) -> dict | None:
    for item in options:
        if str(item.get("id") or "") == str(option_id or ""):
            return item
    return None


def _line_reservation_overlaps(
    reservation: dict,
    *,
    staff_id: str,
    date_text: str,
    start_text: str,
    end_text: str,
    default_duration: int = 60,
) -> bool:
    """既存予約と候補枠が時間重複しているか判定します。"""
    if str(reservation.get("status") or "") == "キャンセル":
        return False
    if str(reservation.get("staff_id") or "").strip() != str(staff_id or "").strip():
        return False
    if str(reservation.get("reservation_date") or "").strip() != str(date_text or "").strip():
        return False

    candidate_start = _parse_hhmm_to_minutes(str(start_text or "")[:5])
    candidate_end = _parse_hhmm_to_minutes(str(end_text or "")[:5])
    reserved_start = _parse_hhmm_to_minutes(str(reservation.get("start_time") or "")[:5])
    reserved_end = _parse_hhmm_to_minutes(str(reservation.get("end_time") or "")[:5])

    if candidate_start is None or candidate_end is None or reserved_start is None:
        return False

    if candidate_end <= candidate_start:
        try:
            candidate_end = candidate_start + max(30, int(default_duration or 60))
        except (TypeError, ValueError):
            candidate_end = candidate_start + 60

    if reserved_end is None or reserved_end <= reserved_start:
        try:
            existing_duration = int(reservation.get("duration") or default_duration or 60)
        except (TypeError, ValueError):
            existing_duration = int(default_duration or 60)
        reserved_end = reserved_start + max(30, existing_duration)

    # 半開区間 [start, end) で判定。10:00-10:30 と 10:30-11:00 は重複しない。
    return candidate_start < reserved_end and candidate_end > reserved_start


def _line_has_reservation_conflict(shop_id: str, staff_id: str, date_text: str, start_text: str, end_text: str, default_duration: int = 60) -> bool:
    """LINE予約確定直前にも使う最終重複チェック。"""
    for reservation in get_reservations(shop_id):
        if _line_reservation_overlaps(
            reservation,
            staff_id=str(staff_id or ""),
            date_text=str(date_text or ""),
            start_text=str(start_text or ""),
            end_text=str(end_text or ""),
            default_duration=int(default_duration or 60),
        ):
            return True
    return False


def _line_slot_options(shop_id: str, staff_id: str, duration: int, days: int = 30, limit: int = 13, offset: int = 0) -> list[dict]:
    """LINE完結予約で表示する日時候補を作ります。

    Web予約のカレンダーと同じ考え方に合わせて、
    - 店舗の営業時間（business_hours）
    - 店舗の定休日
    - 担当者の休み
    - 既存予約との時間重複（開始時刻だけでなく終了時刻まで）
    を見て、30分刻みで候補を返します。
    """
    shop = get_shop(shop_id) or {}
    selected_staff = next(
        (item for item in (shop.get("staff_list") or []) if str(item.get("id") or item.get("staff_id") or "") == str(staff_id)),
        None,
    )

    try:
        duration_minutes = max(30, int(duration or 60))
    except (TypeError, ValueError):
        duration_minutes = 60

    reservations = [r for r in get_reservations(shop_id) if str(r.get("status") or "") != "キャンセル"]
    staff_reservations = [r for r in reservations if str(r.get("staff_id") or "").strip() == str(staff_id or "").strip()]

    time_slots = _build_half_hour_slots(shop.get("business_hours"))
    now_dt = datetime.now(JST)
    today = now_dt.date()
    slots: list[dict] = []
    safe_offset = max(0, int(offset or 0))
    safe_limit = max(1, int(limit or 13))
    target_count = safe_offset + safe_limit

    for i in range(days):
        day = today + timedelta(days=i)
        if _is_shop_holiday(shop, day) or _is_staff_holiday(selected_staff, day):
            continue

        date_text = day.isoformat()
        day_reservations = [r for r in staff_reservations if str(r.get("reservation_date") or "") == date_text]

        for start_text in time_slots:
            try:
                start_time_obj = datetime.strptime(start_text, "%H:%M").time()
            except ValueError:
                continue

            start_dt = datetime.combine(day, start_time_obj, tzinfo=JST)
            end_dt = start_dt + timedelta(minutes=duration_minutes)
            end_text = end_dt.strftime("%H:%M")

            # 直近すぎる枠・過去枠は出さない
            if start_dt <= now_dt + timedelta(hours=1):
                continue

            # 営業時間の最後を超える枠は出さない
            if time_slots:
                business_end_text = _parse_business_hours_range(shop.get("business_hours"))[1]
                try:
                    business_end_time = datetime.strptime(business_end_text, "%H:%M").time()
                    business_end_dt = datetime.combine(day, business_end_time, tzinfo=JST)
                    if end_dt > business_end_dt:
                        continue
                except ValueError:
                    pass

            # 既存予約と時間が1分でも重なる枠は出さない。
            if any(
                _line_reservation_overlaps(
                    reservation,
                    staff_id=str(staff_id or ""),
                    date_text=date_text,
                    start_text=start_dt.strftime("%H:%M"),
                    end_text=end_text,
                    default_duration=duration_minutes,
                )
                for reservation in day_reservations
            ):
                continue

            slots.append({
                "date": date_text,
                "start": start_dt.strftime("%H:%M"),
                "end": end_text,
                "label": f"{day.month}/{day.day} {start_dt.strftime('%H:%M')}",
            })
            if len(slots) >= target_count:
                return slots[safe_offset:target_count]

    return slots[safe_offset:target_count]


def _line_datetime_page_labels(shop_id: str, staff_id: str, duration: int, page: int = 0) -> tuple[list[str], bool]:
    page_size = 12
    safe_page = max(0, int(page or 0))
    offset = safe_page * page_size
    slots = _line_slot_options(shop_id, staff_id, duration, limit=page_size + 1, offset=offset) or []
    visible_slots = slots[:page_size]
    labels = [str(slot.get("label") or "") for slot in visible_slots if str(slot.get("label") or "").strip()]
    has_next = len(slots) > page_size
    if has_next:
        labels.append("次へ")
    return labels, has_next


def _line_datetime_page_message(page: int = 0) -> str:
    safe_page = max(0, int(page or 0))
    if safe_page <= 0:
        return "日時を選んでください。"
    return f"日時を選んでください。（{safe_page + 1}ページ目）"


def _line_customer_from_member(shop_id: str, member: dict) -> dict | None:
    """会員情報を優先した顧客dictを返します。

    非会員時代の顧客名が残っていても、会員登録後は members の氏名・電話番号を
    予約表示と保存に使います。
    """
    if not member:
        return None
    customer_id = int((member or {}).get("customer_id") or 0)
    customer = get_customer_by_id(shop_id, customer_id) if customer_id else None
    if not customer:
        return None
    result = dict(customer)
    result["name"] = str((member or {}).get("name") or result.get("name") or "").strip()
    result["phone"] = normalize_member_phone((member or {}).get("phone") or (member or {}).get("phone_normalized") or result.get("phone") or "")
    result["email"] = str((member or {}).get("email") or result.get("email") or "").strip()
    result["is_member"] = True
    result["member_id"] = int((member or {}).get("id") or 0)
    return result


def get_customer_by_line_user_id(shop_id: str, line_user_id: str) -> dict | None:
    """LINE user_idに紐づく顧客を返します。

    同じLINE user_idが、過去の非会員顧客と会員顧客の両方に残っている場合は、
    必ず会員顧客を優先します。
    """
    ensure_customer_line_user_id_schema()
    clean_shop_id = str(shop_id or "").strip()
    clean_line_user_id = str(line_user_id or "").strip()
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, shop_id, name, phone, email, created_at
            FROM customers
            WHERE shop_id = ? AND line_user_id = ?
            ORDER BY id DESC
            """,
            (clean_shop_id, clean_line_user_id),
        ).fetchall()
    customers = [dict(row) for row in rows]

    # まず、LINEに紐づいている顧客の電話番号から会員情報を探し、会員顧客を優先する。
    for customer in customers:
        phone = normalize_member_phone(customer.get("phone") or "")
        member = get_member_by_phone_normalized(clean_shop_id, phone) if phone else None
        member_customer_id = int((member or {}).get("customer_id") or 0)
        if member and member_customer_id:
            preferred = _line_customer_from_member(clean_shop_id, member)
            if preferred:
                return preferred

    return customers[0] if customers else None


def is_real_line_customer(customer: dict | None) -> bool:
    """LINE予約で自動入力を省略してよい「会員顧客」か判定します。

    非会員予約でも名前・電話番号・LINE user_id は顧客リストへ保存しますが、
    次回も会員登録を案内したいので、電話番号に対応する会員データがある場合だけ
    登録済み扱いにします。
    """
    if not customer:
        return False

    name = str(customer.get("name") or "").strip()
    phone = normalize_member_phone(customer.get("phone") or "")
    if not name or name == "LINE予約" or name.startswith("LINE予約"):
        return False
    if "LINE予約" in name and name.endswith("）"):
        return False
    if not phone:
        return False

    try:
        member = get_member_by_phone_normalized(str(customer.get("shop_id") or ""), phone)
        if not member:
            return False
        member_customer_id = int(member.get("customer_id") or 0)
        customer_id = int(customer.get("id") or 0)
        return bool(member_customer_id == customer_id or (not customer_id and member_customer_id))
    except Exception as exc:
        print("line real customer check error:", repr(exc))
        return False

def build_line_member_register_url(shop_id: str, line_user_id: str) -> str:
    """LINE予約から会員登録へ進むためのURLを作ります。

    登録完了後に会員ページへ飛ばすだけだとLINEに戻りにくいため、
    LINE連携完了ページを next に入れます。
    """
    from urllib.parse import quote

    clean_shop_id = str(shop_id or "").strip()
    clean_line_user_id = str(line_user_id or "").strip()
    next_url = f"/member/{clean_shop_id}/line-register-complete?line_user_id={quote(clean_line_user_id, safe='')}"
    return f"https://www.rakubai.net/member/{clean_shop_id}/register?next={quote(next_url, safe='')}"


def extract_line_user_id_from_next_url(next_url: str) -> str:
    """会員登録のnext_urlに含めたline_user_idを取り出します。"""
    from urllib.parse import parse_qs, urlparse

    try:
        parsed = urlparse(str(next_url or ""))
        values = parse_qs(parsed.query).get("line_user_id") or []
        return str(values[0] or "").strip() if values else ""
    except Exception:
        return ""


def parse_line_customer_info(text: str) -> tuple[str, str] | None:
    """「山田太郎 09012345678」のような入力から名前と電話番号を取り出します。"""
    raw = str(text or "").strip()
    phone_match = re.search(r"0\d[\d\-\s]{8,}\d", raw)
    if not phone_match:
        return None
    phone_raw = phone_match.group(0)
    phone = normalize_member_phone(phone_raw)
    name = (raw[:phone_match.start()] + raw[phone_match.end():]).strip(" 　,，、:：\n\t")
    if not name or not phone:
        return None
    return name, phone


def ensure_line_customer_for_reservation(shop_id: str, line_user_id: str, name: str, phone: str) -> dict:
    """入力された名前・電話番号から顧客を作成または更新し、LINE user_idを紐づけます。"""
    clean_name = str(name or "").strip()
    clean_phone = normalize_member_phone(phone)
    customer = None

    linked_customer = get_customer_by_line_user_id(shop_id, line_user_id)
    if linked_customer:
        customer = update_customer_contact(
            shop_id,
            int(linked_customer.get("id") or 0),
            clean_name,
            clean_phone,
            str(linked_customer.get("email") or ""),
        ) or linked_customer

    if not customer and clean_phone:
        member = get_member_by_phone_normalized(shop_id, clean_phone)
        if member:
            clean_name = str(member.get("name") or clean_name).strip()
            customer_id = int(member.get("customer_id") or 0)
            if customer_id:
                customer = get_customer_by_id(shop_id, customer_id)

    if not customer:
        customer = find_customer(shop_id, clean_name, clean_phone, "")
    if not customer:
        customer = create_customer(shop_id, clean_name, clean_phone, "")
    else:
        customer = update_customer_contact(shop_id, int(customer.get("id") or 0), clean_name, clean_phone, str(customer.get("email") or "")) or customer

    try:
        update_customer_line_user_id(shop_id, int(customer.get("id") or 0), line_user_id)
    except Exception as exc:
        print("line customer link error:", repr(exc))
    return customer

def handle_line_complete_reservation_flow(shop_id: str, user_id: str, access_token: str, message_text: str = "", postback_data: str = "") -> dict:
    """LINE完結予約フロー。

    Quick Reply は MessageAction（押すと通常のテキストが送られる）で統一します。
    postback_data が来た場合も互換のため処理できます。
    """
    shop = get_shop(shop_id)
    if not shop:
        return send_line_message(access_token, user_id, "店舗情報が見つかりませんでした。")

    text = str(message_text or "").strip()
    data = str(postback_data or "").strip()
    normalized_text = text.replace(" ", "").replace("　", "").strip()

    def qr(label: str, send_text: str | None = None) -> dict:
        label_text = str(label or "").strip()[:20]
        return {
            "type": "action",
            "action": {
                "type": "message",
                "label": label_text,
                "text": str(send_text or label or "").strip()[:300],
            },
        }

    def reply_options(message: str, labels: list[str]) -> dict:
        return send_line_selection_buttons(access_token, user_id, message, labels)

    def find_by_text(options: list[dict], value: str) -> dict | None:
        raw = str(value or "").strip()
        compact = raw.replace(" ", "").replace("　", "")
        for item in options:
            item_id = str(item.get("id") or "").strip()
            name = str(item.get("name") or "").strip()
            candidates = {item_id, name, f"{item_id}. {name}", f"{item_id} {name}"}
            candidates_compact = {c.replace(" ", "").replace("　", "") for c in candidates}
            if raw in candidates or compact in candidates_compact:
                return item
        return None

    cancel_words = {"キャンセル", "中止", "やめる", "取消", "取り消し", "いいえ"}
    if normalized_text in cancel_words or data in {"line_reserve:confirm:no", "line_reserve:cancel"}:
        clear_line_reservation_session(shop_id, user_id)
        return send_line_message(access_token, user_id, "予約操作をキャンセルしました。")

    session = get_line_reservation_session(shop_id, user_id) or {}

    # 「非会員で予約する」のような選択肢にも「予約」が含まれるため、
    # 部分一致ではなく開始キーワードの完全一致だけで予約フローを開始します。
    # 予約開始時に、先に会員/非会員状態を判定します。
    start_words = {"予約", "予約する", "予約開始"}
    if normalized_text in start_words or data == "line_reserve:start":
        linked_customer = get_customer_by_line_user_id(shop_id, user_id)
        staff_options = _line_staff_options(shop)
        if not staff_options:
            return send_line_message(access_token, user_id, "現在、選択できる担当者が登録されていません。")

        base_values = dict(
            staff_id="", staff_name="", menu_id="", menu_name="", duration=0, price=0,
            reservation_date="", start_time="", end_time="", customer_id=0, customer_name="", customer_phone="",
        )

        if is_real_line_customer(linked_customer):
            member_values = dict(base_values)
            member_values.update(
                customer_id=int(linked_customer.get("id") or 0),
                customer_name=str(linked_customer.get("name") or ""),
                customer_phone=str(linked_customer.get("phone") or ""),
            )
            upsert_line_reservation_session(shop_id, user_id, step="select_staff", **member_values)
            return reply_options(
                f"{linked_customer.get('name')}様の会員情報で予約を開始します。\n担当者を選んでください。",
                [s["name"] for s in staff_options],
            )

        previous_name = str((linked_customer or {}).get("name") or "").strip()
        previous_phone = normalize_member_phone((linked_customer or {}).get("phone") or "")
        has_previous_guest_info = bool(linked_customer and previous_name and previous_phone and not previous_name.startswith("LINE予約"))
        register_url = build_line_member_register_url(shop_id, user_id)
        if has_previous_guest_info:
            guest_values = dict(base_values)
            guest_values.update(
                customer_id=int((linked_customer or {}).get("id") or 0),
                customer_name=previous_name,
                customer_phone=previous_phone,
            )
            upsert_line_reservation_session(shop_id, user_id, step="select_customer_type", **guest_values)
            return reply_options(
                "前回は非会員としてこちらの情報で予約されています。\n\n"
                f"お名前：{previous_name}\n"
                f"電話番号：{previous_phone}\n\n"
                "会員登録すると次回以降は会員情報を優先して予約できます。\n"
                f"{register_url}\n\n"
                "今回はどうしますか？",
                ["この情報で予約する", "情報を変更する", "会員登録URLを表示"],
            )

        upsert_line_reservation_session(shop_id, user_id, step="select_customer_type", **base_values)
        return reply_options(
            "初回のため、お客様情報が必要です。\n\n"
            "会員登録する場合はこちらから登録してください。\n"
            f"{register_url}\n\n"
            "非会員のまま予約する場合は「非会員で予約する」を選んでください。",
            ["非会員で予約する", "会員登録URLを表示"],
        )

    step = str(session.get("step") or "")

    if step == "select_staff":
        staff_options = _line_staff_options(shop)
        staff_id_from_postback = data.split(":", 2)[2] if data.startswith("line_reserve:staff:") else ""
        staff = _line_find(staff_options, staff_id_from_postback) if staff_id_from_postback else find_by_text(staff_options, text)
        if not staff:
            return reply_options("担当者を選んでください。", [s["name"] for s in staff_options])
        menus = _line_menu_options(shop)
        if not menus:
            clear_line_reservation_session(shop_id, user_id)
            return send_line_message(access_token, user_id, "現在、選択できるメニューが登録されていません。")
        upsert_line_reservation_session(shop_id, user_id, step="select_menu", staff_id=staff["id"], staff_name=staff["name"])
        return reply_options(f"{staff['name']}を選択しました。\nメニューを選んでください。", [m["name"] for m in menus])

    if step == "select_menu":
        menu_options = _line_menu_options(shop)
        menu_id_from_postback = data.split(":", 2)[2] if data.startswith("line_reserve:menu:") else ""
        menu = _line_find(menu_options, menu_id_from_postback) if menu_id_from_postback else find_by_text(menu_options, text)
        if not menu:
            return reply_options("メニューを選んでください。", [m["name"] for m in menu_options])
        labels, _has_next = _line_datetime_page_labels(shop_id, session.get("staff_id"), menu.get("duration") or 60, page=0)
        if not labels:
            clear_line_reservation_session(shop_id, user_id)
            return send_line_message(access_token, user_id, "選択できる日時がありませんでした。別の担当者でお試しください。")
        upsert_line_reservation_session(shop_id, user_id, step="select_datetime", menu_id=menu["id"], menu_name=menu["name"], duration=menu["duration"], price=menu["price"], slot_page=0)
        return reply_options(f"{menu['name']}を選択しました。\n日時を選んでください。", labels)

    if step == "select_datetime":
        selected = None
        current_page = max(0, int(session.get("slot_page") or 0))
        if normalized_text in {"次へ", "次の日時", "もっと見る"}:
            next_page = current_page + 1
            labels, _has_next = _line_datetime_page_labels(shop_id, session.get("staff_id"), int(session.get("duration") or 60), page=next_page)
            if not labels:
                labels, _has_next = _line_datetime_page_labels(shop_id, session.get("staff_id"), int(session.get("duration") or 60), page=current_page)
                return reply_options("これ以上表示できる日時はありません。\n日時を選んでください。", labels)
            upsert_line_reservation_session(shop_id, user_id, slot_page=next_page)
            return reply_options(_line_datetime_page_message(next_page), labels)
        if data.startswith("line_reserve:slot:"):
            # 互換用。data は line_reserve:slot:YYYY-MM-DD:HH:MM:HH:MM 形式。
            payload = data.replace("line_reserve:slot:", "", 1)
            slot_match = re.match(r"^(\d{4}-\d{2}-\d{2}):(\d{1,2}:\d{2}):(\d{1,2}:\d{2})$", payload)
            if slot_match:
                selected = {
                    "date": slot_match.group(1),
                    "start": slot_match.group(2),
                    "end": slot_match.group(3),
                    "label": f"{slot_match.group(1)} {slot_match.group(2)}",
                }
        else:
            slots = _line_slot_options(shop_id, session.get("staff_id"), int(session.get("duration") or 60), limit=500, offset=0) or []
            selected = next((slot for slot in slots if str(slot.get("label") or "") == text), None)
        if not selected:
            labels, _has_next = _line_datetime_page_labels(shop_id, session.get("staff_id"), int(session.get("duration") or 60), page=current_page)
            return reply_options(_line_datetime_page_message(current_page), labels)
        linked_customer = get_customer_by_line_user_id(shop_id, user_id)
        if is_real_line_customer(linked_customer):
            session = upsert_line_reservation_session(
                shop_id, user_id, step="confirm",
                reservation_date=selected.get("date") or "", start_time=selected.get("start") or "", end_time=selected.get("end") or "",
                customer_id=int(linked_customer.get("id") or 0),
                customer_name=str(linked_customer.get("name") or ""),
                customer_phone=str(linked_customer.get("phone") or ""),
            ) or {}
            confirm_text = (
                "この内容で予約しますか？\n\n"
                + f"お名前：{session.get('customer_name')}（LINE予約）\n"
                + f"電話番号：{session.get('customer_phone') or '登録なし'}\n"
                + f"担当者：{session.get('staff_name')}\n"
                + f"メニュー：{session.get('menu_name')}\n"
                + f"日時：{session.get('reservation_date')} {session.get('start_time')}"
            )
            return reply_options(confirm_text, ["はい", "いいえ"])

        previous_name = str((linked_customer or {}).get("name") or "").strip()
        previous_phone = normalize_member_phone((linked_customer or {}).get("phone") or "")
        has_previous_guest_info = bool(linked_customer and previous_name and previous_phone and not previous_name.startswith("LINE予約"))
        upsert_line_reservation_session(
            shop_id, user_id, step="select_customer_type",
            reservation_date=selected.get("date") or "", start_time=selected.get("start") or "", end_time=selected.get("end") or "",
            customer_id=int((linked_customer or {}).get("id") or 0) if has_previous_guest_info else 0,
            customer_name=previous_name if has_previous_guest_info else "",
            customer_phone=previous_phone if has_previous_guest_info else "",
        )
        register_url = build_line_member_register_url(shop_id, user_id)
        if has_previous_guest_info:
            return reply_options(
                "前回は非会員としてこちらの情報で予約されています。\n\n"
                f"お名前：{previous_name}\n"
                f"電話番号：{previous_phone}\n\n"
                "会員登録すると次回以降の予約確認がスムーズになります。\n"
                f"{register_url}\n\n"
                "今回はどうしますか？",
                ["この情報で予約する", "情報を変更する", "会員登録URLを表示"]
            )

        return reply_options(
            "初回のため、お客様情報が必要です。\n\n"
            "会員登録する場合はこちらから登録してください。\n"
            f"{register_url}\n\n"
            "非会員のまま予約する場合は「非会員で予約する」を選んでください。",
            ["非会員で予約する", "会員登録URLを表示"]
        )

    if step == "select_customer_type":
        if normalized_text in {"この情報で予約する", "前回の情報で予約する", "この情報で進む"}:
            customer_id = int(session.get("customer_id") or 0)
            customer_name = str(session.get("customer_name") or "").strip()
            customer_phone = str(session.get("customer_phone") or "").strip()
            if customer_id and customer_name:
                # 予約開始直後に非会員情報を確認した場合は、ここから担当者選択へ進む。
                if not str(session.get("staff_id") or "").strip():
                    staff_options = _line_staff_options(shop)
                    session = upsert_line_reservation_session(shop_id, user_id, step="select_staff") or {}
                    return reply_options(
                        f"{customer_name}様の情報で予約を進めます。\n担当者を選んでください。",
                        [s["name"] for s in staff_options],
                    )
                session = upsert_line_reservation_session(shop_id, user_id, step="confirm") or {}
                confirm_text = (
                    "この内容で予約しますか？\n\n"
                    + f"お名前：{session.get('customer_name')}（LINE予約）\n"
                    + f"電話番号：{session.get('customer_phone')}\n"
                    + f"担当者：{session.get('staff_name')}\n"
                    + f"メニュー：{session.get('menu_name')}\n"
                    + f"日時：{session.get('reservation_date')} {session.get('start_time')}"
                )
                return reply_options(confirm_text, ["はい", "いいえ"])
            upsert_line_reservation_session(shop_id, user_id, step="input_customer_info")
            return send_line_message(access_token, user_id, "お名前と電話番号を送信してください。\n例：山田太郎 09012345678")

        if normalized_text in {"情報を変更する", "変更する", "非会員で予約する", "非会員", "会員登録しない"}:
            upsert_line_reservation_session(shop_id, user_id, step="input_customer_info")
            return send_line_message(
                access_token,
                user_id,
                "非会員予約として、お名前と電話番号を送信してください。\n\n例：山田太郎 09012345678\n\n入力内容は顧客リストに保存します。次回は前回情報を確認して会員登録をご案内します。"
            )
        if normalized_text in {"会員登録urlを表示", "会員登録URLを表示", "会員登録する", "会員登録"}:
            register_url = build_line_member_register_url(shop_id, user_id)
            return reply_options(
                "会員登録はこちらから行ってください。\n"
                f"{register_url}\n\n"
                "登録完了後、このLINEと顧客情報が紐づきます。\n"
                "非会員で進める場合は「非会員で予約する」を選んでください。",
                ["非会員で予約する"]
            )
        if str(session.get("customer_name") or "").strip() and str(session.get("customer_phone") or "").strip():
            return reply_options(
                "前回の情報で予約するか、情報を変更するか、会員登録するかを選んでください。",
                ["この情報で予約する", "情報を変更する", "会員登録URLを表示"]
            )
        return reply_options(
            "会員登録するか、非会員で予約するかを選んでください。",
            ["非会員で予約する", "会員登録URLを表示"]
        )
    if step == "input_customer_info":
        parsed = parse_line_customer_info(text)
        if not parsed:
            return send_line_message(
                access_token,
                user_id,
                "お名前と電話番号を送信してください。\n例：山田太郎 09012345678"
            )
        input_name, input_phone = parsed
        customer = ensure_line_customer_for_reservation(shop_id, user_id, input_name, input_phone)
        next_step = "confirm" if str(session.get("staff_id") or "").strip() else "select_staff"
        session = upsert_line_reservation_session(
            shop_id, user_id, step=next_step,
            customer_id=int(customer.get("id") or 0),
            customer_name=str(customer.get("name") or input_name),
            customer_phone=str(customer.get("phone") or input_phone),
        ) or {}
        if next_step == "select_staff":
            staff_options = _line_staff_options(shop)
            return reply_options(
                "お客様情報を保存しました。\n担当者を選んでください。",
                [s["name"] for s in staff_options],
            )
        confirm_text = (
            "この内容で予約しますか？\n\n"
            + f"お名前：{session.get('customer_name')}（LINE予約）\n"
            + f"電話番号：{session.get('customer_phone')}\n"
            + f"担当者：{session.get('staff_name')}\n"
            + f"メニュー：{session.get('menu_name')}\n"
            + f"日時：{session.get('reservation_date')} {session.get('start_time')}"
        )
        return reply_options(confirm_text, ["はい", "いいえ"])

    if step == "confirm":
        yes_values = {"はい", "予約する", "確定", "お願いします", "yes", "YES"}
        if data == "line_reserve:confirm:yes" or normalized_text in yes_values:
            session = get_line_reservation_session(shop_id, user_id) or {}
            customer_id = int(session.get("customer_id") or 0)
            customer_name = str(session.get("customer_name") or "").strip()
            customer_phone = str(session.get("customer_phone") or "").strip()
            if not customer_id or not customer_name:
                linked_customer = get_customer_by_line_user_id(shop_id, user_id)
                if is_real_line_customer(linked_customer):
                    customer_id = int(linked_customer.get("id") or 0)
                    customer_name = str(linked_customer.get("name") or "").strip()
                    customer_phone = str(linked_customer.get("phone") or "").strip()
            if not customer_id or not customer_name:
                upsert_line_reservation_session(shop_id, user_id, step="select_customer_type")
                register_url = build_line_member_register_url(shop_id, user_id)
                return reply_options(
                    "初回のため、お客様情報が必要です。\n\n"
                    "会員登録する場合はこちらから登録してください。\n"
                    f"{register_url}\n\n"
                    "非会員のまま予約する場合は「非会員で予約する」を選んでください。",
                    ["非会員で予約する", "会員登録URLを表示"]
                )
            reservation_customer_name = f"{customer_name}（LINE予約）"
            duration_minutes = int(session.get("duration") or 60)
            reservation_date = str(session.get("reservation_date") or "")
            start_time = str(session.get("start_time") or "")[:5]
            end_time = str(session.get("end_time") or "")[:5]
            start_minutes = _parse_hhmm_to_minutes(start_time)
            end_minutes = _parse_hhmm_to_minutes(end_time)
            if start_minutes is not None and (end_minutes is None or end_minutes <= start_minutes):
                end_time = _format_minutes_hhmm(start_minutes + max(30, duration_minutes))

            if _line_has_reservation_conflict(shop_id, str(session.get("staff_id") or ""), reservation_date, start_time, end_time, duration_minutes):
                labels, _has_next = _line_datetime_page_labels(shop_id, session.get("staff_id"), duration_minutes, page=0)
                upsert_line_reservation_session(shop_id, user_id, step="select_datetime", reservation_date="", start_time="", end_time="", slot_page=0)
                if labels:
                    return reply_options("申し訳ありません。その日時は先に予約が入りました。別の日時を選んでください。", labels)
                clear_line_reservation_session(shop_id, user_id)
                return send_line_message(access_token, user_id, "申し訳ありません。その日時は先に予約が入りました。現在選択できる日時がありません。")

            reservation = create_reservation(shop_id=shop_id, customer_id=customer_id, customer_name=reservation_customer_name, customer_email="", receive_email=0, staff_id=int(session.get("staff_id") or 0), staff_name=str(session.get("staff_name") or ""), menu_id=int(session.get("menu_id") or 0), menu_name=str(session.get("menu_name") or ""), duration=duration_minutes, price=int(session.get("price") or 0), reservation_date=reservation_date, start_time=start_time, end_time=end_time, status="予約済み", source="line")
            clear_line_reservation_session(shop_id, user_id)
            return send_line_message(access_token, user_id, f"予約が完了しました。\nお名前：{reservation.get('customer_name')}\nご来店をお待ちしております。")
        return reply_options("予約する場合は「はい」を選んでください。", ["はい", "いいえ"])

    return reply_options("予約を始める場合は「予約」と送信してください。", ["予約"])



def ensure_customer_line_user_id_schema() -> None:
    """customers テーブルに line_user_id カラムが無ければ自動追加します。"""
    try:
        with get_connection() as conn:
            columns = [
                str(row[1])
                for row in conn.execute("PRAGMA table_info(customers)").fetchall()
            ]
            if "line_user_id" not in columns:
                conn.execute("ALTER TABLE customers ADD COLUMN line_user_id TEXT")
                conn.commit()
                print("customers.line_user_id column added")
    except Exception as exc:
        print("ensure_customer_line_user_id_schema error:", repr(exc))


def update_customer_line_user_id(shop_id: str, customer_id: int, line_user_id: str) -> None:
    """顧客にLINE user_idを紐づけます。

    会員登録後も過去の非会員顧客に同じLINE user_idが残ると、
    LINE予約時に非会員情報が優先表示されてしまうため、同一店舗内の
    他顧客からは先にLINE user_idを外してから、対象顧客へ付け替えます。
    """
    line_user_id = str(line_user_id or "").strip()
    if not line_user_id:
        return

    ensure_customer_line_user_id_schema()

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE customers
            SET line_user_id = NULL
            WHERE shop_id = ? AND line_user_id = ? AND id <> ?
            """,
            (str(shop_id or "").strip(), line_user_id, int(customer_id)),
        )
        conn.execute(
            """
            UPDATE customers
            SET line_user_id = ?
            WHERE shop_id = ? AND id = ?
            """,
            (line_user_id, str(shop_id or "").strip(), int(customer_id)),
        )
        conn.commit()


def build_reservation_line_message(shop: dict, reservation: dict) -> str:
    """予約完了LINEの本文を作ります。"""
    shop_name = str((shop or {}).get("shop_name") or "店舗")
    customer_name = str((reservation or {}).get("customer_name") or "")
    reservation_date = str((reservation or {}).get("reservation_date") or "")
    start_time = str((reservation or {}).get("start_time") or "")
    end_time = str((reservation or {}).get("end_time") or "")
    staff_name = str((reservation or {}).get("staff_name") or "")
    menu_name = str((reservation or {}).get("menu_name") or "")

    return (
        f"{customer_name}様\n"
        f"{shop_name}のご予約ありがとうございます。\n\n"
        f"■日時\n{reservation_date} {start_time}-{end_time}\n\n"
        f"■メニュー\n{menu_name}\n\n"
        f"■担当\n{staff_name}\n\n"
        f"ご来店をお待ちしております。"
    )


def _request_client_ip(request: Request) -> str:
    forwarded_for = str(request.headers.get("x-forwarded-for") or "").strip()
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    client = request.client
    return str(client.host if client else "")


def _current_actor_context(request: Request) -> dict[str, str]:
    if bool(request.session.get("platform_logged_in")):
        return {
            "actor_type": "platform_admin",
            "actor_id": str(PLATFORM_ADMIN.get("login_id") or ""),
            "actor_name": str(request.session.get("platform_admin_name") or PLATFORM_ADMIN.get("name") or "運営管理者"),
            "shop_id": "",
        }
    store_shop_id = str(request.session.get("store_logged_in_shop_id") or "").strip()
    if store_shop_id:
        return {
            "actor_type": "store_admin",
            "actor_id": str(request.session.get("store_logged_in_login_id") or store_shop_id),
            "actor_name": str(request.session.get("store_logged_in_admin_name") or ""),
            "shop_id": store_shop_id,
        }
    member_id = str(request.session.get("member_logged_in_id") or "").strip()
    member_shop_id = str(request.session.get("member_logged_in_shop_id") or "").strip()
    if member_id and member_shop_id:
        return {
            "actor_type": "member",
            "actor_id": member_id,
            "actor_name": str(request.session.get("member_logged_in_name") or ""),
            "shop_id": member_shop_id,
        }
    return {
        "actor_type": "anonymous",
        "actor_id": "",
        "actor_name": "",
        "shop_id": "",
    }


def _record_audit_log(
    request: Request,
    *,
    actor_type: str | None = None,
    actor_id: str | int | None = None,
    actor_name: str | None = None,
    action: str,
    shop_id: str | None = None,
    target_type: str = "",
    target_id: str | int | None = None,
    target_label: str = "",
    status: str = "success",
    detail: dict[str, object] | None = None,
) -> None:
    context = _current_actor_context(request)
    resolved_actor_type = (actor_type or context.get("actor_type") or "anonymous").strip()
    resolved_actor_id = str(actor_id if actor_id is not None else context.get("actor_id") or "").strip()
    resolved_actor_name = str(actor_name if actor_name is not None else context.get("actor_name") or "").strip()
    resolved_shop_id = str(shop_id if shop_id is not None else context.get("shop_id") or "").strip()
    try:
        create_audit_log(
            actor_type=resolved_actor_type,
            actor_id=resolved_actor_id,
            actor_name=resolved_actor_name,
            action=(action or "").strip(),
            shop_id=resolved_shop_id,
            target_type=(target_type or "").strip(),
            target_id="" if target_id is None else str(target_id),
            target_label=str(target_label or "").strip(),
            status=(status or "success").strip(),
            method=str(request.method or "").upper(),
            path=str(request.url.path or ""),
            ip_address=_request_client_ip(request),
            user_agent=str(request.headers.get("user-agent") or ""),
            detail=detail or {},
            retention_days=90,
        )
    except Exception:
        pass




def _get_audit_api_token() -> str:
    return str(os.getenv("AUDIT_API_TOKEN") or "").strip()


def _require_audit_api_token(request: Request) -> None:
    expected = _get_audit_api_token()
    if not expected:
        raise HTTPException(status_code=503, detail="AUDIT_API_TOKEN が未設定です")
    auth_header = str(request.headers.get("authorization") or "").strip()
    provided = ""
    if auth_header.lower().startswith("bearer "):
        provided = auth_header[7:].strip()
    if not provided:
        provided = str(request.headers.get("x-api-token") or "").strip()
    if secrets.compare_digest(provided, expected):
        return
    raise HTTPException(status_code=401, detail="Unauthorized")




def _get_audit_admin_password() -> str:
    return str(os.getenv("AUDIT_ADMIN_PASSWORD") or "").strip()


def _require_audit_admin_password(password: str) -> None:
    expected = _get_audit_admin_password()
    if not expected:
        raise HTTPException(status_code=503, detail="AUDIT_ADMIN_PASSWORD が未設定です")
    provided = str(password or "").strip()
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="管理パスワードが違います")


def _pick(data: dict[str, object], key: str, default: str = "") -> str:
    return str(data.get(key) or default).strip()

def _normalize_api_datetime(value: str | None, *, end_of_day: bool = False) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    raw = raw.replace("Z", "+00:00")
    candidates = [raw]
    if len(raw) == 10:
        candidates.append(raw + (" 23:59:59" if end_of_day else " 00:00:00"))
        candidates.append(raw + ("T23:59:59" if end_of_day else "T00:00:00"))
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is not None:
                dt = dt.astimezone(JST).replace(tzinfo=None)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise HTTPException(status_code=400, detail="日時形式が不正です")


def _serialize_audit_log_row(row: dict[str, object]) -> dict[str, object]:
    detail_raw = str(row.get("detail_json") or "{}")
    try:
        detail = json.loads(detail_raw)
    except Exception:
        detail = detail_raw

    occurred_at = row.get("occurred_at")
    if occurred_at:
        try:
            dt = datetime.fromisoformat(str(occurred_at))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            occurred_at = dt.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            occurred_at = str(occurred_at)

    return {
        "id": row.get("id"),
        "occurred_at": occurred_at or "",
        "shop_id": row.get("shop_id") or "",
        "shop_name": row.get("shop_name") or "",
        "actor_type": row.get("actor_type") or "",
        "actor_id": row.get("actor_id") or "",
        "actor_name": row.get("actor_name") or "",
        "member_name": row.get("member_name") or "",
        "action": row.get("action") or "",
        "target_type": row.get("target_type") or "",
        "target_id": row.get("target_id") or "",
        "target_label": row.get("target_label") or "",
        "status": row.get("status") or "",
        "method": row.get("method") or "",
        "path": row.get("path") or "",
        "ip_address": row.get("ip_address") or "",
        "user_agent": row.get("user_agent") or "",
        "detail": detail,
    }


def _format_chat_datetime(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return raw[:16]
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(JST).strftime("%Y-%m-%d %H:%M")


PLAN_DETAILS = {
    "free": {
        "code": "free",
        "name": "フリー",
        "tagline": "まず試したい方向け",
        "description": "初めて予約システムを導入する店舗や、まずは小さく始めたい個人店に向いた無料プランです。予約受付・顧客管理・LINE連携など、運用の土台になる機能をしっかり備えています。",
        "price_text": "初期費用0円・月額0円で、必要な基本機能から始められるプランです。",
        "price_display": "0円",
        "target": "まず試したい方向け",
        "badge": "はじめて導入に",
        "summary_title": "まずは無料で導入したい店舗に",
        "summary_text": "予約管理を紙やLINEだけで回していて、まずは業務を整理したい店舗におすすめです。必要な基本機能をおさえながら、コストをかけずに運用をスタートできます。",
        "best_for": [
            "これから予約システムを使い始めたい",
            "小規模でまず試験導入したい",
            "費用をかけずに基本運用を整えたい",
        ],
        "features": [
            "初期費用0円",
            "予約受付月間50人まで",
            "顧客管理50人まで",
            "顧客1人につき写真1枚まで保存可能",
            "登録スタッフ3人まで",
            "LINE連携",
            "ホームページ作成無料",
            "チャット機能（月間メッセージ100通まで）",
        ],
        "max_staff": 3,
        "max_customers": 50,
        "max_reservations": 50,
        "max_photos_per_customer": 1,
        "max_chat_messages": 100,
        "cta_href": "/signup",
        "cta_label": "無料ではじめる",
    },
    "standard": {
        "code": "standard",
        "name": "スタンダード",
        "tagline": "しっかり運用したい方向け",
        "description": "予約数やチャット数を気にせず、日々の予約受付と顧客管理をしっかり回したい店舗向けのプランです。成長中の店舗や、安定して集客・運用していきたい店舗にちょうどよい内容です。",
        "price_text": "日常運用に必要な機能をバランスよく備えた人気プランです。",
        "price_display": "1,650円(税込)",
        "target": "しっかり運用したい方向け",
        "badge": "おすすめ",
        "summary_title": "運用のしやすさと拡張性のバランスが良い",
        "summary_text": "予約受付を無制限で使え、顧客情報や写真も十分に管理できます。個人店から少人数サロン、スタッフ数が増えてきた店舗まで、幅広く使いやすい中心プランです。",
        "best_for": [
            "予約件数を気にせず運用したい",
            "顧客情報や写真をしっかり残したい",
            "スタッフが増えてきて管理を整えたい",
        ],
        "features": [
            "初期費用0円",
            "予約受付無制限",
            "顧客管理300人まで",
            "顧客1人につき写真10枚まで保存可能",
            "登録スタッフ10人まで",
            "LINE連携",
            "ホームページ作成無料",
            "チャット機能（無制限）",
        ],
        "cta_href": "/signup",
        "cta_label": "無料ではじめる",
    },
    "premium": {
        "code": "premium",
        "name": "プレミアム",
        "tagline": "複数店舗・本格運用向け",
        "description": "複数店舗の運営や、本格的な顧客管理・スタッフ管理まで見据えた上位プランです。運用規模が大きい店舗でも、余裕を持って使える構成にしています。",
        "price_text": "複数店舗管理や優先対応まで含めた上位プランです。",
        "price_display": "3,300円(税込)",
        "target": "複数店舗・本格運用向け",
        "badge": "上位プラン",
        "summary_title": "本格運用や多店舗展開に対応",
        "summary_text": "顧客数・写真保存・スタッフ数に大きな余裕があり、複数店舗管理にも対応します。事業拡大中の店舗や、運用負荷を下げながら安定稼働したい店舗に向いています。",
        "best_for": [
            "複数店舗をまとめて管理したい",
            "大人数のスタッフ運用に対応したい",
            "サポート体制も重視したい",
        ],
        "features": [
            "初期費用0円",
            "予約受付無制限",
            "顧客管理無制限",
            "顧客1人につき写真50枚まで保存可能",
            "登録スタッフ50人まで",
            "LINE連携",
            "ホームページ作成無料",
            "チャット機能（無制限）",
            "タイムラインカルテ",
            "複数店舗管理",
            "優先対応",
        ],
        "cta_href": "/#contact",
        "cta_label": "お問合せ",
    },
}

PLAN_COMPARISON_ROWS = [
    {"label": "初期費用", "cells": {"free": "0円", "standard": "0円", "premium": "0円"}},
    {"label": "予約受付", "cells": {"free": "月間50人まで", "standard": "無制限", "premium": "無制限"}},
    {"label": "顧客管理", "cells": {"free": "50人まで", "standard": "300人まで", "premium": "無制限"}},
    {"label": "顧客1人あたりの写真保存", "cells": {"free": "1枚まで", "standard": "10枚まで", "premium": "50枚まで"}},
    {"label": "登録スタッフ数", "cells": {"free": "3人まで", "standard": "10人まで", "premium": "50人まで"}},
    {"label": "LINE連携", "cells": {"free": "対応", "standard": "対応", "premium": "対応"}},
    {"label": "ホームページ作成", "cells": {"free": "無料", "standard": "無料", "premium": "無料"}},
    {"label": "チャット機能", "cells": {"free": "月間メッセージ100通まで", "standard": "無制限", "premium": "無制限"}},
    {"label": "タイムラインカルテ", "cells": {"free": "—", "standard": "—", "premium": "対応"}},
    {"label": "複数店舗管理", "cells": {"free": "—", "standard": "—", "premium": "対応"}},
    {"label": "優先対応", "cells": {"free": "—", "standard": "—", "premium": "対応"}},
]


ADMIN_PLAN_CODE_ALIASES = {
    "free": ("free",),
    "standard": ("standard", "basic"),
    "premium": ("premium", "pro"),
}


def _format_plan_limit(limit_value: object, unit: str = "") -> str:
    if limit_value in (None, "", 0):
        return "無制限"
    try:
        numeric = int(limit_value)
    except (TypeError, ValueError):
        return f"{limit_value}{unit}" if unit else str(limit_value)
    if numeric >= 999999:
        return "無制限"
    return f"{numeric}{unit}" if unit else str(numeric)


def _resolve_admin_plan_id_map(available_plans: list[dict]) -> dict[str, int | None]:
    plan_by_code = {str(plan.get("code") or ""): int(plan.get("id") or 0) for plan in available_plans if plan.get("id")}
    resolved: dict[str, int | None] = {}
    for display_code, aliases in ADMIN_PLAN_CODE_ALIASES.items():
        resolved[display_code] = next((plan_by_code.get(alias) for alias in aliases if plan_by_code.get(alias)), None)
    return resolved


def _resolve_current_display_plan_code(subscription: dict | None) -> str:
    plan_code = str((subscription or {}).get("plan_code") or "").strip().lower()
    if plan_code in PLAN_DETAILS:
        return plan_code
    for display_code, aliases in ADMIN_PLAN_CODE_ALIASES.items():
        if plan_code in aliases:
            return display_code
    return "free"


def _resolve_display_plan_code_from_plan(plan: dict | None) -> str:
    plan_code = str((plan or {}).get("code") or "").strip().lower()
    if plan_code in PLAN_DETAILS:
        return plan_code
    for display_code, aliases in ADMIN_PLAN_CODE_ALIASES.items():
        if plan_code in aliases:
            return display_code
    return "free"


def _find_plan_by_id(available_plans: list[dict], plan_id: int) -> dict | None:
    for plan in available_plans:
        try:
            if int(plan.get("id") or 0) == int(plan_id):
                return plan
        except (TypeError, ValueError):
            continue
    return None


def _get_staff_limit_for_subscription(subscription: dict | None) -> int | None:
    plan_code = _resolve_current_display_plan_code(subscription)
    plan_detail = PLAN_DETAILS.get(plan_code, PLAN_DETAILS["free"])
    limit_value = plan_detail.get("max_staff")
    try:
        limit_number = int(limit_value)
    except (TypeError, ValueError):
        return None
    if limit_number <= 0:
        return None
    return limit_number


def _get_visible_staff_list(shop: dict | None, subscription: dict | None) -> list[dict]:
    staff_list = list((shop or {}).get("staff_list") or [])
    limit_number = _get_staff_limit_for_subscription(subscription)
    if limit_number is None:
        return staff_list
    return staff_list[:limit_number]


def _build_shop_with_visible_staff(shop: dict | None, subscription: dict | None) -> dict:
    shop_data = dict(shop or {})
    shop_data["staff_list"] = _decorate_staff_list_for_display(_get_visible_staff_list(shop_data, subscription))
    return shop_data


def _normalize_staff_default_avatar(value: str | None) -> str:
    normalized = str(value or '').strip().lower()
    return normalized if normalized in {'male', 'female'} else 'male'


def _get_default_staff_avatar_url(default_avatar: str | None) -> str:
    return '/static/default_staff_female.svg' if _normalize_staff_default_avatar(default_avatar) == 'female' else '/static/default_staff_male.svg'


def _resolve_staff_avatar_url(staff: dict | None) -> str:
    photo_url = str((staff or {}).get('photo_url') or '').strip()
    if photo_url:
        return photo_url
    return _get_default_staff_avatar_url((staff or {}).get('default_avatar'))


def _decorate_staff_for_display(staff: dict | None) -> dict:
    item = dict(staff or {})
    item['default_avatar'] = _normalize_staff_default_avatar(item.get('default_avatar'))
    item['avatar_url'] = _resolve_staff_avatar_url(item)
    return item


def _decorate_staff_list_for_display(staff_list: list[dict] | None) -> list[dict]:
    return [_decorate_staff_for_display(staff) for staff in (staff_list or [])]


def _decorate_shop_staff(shop: dict | None) -> dict:
    shop_data = dict(shop or {})
    shop_data['staff_list'] = _decorate_staff_list_for_display(shop_data.get('staff_list') or [])
    return shop_data


def _build_staff_lookup(staff_list: list[dict] | None) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for staff in _decorate_staff_list_for_display(staff_list):
        staff_key = str(staff.get('id') or '').strip()
        if staff_key:
            lookup[staff_key] = staff
    return lookup


def _attach_staff_avatar_to_reservation(reservation: dict | None, staff_lookup: dict[str, dict]) -> dict:
    item = dict(reservation or {})
    staff_key = str(item.get('staff_id') or '').strip()
    matched_staff = staff_lookup.get(staff_key) if staff_key else None
    if matched_staff:
        if not str(item.get('staff_name') or '').strip():
            item['staff_name'] = matched_staff.get('name') or ''
        item['staff_avatar_url'] = matched_staff.get('avatar_url') or _resolve_staff_avatar_url(matched_staff)
        item['staff_default_avatar'] = matched_staff.get('default_avatar') or 'male'
    else:
        item['staff_avatar_url'] = _get_default_staff_avatar_url('male')
        item['staff_default_avatar'] = 'male'
    return item


def _attach_staff_avatar_to_reservations(reservations: list[dict] | None, staff_list: list[dict] | None) -> list[dict]:
    staff_lookup = _build_staff_lookup(staff_list)
    return [_attach_staff_avatar_to_reservation(item, staff_lookup) for item in (reservations or [])]


def _save_staff_photo_file(shop_id: str, staff_id: int, upload: UploadFile) -> str:
    suffix = Path(upload.filename or 'photo.jpg').suffix or '.jpg'
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(4)}{suffix.lower()}"
    file_bytes = upload.file.read()

    s3_settings = _get_customer_photo_s3_settings()
    bucket = s3_settings['bucket']
    if bucket:
        region = s3_settings['region']
        prefix = s3_settings['prefix']
        endpoint_url = s3_settings['endpoint_url'] or None
        public_base_url = s3_settings['public_base_url']
        acl = s3_settings['acl']
        key_parts = [part for part in [prefix, shop_id, 'staff', str(staff_id), filename] if part]
        key = '/'.join(key_parts)
        content_type = (upload.content_type or '').strip() or 'application/octet-stream'

        client_kwargs = {'region_name': region}
        if endpoint_url:
            client_kwargs['endpoint_url'] = endpoint_url
        s3 = boto3.client('s3', **client_kwargs)

        extra_args = {'ContentType': content_type}
        if acl:
            extra_args['ACL'] = acl

        try:
            s3.put_object(Bucket=bucket, Key=key, Body=file_bytes, **extra_args)
        except (ClientError, BotoCoreError) as e:
            raise HTTPException(status_code=500, detail=f'S3 upload failed: {str(e)}')

        if public_base_url:
            return f"{public_base_url}/{key}"
        if endpoint_url:
            return f"{endpoint_url.rstrip('/')}/{bucket}/{key}"
        return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

    staff_dir = Path('data/uploads/shops') / shop_id / 'staff' / str(staff_id)
    staff_dir.mkdir(parents=True, exist_ok=True)
    save_path = staff_dir / filename
    with save_path.open('wb') as fh:
        fh.write(file_bytes)
    relative = save_path.relative_to(Path('data/uploads'))
    return '/uploads/' + str(relative).replace('\\', '/')


def _delete_staff_photo_file(image_url: str) -> None:
    _delete_customer_photo_file(image_url)


def _get_visible_staff_or_404(shop: dict | None, subscription: dict | None, staff_id: int) -> tuple[list[dict], dict]:
    visible_staff_list = _get_visible_staff_list(shop, subscription)
    target_staff = next((staff for staff in visible_staff_list if int(staff.get("id") or 0) == int(staff_id)), None)
    if target_staff is None:
        raise HTTPException(status_code=404, detail="スタッフが見つかりません")
    return visible_staff_list, target_staff


def _build_free_plan_downgrade_summary(shop: dict, customers: list[dict], reservations: list[dict]) -> dict[str, object]:
    free_detail = PLAN_DETAILS["free"]
    free_staff_limit = int(free_detail.get("max_staff") or 0)
    free_customer_limit = int(free_detail.get("max_customers") or 0)
    free_monthly_limit = int(free_detail.get("max_reservations") or 0)
    free_photo_limit = int(free_detail.get("max_photos_per_customer") or 0)

    staff_count = len(shop.get("staff_list", []))
    current_month = date.today().strftime("%Y-%m")
    current_month_reservations = sum(
        1
        for item in reservations
        if str(item.get("reservation_date") or "").startswith(current_month) and str(item.get("status") or "") != "キャンセル"
    )

    hidden_photo_count = 0
    for customer in customers:
        customer_id = int(customer.get("id") or 0)
        if customer_id <= 0:
            continue
        photo_count = len(get_customer_photos(str(shop.get("shop_id") or ""), customer_id))
        hidden_photo_count += max(photo_count - free_photo_limit, 0)

    rows = [
        {
            "label": "スタッフ",
            "current": staff_count,
            "limit": free_staff_limit,
            "hidden": max(staff_count - free_staff_limit, 0),
            "message": "上限を超えた新しい順のスタッフは表示されません。",
        },
        {
            "label": "顧客",
            "current": len(customers),
            "limit": free_customer_limit,
            "hidden": max(len(customers) - free_customer_limit, 0),
            "message": "上限を超えた新しい順の顧客は表示されません。",
        },
        {
            "label": "当月予約",
            "current": current_month_reservations,
            "limit": free_monthly_limit,
            "hidden": max(current_month_reservations - free_monthly_limit, 0),
            "message": "上限を超えた新しい順の当月予約は表示されません。",
        },
        {
            "label": "顧客写真",
            "current": sum(len(get_customer_photos(str(shop.get("shop_id") or ""), int(customer.get("id") or 0))) for customer in customers if int(customer.get("id") or 0) > 0),
            "limit": f"各顧客{free_photo_limit}枚まで",
            "hidden": hidden_photo_count,
            "message": "各顧客で上限を超えた新しい順の写真は表示されません。",
        },
    ]
    impacted = [row for row in rows if int(row.get("hidden") or 0) > 0]
    return {
        "rows": rows,
        "impacted_rows": impacted,
        "has_impact": bool(impacted),
    }


def _build_admin_plan_context(
    subscription: dict | None,
    available_plans: list[dict],
    customers: list[dict],
    reservations: list[dict],
    staff_list: list[dict],
) -> dict[str, object]:
    current_display_code = _resolve_current_display_plan_code(subscription)
    current_plan_detail = PLAN_DETAILS.get(current_display_code, PLAN_DETAILS["free"])
    plan_id_map = _resolve_admin_plan_id_map(available_plans)

    plan_select_items: list[dict[str, object]] = []
    for display_code in ("free", "standard", "premium"):
        detail = PLAN_DETAILS[display_code]
        plan_id = plan_id_map.get(display_code)
        plan_select_items.append({
            "id": plan_id,
            "code": display_code,
            "name": detail["name"],
            "price_display": detail["price_display"],
            "selected": display_code == current_display_code,
            "disabled": plan_id is None,
        })

    comparison_plan_codes = ["free", "standard", "premium"]

    current_month = date.today().strftime("%Y-%m")
    current_month_reservations = sum(
        1
        for item in reservations
        if str(item.get("reservation_date") or "").startswith(current_month) and str(item.get("status") or "") != "キャンセル"
    )

    current_plan_usage_rows = [
        {"label": "予約受付", "current": f"当月{current_month_reservations}件", "limit": PLAN_COMPARISON_ROWS[1]["cells"][current_display_code]},
        {"label": "顧客管理", "current": f"{len(customers)}人", "limit": PLAN_COMPARISON_ROWS[2]["cells"][current_display_code]},
        {"label": "登録スタッフ数", "current": f"{len(staff_list)}人", "limit": PLAN_COMPARISON_ROWS[4]["cells"][current_display_code]},
        {"label": "LINE連携", "current": "対応", "limit": PLAN_COMPARISON_ROWS[5]["cells"][current_display_code]},
        {"label": "ホームページ作成", "current": "利用可", "limit": PLAN_COMPARISON_ROWS[6]["cells"][current_display_code]},
        {"label": "チャット機能", "current": "利用可", "limit": PLAN_COMPARISON_ROWS[7]["cells"][current_display_code]},
        {"label": "複数店舗管理", "current": "—", "limit": PLAN_COMPARISON_ROWS[8]["cells"][current_display_code]},
        {"label": "優先対応", "current": "—", "limit": PLAN_COMPARISON_ROWS[9]["cells"][current_display_code]},
    ]

    return {
        "current_plan_detail": current_plan_detail,
        "current_plan_display_code": current_display_code,
        "plan_select_items": plan_select_items,
        "comparison_plan_codes": comparison_plan_codes,
        "comparison_rows": PLAN_COMPARISON_ROWS,
        "plan_usage_rows": current_plan_usage_rows,
    }



WEEKDAY_MAP = {"月曜日": 0, "火曜日": 1, "水曜日": 2, "木曜日": 3, "金曜日": 4, "土曜日": 5, "日曜日": 6}




def _first_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            value = str(value).strip()
            if value:
                return value
    return ""


def _get_customer_photo_s3_settings() -> dict[str, str]:
    bucket = _first_env("S3_BUCKET", "AWS_S3_BUCKET", "AWS_BUCKET", "BUCKET_NAME", "S3_BUCKET_NAME")
    region = _first_env("AWS_REGION", "AWS_DEFAULT_REGION") or "ap-northeast-1"
    prefix = (_first_env("S3_PREFIX", "AWS_S3_PREFIX") or "shops").strip("/")
    endpoint_url = _first_env("S3_ENDPOINT_URL", "AWS_S3_ENDPOINT_URL")
    public_base_url = _first_env("S3_PUBLIC_BASE_URL", "AWS_S3_PUBLIC_BASE_URL").rstrip("/")
    acl = _first_env("S3_ACL", "AWS_S3_ACL")
    return {
        "bucket": bucket,
        "region": region,
        "prefix": prefix,
        "endpoint_url": endpoint_url,
        "public_base_url": public_base_url,
        "acl": acl,
    }
def _get_customer_photo_policy(subscription: dict | None) -> dict:
    label = str((subscription or {}).get("plan_name") or "現在のプラン")
    return {"enabled": True, "label": label, "max_photos": None}


def _save_customer_photo_file(shop_id: str, customer_id: int, upload: UploadFile) -> str:
    suffix = Path(upload.filename or "photo.jpg").suffix or ".jpg"
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(4)}{suffix.lower()}"
    file_bytes = upload.file.read()

    s3_settings = _get_customer_photo_s3_settings()
    bucket = s3_settings["bucket"]
    if bucket:
        region = s3_settings["region"]
        prefix = s3_settings["prefix"]
        endpoint_url = s3_settings["endpoint_url"] or None
        public_base_url = s3_settings["public_base_url"]
        acl = s3_settings["acl"]
        key_parts = [part for part in [prefix, shop_id, "customers", str(customer_id), filename] if part]
        key = "/".join(key_parts)
        content_type = (upload.content_type or "").strip() or "application/octet-stream"

        client_kwargs = {"region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        s3 = boto3.client("s3", **client_kwargs)

        extra_args = {"ContentType": content_type}
        if acl:
            extra_args["ACL"] = acl

        try:
            s3.put_object(Bucket=bucket, Key=key, Body=file_bytes, **extra_args)
        except (ClientError, BotoCoreError) as e:
            raise HTTPException(status_code=500, detail=f"S3 upload failed: {str(e)}")

        if public_base_url:
            return f"{public_base_url}/{key}"
        if endpoint_url:
            return f"{endpoint_url.rstrip('/')}/{bucket}/{key}"
        return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

    customer_dir = Path("data/uploads/shops") / shop_id / "customers" / str(customer_id)
    customer_dir.mkdir(parents=True, exist_ok=True)
    save_path = customer_dir / filename
    with save_path.open("wb") as fh:
        fh.write(file_bytes)
    relative = save_path.relative_to(Path("data/uploads"))
    return "/uploads/" + str(relative).replace("\\", "/")


def _normalize_customer_photo_url(image_url: str) -> str:
    if not image_url:
        return ""
    cleaned = str(image_url).strip()
    if cleaned.startswith("/uploads/uploads/"):
        return "/uploads/" + cleaned[len("/uploads/uploads/"):]
    return cleaned


def _delete_local_upload_from_url(image_url: str) -> None:
    if not image_url:
        return
    cleaned = _normalize_customer_photo_url(image_url).split("?", 1)[0]
    if not cleaned.startswith("/uploads/"):
        return
    target = Path("data") / cleaned.lstrip("/")
    try:
        if target.exists():
            target.unlink()
    except OSError:
        pass


def _delete_customer_photo_file(image_url: str) -> None:
    if not image_url:
        return
    cleaned = _normalize_customer_photo_url(image_url).split("?", 1)[0]
    if cleaned.startswith("/uploads/"):
        _delete_local_upload_from_url(cleaned)
        return

    s3_settings = _get_customer_photo_s3_settings()
    bucket = s3_settings["bucket"]
    if not bucket:
        return

    parsed = urlparse(cleaned)
    path = parsed.path.lstrip("/")
    host = (parsed.netloc or "").lower()
    key = ""

    if path.startswith(f"{bucket}/"):
        key = path[len(bucket) + 1:]
    elif host.startswith(f"{bucket}.s3") or host == f"{bucket}.s3.amazonaws.com":
        key = path
    else:
        public_base_url = s3_settings["public_base_url"]
        if public_base_url and cleaned.startswith(public_base_url + "/"):
            key = cleaned[len(public_base_url) + 1:]
        endpoint_url = s3_settings["endpoint_url"].rstrip("/")
        if not key and endpoint_url and cleaned.startswith(endpoint_url + f"/{bucket}/"):
            key = cleaned[len(endpoint_url) + len(bucket) + 2:]

    if not key:
        return

    region = s3_settings["region"]
    endpoint_url = s3_settings["endpoint_url"] or None
    client_kwargs = {"region_name": region}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    s3 = boto3.client("s3", **client_kwargs)
    s3.delete_object(Bucket=bucket, Key=key)




def _is_premium_subscription(subscription: dict | None) -> bool:
    current_display_code = _resolve_current_display_plan_code(subscription)
    return current_display_code == "premium"


def _parse_hhmm_to_minutes(value: str | None) -> int | None:
    if not value:
        return None
    try:
        hours, minutes = str(value).split(":", 1)
        return int(hours) * 60 + int(minutes)
    except (TypeError, ValueError):
        return None


def _format_minutes_hhmm(total_minutes: int) -> str:
    hours = max(0, total_minutes) // 60
    minutes = max(0, total_minutes) % 60
    return f"{hours:02d}:{minutes:02d}"


def _build_timeline_karte_context(shop: dict, reservations: list[dict], selected_date_obj: date) -> dict[str, object]:
    selected_date = selected_date_obj.isoformat()
    configured_staff_list = list(shop.get("staff_list", []))
    day_reservations = [
        item for item in reservations
        if str(item.get("reservation_date") or "") == selected_date
    ]

    reservation_start_minutes: list[int] = []
    reservation_end_minutes: list[int] = []
    for item in day_reservations:
        start_minutes = _parse_hhmm_to_minutes(item.get("start_time"))
        end_minutes = _parse_hhmm_to_minutes(item.get("end_time"))
        if start_minutes is not None:
            reservation_start_minutes.append(start_minutes)
        if end_minutes is not None:
            reservation_end_minutes.append(end_minutes)

    default_start = 9 * 60
    default_end = 21 * 60
    if reservation_start_minutes:
        default_start = min(default_start, min(reservation_start_minutes))
    if reservation_end_minutes:
        default_end = max(default_end, max(reservation_end_minutes))

    timeline_start = max(0, (default_start // 30) * 30)
    timeline_end = min(24 * 60, ((default_end + 29) // 30) * 30)
    if timeline_end <= timeline_start:
        timeline_end = min(24 * 60, timeline_start + (12 * 60))

    slot_height = 56
    slots = []
    current_slot = timeline_start
    while current_slot <= timeline_end:
        slots.append({"label": _format_minutes_hhmm(current_slot), "minutes": current_slot})
        current_slot += 30

    total_height = max((timeline_end - timeline_start) * slot_height // 30, slot_height * 6)

    reservations_by_staff: dict[str, list[dict]] = {}
    reservation_staff_meta: dict[str, dict] = {}
    unassigned_key = "__unassigned__"
    for item in day_reservations:
        raw_staff_id = item.get("staff_id")
        raw_staff_name = str(item.get("staff_name") or "").strip()
        staff_key = str(raw_staff_id).strip() if raw_staff_id not in (None, "") else ""
        if not staff_key:
            staff_key = raw_staff_name or unassigned_key
        reservations_by_staff.setdefault(staff_key, []).append(item)
        if staff_key not in reservation_staff_meta:
            reservation_staff_meta[staff_key] = {
                "id": raw_staff_id if raw_staff_id not in (None, "") else staff_key,
                "name": raw_staff_name or ("未割り当て" if staff_key == unassigned_key else f"スタッフ{staff_key}"),
            }

    staff_columns = []
    seen_staff_keys: set[str] = set()
    merged_staff_list: list[dict] = []

    for staff in configured_staff_list:
        staff_key = str(staff.get("id") or "").strip()
        if not staff_key:
            continue
        merged_staff_list.append({
            "id": staff.get("id"),
            "name": staff.get("name") or reservation_staff_meta.get(staff_key, {}).get("name") or f"スタッフ{staff_key}",
            "avatar_url": staff.get("avatar_url") or _resolve_staff_avatar_url(staff),
            "default_avatar": staff.get("default_avatar") or 'male',
        })
        seen_staff_keys.add(staff_key)

    for staff_key, meta in reservation_staff_meta.items():
        if staff_key in seen_staff_keys:
            continue
        merged_staff_list.append({
            "id": meta.get("id"),
            "name": meta.get("name"),
            "avatar_url": _get_default_staff_avatar_url('male'),
            "default_avatar": 'male',
        })
        seen_staff_keys.add(staff_key)

    for staff in merged_staff_list:
        raw_staff_id = staff.get("id")
        staff_key = str(raw_staff_id).strip() if raw_staff_id not in (None, "") else ""
        if not staff_key:
            staff_key = str(staff.get("name") or "").strip() or unassigned_key
        items = []
        for item in sorted(reservations_by_staff.get(staff_key, []), key=lambda x: (str(x.get("start_time") or ""), int(x.get("id") or 0))):
            start_minutes = _parse_hhmm_to_minutes(item.get("start_time"))
            end_minutes = _parse_hhmm_to_minutes(item.get("end_time"))
            if start_minutes is None:
                continue
            if end_minutes is None or end_minutes <= start_minutes:
                duration = int(item.get("duration") or 30)
                end_minutes = start_minutes + max(duration, 30)
            top = max(0, (start_minutes - timeline_start) * slot_height / 30)
            height = max(44, (end_minutes - start_minutes) * slot_height / 30)
            status = str(item.get("status") or "予約済み")
            items.append({
                "id": item.get("id"),
                "top": round(top, 2),
                "height": round(height, 2),
                "start_time": item.get("start_time"),
                "end_time": item.get("end_time"),
                "customer_name": item.get("customer_name"),
                "menu_name": item.get("menu_name"),
                "price": item.get("price"),
                "reservation_date": item.get("reservation_date"),
                "staff_id": item.get("staff_id"),
                "staff_name": item.get("staff_name"),
                "status": status,
                "status_class": "cancelled" if status == "キャンセル" else ("done" if status == "来店済み" else "reserved"),
            })
        staff_columns.append({
            "id": staff.get("id"),
            "name": staff.get("name"),
            "avatar_url": staff.get("avatar_url") or _resolve_staff_avatar_url(staff),
            "default_avatar": staff.get("default_avatar") or 'male',
            "reservations": items,
        })

    now_line_top = None
    if selected_date_obj == date.today():
        current_minutes = datetime.now().hour * 60 + datetime.now().minute
        if timeline_start <= current_minutes <= timeline_end:
            now_line_top = round((current_minutes - timeline_start) * slot_height / 30, 2)

    return {
        "timeline_selected_date": selected_date,
        "timeline_slots": slots,
        "timeline_staff_columns": staff_columns,
        "timeline_total_height": total_height,
        "timeline_slot_height": slot_height,
        "timeline_now_line_top": now_line_top,
    }

def _safe_parse_date(value: str | None, fallback: date | None = None) -> date:
    if value:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            pass
    return fallback or date.today()


def _to_hiragana(value: str) -> str:
    result: list[str] = []
    for char in unicodedata.normalize("NFKC", value or ""):
        code = ord(char)
        if 0x30A1 <= code <= 0x30F6:
            result.append(chr(code - 0x60))
        else:
            result.append(char)
    return "".join(result)


def _customer_name_sort_key(value: str) -> tuple[str, str]:
    normalized = unicodedata.normalize("NFKC", value or "").strip().lower()
    reading = _to_hiragana(normalized)
    return (reading, normalized)


def _build_customer_visit_counts(reservations: list[dict]) -> dict[int, int]:
    visit_counts: dict[int, int] = {}
    for reservation in reservations:
        customer_id = reservation.get("customer_id")
        try:
            customer_id = int(customer_id)
        except (TypeError, ValueError):
            continue
        visit_counts[customer_id] = visit_counts.get(customer_id, 0) + 1
    return visit_counts


def _sort_customer_items(customer_items: list[dict], sort_order: str) -> tuple[list[dict], str]:
    normalized_sort = sort_order if sort_order in {"new", "name", "visits"} else "new"

    if normalized_sort == "name":
        sorted_items = sorted(
            customer_items,
            key=lambda item: (_customer_name_sort_key(str(item.get("name") or "")), -int(item.get("id") or 0)),
        )
    elif normalized_sort == "visits":
        sorted_items = sorted(
            customer_items,
            key=lambda item: (-int(item.get("visit_count") or 0), _customer_name_sort_key(str(item.get("name") or "")), -int(item.get("id") or 0)),
        )
    else:
        sorted_items = sorted(customer_items, key=lambda item: int(item.get("id") or 0), reverse=True)

    return sorted_items, normalized_sort


def _build_time_slots() -> list[str]:
    return [f"{hour:02d}:00" for hour in range(10, 18)]



def _parse_business_hours_range(business_hours: str | None) -> tuple[str, str]:
    value = str(business_hours or '').strip()
    match = re.search(r'(\d{1,2}:\d{2}).*?(\d{1,2}:\d{2})', value)
    if not match:
        return ('10:00', '19:00')
    start_time, end_time = match.group(1), match.group(2)
    return (start_time, end_time)


def _build_half_hour_slots(business_hours: str | None) -> list[str]:
    start_raw, end_raw = _parse_business_hours_range(business_hours)
    try:
        start_dt = datetime.strptime(start_raw, '%H:%M')
        end_dt = datetime.strptime(end_raw, '%H:%M')
    except ValueError:
        return [f"{hour:02d}:00" for hour in range(10, 19)]

    if end_dt <= start_dt:
        end_dt = start_dt + timedelta(hours=9)

    slots: list[str] = []
    current = start_dt
    safety = 0
    while current < end_dt and safety < 96:
        slots.append(current.strftime('%H:%M'))
        current += timedelta(minutes=30)
        safety += 1
    return slots or [f"{hour:02d}:00" for hour in range(10, 19)]


def _normalize_week_start(base_date: date | None) -> date:
    base = base_date or date.today()
    return base - timedelta(days=((base.weekday() + 1) % 7))


def _get_staff_holiday_dates(staff: dict | None) -> set[str]:
    raw = (staff or {}).get('holiday_dates') or []
    if isinstance(raw, str):
        raw = [item.strip() for item in raw.split(',') if item.strip()]
    dates: set[str] = set()
    for item in raw or []:
        value = str(item or '').strip()
        if not value:
            continue
        try:
            dates.add(datetime.strptime(value, '%Y-%m-%d').date().isoformat())
        except ValueError:
            continue
    return dates


def _is_shop_holiday(shop: dict, target_day: date) -> bool:
    holiday_idx = WEEKDAY_MAP.get(str(shop.get('holiday') or ''))
    return holiday_idx is not None and target_day.weekday() == holiday_idx


def _is_staff_holiday(staff: dict | None, target_day: date) -> bool:
    return target_day.isoformat() in _get_staff_holiday_dates(staff)


def _build_week_availability_matrix(*, shop: dict, reservations: list[dict], selected_date: date, week_start: date | None = None, staff_id: int | str | None = None) -> tuple[list[dict], list[str], list[dict]]:
    start_of_week = _normalize_week_start(week_start or selected_date)
    normalized_staff_id = str(staff_id).strip() if staff_id not in (None, '') else ''
    selected_staff = next((item for item in shop.get('staff_list', []) if str(item.get('id') or '').strip() == normalized_staff_id), None) if normalized_staff_id else None
    week_days: list[dict] = []
    for offset in range(7):
        current_day = start_of_week + timedelta(days=offset)
        is_shop_holiday = _is_shop_holiday(shop, current_day)
        is_staff_holiday = _is_staff_holiday(selected_staff, current_day)
        week_days.append({
            'date': current_day.isoformat(),
            'label': f"{current_day.month}/{current_day.day}",
            'weekday': '日月火水木金土'[offset],
            'is_today': current_day == date.today(),
            'is_holiday': is_shop_holiday or is_staff_holiday,
            'is_shop_holiday': is_shop_holiday,
            'is_staff_holiday': is_staff_holiday,
            'is_selected': current_day == selected_date,
        })

    time_slots = _build_half_hour_slots(shop.get('business_hours'))
    active_reservations = [r for r in reservations if str(r.get('status') or '') != 'キャンセル']
    if normalized_staff_id:
        active_reservations = [r for r in active_reservations if str(r.get('staff_id') or '').strip() == normalized_staff_id]
    reserved_keys: set[tuple[str, str]] = set()
    for item in active_reservations:
        reservation_date = str(item.get('reservation_date') or '')
        start_minutes = _parse_hhmm_to_minutes(str(item.get('start_time') or '')[:5])
        end_minutes = _parse_hhmm_to_minutes(str(item.get('end_time') or '')[:5])
        if start_minutes is None:
            continue
        if end_minutes is None or end_minutes <= start_minutes:
            duration = int(item.get('duration') or 30)
            end_minutes = start_minutes + max(duration, 30)

        current_minutes = start_minutes
        safety = 0
        while current_minutes < end_minutes and safety < 96:
            reserved_keys.add((reservation_date, _format_minutes_hhmm(current_minutes)))
            current_minutes += 30
            safety += 1

    weekly_rows: list[dict] = []
    for slot in time_slots:
        cells = []
        for day in week_days:
            if day['is_holiday']:
                symbol = '休'
                cell_class = 'is-holiday'
            elif (day['date'], slot) in reserved_keys:
                symbol = '×'
                cell_class = 'is-booked'
            else:
                symbol = '◎'
                cell_class = 'is-open'
            cells.append({
                'date': day['date'],
                'time': slot,
                'symbol': symbol,
                'cell_class': cell_class,
                'is_selected': day['is_selected'],
            })
        weekly_rows.append({'time': slot, 'cells': cells})

    return week_days, time_slots, weekly_rows


def _build_public_week_availability_matrix(*, shop: dict, reservations: list[dict], selected_date: date, selected_staff_id: str, selected_menu: dict | None, week_start: date | None = None) -> tuple[list[dict], list[str], list[dict]]:
    start_of_week = _normalize_week_start(week_start or selected_date)
    duration = int((selected_menu or {}).get('duration') or 60)
    time_slots = _build_half_hour_slots(shop.get('business_hours'))
    now_dt = datetime.now()
    today_obj = date.today()
    active_reservations = [r for r in reservations if str(r.get('status') or '') != 'キャンセル']
    selected_staff = next((item for item in shop.get('staff_list', []) if str(item.get('id') or '') == str(selected_staff_id)), None) if selected_staff_id else None

    week_days: list[dict] = []
    for offset in range(7):
        current_day = start_of_week + timedelta(days=offset)
        is_shop_holiday = _is_shop_holiday(shop, current_day)
        is_staff_holiday = _is_staff_holiday(selected_staff, current_day)
        week_days.append({
            'date': current_day.isoformat(),
            'label': f"{current_day.month}/{current_day.day}",
            'weekday': '日月火水木金土'[offset],
            'is_today': current_day == today_obj,
            'is_selected': current_day == selected_date,
            'is_holiday': is_shop_holiday or is_staff_holiday,
            'is_shop_holiday': is_shop_holiday,
            'is_staff_holiday': is_staff_holiday,
        })

    reservations_by_date: dict[tuple[str, str], list[dict]] = {}
    if selected_staff_id:
        for item in active_reservations:
            if str(item.get('staff_id') or '') != str(selected_staff_id):
                continue
            key = (str(item.get('reservation_date') or ''), str(item.get('staff_id') or ''))
            reservations_by_date.setdefault(key, []).append(item)

    weekly_rows: list[dict] = []
    for slot in time_slots:
        start_dt = datetime.strptime(slot, '%H:%M')
        end_dt = start_dt + timedelta(minutes=duration)
        cells = []
        for day in week_days:
            is_available = False
            is_closed = bool(day['is_holiday'])
            if selected_staff_id and selected_menu and not is_closed:
                slot_is_past = day['date'] < today_obj.isoformat() or (day['date'] == today_obj.isoformat() and start_dt.time() <= now_dt.time())
                day_reservations = reservations_by_date.get((day['date'], str(selected_staff_id)), [])
                slot_is_conflict = any(
                    start_dt < datetime.strptime(str(r.get('end_time')), '%H:%M') and end_dt > datetime.strptime(str(r.get('start_time')), '%H:%M')
                    for r in day_reservations
                )
                is_available = not slot_is_past and not slot_is_conflict
            symbol = '休' if is_closed else ('◎' if is_available else '×')
            cell_class = 'is-holiday' if is_closed else ('is-open' if is_available else 'is-booked')
            cells.append({
                'date': day['date'],
                'time': slot,
                'symbol': symbol,
                'cell_class': cell_class,
                'is_available': is_available and not is_closed,
                'is_closed': is_closed,
                'is_selected': day['is_selected'],
            })
        weekly_rows.append({'time': slot, 'cells': cells})

    return week_days, time_slots, weekly_rows

def _send_reservation_mail(*, to_email: str, shop: dict, reservation_date: str, start_time: str, reply_to_email: str = '') -> None:
    mail_settings = _get_mail_runtime_settings()
    smtp_user = str(mail_settings.get('smtp_user') or '').strip()
    smtp_password = str(mail_settings.get('smtp_password') or '').strip()
    from_email = str(mail_settings.get('from_email') or '').strip()
    if not to_email or not from_email or not smtp_user or not smtp_password:
        return

    subject = f"【{shop.get('shop_name', '店舗')}】ご予約を受け付けました"
    body = f"""{shop.get('shop_name', '店舗')} のご予約ありがとうございます。

店舗名: {shop.get('shop_name', '')}
ご予約日時: {reservation_date} {start_time}

ご来店をお待ちしております。"""
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = formataddr((str(mail_settings.get('from_name') or '予約システム'), from_email))
    msg['To'] = to_email
    if reply_to_email:
        msg['Reply-To'] = reply_to_email
    try:
        with smtplib.SMTP(str(mail_settings.get('smtp_host') or 'smtp.gmail.com'), int(mail_settings.get('smtp_port') or 587), timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
    except Exception as exc:
        print(f"[_send_reservation_mail] failed: {exc}")



def _get_no_reply_from_email() -> str:
    mail_settings = _get_mail_runtime_settings()
    configured = (
        _first_env('NO_REPLY_FROM_EMAIL', 'MAIL_NO_REPLY_FROM')
        or str(mail_settings.get('no_reply_from_email') or '').strip()
        or ''
    )
    if configured:
        return configured
    base_from = str(mail_settings.get('from_email') or '').strip()
    if '@' not in base_from:
        return base_from
    local_part, domain_part = base_from.split('@', 1)
    if local_part.lower() == 'no-reply':
        return base_from
    return f"no-reply@{domain_part}"


def _render_reminder_template(template: str, reminder: dict) -> str:
    reservation_at_text = str(reminder.get('reservation_at') or '').strip()
    reservation_date = str(reminder.get('reservation_date') or '').strip()
    reservation_time = str(reminder.get('start_time') or '').strip()
    if reservation_at_text:
        try:
            reservation_at = datetime.fromisoformat(reservation_at_text)
            reservation_date = reservation_at.strftime('%Y-%m-%d')
            reservation_time = reservation_at.strftime('%H:%M')
        except ValueError:
            pass
    replacements = {
        '{{shop_name}}': str(reminder.get('shop_name') or '').strip(),
        '{{customer_name}}': str(reminder.get('customer_name') or '').strip(),
        '{{reservation_date}}': reservation_date,
        '{{reservation_time}}': reservation_time,
        '{{staff_name}}': str(reminder.get('staff_name') or '').strip(),
        '{{menu_name}}': str(reminder.get('menu_name') or '').strip(),
    }
    content = str(template or '')
    for key, value in replacements.items():
        content = content.replace(key, value)
    return content


def _send_reservation_reminder_mail(reminder: dict) -> bool:
    mail_settings = _get_mail_runtime_settings()
    smtp_user = str(mail_settings.get('smtp_user') or '').strip()
    smtp_password = str(mail_settings.get('smtp_password') or '').strip()
    from_email = _get_no_reply_from_email().strip()
    if not str(reminder.get('customer_email') or '').strip() or not from_email or not smtp_user or not smtp_password:
        return False

    is_day_before = str(reminder.get('reminder_kind') or '') == 'day_before'
    default_subject = '【{{shop_name}}】明日のご予約について' if is_day_before else '【{{shop_name}}】まもなくご予約のお時間です'
    default_body = (
        '''{{customer_name}}様

いつも{{shop_name}}をご利用いただきありがとうございます。

明日、以下の内容でご予約をいただいております。

■日時
{{reservation_date}} {{reservation_time}}

ご来店を心よりお待ちしております。

※本メールは送信専用です。'''
        if is_day_before else
        '''{{customer_name}}様

{{shop_name}}でございます。

本日、以下のお時間でご予約をいただいております。

■日時
{{reservation_date}} {{reservation_time}}

ご来店の際はお気をつけてお越しください。

※本メールは送信専用です。'''
    )
    subject_template_value = reminder.get('reminder_day_before_subject') if is_day_before else reminder.get('reminder_same_day_subject')
    body_template_value = reminder.get('reminder_day_before_body') if is_day_before else reminder.get('reminder_same_day_body')
    subject_template = str(subject_template_value or '').strip() or default_subject
    body_template = str(body_template_value or '').strip() or default_body

    subject = _render_reminder_template(subject_template, reminder)
    body = _render_reminder_template(body_template, reminder)

    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = formataddr((str(mail_settings.get('from_name') or '予約システム'), from_email))
    msg['To'] = str(reminder.get('customer_email') or '').strip()
    try:
        with smtplib.SMTP(str(mail_settings.get('smtp_host') or 'smtp.gmail.com'), int(mail_settings.get('smtp_port') or 587), timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
        return True
    except Exception as exc:
        print(f"[_send_reservation_reminder_mail] failed: {exc}")
        return False




def _enrich_reminder_for_line(reminder: dict) -> dict:
    """get_due_reservation_reminders の結果にLINE送信用情報を補完します。"""
    data = dict(reminder or {})
    shop_id = str(data.get("shop_id") or "").strip()
    customer_id = data.get("customer_id")

    try:
        with get_connection() as conn:
            if not str(data.get("line_user_id") or "").strip() and customer_id not in (None, ""):
                row = conn.execute(
                    "SELECT line_user_id FROM customers WHERE shop_id = ? AND id = ? LIMIT 1",
                    (shop_id, int(customer_id)),
                ).fetchone()
                if row:
                    data["line_user_id"] = row["line_user_id"] if hasattr(row, "keys") else row[0]

            if not str(data.get("line_channel_access_token") or "").strip():
                row = conn.execute(
                    "SELECT line_channel_access_token FROM shops WHERE shop_id = ? LIMIT 1",
                    (shop_id,),
                ).fetchone()
                if row:
                    data["line_channel_access_token"] = row["line_channel_access_token"] if hasattr(row, "keys") else row[0]
    except Exception as exc:
        print("[_enrich_reminder_for_line] failed:", repr(exc))

    return data



def _send_reservation_reminder_line(reminder: dict) -> bool:
    """既存のメールリマインド設定を使ってLINEリマインドを送信します。"""
    ensure_customer_line_user_id_schema()

    line_user_id = str(reminder.get("line_user_id") or "").strip()
    access_token = str(reminder.get("line_channel_access_token") or "").strip()
    if not line_user_id or not access_token:
        return False

    is_day_before = str(reminder.get("reminder_kind") or "") == "day_before"

    default_body = (
        """{{customer_name}}様

いつも{{shop_name}}をご利用いただきありがとうございます。

明日、以下の内容でご予約をいただいております。

■日時
{{reservation_date}} {{reservation_time}}

ご来店を心よりお待ちしております。"""
        if is_day_before else
        """{{customer_name}}様

{{shop_name}}でございます。

本日、以下のお時間でご予約をいただいております。

■日時
{{reservation_date}} {{reservation_time}}

ご来店の際はお気をつけてお越しください。"""
    )

    body_template_value = reminder.get("reminder_day_before_body") if is_day_before else reminder.get("reminder_same_day_body")
    body_template = str(body_template_value or "").strip() or default_body
    body = _render_reminder_template(body_template, reminder)

    result = send_line_message(
        access_token=access_token,
        user_id=line_user_id,
        message=body,
    )
    print("[_send_reservation_reminder_line] result:", result)
    return bool(result.get("ok"))



def _process_reservation_reminders() -> None:
    reminder_now = datetime.now(ZoneInfo('Asia/Tokyo')).replace(second=0, microsecond=0).replace(tzinfo=None)

    ensure_customer_line_user_id_schema()

    for reminder_raw in get_due_reservation_reminders(reminder_now):
        reminder = _enrich_reminder_for_line(dict(reminder_raw))
        mail_sent = False
        line_sent = False

        try:
            mail_sent = _send_reservation_reminder_mail(reminder)
        except Exception as exc:
            print(f"[_process_reservation_reminders] mail failed: {exc}")

        try:
            line_sent = _send_reservation_reminder_line(reminder)
        except Exception as exc:
            print(f"[_process_reservation_reminders] line failed: {exc}")

        if mail_sent or line_sent:
            mark_reservation_reminder_sent(
                str(reminder.get('shop_id') or ''),
                int(reminder.get('id') or 0),
                str(reminder.get('reminder_kind') or ''),
                reminder_now.isoformat(timespec='minutes'),
            )



@app.get("/send-reminders")
def send_all_reminders_endpoint():
    _process_reservation_reminders()
    return {"ok": True, "message": "メール・LINEリマインド処理を実行しました"}



def _start_reservation_reminder_worker() -> None:
    if getattr(app.state, 'reservation_reminder_worker_started', False):
        return
    app.state.reservation_reminder_worker_started = True

    def _worker() -> None:
        while True:
            try:
                _process_reservation_reminders()
            except Exception as exc:
                print(f"[_start_reservation_reminder_worker] failed: {exc}")
            time.sleep(60)

    thread = threading.Thread(target=_worker, name='reservation-reminder-worker', daemon=True)
    thread.start()
    app.state.reservation_reminder_worker = thread


def parse_month_string(month_text: str | None) -> tuple[int, int]:
    today = date.today()
    if not month_text:
        return today.year, today.month
    try:
        year_text, month_value = month_text.split("-", 1)
        year = int(year_text)
        month = int(month_value)
        if 1 <= month <= 12:
            return year, month
    except (ValueError, AttributeError):
        pass
    return today.year, today.month


def shift_month(year: int, month: int, offset: int) -> tuple[int, int]:
    month_index = (year * 12 + (month - 1)) + offset
    return month_index // 12, month_index % 12 + 1


def build_public_calendar_days(year: int, month: int, holiday_weekday: int | None = None) -> list[dict]:
    cal = calendar.Calendar(firstweekday=6)
    today = date.today()
    days = []
    for week in cal.monthdatescalendar(year, month):
        for current_day in week:
            days.append({
                "date": current_day.isoformat(),
                "day": current_day.day,
                "is_current_month": current_day.month == month,
                "is_today": current_day == today,
                "is_holiday": holiday_weekday is not None and current_day.weekday() == holiday_weekday,
            })
    return days


def require_platform_login(request: Request):
    if request.session.get("platform_logged_in"):
        return None
    return RedirectResponse("/platform/login", status_code=303)


def require_store_login(request: Request, shop_id: str):
    requested_shop_id = (shop_id or "").strip().lower()
    logged_in_shop_id = str(request.session.get("store_logged_in_shop_id") or "").strip().lower()
    if logged_in_shop_id == requested_shop_id:
        return None

    if logged_in_shop_id:
        requested_shop = get_shop_management_data(requested_shop_id)
        if requested_shop and int(requested_shop.get('is_child_shop') or 0) == 1:
            parent_shop_id = str(requested_shop.get('parent_shop_id') or '').strip().lower()
            if parent_shop_id and parent_shop_id == logged_in_shop_id:
                return None

    return RedirectResponse("/store-login", status_code=303)


def build_shop_booking_context(shop_id: str, request: Request, error_message: str = ""):
    shop = get_shop(shop_id)
    if shop is None:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    subscription = get_shop_subscription(shop_id) or {'status': 'active', 'plan_name': 'Free', 'show_ads': False}
    shop = _build_shop_with_visible_staff(shop, subscription)

    month_value = request.query_params.get("month") or date.today().strftime("%Y-%m")
    try:
        current_month = datetime.strptime(month_value, "%Y-%m").date().replace(day=1)
    except ValueError:
        current_month = date.today().replace(day=1)

    selected_date_obj = _safe_parse_date(request.query_params.get("reservation_date"), date.today())
    selected_date = selected_date_obj.isoformat()
    selected_staff_id = request.query_params.get("staff_id") or ""
    selected_menu_id = request.query_params.get("menu_id") or ""
    selected_start_time = request.query_params.get("start_time") or ""
    week_start_query = _safe_parse_date(request.query_params.get("week_start"), selected_date_obj)
    week_start = _normalize_week_start(week_start_query)

    today = date.today()
    reservations = [r for r in get_reservations(shop_id) if str(r.get('status')) != 'キャンセル']
    selected_menu = next((m for m in shop.get('menus', []) if str(m.get('id')) == str(selected_menu_id)), None)
    selected_staff = next((s for s in shop.get('staff_list', []) if str(s.get('id')) == str(selected_staff_id)), None)

    cal = calendar.Calendar(firstweekday=6)
    days = []
    for week in cal.monthdatescalendar(current_month.year, current_month.month):
        for day in week:
            is_current = day.month == current_month.month
            is_closed = _is_shop_holiday(shop, day) or _is_staff_holiday(selected_staff, day)
            is_past = day < today
            day_reservations = [r for r in reservations if r.get('reservation_date') == day.isoformat()]
            days.append({
                'date': day.isoformat(),
                'day': day.day,
                'count': len(day_reservations),
                'is_current_month': is_current,
                'is_today': day == today,
                'is_closed': is_closed,
                'is_past': is_past,
                'is_selected': day.isoformat() == selected_date,
                'is_clickable': is_current and not is_closed and not is_past,
            })

    if selected_staff is None and selected_staff_id:
        selected_staff_id = ""
        selected_menu_id = ""
        selected_start_time = ""
        selected_menu = None
    if selected_staff and selected_menu and not _staff_allows_menu(selected_staff, selected_menu.get('id')):
        selected_menu = None
        selected_menu_id = ""
        selected_start_time = ""

    available_slots = []
    selected_slot = None
    if selected_menu and selected_staff:
        duration = int(selected_menu.get('duration', 60) or 60)
        current_dt = datetime.now()
        selected_reservations = [r for r in reservations if r.get('reservation_date') == selected_date and str(r.get('staff_id')) == str(selected_staff_id)]
        selected_day_is_holiday = _is_shop_holiday(shop, selected_date_obj) or _is_staff_holiday(selected_staff, selected_date_obj)
        for start_time in _build_half_hour_slots(shop.get('business_hours')):
            start_dt = datetime.strptime(start_time, '%H:%M')
            end_dt = start_dt + timedelta(minutes=duration)
            slot_is_past = selected_date_obj < today or (selected_date_obj == today and start_dt.time() <= current_dt.time())
            slot_is_conflict = any(
                start_dt < datetime.strptime(str(r.get('end_time')), '%H:%M') and end_dt > datetime.strptime(str(r.get('start_time')), '%H:%M')
                for r in selected_reservations
            )
            slot = {
                'start_time': start_time,
                'end_time': end_dt.strftime('%H:%M'),
                'is_available': (not selected_day_is_holiday) and (not slot_is_past) and (not slot_is_conflict),
                'is_conflict': slot_is_conflict or selected_day_is_holiday,
            }
            if selected_start_time == start_time:
                selected_slot = slot
            available_slots.append(slot)

    member = _get_member_for_shop_session(request, shop_id)
    member_page_url = f"/member/{shop_id}/mypage" if member else f"/member/{shop_id}/login?next=/member/{shop_id}/mypage"
    form_data = {
        'customer_name': request.query_params.get('customer_name') or str((member or {}).get('name') or ''),
        'phone': request.query_params.get('phone') or str((member or {}).get('phone') or ''),
        'email': request.query_params.get('email') or str((member or {}).get('email') or ''),
        'receive_email': request.query_params.get('receive_email', '1'),
    }

    prev_month = (current_month.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m')
    next_month = (current_month.replace(day=28) + timedelta(days=4)).replace(day=1).strftime('%Y-%m')
    public_week_days, public_time_slots, public_weekly_rows = _build_public_week_availability_matrix(
        shop=shop,
        reservations=reservations,
        selected_date=selected_date_obj,
        selected_staff_id=selected_staff_id,
        selected_menu=selected_menu,
        week_start=week_start,
    )


    return {
        'request': request,
        'shop': shop,
        'shop_id': shop_id,
        'subscription': subscription,
        'staff_list': shop.get('staff_list', []),
        'menus': shop.get('menus', []),
        'selected_staff_id': selected_staff_id,
        'selected_menu_id': selected_menu_id,
        'selected_date': selected_date,
        'selected_slot': selected_slot,
        'selected_start_time': selected_start_time,
        'available_slots': available_slots,
        'calendar_days': days,
        'calendar_month_value': current_month.strftime('%Y-%m'),
        'calendar_month_label': current_month.strftime('%Y年%m月'),
        'calendar_prev_month': prev_month,
        'calendar_next_month': next_month,
        'selected_menu': selected_menu,
        'selected_staff': selected_staff,
        'selected_date_is_holiday': (_is_shop_holiday(shop, selected_date_obj) or _is_staff_holiday(selected_staff, selected_date_obj)),
        'public_week_days': public_week_days,
        'public_time_slots': public_time_slots,
        'public_weekly_rows': public_weekly_rows,
        'week_start': week_start.isoformat(),
        'prev_week_start': (week_start - timedelta(days=7)).isoformat(),
        'next_week_start': (week_start + timedelta(days=7)).isoformat(),
        'form_data': form_data,
        'member': member,
        'member_page_url': member_page_url,
        'line_user_id': str(request.session.get('line_user_id') or ''),
        'line_display_name': str(request.session.get('line_display_name') or ''),
        'line_booking_entry_url': f"/shop/{shop_id}/line-reserve",
        'line_official_url': str((get_shop_line_settings(shop_id) or {}).get("line_official_url") or "").strip(),
        'error_message': error_message,
    }


@app.on_event("startup")
def startup():
    init_db()
    ensure_line_settings_schema()
    _start_reservation_reminder_worker()


def _get_mail_runtime_settings() -> dict[str, object]:
    settings = get_system_mail_settings() or {}

    smtp_host = _first_env("SMTP_HOST") or str(settings.get("smtp_host") or "").strip() or "smtp.gmail.com"
    smtp_port_raw = _first_env("SMTP_PORT") or str(settings.get("smtp_port") or "").strip() or "587"
    try:
        smtp_port = int(smtp_port_raw)
    except (TypeError, ValueError):
        smtp_port = 587

    smtp_user = (
        _first_env("SMTP_USER", "SMTP_USERNAME")
        or str(settings.get("smtp_username") or "").strip()
        or _first_env("MAIL_FROM", "FROM_EMAIL")
        or str(settings.get("from_email") or "").strip()
    )
    smtp_password = _first_env("SMTP_PASS", "SMTP_PASSWORD") or str(settings.get("smtp_password") or "").strip()
    from_email = (
        _first_env("MAIL_FROM", "FROM_EMAIL")
        or str(settings.get("from_email") or "").strip()
        or smtp_user
    )
    from_name = str(settings.get("from_name") or "").strip() or "らくばい"

    return {
        "smtp_host": smtp_host,
        "smtp_port": smtp_port,
        "smtp_user": smtp_user,
        "smtp_password": smtp_password,
        "from_email": from_email,
        "from_name": from_name,
    }


def _send_shop_registration_verification_mail(*, to_email: str, code: str) -> bool:
    mail_settings = _get_mail_runtime_settings()
    smtp_user = str(mail_settings.get('smtp_user') or '').strip()
    smtp_password = str(mail_settings.get('smtp_password') or '').strip()
    from_email = str(mail_settings.get('from_email') or '').strip()
    if not to_email or not from_email or not smtp_user or not smtp_password:
        print('[_send_shop_registration_verification_mail] missing SMTP settings')
        return False

    subject = '【らくばい】店舗登録確認コード'
    body = (
        'らくばい の店舗登録確認コードです。\n\n'
        f'確認コード: {code}\n\n'
        'このコードの有効期限は10分です。\n'
        'このメールに心当たりがない場合は、このまま破棄してください。\n'
    )

    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = formataddr((str(mail_settings.get('from_name') or 'らくばい'), from_email))
    msg['To'] = to_email

    try:
        with smtplib.SMTP(str(mail_settings.get('smtp_host') or 'smtp.gmail.com'), int(mail_settings.get('smtp_port') or 587), timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
        return True
    except Exception as exc:
        print(f"[_send_shop_registration_verification_mail] failed: {exc}")
        return False


def _send_contact_mail(*, name: str, company: str, email: str, phone: str, category: str, message: str) -> bool:
    mail_settings = _get_mail_runtime_settings()
    smtp_user = str(mail_settings.get('smtp_user') or '').strip()
    smtp_password = str(mail_settings.get('smtp_password') or '').strip()
    from_email = str(mail_settings.get('from_email') or '').strip()
    to_email = 'info@rakubai.net'
    if not from_email or not smtp_user or not smtp_password:
        print('[_send_contact_mail] missing SMTP settings')
        return False

    subject = f"【らくばいお問い合わせ】{category} / {name}"
    body = f"""らくばいトップページからお問い合わせがありました。

お名前: {name}
店舗名・会社名: {company or '-'}
メールアドレス: {email}
電話番号: {phone or '-'}
お問い合わせ種別: {category}

お問い合わせ内容:
{message}
"""
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = formataddr((str(mail_settings.get('from_name') or 'らくばい'), from_email))
    msg['To'] = to_email
    msg['Reply-To'] = email
    try:
        with smtplib.SMTP(str(mail_settings.get('smtp_host') or 'smtp.gmail.com'), int(mail_settings.get('smtp_port') or 587), timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
        return True
    except Exception as exc:
        print(f"[_send_contact_mail] failed: {exc}")
        return False


@app.get("/", response_class=HTMLResponse)
def top_page(request: Request):
    contact_status = str(request.query_params.get('contact') or '').strip().lower()
    contact_messages = {
        'success': 'お問い合わせを受け付けました。内容を確認のうえ、順次ご返信いたします。',
        'error': 'お問い合わせの送信に失敗しました。時間をおいて再度お試しください。',
        'invalid': '入力内容を確認してください。必須項目が未入力か、送信内容に不備があります。',
    }
    return templates.TemplateResponse(
        request=request,
        name="top.html",
        context={
            "request": request,
            "contact_status": contact_status,
            "contact_message": contact_messages.get(contact_status, ''),
        },
    )


@app.post("/contact")
def top_contact_submit(
    name: str = Form(""),
    company: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    category: str = Form("サービスについて"),
    message: str = Form(""),
    website: str = Form(""),
    agree: str = Form(""),
):
    if website.strip():
        return RedirectResponse(url='/?contact=success#contact', status_code=303)
    normalized_name = name.strip()
    normalized_email = email.strip()
    normalized_message = message.strip()
    normalized_category = category.strip() or 'サービスについて'
    if not normalized_name or not normalized_email or not normalized_message or agree != '1':
        return RedirectResponse(url='/?contact=invalid#contact', status_code=303)
    if len(normalized_message) > 3000:
        return RedirectResponse(url='/?contact=invalid#contact', status_code=303)
    sent = _send_contact_mail(
        name=normalized_name,
        company=company.strip(),
        email=normalized_email,
        phone=phone.strip(),
        category=normalized_category,
        message=normalized_message,
    )
    status = 'success' if sent else 'error'
    return RedirectResponse(url=f'/?contact={status}#contact', status_code=303)


@app.get("/plans/{plan_code}", response_class=HTMLResponse)
def plan_detail_page(request: Request, plan_code: str):
    plan = PLAN_DETAILS.get(plan_code)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return templates.TemplateResponse(
        request=request,
        name="plan_detail.html",
        context={"request": request, "plan": plan, "plans": PLAN_DETAILS, "comparison_rows": PLAN_COMPARISON_ROWS},
    )


@app.get("/store-login", response_class=HTMLResponse)
def store_login_page(request: Request):
    initial_shop_id = str(request.query_params.get("shop_id") or "").strip()
    return templates.TemplateResponse(
        request=request,
        name="store_login.html",
        context={
            "request": request,
            "error_message": "",
            "form": {"shop_id": initial_shop_id},
        },
    )


@app.post("/store-login")
def store_login_submit(
    request: Request,
    shop_id: str = Form(...),
    password: str = Form(...),
):
    raw_shop_id = (shop_id or "").strip()
    normalized_shop_id = raw_shop_id.lower()
    password = (password or "").strip()

    if not normalized_shop_id:
        return templates.TemplateResponse(
            request=request,
            name="store_login.html",
            context={
                "request": request,
                "error_message": "ログインID（店舗ID）を入力してください。",
                "form": {"shop_id": raw_shop_id},
            },
            status_code=400,
        )
    if not password:
        return templates.TemplateResponse(
            request=request,
            name="store_login.html",
            context={
                "request": request,
                "error_message": "パスワードを入力してください。",
                "form": {"shop_id": raw_shop_id},
            },
            status_code=400,
        )

    user = authenticate_admin_user(normalized_shop_id, normalized_shop_id, password)
    if not user:
        _record_audit_log(
            request,
            actor_type="store_admin",
            actor_id=normalized_shop_id,
            action="login",
            shop_id=normalized_shop_id,
            status="failure",
            detail={"login_id": raw_shop_id},
        )
        return templates.TemplateResponse(
            request=request,
            name="store_login.html",
            context={
                "request": request,
                "error_message": "ログインID（店舗ID）またはパスワードが正しくありません。",
                "form": {"shop_id": raw_shop_id},
            },
            status_code=400,
        )

    request.session["store_logged_in_shop_id"] = normalized_shop_id
    request.session["store_logged_in_login_id"] = str(user.get("login_id") or normalized_shop_id)
    request.session["store_logged_in_admin_name"] = str(user.get("name") or "")
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(user.get("login_id") or normalized_shop_id),
        actor_name=str(user.get("name") or ""),
        action="login",
        shop_id=normalized_shop_id,
        detail={"login_id": str(user.get("login_id") or normalized_shop_id)},
    )
    return RedirectResponse(f"/admin/{normalized_shop_id}", status_code=303)


@app.get("/platform/login", response_class=HTMLResponse)
def platform_login_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="platform/login.html",
        context={"error_message": ""},
    )


@app.post("/platform/login")
def platform_login(request: Request, login_id: str = Form(...), password: str = Form(...)):
    if login_id == PLATFORM_ADMIN.get("login_id") and password == PLATFORM_ADMIN.get("password"):
        request.session["platform_logged_in"] = True
        request.session["platform_admin_name"] = PLATFORM_ADMIN.get("name", "運営管理者")
        _record_audit_log(
            request,
            actor_type="platform_admin",
            actor_id=str(login_id or ""),
            actor_name=str(PLATFORM_ADMIN.get("name") or "運営管理者"),
            action="login",
        )
        return RedirectResponse("/platform/shops", status_code=303)
    _record_audit_log(
        request,
        actor_type="platform_admin",
        actor_id=str(login_id or ""),
        action="login",
        status="failure",
    )
    return templates.TemplateResponse(
        request=request,
        name="platform/login.html",
        context={"error_message": "ログインIDまたはパスワードが違います。"},
        status_code=400,
    )


@app.post("/platform/logout")
def platform_logout(request: Request):
    admin_name = str(request.session.get("platform_admin_name") or PLATFORM_ADMIN.get("name") or "運営管理者")
    _record_audit_log(
        request,
        actor_type="platform_admin",
        actor_id=str(PLATFORM_ADMIN.get("login_id") or ""),
        actor_name=admin_name,
        action="logout",
    )
    request.session.clear()
    return RedirectResponse("/", status_code=303)


@app.get("/platform/shops", response_class=HTMLResponse)
def platform_shops(request: Request):
    redirect = require_platform_login(request)
    if redirect:
        return redirect
    context = {
        "request": request,
        "shops": get_all_shops_for_platform(),
        "message": request.query_params.get("saved", ""),
        "error_message": "",
        "current_admin_name": request.session.get("platform_admin_name", PLATFORM_ADMIN.get("name", "運営管理者")),
    }
    return templates.TemplateResponse(
        request=request,
        name="platform/shops.html",
        context=context,
    )


@app.get("/platform/settings/mail", response_class=HTMLResponse)
def platform_mail_settings_page(request: Request):
    redirect = require_platform_login(request)
    if redirect:
        return redirect
    return templates.TemplateResponse(
        request=request,
        name="platform/mail_settings.html",
        context={
            "request": request,
            "settings": get_system_mail_settings(),
            "message": request.query_params.get("saved", ""),
            "error_message": "",
            "current_admin_name": request.session.get("platform_admin_name", PLATFORM_ADMIN.get("name", "運営管理者")),
        },
    )


@app.post("/platform/settings/mail")
def platform_mail_settings_save(
    request: Request,
    from_email: str = Form(""),
    from_name: str = Form("予約システム"),
    smtp_host: str = Form("smtp.gmail.com"),
    smtp_port: str = Form("587"),
    smtp_username: str = Form(""),
    smtp_password: str = Form(""),
):
    redirect = require_platform_login(request)
    if redirect:
        return redirect
    update_system_mail_settings(
        from_email=from_email,
        from_name=from_name,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_username=smtp_username,
        smtp_password=smtp_password,
    )
    return RedirectResponse("/platform/settings/mail?saved=送信元メール設定を保存しました。", status_code=303)


@app.get("/platform/shops/{shop_id}/edit", response_class=HTMLResponse)
def platform_shop_edit_page(request: Request, shop_id: str):
    redirect = require_platform_login(request)
    if redirect:
        return redirect
    shop = get_shop_management_data(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    return templates.TemplateResponse(
        request=request,
        name="platform/shop_edit.html",
        context={
            "shop": shop,
            "available_plans": get_plans(),
            "message": request.query_params.get("saved", ""),
            "error_message": "",
            "current_admin_name": request.session.get("platform_admin_name", PLATFORM_ADMIN.get("name", "運営管理者")),
        },
    )


@app.post("/platform/shops/{shop_id}/edit")
def platform_shop_edit_save(
    request: Request,
    shop_id: str,
    shop_name: str = Form(...),
    phone: str = Form(""),
    address: str = Form(""),
    business_hours: str = Form(""),
    holiday: str = Form(""),
    catch_copy: str = Form(""),
    description: str = Form(""),
    reply_to_email: str = Form(""),
    plan_id: int = Form(0),
    subscription_status: str = Form("active"),
):
    redirect = require_platform_login(request)
    if redirect:
        return redirect
    update_shop_basic_info(shop_id, shop_name=shop_name, phone=phone, address=address, business_hours=business_hours, holiday=holiday, catch_copy=catch_copy, description=description, reply_to_email=reply_to_email)
    _record_audit_log(
        request,
        actor_type="platform_admin",
        actor_id=str(PLATFORM_ADMIN.get("login_id") or ""),
        actor_name=str(request.session.get("platform_admin_name") or PLATFORM_ADMIN.get("name") or "運営管理者"),
        action="shop_update",
        shop_id=shop_id,
        target_type="shop",
        target_id=shop_id,
        target_label=shop_name,
    )
    if plan_id:
        update_shop_subscription(shop_id, plan_id=plan_id, status=subscription_status)
        _record_audit_log(
            request,
            actor_type="platform_admin",
            actor_id=str(PLATFORM_ADMIN.get("login_id") or ""),
            actor_name=str(request.session.get("platform_admin_name") or PLATFORM_ADMIN.get("name") or "運営管理者"),
            action="shop_subscription_update",
            shop_id=shop_id,
            target_type="shop",
            target_id=shop_id,
            target_label=shop_name,
            detail={"plan_id": int(plan_id), "subscription_status": subscription_status},
        )
    return RedirectResponse(f"/platform/shops/{shop_id}/edit?saved=店舗情報を保存しました。", status_code=303)


@app.get("/platform/templates", response_class=HTMLResponse)
def platform_templates_page(request: Request):
    redirect = require_platform_login(request)
    if redirect:
        return redirect
    from app.db import get_homepage_templates
    return templates.TemplateResponse(
        request=request,
        name="platform/templates.html",
        context={
            "templates_list": get_homepage_templates(),
            "message": request.query_params.get("saved", ""),
            "error_message": "",
        },
    )


@app.post("/platform/templates")
def platform_templates_create(
    request: Request,
    code: str = Form(...),
    name: str = Form(...),
    description: str = Form(""),
    primary: str = Form("#2ec4b6"),
    background: str = Form("#ffffff"),
    surface: str = Form("#ffffff"),
    accent: str = Form("#f7fffe"),
    text_color: str = Form("#1f2937"),
    subtext: str = Form("#6b7280"),
    hero_style: str = Form("split"),
    section_style: str = Form("cards"),
    button_style: str = Form("rounded"),
    preset_key: str = Form("custom"),
):
    redirect = require_platform_login(request)
    if redirect:
        return redirect
    from app.db import create_homepage_template
    create_homepage_template(code=code, name=name, description=description, theme={
        "primary": primary,
        "background": background,
        "surface": surface,
        "accent": accent,
        "text_color": text_color,
        "subtext": subtext,
        "hero_style": hero_style,
        "section_style": section_style,
        "button_style": button_style,
        "preset_key": preset_key,
    })
    return RedirectResponse("/platform/templates?saved=テンプレートを追加しました。", status_code=303)


@app.get("/terms", response_class=HTMLResponse)
def terms_page(request: Request):
    return templates.TemplateResponse(request=request, name="terms.html", context={"request": request})


@app.get("/privacy", response_class=HTMLResponse)
def privacy_page(request: Request):
    return templates.TemplateResponse(request=request, name="privacy.html", context={"request": request})


@app.get("/tokutei", response_class=HTMLResponse)
def tokutei_page(request: Request):
    return templates.TemplateResponse(request=request, name="tokutei.html", context={"request": request})


@app.get("/policy", response_class=HTMLResponse)
def policy_page(request: Request):
    return templates.TemplateResponse(request=request, name="policy.html", context={"request": request})


@app.get("/signup", response_class=HTMLResponse)
def signup_page(request: Request, error: str | None = None):
    return templates.TemplateResponse(
        request=request,
        name="shop_signup.html",
        context={"shops": get_all_shops_for_platform(), "error_message": error or ""},
    )


@app.post("/signup")
def signup_submit(
    request: Request,
    shop_name: str = Form(...),
    owner_name: str = Form(...),
    phone: str = Form(...),
    email: str = Form(...),
    login_id: str = Form(...),
    password: str = Form(...),
    password_confirm: str = Form(...),
    agree_terms: str | None = Form(None),
):
    from urllib.parse import quote

    shop_id = (login_id or "").strip().lower()
    shop_name = (shop_name or "").strip()
    owner_name = (owner_name or "").strip()
    phone = (phone or "").strip()
    email = (email or "").strip().lower()
    login_id = (login_id or "").strip()

    if not shop_name:
        error_message = "店舗名を入力してください。"
    elif not owner_name:
        error_message = "管理者名を入力してください。"
    elif not phone:
        error_message = "電話番号を入力してください。"
    elif not email:
        error_message = "メールアドレスを入力してください。"
    elif '@' not in email:
        error_message = "メールアドレスの形式が正しくありません。"
    elif not agree_terms:
        error_message = "利用規約等への同意が必要です。"
    elif len(login_id) < 4:
        error_message = "ログインIDは4文字以上で入力してください。"
    elif len(password or "") < 6:
        error_message = "パスワードは6文字以上で入力してください。"
    elif password != password_confirm:
        error_message = "確認用パスワードが一致しません。"
    else:
        error_message = ""

    if error_message:
        return templates.TemplateResponse(
            request=request,
            name="shop_signup.html",
            context={
                "shops": get_all_shops_for_platform(),
                "error_message": error_message,
                "form": {
                    "shop_name": shop_name,
                    "owner_name": owner_name,
                    "phone": phone,
                    "email": email,
                    "login_id": login_id,
                },
            },
            status_code=400,
        )

    code = f"{secrets.randbelow(1_000_000):06d}"

    try:
        verification = create_shop_registration_verification(
            shop_name=shop_name,
            owner_name=owner_name,
            phone=phone,
            email=email,
            login_id=login_id,
            password=password,
            code=code,
        )
    except Exception as exc:
        return templates.TemplateResponse(
            request=request,
            name="shop_signup.html",
            context={
                "shops": get_all_shops_for_platform(),
                "error_message": str(exc),
                "form": {
                    "shop_name": shop_name,
                    "owner_name": owner_name,
                    "phone": phone,
                    "email": email,
                    "login_id": login_id,
                },
            },
            status_code=400,
        )

    sent = _send_shop_registration_verification_mail(to_email=email, code=code)
    if not sent:
        return templates.TemplateResponse(
            request=request,
            name="shop_signup.html",
            context={
                "shops": get_all_shops_for_platform(),
                "error_message": "確認コードメールの送信に失敗しました。時間をおいて再度お試しください。",
                "form": {
                    "shop_name": shop_name,
                    "owner_name": owner_name,
                    "phone": phone,
                    "email": email,
                    "login_id": login_id,
                },
            },
            status_code=400,
        )

    verify_url = f"/signup/verify?shop_id={quote(str(verification.get('shop_id') or ''), safe='')}&token={quote(str(verification.get('token') or ''), safe='')}"
    return RedirectResponse(url=verify_url, status_code=303)


@app.get("/signup/verify", response_class=HTMLResponse)
def signup_verify_page(request: Request, shop_id: str, token: str, error: str | None = None):
    verification = get_shop_registration_verification(shop_id, token)
    if verification is None:
        return RedirectResponse(url="/signup?error=確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。", status_code=303)

    return templates.TemplateResponse(
        request=request,
        name="shop_signup_verify.html",
        context={
            "request": request,
            "token": token,
            "shop_id": shop_id,
            "pending_email": str(verification.get('email') or ''),
            "error_message": error or "",
        },
    )


@app.post("/signup/verify")
def signup_verify_submit(
    request: Request,
    shop_id: str = Form(...),
    token: str = Form(...),
    code: str = Form(...),
):
    from urllib.parse import quote

    verification = get_shop_registration_verification(shop_id, token)
    if verification is None:
        return RedirectResponse(url="/signup?error=確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。", status_code=303)

    normalized_code = ''.join(ch for ch in str(code or '') if ch.isdigit())
    if len(normalized_code) != 6:
        return RedirectResponse(
            url=f"/signup/verify?shop_id={quote(shop_id, safe='')}&token={quote(token, safe='')}&error=確認コードは6桁の数字で入力してください。",
            status_code=303,
        )

    try:
        verified = verify_shop_registration_code(shop_id, token, normalized_code)
        if verified is None:
            return RedirectResponse(
                url=f"/signup/verify?shop_id={quote(shop_id, safe='')}&token={quote(token, safe='')}&error=確認コードが正しくありません。",
                status_code=303,
            )
        consume_shop_registration_verification(shop_id, token)
    except ValueError as exc:
        return RedirectResponse(
            url=f"/signup/verify?shop_id={quote(shop_id, safe='')}&token={quote(token, safe='')}&error={quote(str(exc), safe='')}",
            status_code=303,
        )

    return RedirectResponse(
        url="/store-login?registered=店舗を登録しました。ログインしてください。",
        status_code=303,
    )


app.include_router(admin_router)
app.include_router(admin_patch.router, prefix="/admin")


@app.get("/admin/login/{shop_id}")
def admin_login_redirect(shop_id: str):
    normalized_shop_id = (shop_id or "").strip().lower()
    return RedirectResponse(f"/store-login?shop_id={normalized_shop_id}", status_code=303)


@app.get("/admin/{shop_id}/settings", response_class=HTMLResponse)
def admin_settings_page(request: Request, shop_id: str):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop_management_data(normalized_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="見つかりません")

    admin_users = get_admin_users(normalized_shop_id)
    subscription = get_shop_subscription(normalized_shop_id) or {}
    available_plans = get_plans(active_only=True)
    child_shops = get_child_shops(normalized_shop_id)
    parent_shop = get_parent_shop(normalized_shop_id)
    success_message = request.query_params.get("saved", "")
    error_message = request.query_params.get("error", "")
    template_name = "admin/tool/settings.html" if shop.get("admin_ui_mode") == "tool" else "admin/settings.html"

    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": normalized_shop_id,
            "admin_users": admin_users,
            "subscription": subscription,
            "available_plans": available_plans,
            "child_shops": child_shops,
            "parent_shop": parent_shop,
            "is_child_shop": int(shop.get("is_child_shop") or 0) == 1,
            "is_parent_premium": _is_premium_subscription(subscription),
            "current_admin_name": request.session.get("store_logged_in_admin_name") or (admin_users[0].get("name") if admin_users else ""),
            "staff_list": shop.get("staff_list", []),
            "menus": shop.get("menus", []),
            "customers": get_customers(normalized_shop_id),
            "today_reservations": [item for item in get_reservations(normalized_shop_id) if str(item.get("reservation_date") or "") == date.today().isoformat()],
            "active_page": "settings",
            "success_message": success_message,
            "error_message": error_message,
            "message": success_message,
        },
    )


@app.post("/admin/{shop_id}/child-shops")
def admin_child_shop_create(
    request: Request,
    shop_id: str,
    child_shop_name: str = Form(...),
    child_shop_id: str = Form(...),
    child_password: str = Form(...),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop_management_data(normalized_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="見つかりません")
    if int(shop.get("is_child_shop") or 0) == 1:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=子店舗からは子店舗を追加できません。", status_code=303)

    subscription = get_shop_subscription(normalized_shop_id) or {}
    if not _is_premium_subscription(subscription):
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=子店舗追加はプレミアムプラン専用です。", status_code=303)

    try:
        create_child_shop_under_parent(
            parent_shop_id=normalized_shop_id,
            child_shop_id=child_shop_id,
            child_shop_name=child_shop_name,
            password=child_password,
        )
    except ValueError as exc:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error={str(exc)}", status_code=303)

    return RedirectResponse(f"/admin/{normalized_shop_id}/settings?saved=子店舗を追加しました。", status_code=303)


@app.post("/admin/{shop_id}/settings")
def admin_settings_save(
    request: Request,
    shop_id: str,
    shop_name: str = Form(...),
    catch_copy: str = Form(""),
    description: str = Form(""),
    phone: str = Form(""),
    address: str = Form(""),
    business_hours: str = Form(""),
    holiday: str = Form(""),
    admin_ui_mode: str = Form("web"),
    primary_color: str = Form("#2ec4b6"),
    primary_dark: str = Form("#159a90"),
    accent_bg: str = Form("#f7fffe"),
    heading_bg_color: str = Form("#ff6f91"),
    reminder_enabled: str = Form("0"),
    reminder_day_before_enabled: str = Form("0"),
    reminder_day_before_time: str = Form("20:00"),
    reminder_same_day_enabled: str = Form("0"),
    reminder_same_day_hours_before: str = Form("1"),
    reminder_day_before_subject: str = Form(""),
    reminder_day_before_body: str = Form(""),
    reminder_same_day_subject: str = Form(""),
    reminder_same_day_body: str = Form(""),
    menu_name: list[str] = Form([]),
    menu_duration: list[str] = Form([]),
    menu_price: list[str] = Form([]),
    menu_description: list[str] = Form([]),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    menus = []
    row_count = max(len(menu_name), len(menu_duration), len(menu_price), len(menu_description))
    for idx in range(row_count):
        name = (menu_name[idx] if idx < len(menu_name) else "").strip()
        duration_text = (menu_duration[idx] if idx < len(menu_duration) else "").strip()
        price_text = (menu_price[idx] if idx < len(menu_price) else "").strip()
        description_text = (menu_description[idx] if idx < len(menu_description) else "").strip()
        if not any([name, duration_text, price_text, description_text]):
            continue
        try:
            duration_value = int(duration_text or 0)
        except ValueError:
            duration_value = 0
        try:
            price_value = int(price_text or 0)
        except ValueError:
            price_value = 0
        menus.append({
            "name": name,
            "duration": duration_value,
            "price": price_value,
            "description": description_text,
        })

    try:
        reminder_same_day_hours_before_value = max(0, int(str(reminder_same_day_hours_before or '0').strip() or 0))
    except ValueError:
        reminder_same_day_hours_before_value = 1

    update_shop_basic_info(
        normalized_shop_id,
        shop_name=shop_name,
        phone=phone,
        address=address,
        business_hours=business_hours,
        holiday=holiday,
        catch_copy=catch_copy,
        description=description,
        admin_ui_mode=admin_ui_mode,
        primary_color=primary_color,
        primary_dark=primary_dark,
        accent_bg=accent_bg,
        heading_bg_color=heading_bg_color,
        reminder_enabled=1 if reminder_enabled == '1' else 0,
        reminder_day_before_enabled=1 if reminder_day_before_enabled == '1' else 0,
        reminder_day_before_time=str(reminder_day_before_time or '20:00').strip() or '20:00',
        reminder_same_day_enabled=1 if reminder_same_day_enabled == '1' else 0,
        reminder_same_day_hours_before=reminder_same_day_hours_before_value,
        reminder_day_before_subject=reminder_day_before_subject,
        reminder_day_before_body=reminder_day_before_body,
        reminder_same_day_subject=reminder_same_day_subject,
        reminder_same_day_body=reminder_same_day_body,
        menus=menus,
    )
    return RedirectResponse(f"/admin/{normalized_shop_id}/settings?saved=店舗設定を保存しました。", status_code=303)


def _build_admin_common_context(request: Request, shop_id: str) -> tuple[str, dict, list, list, list, dict, list, str]:
    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop_management_data(normalized_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="見つかりません")

    reservations = get_reservations(normalized_shop_id)
    customers = get_customers(normalized_shop_id)
    admin_users = get_admin_users(normalized_shop_id)
    subscription = get_shop_subscription(normalized_shop_id) or {}
    shop = _build_shop_with_visible_staff(shop, subscription)
    reservations = _attach_staff_avatar_to_reservations(reservations, shop.get('staff_list', []))
    available_plans = get_plans(active_only=True)
    current_admin_name = request.session.get("store_logged_in_admin_name") or (admin_users[0].get("name") if admin_users else "")
    return normalized_shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name


@app.get("/admin/{shop_id}", response_class=HTMLResponse)
@app.get("/admin/{shop_id}/dashboard", response_class=HTMLResponse)
def admin_page(request: Request, shop_id: str, error_message: str = ""):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name = _build_admin_common_context(request, shop_id)

    month_value = request.query_params.get("month") or date.today().strftime("%Y-%m")
    try:
        current_month = datetime.strptime(month_value, "%Y-%m").date().replace(day=1)
    except ValueError:
        current_month = date.today().replace(day=1)
    calendar_month_value = current_month.strftime("%Y-%m")
    calendar_month_label = current_month.strftime("%Y年%m月")
    prev_month_date = (current_month.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month_date = (current_month.replace(day=28) + timedelta(days=4)).replace(day=1)
    calendar_prev_month = prev_month_date.strftime("%Y-%m")
    calendar_next_month = next_month_date.strftime("%Y-%m")

    selected_date_obj = _safe_parse_date(request.query_params.get("date"), date.today())
    selected_date = selected_date_obj.isoformat()
    week_start = _normalize_week_start(_safe_parse_date(request.query_params.get("week_start"), selected_date_obj))

    holiday_idx = WEEKDAY_MAP.get(str(shop.get("holiday") or ""))
    cal = calendar.Calendar(firstweekday=6)
    today_obj = date.today()
    active_reservations = [r for r in reservations if str(r.get("status") or "") != "キャンセル"]
    counts_by_date = {}
    for item in active_reservations:
        key = str(item.get("reservation_date") or "")
        counts_by_date[key] = counts_by_date.get(key, 0) + 1

    calendar_days = []
    for week in cal.monthdatescalendar(current_month.year, current_month.month):
        for current_day in week:
            iso = current_day.isoformat()
            calendar_days.append({
                "date": iso,
                "day": current_day.day,
                "count": counts_by_date.get(iso, 0),
                "is_current_month": current_day.month == current_month.month,
                "is_today": current_day == today_obj,
                "is_holiday": holiday_idx is not None and current_day.weekday() == holiday_idx,
            })

    selected_day_schedule = sorted(
        [r for r in reservations if str(r.get("reservation_date") or "") == selected_date],
        key=lambda x: (str(x.get("start_time") or ""), int(x.get("id") or 0)),
    )
    selected_day_count = len([r for r in selected_day_schedule if str(r.get("status") or "") != "キャンセル"])

    today = today_obj.isoformat()
    today_sales = sum(int(item.get("price") or 0) for item in reservations if item.get("reservation_date") == today and item.get("status") == "来店済み")
    today_completed_count = sum(1 for item in reservations if item.get("reservation_date") == today and item.get("status") == "来店済み")
    total_sales = sum(int(item.get("price") or 0) for item in reservations if item.get("status") == "来店済み")
    completed_count = sum(1 for item in reservations if item.get("status") == "来店済み")
    week_days, time_slots, weekly_rows = _build_week_availability_matrix(
        shop=shop,
        reservations=reservations,
        selected_date=selected_date_obj,
        week_start=week_start,
    )
    plan_context = _build_admin_plan_context(
        subscription=subscription,
        available_plans=available_plans,
        customers=customers,
        reservations=reservations,
        staff_list=shop.get("staff_list", []),
    )
    unread_chat_items = [_serialize_unread_chat_item(item) for item in get_admin_unread_chat_summary(shop_id)]
    template_name = "admin/tool/dashboard.html" if shop.get("admin_ui_mode") == "tool" else "admin/dashboard.html"

    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "customers": customers,
            "reservations": reservations,
            "staff_list": shop.get("staff_list", []),
            "menus": shop.get("menus", []),
            "today": today,
            "error_message": error_message,
            "sales_summary": {
                "today_sales": today_sales,
                "today_completed_count": today_completed_count,
                "total_sales": total_sales,
                "completed_count": completed_count,
            },
            "admin_users": admin_users,
            "subscription": subscription,
            "subscription_status_label": _format_admin_subscription_status_label(subscription),
            "available_plans": available_plans,
            **plan_context,
            "current_admin_name": current_admin_name,
            "calendar_days": calendar_days,
            "calendar_month_label": calendar_month_label,
            "calendar_month_value": calendar_month_value,
            "calendar_prev_month": calendar_prev_month,
            "calendar_next_month": calendar_next_month,
            "selected_date": selected_date,
            "selected_day_schedule": selected_day_schedule,
            "selected_day_count": selected_day_count,
            "week_days": week_days,
            "time_slots": time_slots,
            "weekly_rows": weekly_rows,
            "week_start": week_start.isoformat(),
            "prev_week_start": (week_start - timedelta(days=7)).isoformat(),
            "next_week_start": (week_start + timedelta(days=7)).isoformat(),
            "unread_chat_items": unread_chat_items,
            "active_page": "dashboard",
        },
    )


def _format_analysis_price(value) -> str:
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"


def _analysis_period_key(reservation_date: date, period: str) -> tuple[str, str]:
    if period == "month":
        return reservation_date.strftime("%Y-%m"), reservation_date.strftime("%Y年%m月")
    if period == "week":
        week_start = reservation_date - timedelta(days=reservation_date.weekday())
        return week_start.isoformat(), f"{week_start.strftime('%m/%d')}週"
    return reservation_date.isoformat(), reservation_date.strftime("%m/%d")


def _source_label(source: str) -> str:
    labels = {
        "admin": "管理画面",
        "line": "LINE",
        "web": "WEB",
        "google": "Google検索",
        "phone": "電話",
        "store": "店頭",
    }
    key = (source or "").strip().lower()
    return labels.get(key, source or "未設定")


def _build_analysis_context(reservations: list[dict], customers: list[dict], period: str) -> dict:
    period = period if period in {"day", "week", "month"} else "day"
    active_reservations = [r for r in reservations if str(r.get("status") or "") != "キャンセル"]
    completed_reservations = [r for r in active_reservations if str(r.get("status") or "") == "来店済み"]

    total_sales = sum(int(r.get("price") or 0) for r in completed_reservations)
    completed_count = len(completed_reservations)

    customer_counts: dict[str, int] = {}
    for r in active_reservations:
        customer_key = str(r.get("customer_id") or r.get("customer_email") or r.get("customer_name") or "").strip()
        if customer_key:
            customer_counts[customer_key] = customer_counts.get(customer_key, 0) + 1
    repeat_customers = sum(1 for count in customer_counts.values() if count >= 2)
    repeat_rate = round((repeat_customers / len(customer_counts) * 100), 1) if customer_counts else 0

    grouped: dict[str, dict] = {}
    for r in active_reservations:
        parsed_date = _safe_parse_date(r.get("reservation_date"), None)
        if not parsed_date:
            continue
        key, label = _analysis_period_key(parsed_date, period)
        if key not in grouped:
            grouped[key] = {"key": key, "label": label, "count": 0, "sales_raw": 0}
        grouped[key]["count"] += 1
        if str(r.get("status") or "") == "来店済み":
            grouped[key]["sales_raw"] += int(r.get("price") or 0)

    rows = [grouped[key] for key in sorted(grouped.keys())][-14:]
    max_count = max([row["count"] for row in rows] or [1])
    max_sales = max([row["sales_raw"] for row in rows] or [1])
    period_summary = []
    for row in rows:
        period_summary.append({
            "label": row["label"],
            "count": row["count"],
            "sales": _format_analysis_price(row["sales_raw"]),
            "count_height": max(4, round(row["count"] / max_count * 100)) if max_count else 4,
            "sales_height": max(4, round(row["sales_raw"] / max_sales * 100)) if max_sales else 4,
        })

    source_grouped: dict[str, dict] = {}
    for r in active_reservations:
        label = _source_label(str(r.get("source") or ""))
        if label not in source_grouped:
            source_grouped[label] = {"label": label, "count": 0, "sales_raw": 0}
        source_grouped[label]["count"] += 1
        if str(r.get("status") or "") == "来店済み":
            source_grouped[label]["sales_raw"] += int(r.get("price") or 0)

    total_source_count = sum(item["count"] for item in source_grouped.values()) or 1
    source_summary = []
    for item in sorted(source_grouped.values(), key=lambda x: (-x["count"], x["label"])):
        source_summary.append({
            "label": item["label"],
            "count": item["count"],
            "sales": _format_analysis_price(item["sales_raw"]),
            "percent": round(item["count"] / total_source_count * 100, 1),
        })

    return {
        "selected_period": period,
        "selected_period_label": {"day": "日別", "week": "週別", "month": "月別"}.get(period, "日別"),
        "analysis_summary": {
            "reservation_count": len(active_reservations),
            "total_sales": _format_analysis_price(total_sales),
            "repeat_rate": repeat_rate,
            "completed_count": completed_count,
        },
        "period_summary": period_summary,
        "source_summary": source_summary,
    }


@app.get("/admin/{shop_id}/analysis", response_class=HTMLResponse)
def admin_analysis_page(request: Request, shop_id: str, period: str = "day"):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name = _build_admin_common_context(request, shop_id)
    template_name = "admin/tool/analysis.html" if shop.get("admin_ui_mode") == "tool" else "admin/analysis.html"
    plan_context = _build_admin_plan_context(
        subscription=subscription,
        available_plans=available_plans,
        customers=customers,
        reservations=reservations,
        staff_list=shop.get("staff_list", []),
    )
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "customers": customers,
            "reservations": reservations,
            "admin_users": admin_users,
            "subscription": subscription,
            "subscription_status_label": _format_admin_subscription_status_label(subscription),
            "available_plans": available_plans,
            **plan_context,
            "current_admin_name": current_admin_name,
            "active_page": "analysis",
            **_build_analysis_context(reservations, customers, period),
        },
    )


@app.get("/admin/{shop_id}/reservations", response_class=HTMLResponse)
def admin_reservations_page(request: Request, shop_id: str, error_message: str = ""):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name = _build_admin_common_context(request, shop_id)
    month_value = request.query_params.get("month") or date.today().strftime("%Y-%m")
    try:
        current_month = datetime.strptime(month_value, "%Y-%m").date().replace(day=1)
    except ValueError:
        current_month = date.today().replace(day=1)

    filter_date = request.query_params.get("date") or date.today().isoformat()
    selected_date_obj = _safe_parse_date(filter_date, date.today())
    selected_date = selected_date_obj.isoformat()
    week_start = _normalize_week_start(_safe_parse_date(request.query_params.get("week_start"), selected_date_obj))
    filter_staff = (request.query_params.get("staff_id") or "").strip()
    selected_staff = next((s for s in shop.get("staff_list", []) if str(s.get("id")) == filter_staff), None)

    filtered_reservations = reservations
    if filter_staff:
        filtered_reservations = [r for r in filtered_reservations if str(r.get("staff_id") or "") == filter_staff]

    holiday_idx = WEEKDAY_MAP.get(str(shop.get("holiday") or ""))
    cal = calendar.Calendar(firstweekday=6)
    counts_by_date = {}
    for item in filtered_reservations:
        if str(item.get("status") or "") == "キャンセル":
            continue
        key = str(item.get("reservation_date") or "")
        counts_by_date[key] = counts_by_date.get(key, 0) + 1

    calendar_days = []
    today_obj = date.today()
    for week in cal.monthdatescalendar(current_month.year, current_month.month):
        for current_day in week:
            iso = current_day.isoformat()
            calendar_days.append({
                "date": iso,
                "day": current_day.day,
                "count": counts_by_date.get(iso, 0),
                "is_current_month": current_day.month == current_month.month,
                "is_today": current_day == today_obj,
                "is_holiday": holiday_idx is not None and current_day.weekday() == holiday_idx,
            })

    selected_day_schedule = sorted(
        [r for r in filtered_reservations if str(r.get("reservation_date") or "") == selected_date],
        key=lambda x: (str(x.get("start_time") or ""), int(x.get("id") or 0)),
    )
    selected_day_count = len([r for r in selected_day_schedule if str(r.get("status") or "") != "キャンセル"])
    selected_day_sales = sum(int(item.get("price") or 0) for item in selected_day_schedule if str(item.get("status") or "") == "来店済み")
    selected_day_completed_count = sum(1 for item in selected_day_schedule if str(item.get("status") or "") == "来店済み")
    reservation_items = sorted(
        filtered_reservations,
        key=lambda x: (str(x.get("reservation_date") or ""), str(x.get("start_time") or ""), int(x.get("id") or 0)),
        reverse=True,
    )
    completed_items = [item for item in filtered_reservations if str(item.get("status") or "") == "来店済み"]
    view_sales_summary = {
        "total_sales": sum(int(item.get("price") or 0) for item in completed_items),
        "completed_count": len(completed_items),
    }

    week_days, time_slots, weekly_rows = _build_week_availability_matrix(
        shop=shop,
        reservations=filtered_reservations,
        selected_date=selected_date_obj,
        week_start=week_start,
        staff_id=filter_staff or None,
    )

    now = datetime.now()
    current_time_label = now.strftime("%H:%M")
    current_slot_label = _format_minutes_hhmm((now.hour * 60 + now.minute) // 30 * 30)
    current_minutes = now.hour * 60 + now.minute

    active_reservations = [r for r in reservations if str(r.get("status") or "") != "キャンセル"]
    staff_now_list = []
    for staff in shop.get("staff_list", []):
        staff_id_value = staff.get("id")
        staff_id_text = str(staff_id_value or "").strip()
        now_reservation = None
        for reservation in active_reservations:
            if str(reservation.get("staff_id") or "").strip() != staff_id_text:
                continue
            if str(reservation.get("reservation_date") or "") != selected_date:
                continue
            start_minutes = _parse_hhmm_to_minutes(str(reservation.get("start_time") or "")[:5])
            end_minutes = _parse_hhmm_to_minutes(str(reservation.get("end_time") or "")[:5])
            if start_minutes is None:
                continue
            if end_minutes is None or end_minutes <= start_minutes:
                duration = int(reservation.get("duration") or 30)
                end_minutes = start_minutes + max(duration, 30)
            if start_minutes <= current_minutes < end_minutes:
                now_reservation = reservation
                break
        if now_reservation:
            now_status = str(now_reservation.get("menu_name") or "対応中")
            now_detail = f"{str(now_reservation.get('start_time') or '')[:5]}〜{str(now_reservation.get('end_time') or '')[:5]} / {str(now_reservation.get('customer_name') or '予約あり')}"
        else:
            now_status = "予定なし"
            now_detail = f"{current_slot_label} 時点で対応中の予約はありません"

        jump_url = f"/admin/{shop_id}/staff/{staff_id_text}?date={selected_date}&week_start={week_start.isoformat()}"
        staff_now_list.append({
            "id": staff_id_value,
            "name": staff.get("name") or f"スタッフ{staff_id_text}",
            "avatar_url": staff.get('avatar_url') or _resolve_staff_avatar_url(staff),
            "now_status": now_status,
            "now_detail": now_detail,
            "jump_url": jump_url,
        })

    template_name = "admin/tool/reservations.html" if shop.get("admin_ui_mode") == "tool" else "admin/reservations.html"
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "customers": customers,
            "staff_list": shop.get("staff_list", []),
            "menus": shop.get("menus", []),
            "reservations": filtered_reservations,
            "reservation_items": reservation_items,
            "today": today_obj.isoformat(),
            "filter_date": filter_date,
            "filter_staff": filter_staff,
            "selected_staff": selected_staff,
            "calendar_days": calendar_days,
            "calendar_month_label": current_month.strftime("%Y年%m月"),
            "calendar_month_value": current_month.strftime("%Y-%m"),
            "calendar_prev_month": ((current_month.replace(day=1) - timedelta(days=1)).replace(day=1)).strftime("%Y-%m"),
            "calendar_next_month": ((current_month.replace(day=28) + timedelta(days=4)).replace(day=1)).strftime("%Y-%m"),
            "selected_date": selected_date,
            "selected_day_schedule": selected_day_schedule,
            "selected_day_count": selected_day_count,
            "selected_day_sales": selected_day_sales,
            "selected_day_completed_count": selected_day_completed_count,
            "view_sales_summary": view_sales_summary,
            "week_days": week_days,
            "time_slots": time_slots,
            "weekly_rows": weekly_rows,
            "week_start": week_start.isoformat(),
            "prev_week_start": (week_start - timedelta(days=7)).isoformat(),
            "next_week_start": (week_start + timedelta(days=7)).isoformat(),
            "error_message": error_message,
            "admin_users": admin_users,
            "subscription": subscription,
            "available_plans": available_plans,
            "current_admin_name": current_admin_name,
            "reservation_blocks": [],
            "staff_now_list": staff_now_list,
            "current_time_label": current_time_label,
            "current_slot_label": current_slot_label,
            "active_page": "reservations",
        },
    )


@app.get("/admin/{shop_id}/timeline-karte", response_class=HTMLResponse)
def admin_timeline_karte_page(request: Request, shop_id: str):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name = _build_admin_common_context(request, shop_id)
    if not _is_premium_subscription(subscription):
        return RedirectResponse(f"/admin/{shop_id}/reservations?error_message=タイムラインカルテはプレミアムプラン専用です。", status_code=303)

    selected_date_obj = _safe_parse_date(request.query_params.get("date"), date.today())
    timeline_context = _build_timeline_karte_context(shop=shop, reservations=reservations, selected_date_obj=selected_date_obj)
    template_name = "admin/tool/timeline_karte.html" if shop.get("admin_ui_mode") == "tool" else "admin/timeline_karte.html"
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "customers": customers,
            "staff_list": shop.get("staff_list", []),
            "menus": shop.get("menus", []),
            "reservations": reservations,
            "today": date.today().isoformat(),
            "admin_users": admin_users,
            "subscription": subscription,
            "available_plans": available_plans,
            "current_admin_name": current_admin_name,
            "active_page": "timeline_karte",
            "timeline_auto_refresh_seconds": 60,
            **timeline_context,
        },
    )


@app.get("/admin/{shop_id}/reservations/{reservation_id}", response_class=HTMLResponse)
def admin_reservation_detail_page(request: Request, shop_id: str, reservation_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name = _build_admin_common_context(request, shop_id)
    reservation = next((item for item in reservations if int(item.get("id") or 0) == reservation_id), None)
    if reservation is None:
        raise HTTPException(status_code=404, detail="予約が見つかりません")

    customer = None
    customer_id = int(reservation.get("customer_id") or 0)
    if customer_id:
        customer = next((item for item in customers if int(item.get("id") or 0) == customer_id), None)

    template_name = "admin/tool/reservation_detail.html" if shop.get("admin_ui_mode") == "tool" else "admin/reservation_detail.html"
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "reservation": reservation,
            "customer": customer,
            "customers": customers,
            "staff_list": shop.get("staff_list", []),
            "menus": shop.get("menus", []),
            "reservations": reservations,
            "today": date.today().isoformat(),
            "admin_users": admin_users,
            "subscription": subscription,
            "available_plans": available_plans,
            "current_admin_name": current_admin_name,
            "active_page": "reservations",
        },
    )


@app.get("/admin/{shop_id}/customers", response_class=HTMLResponse)
def admin_customers_page(request: Request, shop_id: str, error_message: str = ""):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name = _build_admin_common_context(request, shop_id)
    keyword = (request.query_params.get("q") or "").strip()
    sort_order = (request.query_params.get("sort") or "new").strip()
    visit_counts = _build_customer_visit_counts(reservations)
    member_customer_ids = get_member_customer_ids(shop_id)
    customer_items = [
        {
            **customer,
            "visit_count": visit_counts.get(int(customer.get("id") or 0), 0),
            "is_member": int(customer.get("id") or 0) in member_customer_ids,
            "membership_label": "会員" if int(customer.get("id") or 0) in member_customer_ids else "非会員",
        }
        for customer in customers
    ]
    if keyword:
        lowered = keyword.lower()
        customer_items = [
            item for item in customer_items
            if lowered in str(item.get("name") or "").lower()
            or lowered in str(item.get("phone") or "").lower()
            or lowered in str(item.get("email") or "").lower()
        ]
    customer_items, sort_order = _sort_customer_items(customer_items, sort_order)
    member_customer_items = [item for item in customer_items if item.get("is_member")]
    non_member_customer_items = [item for item in customer_items if not item.get("is_member")]
    unread_chat_items = [_serialize_unread_chat_item(item) for item in get_admin_unread_chat_summary(shop_id)]
    unread_chat_map = {int(item.get("customer_id") or 0): int(item.get("unread_count") or 0) for item in unread_chat_items}
    template_name = "admin/tool/customers.html" if shop.get("admin_ui_mode") == "tool" else "admin/customers.html"
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "customers": customers,
            "customer_items": customer_items,
            "member_customer_items": member_customer_items,
            "non_member_customer_items": non_member_customer_items,
            "keyword": keyword,
            "sort_order": sort_order,
            "today": date.today().isoformat(),
            "reservations": reservations,
            "admin_users": admin_users,
            "subscription": subscription,
            "available_plans": available_plans,
            "current_admin_name": current_admin_name,
            "error_message": error_message,
            "success_message": request.query_params.get("saved", ""),
            "active_page": "customers",
            "unread_chat_items": unread_chat_items,
            "unread_chat_map": unread_chat_map,
        },
    )





@app.get("/admin/{shop_id}/line-settings", response_class=HTMLResponse)
def admin_line_settings(request: Request, shop_id: str):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

    settings = get_shop_line_settings(shop_id)
    recent_line_users = get_recent_line_webhook_users(shop_id)
    line_mode = normalize_line_mode(settings.get("line_mode"))
    liff_id = str(settings.get("line_liff_id") or "").strip()
    channel_access_token = str(settings.get("line_channel_access_token") or "").strip()
    channel_secret = str(settings.get("line_channel_secret") or "").strip()
    official_url = str(settings.get("line_official_url") or "").strip()
    saved = str(request.query_params.get("saved") or "").strip()

    base_url = str(request.base_url).rstrip("/")
    liff_endpoint_url = f"{base_url}/shop/{shop_id}/line-reserve"
    developers_url = "https://developers.line.biz/console/"
    shop_name = str(shop.get("shop_name") or shop_id)
    webhook_url = f"https://www.rakubai.net/line/webhook/{shop_id}/"
    recent_line_user_options_html = ""
    if recent_line_users:
        for item in recent_line_users:
            uid = str(item.get("line_user_id") or "").strip()
            updated = str(item.get("updated_at") or "").strip()
            msg = str(item.get("message_text") or "").strip()
            if uid:
                recent_line_user_options_html += (
                    f'<div class="line-user-row">'
                    f'<div class="line-user-id">{uid}</div>'
                    f'<button type="button" class="btn-secondary" onclick="setTestLineUserId(\'{uid}\')">このIDを使う</button>'
                    f'<div class="line-user-meta">取得日時：{updated} / メッセージ：{msg}</div>'
                    f'</div>'
                )
    else:
        recent_line_user_options_html = '<div class="empty-line-user">まだ取得されていません。Webhook URLを保存後、公式LINEに「テスト」と送ってから、この画面を再読み込みしてください。</div>'


    html = """
<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>LINE連携設定</title>
  <style>
    body {
      margin: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f5f7fb;
      color: #111827;
    }
    .wrap {
      max-width: 1120px;
      margin: 0 auto;
      padding: 24px 16px 44px;
    }
    .hero {
      border-radius: 28px;
      padding: 28px;
      color: #fff;
      background: linear-gradient(135deg, #16a34a, #22c55e);
      box-shadow: 0 18px 48px rgba(22, 163, 74, .22);
      margin-bottom: 18px;
    }
    .hero h1 {
      margin: 0 0 8px;
      font-size: 30px;
    }
    .hero p {
      margin: 0;
      line-height: 1.75;
      opacity: .96;
      font-weight: 600;
    }
    .hero .small {
      margin-top: 10px;
      font-size: 13px;
      opacity: .92;
    }
    .grid {
      display: grid;
      grid-template-columns: 1.08fr .92fr;
      gap: 16px;
      align-items: start;
    }
    .card {
      background: #fff;
      border-radius: 24px;
      padding: 22px;
      box-shadow: 0 12px 36px rgba(15, 23, 42, .08);
    }
    h2 {
      margin: 0 0 16px;
      font-size: 22px;
    }
    .saved {
      background: #dcfce7;
      color: #166534;
      border-radius: 14px;
      padding: 12px 14px;
      margin-bottom: 14px;
      font-weight: 900;
    }
    .step {
      display: grid;
      grid-template-columns: 38px 1fr;
      gap: 12px;
      padding: 14px 0;
      border-bottom: 1px solid #e5e7eb;
    }
    .step:last-child {
      border-bottom: 0;
    }
    .num {
      width: 38px;
      height: 38px;
      border-radius: 999px;
      display: flex;
      align-items: center;
      justify-content: center;
      background: #dcfce7;
      color: #15803d;
      font-weight: 900;
      font-size: 16px;
    }
    .step-title {
      display: block;
      font-weight: 900;
      margin-bottom: 5px;
      font-size: 15px;
    }
    .step-text {
      color: #6b7280;
      line-height: 1.7;
      font-size: 14px;
    }
    .step-text b {
      color: #111827;
    }
    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 14px 0;
    }
    .btn, button {
      appearance: none;
      border: 0;
      border-radius: 999px;
      padding: 12px 18px;
      font-weight: 900;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 7px;
      cursor: pointer;
      font-size: 14px;
      line-height: 1;
    }
    .btn-primary, button.btn-primary {
      background: #16a34a;
      color: white;
    }
    .btn-secondary, button.btn-secondary {
      background: #eef2f7;
      color: #374151;
    }
    .copybox {
      width: 100%;
      background: #f8fafc;
      border: 1px solid #dbe4ef;
      border-radius: 16px;
      padding: 14px;
      font-size: 14px;
      line-height: 1.55;
      overflow-wrap: anywhere;
      color: #0f172a;
      margin-top: 8px;
    }
    .notice {
      margin-top: 12px;
      padding: 13px 14px;
      border-radius: 16px;
      background: #fff7ed;
      border: 1px solid #fed7aa;
      color: #9a3412;
      line-height: 1.65;
      font-size: 13px;
      font-weight: 650;
    }
    .status {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 10px 0 14px;
    }
    .pill {
      background: #eef2f7;
      color: #374151;
      border-radius: 999px;
      padding: 7px 11px;
      font-size: 13px;
      font-weight: 900;
    }
    .pill.ok {
      background: #dcfce7;
      color: #166534;
    }
    label {
      display: block;
      margin: 14px 0 7px;
      font-weight: 900;
    }
    input, textarea {
      width: 100%;
      border: 1px solid #d1d5db;
      border-radius: 15px;
      padding: 13px 14px;
      font-size: 15px;
      background: #fff;
      box-sizing: border-box;
    }
    textarea {
      min-height: 88px;
      resize: vertical;
      font-family: inherit;
    }
    .hint {
      color: #6b7280;
      font-size: 13px;
      line-height: 1.65;
      margin-top: 7px;
    }
    .checklist {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 18px;
      padding: 14px 16px;
      margin: 12px 0 0;
    }
    .checklist ul {
      margin: 8px 0 0;
      padding-left: 20px;
      line-height: 1.9;
      color: #374151;
      font-size: 14px;
    }
    .manual {
      background: #f0fdf4;
      border: 1px solid #bbf7d0;
      color: #14532d;
      padding: 13px 14px;
      border-radius: 16px;
      line-height: 1.7;
      font-size: 13px;
      font-weight: 700;
      margin: 10px 0 0;
    }
    .mode-guide {
      display: none;
    }
    .mode-guide.is-active {
      display: block;
    }
    .mode-title {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 8px 13px;
      background: #dcfce7;
      color: #166534;
      font-weight: 900;
      margin: 8px 0 12px;
    }
    .mode-lead {
      color: #374151;
      font-weight: 700;
      line-height: 1.8;
      margin: 0 0 12px;
    }
    .mini {
      color: #6b7280;
      font-size: 13px;
      line-height: 1.7;
      margin-top: 16px;
    }
    @media (max-width: 900px) {
      .grid {
        grid-template-columns: 1fr;
      }
      .hero h1 {
        font-size: 25px;
      }
    }
  
        .line-user-list {
          border: 1px solid #dbe4f0;
          border-radius: 16px;
          background: #f8fafc;
          padding: 12px;
          margin-top: 8px;
        }
        .line-user-row {
          padding: 10px 0;
          border-bottom: 1px solid #e5e7eb;
        }
        .line-user-row:last-child {
          border-bottom: 0;
        }
        .line-user-id {
          font-weight: 900;
          word-break: break-all;
          color: #0f172a;
          margin-bottom: 8px;
        }
        .line-user-meta {
          color: #64748b;
          font-size: 12px;
          margin-top: 6px;
          line-height: 1.6;
        }
        .empty-line-user {
          color: #64748b;
          line-height: 1.7;
          font-weight: 700;
        }

      </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>LINE連携設定</h1>
      <p>{{SHOP_NAME}} の公式LINEと予約ページを連携します。ログインからLIFF ID保存まで、1つずつ進めます。</p>
      <div class="small">この設定は最初の1回だけです。設定後は、お客様がLINEから予約した時にLINE user_idを自動で紐づけられます。</div>
    </div>

    <div class="grid">
      <section class="card">
        <h2>店舗スタッフ向け：操作説明</h2>
        <div class="hint">右側の「LINE連携モード」を変更すると、この説明も自動で切り替わります。</div>

        <div id="guide-off" class="mode-guide">
          <div class="mode-title">利用しない</div>
          <p class="mode-lead">LINE連携は無効です。予約通知やリマインドはメール中心で運用します。</p>

          <div class="step">
            <div class="num">1</div>
            <div>
              <span class="step-title">LINE設定は入力しなくてOKです</span>
              <div class="step-text">LINE通知を使わない場合は、このモードのままで保存してください。</div>
            </div>
          </div>

          <div class="step">
            <div class="num">2</div>
            <div>
              <span class="step-title">予約ページは通常どおり使えます</span>
              <div class="step-text">お客様はWeb予約を行い、店舗は通常の予約管理画面で確認できます。</div>
            </div>
          </div>

          <div class="notice">あとから簡単モードやLINE完結モードへ変更できます。</div>
        </div>

        <div id="guide-login" class="mode-guide">
          <div class="mode-title">簡単モード：Web予約＋LINE通知</div>
          <p class="mode-lead">はじめての店舗におすすめです。予約は通常のWebページで行い、予約完了通知とリマインドをLINEで送ります。</p>

          <div class="actions">
            <a class="btn btn-primary" href="{{DEVELOPERS_URL}}" target="_blank" rel="noopener">1. LINE Developersを開く</a>
          </div>

          <div class="step">
            <div class="num">1</div>
            <div>
              <span class="step-title">LINE Developersにログインします</span>
              <div class="step-text">LINEアカウントでログインし、店舗用のプロバイダーを選びます。なければ店舗名で作成してください。</div>
            </div>
          </div>

          <div class="step">
            <div class="num">2</div>
            <div>
              <span class="step-title">プロバイダーを作成します</span>
              <div class="step-text">まだプロバイダーがない場合は、店舗名または運営会社名で新しく作成してください。すでに店舗用のプロバイダーがある場合は、そのプロバイダーを選びます。</div>
            </div>
          </div>

          <div class="step">
            <div class="num">3</div>
            <div>
              <span class="step-title">LINEログインチャネルを作成します</span>
              <div class="step-text">
                作成したプロバイダーの中で、LINEログインチャネルを作成します。入力項目は次の内容を目安にしてください。<br><br>
                <b>サービス提供地域：</b>店舗がある国を選びます。日本の店舗なら「日本」を選択します。<br>
                <b>会社・事業者の所在国・地域：</b>店舗または運営会社の所在地を選びます。日本で運営している場合は「日本」でOKです。<br>
                <b>チャネル名：</b>お客様に見えても分かりやすい名前にします。例：「店舗名 予約」「店舗名 LINE予約」。<br>
                <b>チャネル説明：</b>用途を書きます。例：「Web予約時にLINE通知を受け取るためのログインチャネルです。」<br>
                <b>アプリタイプ：</b>簡単モードでは <b>Webアプリ</b> を選択します。<br>
                <b>メールアドレス：</b>LINE Developersからのお知らせを受け取れる店舗または運営者のメールアドレスを入力します。<br>
                <b>プライバシーポリシーURL：</b>用意している場合は予約サイトや店舗サイトのプライバシーポリシーページURLを入力します。未整備なら先にページを用意するのがおすすめです。<br>
                <b>利用規約URL：</b>用意している場合は予約サイトの利用規約ページURLを入力します。予約、キャンセル、通知、個人情報の扱いが分かる内容にしておくと安心です。<br>
                <b>LINE開発者契約への同意：</b>内容を確認し、問題なければチェックを入れて作成します。
              </div>
            </div>
          </div>

          <div class="step">
            <div class="num">4</div>
            <div>
              <span class="step-title">チャネルシークレットをコピーします</span>
              <div class="step-text">作成したLINEログインチャネルの基本設定から、チャネルシークレットを右側に貼り付けます。</div>
            </div>
          </div>

          <div class="step">
            <div class="num">5</div>
            <div>
              <span class="step-title">Messaging APIとWebhookを設定します</span>
              <div class="step-text">
                ここでは、LINE通知を送るための準備を、順番どおりに設定します。<br>
                この順番どおりに進めれば、user_id取得とテスト送信までできます。<br><br>

                <b>① LINE公式アカウントを用意します</b><br>
                すでに店舗の公式LINEアカウントがある場合は、そのアカウントを使います。<br>
                まだ無い場合は、先にLINE Official Account Managerで公式LINEアカウントを作成します。<br><br>

                <b>② LINE Official Account ManagerでMessaging APIを有効にします</b><br>
                LINE Official Account Managerで店舗の公式LINEアカウントを開きます。<br>
                右上の <b>設定（歯車マーク）</b> を開きます。<br>
                左メニューの <b>Messaging API</b> を開きます。<br>
                <b>「Messaging APIを利用する」</b> が表示されている場合は押します。<br>
                この操作をすると、LINE通知用のMessaging APIが使える状態になります。<br><br>

                <b>③ プロバイダーを選びます</b><br>
                Messaging APIを有効にする途中で、プロバイダーを選ぶ画面が出る場合があります。<br>
                店舗用のプロバイダーがある場合はそれを選びます。<br>
                無い場合は、店舗名または運営会社名で新しく作成します。<br><br>

                <b>④ この管理画面のWebhook URLをコピーします</b><br>
                右側に <b>あなたのWebhook URLはこれです</b> が表示されています。<br>
                これは、この予約システムがLINEからの通知を受け取るためのURLです。<br>
                このURLをコピーします。<br><br>

                <b>⑤ LINE Official Account ManagerにWebhook URLを貼り付けます</b><br>
                LINE Official Account Managerに戻り、店舗の公式LINEアカウントを開きます。<br>
                右上の <b>設定（歯車マーク）</b> → 左メニューの <b>Messaging API</b> を開きます。<br>
                <b>Webhook URL</b> 欄に、④でコピーしたURLを貼り付けます。<br>
                その後、<b>Webhookの利用</b> をONにします。<br><br>

                <b>⑥ LINE DevelopersでMessaging APIチャネルを開きます</b><br>
                LINE Developersを開き、③で選んだプロバイダーを開きます。<br>
                チャネル一覧に <b>Messaging API</b> と書かれたチャネルが表示されます。<br>
                それを開きます。<br>
                ※ <b>LINEログイン</b> と書かれたチャネルではありません。通知に使うのは <b>Messaging API</b> です。<br><br>

                <b>⑦ チャネルアクセストークンを発行します</b><br>
                Messaging APIチャネルの詳細画面で、<b>Messaging API設定</b> タブを開きます。<br>
                下の方にある <b>チャネルアクセストークン</b> の項目を探します。<br>
                <b>発行</b> または <b>再発行</b> を押して、長い文字列のアクセストークンを表示します。<br><br>

                <b>⑧ アクセストークンをこの画面に貼り付けます</b><br>
                表示された長い文字列をコピーして、右側の <b>チャネルアクセストークン（長期）</b> 欄に貼り付けます。<br><br>

                <b>⑨ user_idを取得します</b><br>
                Webhook URLを設定してONにしたあと、自分のスマホから店舗の公式LINEへ「テスト」と送ります。<br>
                この管理画面を再読み込みすると、右側の <b>直近で取得したLINE user_id</b> に表示されます。<br><br>

                <b>⑩ LINEテスト送信をします</b><br>
                表示されたLINE user_idをコピーして、<b>テスト送信先LINE user_id</b> に貼り付けます。<br>
                <b>LINEテスト送信</b> を押して、スマホにテストメッセージが届けば設定完了です。<br><br>

                <b>注意点</b><br>
                ・Webhook URLは手入力で作るものではなく、この管理画面に自動表示されるものを使います。<br>
                ・WebhookをONにしないと、user_idは取得できません。<br>
                ・LINEログインチャネルとMessaging APIチャネルは別物です。<br>
                ・アクセストークンがないと、予約完了通知やリマインドをLINEで送れません。<br>
                ・アクセストークンは外部に漏れないように管理してください。
              </div>
            </div>
          </div>

          <div class="step">
            <div class="num">6</div>
            <div>
              <span class="step-title">保存して通常予約ページでテストします</span>
              <div class="step-text">簡単モードではLIFF IDは不要です。予約ページ側に「LINEで通知を受け取る」導線を追加して使います。</div>
            </div>
          </div>

          <div class="manual">まず店舗に使ってもらうなら、この簡単モードが一番おすすめです。</div>
        </div>

        
          <div class="step">
            <div class="num">7</div>
            <div>
              <span class="step-title">テスト送信用のLINE user_idを取得します</span>
              <div class="step-text">
                右側に表示されているWebhook URLを、LINE Official Account Managerの「設定 → Messaging API → Webhook URL」に貼り付けます。<br>
                Webhookの利用をONにしたあと、管理者のスマホから店舗の公式LINEへ「テスト」と送ってください。<br>
                送信すると、この画面右側の「直近で取得したLINE user_id」に自動で表示されます。
              </div>
            </div>
          </div>


        <div id="guide-liff" class="mode-guide">
          <div class="mode-title">LINE完結モード：LIFF予約＋LINE通知</div>
          <p class="mode-lead">LINEアプリ内で予約まで完結させる上級者向けモードです。設定項目は多いですが、お客様の体験は一番スムーズです。</p>

          <div class="actions">
            <a class="btn btn-primary" href="{{DEVELOPERS_URL}}" target="_blank" rel="noopener">1. LINE Developersを開く</a>
            <button type="button" class="btn-secondary" onclick="copyText('liff-url')">LIFF URLをコピー</button>
          </div>

          <div class="step">
            <div class="num">1</div>
            <div>
              <span class="step-title">プロバイダーとLINEログインチャネルを作成します</span>
              <div class="step-text">
                まだプロバイダーがない場合は、店舗名または運営会社名で新しく作成してください。その中でLINEログインチャネルを作成します。<br><br>
                <b>サービス提供地域：</b>店舗がある国を選びます。日本の店舗なら「日本」を選択します。<br>
                <b>会社・事業者の所在国・地域：</b>店舗または運営会社の所在地を選びます。<br>
                <b>チャネル名：</b>例：「店舗名 LINE予約」。<br>
                <b>チャネル説明：</b>例：「LINEアプリ内で予約するためのログインチャネルです。」<br>
                <b>アプリタイプ：</b><b>Webアプリ</b> を選択します。<br>
                <b>メールアドレス：</b>店舗または運営者のメールアドレスを入力します。<br>
                <b>プライバシーポリシーURL・利用規約URL：</b>用意しているページURLを入力します。未整備の場合は先にページを用意するのがおすすめです。<br>
                <b>LINE開発者契約への同意：</b>内容を確認し、問題なければチェックします。
              </div>
            </div>
          </div>

          <div class="step">
            <div class="num">2</div>
            <div>
              <span class="step-title">LIFFアプリを追加します</span>
              <div class="step-text">LINEログインチャネルのLIFFタブで追加します。サイズは <b>Full</b>、Scopeは <b>profile</b>、ボットリンク機能は <b>ON</b> にします。</div>
            </div>
          </div>

          <div class="step">
            <div class="num">3</div>
            <div>
              <span class="step-title">LIFF URLに下のURLを貼り付けます</span>
              <div class="step-text">「LIFF URLをコピー」ボタンを押して、LINE DevelopersのLIFF URL欄に貼ります。</div>
              <div id="liff-url" class="copybox">{{LIFF_URL}}</div>
              <div class="notice">注意：このURLが localhost の場合、LINEアプリからは開けません。本番またはngrokの https URL を使ってください。</div>
            </div>
          </div>

          <div class="step">
            <div class="num">4</div>
            <div>
              <span class="step-title">LIFF IDを右側に貼り付けます</span>
              <div class="step-text">例：<b>2009893827-D6LOyXp5</b> のような文字列です。</div>
            </div>
          </div>

          <div class="step">
            <div class="num">5</div>
            <div>
              <span class="step-title">Messaging API情報も保存します</span>
              <div class="step-text">予約完了通知やリマインドを送るため、長期アクセストークンとチャネルシークレットを右側に貼り付けます。</div>
            </div>
          </div>

          <div class="manual">公式LINEの予約ボタンには、保存後に「LINE予約入口を開く」のURLを設定します。</div>
        </div>
      </section>

      <section class="card">
        <h2>保存する情報</h2>
        {{SAVED_MESSAGE}}

        <div class="status">
          <span class="pill {{LIFF_PILL_CLASS}}">LIFF ID：{{LIFF_STATUS}}</span>
          <span class="pill {{TOKEN_PILL_CLASS}}">アクセストークン：{{TOKEN_STATUS}}</span>
          <span class="pill {{SECRET_PILL_CLASS}}">シークレット：{{SECRET_STATUS}}</span>
        </div>

        <form method="post">
          <label>LINE連携モード</label>
          <select id="line_mode_select" name="line_mode">
            <option value="off" {{MODE_OFF_SELECTED}}>利用しない（メール中心）</option>
            <option value="login" {{MODE_LOGIN_SELECTED}}>簡単モード：Web予約＋LINE通知</option>
            <option value="liff" {{MODE_LIFF_SELECTED}}>LINE完結モード：LIFF予約＋LINE通知</option>
          </select>
          <div class="hint">まずは簡単モード、LINE内で完結させたい店舗だけLINE完結モードを選びます。</div>

          <label>LIFF ID</label>
          <input name="line_liff_id" value="{{LIFF_ID}}" placeholder="例：2009893827-D6LOyXp5">

          <label>チャネルアクセストークン（長期）</label>
          <textarea name="line_channel_access_token" placeholder="Messaging API設定にある長期アクセストークン">{{ACCESS_TOKEN}}</textarea>
          <div class="hint">予約完了通知やリマインドLINEを送るために使います。</div>

          <label>チャネルシークレット</label>
          <input name="line_channel_secret" value="{{CHANNEL_SECRET}}" placeholder="Messaging APIチャネルのチャネルシークレット">

          <label>公式LINE URL（任意）</label>
          <input name="line_official_url" value="{{OFFICIAL_URL}}" placeholder="例：https://lin.ee/xxxx">

          <div class="actions">
            
          <div class="form-section">
            
          <div class="form-section">
            <label>あなたのWebhook URLはこれです</label>
            <div class="copybox" style="display:flex;gap:10px;align-items:center;justify-content:space-between;">
              <span id="webhook-url-text">__WEBHOOK_URL__</span>
              <button type="button" class="btn-secondary" onclick="copyWebhookUrl()" style="white-space:nowrap;">コピー</button>
            </div>
            <p class="hint"><b>このURLをそのまま貼り付ければOKです。</b><br>
              このURLをそのままコピーして、LINEのWebhook URL欄に貼り付けて保存してください。<br>
              後の手順で、LINE Official Account ManagerのWebhook URL欄に貼り付けます。
            </p>
          </div>

            <label>テスト送信先LINE user_id</label>
            <input type="text" name="test_line_user_id" placeholder="例：Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" form="line-test-form">
            <p class="hint">管理者やテスト用ユーザーのLINE user_idを入力して、保存済みアクセストークンで送信テストできます。</p>
          </div>

          <form id="line-test-form" method="post" action="/admin/{shop_id}/line-settings/test" style="margin:0;"></form>

<button type="submit" class="btn-primary">保存する</button>
          <button type="submit" form="line-test-form" class="btn-secondary">LINEテスト送信</button>
            <a class="btn btn-secondary" href="/shop/{{SHOP_ID}}/line-reserve" target="_blank" rel="noopener">LINE予約入口を開く</a>
            <a class="btn btn-secondary" href="/admin/{{SHOP_ID}}">管理画面へ戻る</a>
          </div>
        </form>

        <div class="checklist">
          <b>保存前チェック</b>
          <ul>
            <li>LINEログインチャネルでLIFFを作った</li>
            <li>LIFF URLに左のURLを貼った</li>
            <li>Scopeは profile を選んだ</li>
            <li>LIFF IDを右側に貼った</li>
            <li>Messaging APIの長期アクセストークンを貼った</li>
          </ul>
        </div>

        <p class="mini">
          ここに保存した情報は、この店舗専用です。他店舗の公式LINEとは混ざりません。
        </p>
      </section>
    </div>
  </div>

<script>
function copyText(id) {
  const el = document.getElementById(id);
  const text = el ? el.innerText.trim() : "";
  if (!text) return;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(function() {
      alert("コピーしました");
    }).catch(function() {
      fallbackCopy(text);
    });
  } else {
    fallbackCopy(text);
  }
}
function fallbackCopy(text) {
  const ta = document.createElement("textarea");
  ta.value = text;
  document.body.appendChild(ta);
  ta.select();
  document.execCommand("copy");
  document.body.removeChild(ta);
  alert("コピーしました");
}
function updateModeGuide() {
  const select = document.getElementById("line_mode_select");
  const mode = select ? select.value : "off";
  document.querySelectorAll(".mode-guide").forEach(function(el) {
    el.classList.remove("is-active");
  });
  const target = document.getElementById("guide-" + mode);
  if (target) {
    target.classList.add("is-active");
  } else {
    const off = document.getElementById("guide-off");
    if (off) off.classList.add("is-active");
  }
}
document.addEventListener("DOMContentLoaded", function() {
  updateModeGuide();
  const select = document.getElementById("line_mode_select");
  if (select) {
    select.addEventListener("change", updateModeGuide);
  }
});
</script>

<script>
function copyWebhookUrl() {
  const el = document.getElementById("webhook-url-text");
  if (!el) return;
  const url = el.innerText.trim();
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(url).then(function() {
      alert("Webhook URLをコピーしました");
    }).catch(function() {
      fallbackCopyWebhookUrl(url);
    });
  } else {
    fallbackCopyWebhookUrl(url);
  }
}
function fallbackCopyWebhookUrl(url) {
  const input = document.createElement("textarea");
  input.value = url;
  document.body.appendChild(input);
  input.select();
  document.execCommand("copy");
  document.body.removeChild(input);
  alert("Webhook URLをコピーしました");
}
</script>


<script>
function setTestLineUserId(userId) {
  const input = document.querySelector('input[name="test_line_user_id"]');
  if (input) {
    input.value = userId;
    input.focus();
  }
}
</script>

</body>
</html>
"""

    replacements = {
        "{{SHOP_NAME}}": shop_name,
        "{{DEVELOPERS_URL}}": developers_url,
        "{{LIFF_URL}}": liff_endpoint_url,
        "{{SAVED_MESSAGE}}": f'<div class="saved">{saved}</div>' if saved else "",
        "{{MODE_OFF_SELECTED}}": "selected" if line_mode == "off" else "",
        "{{MODE_LOGIN_SELECTED}}": "selected" if line_mode == "login" else "",
        "{{MODE_LIFF_SELECTED}}": "selected" if line_mode == "liff" else "",
        "{{LIFF_PILL_CLASS}}": "ok" if liff_id else "",
        "{{LIFF_STATUS}}": "設定済み" if liff_id else "未設定",
        "{{TOKEN_PILL_CLASS}}": "ok" if channel_access_token else "",
        "{{TOKEN_STATUS}}": "設定済み" if channel_access_token else "未設定",
        "{{SECRET_PILL_CLASS}}": "ok" if channel_secret else "",
        "{{SECRET_STATUS}}": "設定済み" if channel_secret else "未設定",
        "{{LIFF_ID}}": liff_id,
        "{{ACCESS_TOKEN}}": channel_access_token,
        "{{CHANNEL_SECRET}}": channel_secret,
        "{{OFFICIAL_URL}}": official_url,
        "{{SHOP_ID}}": shop_id,
    }
    for key, value in replacements.items():
        html = html.replace(key, value)

    return HTMLResponse(html.replace("__WEBHOOK_URL__", webhook_url).replace("__RECENT_LINE_USERS__", recent_line_user_options_html).replace("{shop_id}", shop_id))
@app.post("/admin/{shop_id}/line-settings")
async def save_line_settings(request: Request, shop_id: str):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    form = await request.form()
    line_mode = normalize_line_mode(form.get("line_mode"))
    line_liff_id = str(form.get("line_liff_id") or "").strip()
    line_channel_access_token = str(form.get("line_channel_access_token") or "").strip()
    line_channel_secret = str(form.get("line_channel_secret") or "").strip()
    line_official_url = str(form.get("line_official_url") or "").strip()

    with get_connection() as conn:
        ensure_line_setting_columns(conn)
        conn.execute(
            """
            UPDATE shops
            SET
                line_mode = ?,
                line_liff_id = ?,
                line_channel_access_token = ?,
                line_channel_secret = ?,
                line_official_url = ?,
                line_webhook_enabled = 1
            WHERE shop_id = ?
            """,
            (
                line_mode,
                line_liff_id,
                line_channel_access_token,
                line_channel_secret,
                line_official_url,
                str(shop_id or "").strip(),
            ),
        )
        conn.commit()

    return RedirectResponse(
        f"/admin/{shop_id}/line-settings?saved=LINE設定を保存しました",
        status_code=303,
    )














@app.on_event("startup")
def _line_reminder_startup() -> None:
    ensure_customer_line_user_id_schema()
    ensure_line_webhook_test_schema()
    try:
        _start_reservation_reminder_worker()
    except Exception as exc:
        print("[startup] reminder worker start failed:", repr(exc))



@app.post("/line/webhook/{shop_id}/")
@app.post("/line/webhook/{shop_id}")
async def line_webhook_receive(shop_id: str, request: Request):
    try:
        payload = await request.json()
    except Exception as exc:
        print("LINE webhook json error:", repr(exc))
        payload = {}

    print("===== LINE WEBHOOK DEBUG =====")
    print("shop_id:", shop_id)
    print("body:", payload)
    print("==============================")

    saved_count = 0
    sent_count = 0
    settings = get_shop_line_settings(shop_id)
    access_token = str(settings.get("line_channel_access_token") or "").strip()
    line_mode = normalize_line_mode(settings.get("line_mode"))

    events = payload.get("events", []) if isinstance(payload, dict) else []
    if not isinstance(events, list):
        events = []

    for event in events:
        if not isinstance(event, dict):
            continue
        source = event.get("source") or {}
        if not isinstance(source, dict):
            source = {}
        user_id = str(source.get("userId") or "").strip()
        if not user_id:
            continue
        message = event.get("message") or {}
        message_text = str(message.get("text") or "").strip() if isinstance(message, dict) else ""
        postback = event.get("postback") or {}
        postback_data = str(postback.get("data") or "").strip() if isinstance(postback, dict) else ""
        save_line_webhook_user(shop_id, user_id, event_type=str(event.get("type") or ""), message_text=message_text or postback_data)
        saved_count += 1
        if str(event.get("type") or "") not in {"message", "postback"}:
            continue
        normalized_text = message_text.replace(" ", "").replace("　", "").strip()
        active_session = get_line_reservation_session(shop_id, user_id)
        is_cancel_action = normalized_text in {"キャンセル", "中止", "やめる", "取消", "取り消し", "いいえ"}
        start_words = {"予約", "予約する", "予約開始"}
        is_reservation_action = (
            normalized_text in start_words
            or postback_data.startswith("line_reserve:")
            or active_session is not None
            or is_cancel_action
        )

        send_result = {"ok": True, "reason": "no auto reply"}
        if line_mode == "liff" and is_reservation_action:
            send_result = handle_line_complete_reservation_flow(shop_id=shop_id, user_id=user_id, access_token=access_token, message_text=message_text, postback_data=postback_data)
        elif normalized_text in {"予約", "予約する", "予約開始"}:
            send_result = send_line_reservation_button(access_token=access_token, user_id=user_id, shop_id=shop_id)
        else:
            # 予約キーワード以外には自動返信しない。通常のLINEチャットとしてそのまま使えるようにする。
            print("LINE auto reply skipped: normal chat message")
        if send_result.get("ok") and send_result.get("reason") != "no auto reply":
            sent_count += 1
        print("LINE reply result:", send_result)

    return JSONResponse({"ok": True, "saved_count": saved_count, "sent_count": sent_count}, status_code=200)


@app.post("/admin/{shop_id}/line-settings/test")
def admin_line_settings_test_send(
    request: Request,
    shop_id: str,
    test_line_user_id: str = Form(""),
):
    login_redirect = require_store_login(request, shop_id)
    if login_redirect:
        return login_redirect

    settings = get_shop_line_settings(shop_id)
    access_token = str(settings.get("line_channel_access_token") or "").strip()
    test_line_user_id = str(test_line_user_id or "").strip()

    if not access_token:
        request.session["flash_error"] = "チャネルアクセストークンが未設定です。保存してからテストしてください。"
        return RedirectResponse(f"/admin/{shop_id}/line-settings", status_code=303)

    if not test_line_user_id:
        request.session["flash_error"] = "テスト送信先のLINE user_idを入力してください。"
        return RedirectResponse(f"/admin/{shop_id}/line-settings", status_code=303)

    shop = get_shop(shop_id) or {}
    shop_name = str(shop.get("shop_name") or shop_id)
    message = (
        f"【{shop_name}】LINEテスト送信です。\\n"
        "このメッセージが届いていれば、LINE通知設定は有効です。"
    )

    result = send_line_message(access_token, test_line_user_id, message)
    if result.get("ok"):
        request.session["flash_success"] = "LINEテスト送信に成功しました。"
    else:
        reason = result.get("reason") or result.get("response") or "送信に失敗しました。"
        request.session["flash_error"] = f"LINEテスト送信に失敗しました：{reason}"

    return RedirectResponse(f"/admin/{shop_id}/line-settings", status_code=303)



@app.get("/admin/{shop_id}/customers/{customer_id}/line", response_class=HTMLResponse)
def admin_customer_line_setting_page(request: Request, shop_id: str, customer_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    customer = get_customer_by_id(shop_id, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="顧客が見つかりません")

    line_user_id = str(customer.get("line_user_id") or "")
    customer_name = str(customer.get("name") or "")
    saved = str(request.query_params.get("saved") or "")

    return HTMLResponse(f"""
<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>LINE ID設定</title>
  <style>
    body {{
      margin: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f6f7fb;
      color: #111827;
    }}
    .wrap {{ max-width: 760px; margin: 24px auto; padding: 16px; }}
    .hero {{
      border-radius: 24px;
      padding: 24px;
      color: #fff;
      background: linear-gradient(135deg, #16a34a, #22c55e);
      box-shadow: 0 16px 40px rgba(22, 163, 74, .22);
      margin-bottom: 18px;
    }}
    .hero h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .hero p {{ margin: 0; opacity: .95; }}
    .card {{
      background: #fff;
      border-radius: 22px;
      padding: 22px;
      box-shadow: 0 12px 36px rgba(15, 23, 42, .08);
    }}
    label {{ display: block; font-weight: 800; margin-bottom: 8px; }}
    input {{
      width: 100%;
      box-sizing: border-box;
      border: 1px solid #d1d5db;
      border-radius: 14px;
      padding: 14px;
      font-size: 16px;
    }}
    .hint {{ margin-top: 8px; color: #6b7280; font-size: 13px; line-height: 1.6; }}
    .actions {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 18px; }}
    button, a.btn {{
      border: 0;
      border-radius: 999px;
      padding: 12px 18px;
      font-weight: 800;
      cursor: pointer;
      text-decoration: none;
      display: inline-block;
    }}
    button {{ color: #fff; background: #16a34a; }}
    a.btn {{ color: #374151; background: #eef2f7; }}
    .saved {{
      background: #dcfce7;
      color: #166534;
      border-radius: 14px;
      padding: 12px 14px;
      margin-bottom: 14px;
      font-weight: 800;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>LINE ID設定</h1>
      <p>{customer_name} 様のLINE user_idを登録します。</p>
    </div>

    <div class="card">
      {f'<div class="saved">{saved}</div>' if saved else ''}
      <form method="post">
        <label>LINE user_id</label>
        <input name="line_user_id" value="{line_user_id}" placeholder="Uxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx">
        <div class="hint">
          お客様が公式LINEにメッセージを送った時、Webhookログに出る <strong>LINE user_id</strong> を貼り付けて保存します。
        </div>
        <div class="actions">
          <button type="submit">保存する</button>
          <a class="btn" href="/admin/{shop_id}/customers">顧客一覧に戻る</a>
        </div>
      </form>
    </div>
  </div>
</body>
</html>
""")


@app.post("/admin/{shop_id}/customers/{customer_id}/line")
async def admin_customer_line_setting_save(request: Request, shop_id: str, customer_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    customer = get_customer_by_id(shop_id, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="顧客が見つかりません")

    form = await request.form()
    line_user_id = str(form.get("line_user_id") or "").strip()
    update_customer_line_user_id(shop_id, customer_id, line_user_id)

    return RedirectResponse(
        f"/admin/{shop_id}/customers/{customer_id}/line?saved=LINE IDを保存しました",
        status_code=303,
    )


@app.get("/admin/{shop_id}/customers/{customer_id}", response_class=HTMLResponse)
def admin_customer_detail_page(request: Request, shop_id: str, customer_id: int, error_message: str = ""):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name = _build_admin_common_context(request, shop_id)
    customer = get_customer_by_id(shop_id, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="顧客が見つかりません")
    customer_notes = get_customer_notes(shop_id, customer_id)
    customer_photos = [
        {**photo, "image_url": _normalize_customer_photo_url(str(photo.get("image_url") or ""))}
        for photo in get_customer_photos(shop_id, customer_id)
    ]
    linked_member = get_member_by_customer_id(shop_id, customer_id)
    chat_messages = [_serialize_chat_message(item) for item in list_chat_messages(shop_id, customer_id, limit=200)]
    mark_chat_messages_read_for_admin(shop_id, customer_id)
    customer_photo_policy = _get_customer_photo_policy(subscription)
    max_photos = customer_photo_policy.get("max_photos")
    customer_photo_remaining = None if max_photos is None else max(int(max_photos) - len(customer_photos), 0)
    chat_limit = _chat_limit_for_subscription(subscription)
    shop_chat_sent_this_month = count_shop_chat_messages_in_month(shop_id)
    template_name = "admin/tool/customer_detail.html" if shop.get("admin_ui_mode") == "tool" else "admin/customer_detail.html"
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "customer": customer,
            "customer_notes": customer_notes,
            "customer_photos": customer_photos,
            "customer_photo_policy": customer_photo_policy,
            "customer_photo_remaining": customer_photo_remaining,
            "chat_messages": chat_messages,
            "shop_chat_limit": chat_limit,
            "shop_chat_sent_this_month": shop_chat_sent_this_month,
            "shop_chat_remaining": None if chat_limit is None else max(chat_limit - shop_chat_sent_this_month, 0),
            "member_chat_enabled": linked_member is not None,
            "linked_member": linked_member,
            "customers": customers,
            "reservations": reservations,
            "staff_list": shop.get("staff_list", []),
            "subscription": subscription,
            "available_plans": available_plans,
            "admin_users": admin_users,
            "current_admin_name": current_admin_name,
            "error_message": error_message,
            "success_message": request.query_params.get("saved", ""),
            "active_page": "customers",
        },
    )


@app.post("/admin/{shop_id}/customers/{customer_id}/update")
def admin_update_customer(
    request: Request,
    shop_id: str,
    customer_id: int,
    name: str = Form(...),
    phone: str = Form(""),
    email: str = Form(""),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    if get_customer_by_id(shop_id, customer_id) is None:
        raise HTTPException(status_code=404, detail="顧客が見つかりません")
    name = (name or "").strip()
    if not name:
        return admin_customer_detail_page(request, shop_id, customer_id, error_message="顧客名を入力してください。")
    updated_customer = update_customer(shop_id, customer_id, name, (phone or "").strip(), (email or "").strip().lower())
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="customer_update",
        shop_id=shop_id,
        target_type="customer",
        target_id=customer_id,
        target_label=str((updated_customer or {}).get("name") or name),
    )
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}?saved=顧客情報を更新しました", status_code=303)


@app.post("/admin/{shop_id}/customers/{customer_id}/delete")
def admin_delete_customer_route(request: Request, shop_id: str, customer_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    customer = get_customer_by_id(shop_id, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="顧客が見つかりません")
    for photo in get_customer_photos(shop_id, customer_id):
        _delete_customer_photo_file(str(photo.get("image_url") or ""))
    delete_customer(shop_id, customer_id)
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="customer_delete",
        shop_id=shop_id,
        target_type="customer",
        target_id=customer_id,
        target_label=str(customer.get("name") or ""),
    )
    return RedirectResponse(f"/admin/{shop_id}/customers?saved=顧客を削除しました", status_code=303)


@app.post("/admin/{shop_id}/customers/{customer_id}/notes")
def admin_add_customer_note_route(
    request: Request,
    shop_id: str,
    customer_id: int,
    title: str = Form(...),
    content: str = Form(...),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    if get_customer_by_id(shop_id, customer_id) is None:
        raise HTTPException(status_code=404, detail="顧客が見つかりません")
    title = (title or "").strip()
    content = (content or "").strip()
    if not title or not content:
        return admin_customer_detail_page(request, shop_id, customer_id, error_message="タイトルと内容を入力してください。")
    note = add_customer_note(shop_id, customer_id, title, content)
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="customer_note_create",
        shop_id=shop_id,
        target_type="customer_note",
        target_id=int(note.get("id") or 0),
        target_label=title,
        detail={"customer_id": customer_id},
    )
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}?saved=追加情報を保存しました", status_code=303)


@app.post("/admin/{shop_id}/customers/{customer_id}/notes/{note_id}/delete")
def admin_delete_customer_note_route(request: Request, shop_id: str, customer_id: int, note_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    deleted_note = delete_customer_note(shop_id, customer_id, note_id)
    if deleted_note is not None:
        _record_audit_log(
            request,
            actor_type="store_admin",
            actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
            actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
            action="customer_note_delete",
            shop_id=shop_id,
            target_type="customer_note",
            target_id=note_id,
            target_label=str(deleted_note.get("title") or ""),
            detail={"customer_id": customer_id},
        )
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}?saved=追加情報を削除しました", status_code=303)


@app.post("/admin/{shop_id}/customers/{customer_id}/photos")
def admin_add_customer_photo_route(
    request: Request,
    shop_id: str,
    customer_id: int,
    photo: UploadFile = File(...),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    if get_customer_by_id(shop_id, customer_id) is None:
        raise HTTPException(status_code=404, detail="顧客が見つかりません")
    customer_photos = get_customer_photos(shop_id, customer_id)
    customer_photo_policy = _get_customer_photo_policy(get_shop_subscription(shop_id))
    max_photos = customer_photo_policy.get("max_photos")
    if not customer_photo_policy.get("enabled"):
        return admin_customer_detail_page(request, shop_id, customer_id, error_message="現在のプランでは顧客写真を保存できません。")
    if max_photos is not None and len(customer_photos) >= int(max_photos):
        return admin_customer_detail_page(request, shop_id, customer_id, error_message="顧客写真の保存上限に達しています。")
    if not (photo.filename or "").strip():
        return admin_customer_detail_page(request, shop_id, customer_id, error_message="写真ファイルを選択してください。")
    image_url = _save_customer_photo_file(shop_id, customer_id, photo)
    saved_photo = add_customer_photo(shop_id, customer_id, image_url)
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="customer_photo_create",
        shop_id=shop_id,
        target_type="customer_photo",
        target_id=int(saved_photo.get("id") or 0),
        target_label=str(photo.filename or ""),
        detail={"customer_id": customer_id},
    )
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}?saved=写真を保存しました", status_code=303)


@app.post("/admin/{shop_id}/customers/{customer_id}/photos/{photo_id}/delete")
def admin_delete_customer_photo_route(request: Request, shop_id: str, customer_id: int, photo_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    deleted = delete_customer_photo(shop_id, customer_id, photo_id)
    if deleted is not None:
        _delete_customer_photo_file(str(deleted.get("image_url") or ""))
        _record_audit_log(
            request,
            actor_type="store_admin",
            actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
            actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
            action="customer_photo_delete",
            shop_id=shop_id,
            target_type="customer_photo",
            target_id=photo_id,
            detail={"customer_id": customer_id},
        )
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}?saved=写真を削除しました", status_code=303)


@app.get("/admin/{shop_id}/website", response_class=HTMLResponse)
def admin_website_page(request: Request, shop_id: str):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name = _build_admin_common_context(request, shop_id)
    homepage_settings = get_shop_homepage_settings(shop_id) or {}
    homepage_sections = get_shop_homepage_sections(shop_id)
    homepage_samples = get_all_samples()
    template_name = "admin/tool/website.html" if shop.get("admin_ui_mode") == "tool" else "admin/website.html"
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "homepage_settings": homepage_settings,
            "homepage_sections": homepage_sections,
            "homepage_samples": homepage_samples,
            "today": date.today().isoformat(),
            "customers": customers,
            "today_reservations": [r for r in reservations if str(r.get("reservation_date") or "") == date.today().isoformat()],
            "admin_users": admin_users,
            "subscription": subscription,
            "available_plans": available_plans,
            "current_admin_name": current_admin_name,
            "success_message": request.query_params.get("saved", ""),
            "active_page": "website",
        },
    )


@app.post("/admin/logout/{shop_id}")
def admin_logout(request: Request, shop_id: str):
    actor_id = str(request.session.get("store_logged_in_login_id") or shop_id)
    actor_name = str(request.session.get("store_logged_in_admin_name") or "")
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=actor_id,
        actor_name=actor_name,
        action="logout",
        shop_id=shop_id,
    )
    request.session.pop("store_logged_in_shop_id", None)
    request.session.pop("store_logged_in_login_id", None)
    request.session.pop("store_logged_in_admin_name", None)
    return RedirectResponse("/store-login", status_code=303)


@app.get("/admin/{shop_id}/subscription/confirm", response_class=HTMLResponse)
def admin_subscription_confirm_page(
    request: Request,
    shop_id: str,
    plan_id: int,
    status: str = "active",
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id, shop, reservations, customers, admin_users, subscription, available_plans, current_admin_name = _build_admin_common_context(request, shop_id)
    if int(shop.get("is_child_shop") or 0) == 1:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=子店舗では契約プランを変更できません。", status_code=303)
    normalized_status = (status or "").strip().lower()
    if normalized_status not in {"active", "trial", "canceled"}:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=契約状態の指定が正しくありません。", status_code=303)

    target_plan = _find_plan_by_id(available_plans, plan_id)
    if target_plan is None:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=変更先プランが見つかりません。", status_code=303)

    current_display_code = _resolve_current_display_plan_code(subscription)
    target_display_code = _resolve_display_plan_code_from_plan(target_plan)
    if not (current_display_code in {"standard", "premium"} and target_display_code == "free"):
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings", status_code=303)

    template_name = "admin/tool/subscription_confirm.html" if shop.get("admin_ui_mode") == "tool" else "admin/subscription_confirm.html"
    downgrade_summary = _build_free_plan_downgrade_summary(shop, customers, reservations)
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": normalized_shop_id,
            "subscription": subscription,
            "available_plans": available_plans,
            "current_admin_name": current_admin_name,
            "admin_users": admin_users,
            "customers": customers,
            "today_reservations": [item for item in reservations if str(item.get("reservation_date") or "") == date.today().isoformat()],
            "staff_list": shop.get("staff_list", []),
            "active_page": "settings",
            "current_plan_name": subscription.get("plan_name") or PLAN_DETAILS.get(current_display_code, PLAN_DETAILS["free"])["name"],
            "target_plan_name": target_plan.get("name") or PLAN_DETAILS["free"]["name"],
            "plan_id": int(target_plan.get("id") or plan_id),
            "status": normalized_status,
            "downgrade_summary": downgrade_summary,
            "error_message": request.query_params.get("error", ""),
        },
    )


@app.post("/admin/{shop_id}/subscription")
def admin_update_subscription(
    request: Request,
    shop_id: str,
    plan_id: int = Form(...),
    status: str = Form(...),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop_management_data(normalized_shop_id)
    if shop and int(shop.get("is_child_shop") or 0) == 1:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=子店舗では契約プランを変更できません。", status_code=303)
    normalized_status = (status or "").strip().lower()
    if normalized_status not in {"active", "trial", "canceled"}:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=契約状態の指定が正しくありません。", status_code=303)

    subscription = get_shop_subscription(normalized_shop_id) or {}
    available_plans = get_plans(active_only=True)
    target_plan = _find_plan_by_id(available_plans, plan_id)
    if target_plan is None:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=変更先プランが見つかりません。", status_code=303)

    current_display_code = _resolve_current_display_plan_code(subscription)
    target_display_code = _resolve_display_plan_code_from_plan(target_plan)
    if current_display_code in {"standard", "premium"} and target_display_code == "free":
        return RedirectResponse(
            f"/admin/{normalized_shop_id}/subscription/confirm?plan_id={int(target_plan.get('id') or plan_id)}&status={normalized_status}",
            status_code=303,
        )

    update_shop_subscription(normalized_shop_id, int(target_plan.get("id") or plan_id), normalized_status)
    return RedirectResponse(f"/admin/{normalized_shop_id}/settings?saved=プランを更新しました。", status_code=303)


@app.post("/admin/{shop_id}/subscription/confirm")
def admin_confirm_subscription_update(
    request: Request,
    shop_id: str,
    plan_id: int = Form(...),
    status: str = Form(...),
    consent_free_plan_downgrade: str = Form(""),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop_management_data(normalized_shop_id)
    if shop and int(shop.get("is_child_shop") or 0) == 1:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=子店舗では契約プランを変更できません。", status_code=303)
    normalized_status = (status or "").strip().lower()
    if normalized_status not in {"active", "trial", "canceled"}:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=契約状態の指定が正しくありません。", status_code=303)

    available_plans = get_plans(active_only=True)
    target_plan = _find_plan_by_id(available_plans, plan_id)
    if target_plan is None:
        return RedirectResponse(f"/admin/{normalized_shop_id}/settings?error=変更先プランが見つかりません。", status_code=303)

    subscription = get_shop_subscription(normalized_shop_id) or {}
    current_display_code = _resolve_current_display_plan_code(subscription)
    target_display_code = _resolve_display_plan_code_from_plan(target_plan)
    if current_display_code in {"standard", "premium"} and target_display_code == "free":
        if consent_free_plan_downgrade != "yes":
            return RedirectResponse(
                f"/admin/{normalized_shop_id}/subscription/confirm?plan_id={int(target_plan.get('id') or plan_id)}&status={normalized_status}&error=同意にチェックを入れてください。",
                status_code=303,
            )

    update_shop_subscription(normalized_shop_id, int(target_plan.get("id") or plan_id), normalized_status)
    return RedirectResponse(f"/admin/{normalized_shop_id}/settings?saved=プランを更新しました。", status_code=303)


@app.post("/admin/{shop_id}/customers")
def admin_create_customer(
    request: Request,
    shop_id: str,
    name: str = Form(...),
    phone: str = Form(""),
    email: str = Form(""),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    name = (name or "").strip()
    phone = (phone or "").strip()
    email = (email or "").strip().lower()
    if not name:
        return admin_customers_page(request, shop_id, error_message="顧客名を入力してください。")
    customer = create_customer(shop_id, name, phone, email)
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="customer_create",
        shop_id=shop_id,
        target_type="customer",
        target_id=int(customer.get("id") or 0),
        target_label=str(customer.get("name") or ""),
    )
    return RedirectResponse(f"/admin/{shop_id}/customers?saved=顧客を追加しました", status_code=303)


@app.post("/admin/{shop_id}/reservations")
def admin_create_reservation(
    request: Request,
    shop_id: str,
    customer_id: int = Form(...),
    staff_id: int = Form(...),
    menu_id: int = Form(...),
    reservation_date: str = Form(...),
    start_time: str = Form(...),
    line_user_id: str = Form(""),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="見つかりません")

    customers = get_customers(shop_id)
    customer = next((c for c in customers if int(c.get("id") or 0) == int(customer_id)), None)
    staff = next((s for s in shop.get("staff_list", []) if int(s.get("id") or 0) == int(staff_id)), None)
    menu = next((m for m in shop.get("menus", []) if int(m.get("id") or 0) == int(menu_id)), None)
    if not customer or not staff or not menu:
        return admin_page(request, shop_id, error_message="顧客・スタッフ・メニューの指定を確認してください。")
    if not _staff_allows_menu(staff, menu_id):
        return admin_page(request, shop_id, error_message="選択したスタッフではこのメニューを予約できません。")
    reservation_day = _safe_parse_date(reservation_date, date.today())
    if _is_shop_holiday(shop, reservation_day) or _is_staff_holiday(staff, reservation_day):
        return admin_page(request, shop_id, error_message="選択した日はこのスタッフの休日のため予約できません。")
    try:
        start_dt = datetime.strptime(start_time, "%H:%M")
    except ValueError:
        return admin_page(request, shop_id, error_message="開始時間の形式が正しくありません。")
    duration = int(menu.get("duration", 60) or 60)
    end_time = (start_dt + timedelta(minutes=duration)).strftime("%H:%M")
    reservation = create_reservation(
        shop_id=shop_id,
        customer_id=int(customer["id"]),
        customer_name=str(customer.get("name") or ""),
        customer_email=str(customer.get("email") or ""),
        receive_email=0,
        staff_id=int(staff_id),
        staff_name=str(staff.get("name") or ""),
        menu_id=int(menu_id),
        menu_name=str(menu.get("name") or ""),
        duration=duration,
        price=int(menu.get("price", 0) or 0),
        reservation_date=reservation_date,
        start_time=start_time,
        end_time=end_time,
        source="admin",
    )
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="reservation_create",
        shop_id=shop_id,
        target_type="reservation",
        target_id=int(reservation.get("id") or 0),
        target_label=str(customer.get("name") or ""),
        detail={"status": str(reservation.get("status") or "")},
    )
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}/line?saved=LINE IDを保存しました", status_code=303)


@app.post("/admin/{shop_id}/reservations/{reservation_id}/done")
def admin_mark_reservation_done(request: Request, shop_id: str, reservation_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    update_reservation_status(shop_id, reservation_id, "来店済み")
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="reservation_status_update",
        shop_id=shop_id,
        target_type="reservation",
        target_id=reservation_id,
        detail={"status": "来店済み"},
    )
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}/line?saved=LINE IDを保存しました", status_code=303)


@app.post("/admin/{shop_id}/reservations/{reservation_id}/cancel")
def admin_mark_reservation_cancel(request: Request, shop_id: str, reservation_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    update_reservation_status(shop_id, reservation_id, "キャンセル")
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="reservation_status_update",
        shop_id=shop_id,
        target_type="reservation",
        target_id=reservation_id,
        detail={"status": "キャンセル"},
    )
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}/line?saved=LINE IDを保存しました", status_code=303)


@app.get("/admin/{shop_id}/booking-page/editor", response_class=HTMLResponse)
def admin_booking_page_editor(request: Request, shop_id: str):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    context = build_shop_booking_context(shop_id, request)
    context["edit_mode"] = True
    context["active_page"] = "booking_page_editor"
    return templates.TemplateResponse(
        request=request,
        name="shop/index.html",
        context=context,
    )


@app.post("/admin/{shop_id}/booking-page/editor/save")
async def admin_booking_page_editor_save(request: Request, shop_id: str):
    redirect = require_store_login(request, shop_id)
    if redirect:
        raise HTTPException(status_code=401, detail="ログインしてください")
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

    payload = await request.json()
    fields = payload if isinstance(payload, dict) else {}
    update_shop_basic_info(
        shop_id,
        shop_name=str(fields.get("shop_name") or shop.get("shop_name") or "").strip(),
        phone=str(fields.get("phone") or shop.get("phone") or "").strip(),
        address=str(fields.get("address") or shop.get("address") or "").strip(),
        business_hours=str(fields.get("business_hours") or shop.get("business_hours") or "").strip(),
        holiday=str(fields.get("holiday") or shop.get("holiday") or "").strip(),
        catch_copy=str(fields.get("catch_copy") or shop.get("catch_copy") or "").strip(),
        description=str(fields.get("description") or shop.get("description") or "").strip(),
        reply_to_email=str(shop.get("reply_to_email") or "").strip(),
        admin_ui_mode=str(shop.get("admin_ui_mode") or "web").strip(),
        primary_color=str(shop.get("primary_color") or "#2ec4b6").strip(),
        primary_dark=str(shop.get("primary_dark") or "#159a90").strip(),
        accent_bg=str(shop.get("accent_bg") or "#f7fffe").strip(),
        menus=shop.get("menus") or [],
    )
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="shop_update",
        shop_id=shop_id,
        target_type="shop",
        target_id=shop_id,
        target_label=str(fields.get("shop_name") or shop.get("shop_name") or ""),
    )
    return JSONResponse({"ok": True})


@app.get("/shop/{shop_id}", response_class=HTMLResponse)
def shop_page(request: Request, shop_id: str):
    line_user_id = str(request.query_params.get("line_user_id") or "").strip()
    if line_user_id:
        request.session["line_user_id"] = line_user_id
        request.session["line_shop_id"] = shop_id
        print("予約ページでLINE user_idを保存:", shop_id, line_user_id)

    context = build_shop_booking_context(shop_id, request)
    return templates.TemplateResponse(
        request=request,
        name="shop/index.html",
        context=context,
    )



@app.get("/shop/{shop_id}/line-reserve", response_class=HTMLResponse)
def shop_line_reserve_entry(request: Request, shop_id: str):
    """
    公式LINEの「予約する」ボタンから開く入口ページ。
    LIFFでLINE user_idを取得して、セッションに保存してから予約ページへ移動します。
    """
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

    line_settings = get_shop_line_settings(shop_id)
    line_mode = normalize_line_mode(line_settings.get("line_mode"))
    if line_mode != "liff":
        return RedirectResponse(f"/shop/{shop_id}", status_code=303)
    liff_id = str(line_settings.get("line_liff_id") or "").strip()

    if not liff_id:
        return HTMLResponse(f"""
<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>LINE予約設定</title>
  <style>
    body {{ margin:0; font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:#f6f7fb; color:#111827; }}
    .wrap {{ max-width:720px; margin:24px auto; padding:18px; }}
    .card {{ background:#fff; border-radius:22px; padding:22px; box-shadow:0 14px 36px rgba(15,23,42,.08); }}
    h1 {{ margin:0 0 12px; font-size:24px; }}
    p {{ line-height:1.7; color:#4b5563; }}
    code {{ background:#eef2f7; border-radius:8px; padding:3px 6px; }}
    a {{ color:#0f766e; font-weight:700; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>LIFF IDが未設定です</h1>
      <p>管理画面の <strong>LINE連携設定</strong> で <code>LIFF ID</code> を保存してください。</p>
      <p>設定後、公式LINEの「予約する」ボタンにはこのURLを設定します。</p>
      <p><code>/shop/{shop_id}/line-reserve</code></p>
      <p><a href="/shop/{shop_id}">通常の予約ページへ進む</a></p>
    </div>
  </div>
</body>
</html>
""")

    return HTMLResponse(f"""
<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>LINE予約へ進む</title>
  <style>
    body {{ margin:0; font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:#f6f7fb; color:#111827; }}
    .wrap {{ min-height:100vh; display:flex; align-items:center; justify-content:center; padding:18px; box-sizing:border-box; }}
    .card {{ width:min(520px,100%); background:#fff; border-radius:24px; padding:26px; text-align:center; box-shadow:0 18px 44px rgba(15,23,42,.10); }}
    .badge {{ display:inline-flex; align-items:center; justify-content:center; width:62px; height:62px; border-radius:50%; background:#dcfce7; color:#16a34a; font-size:30px; margin-bottom:12px; }}
    h1 {{ margin:0 0 10px; font-size:24px; }}
    p {{ color:#6b7280; line-height:1.7; }}
    .error {{ display:none; margin-top:14px; padding:12px; border-radius:14px; background:#fef2f2; color:#991b1b; text-align:left; font-size:14px; }}
    a {{ color:#0f766e; font-weight:700; }}
  </style>
  <script src="https://static.line-scdn.net/liff/edge/2/sdk.js"></script>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="badge">✓</div>
      <h1>LINE予約を準備しています</h1>
      <p>このまま予約ページへ進みます。</p>
      <div id="error" class="error"></div>
    </div>
  </div>

<script>
(async function() {{
  const errorBox = document.getElementById("error");

  function showError(message) {{
    errorBox.style.display = "block";
    errorBox.innerHTML = message + '<br><br><a href="/shop/{shop_id}">通常の予約ページへ進む</a>';
  }}

  try {{
    await liff.init({{ liffId: "{liff_id}" }});

    if (!liff.isLoggedIn()) {{
      liff.login({{ redirectUri: window.location.href }});
      return;
    }}

    const profile = await liff.getProfile();
    const lineUserId = profile && profile.userId ? profile.userId : "";

    if (!lineUserId) {{
      showError("LINE user_idを取得できませんでした。");
      return;
    }}

    const response = await fetch("/shop/{shop_id}/line-session", {{
      method: "POST",
      headers: {{
        "Content-Type": "application/json"
      }},
      body: JSON.stringify({{
        line_user_id: lineUserId,
        display_name: profile.displayName || ""
      }})
    }});

    if (!response.ok) {{
      showError("LINE情報の保存に失敗しました。");
      return;
    }}

    window.location.href = "/shop/{shop_id}";
  }} catch (error) {{
    showError("LIFF連携でエラーが発生しました: " + String(error));
  }}
}})();
</script>
</body>
</html>
""")


@app.post("/shop/{shop_id}/line-session")
async def shop_line_session(request: Request, shop_id: str):
    """
    LIFFで取得したLINE user_idを一時保存します。
    予約フォーム送信時に、この値を顧客へ紐づけます。
    """
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

    body = await request.json()
    line_user_id = str((body or {}).get("line_user_id") or "").strip()
    display_name = str((body or {}).get("display_name") or "").strip()

    if not line_user_id:
        raise HTTPException(status_code=400, detail="LINE user_idが空です")

    request.session["line_user_id"] = line_user_id
    request.session["line_display_name"] = display_name
    request.session["line_shop_id"] = shop_id

    return JSONResponse({"ok": True})


@app.post("/shop/{shop_id}/confirm", response_class=HTMLResponse)
def shop_confirm(
    request: Request,
    shop_id: str,
    reservation_date: str = Form(...),
    start_time: str = Form(...),
    customer_name: str = Form(...),
    phone: str = Form(""),
    email: str = Form(""),
    receive_email: str = Form("1"),
    staff_id: int = Form(...),
    menu_id: int = Form(...),
):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    subscription = get_shop_subscription(shop_id) or {'status': 'active', 'plan_name': 'Free', 'show_ads': False}
    shop = _build_shop_with_visible_staff(shop, subscription)
    staff = next((s for s in shop.get("staff_list", []) if int(s["id"]) == int(staff_id)), None)
    menu = next((m for m in shop.get("menus", []) if int(m["id"]) == int(menu_id)), None)
    if not staff or not menu:
        ctx = build_shop_booking_context(shop_id, request, "スタッフまたはメニューが見つかりません。")
        return templates.TemplateResponse(
            request=request,
            name="shop/index.html",
            context=ctx,
            status_code=400,
        )
    if not _staff_allows_menu(staff, menu_id):
        ctx = build_shop_booking_context(shop_id, request, "選択したスタッフではこのメニューを予約できません。")
        return templates.TemplateResponse(
            request=request,
            name="shop/index.html",
            context=ctx,
            status_code=400,
        )
    reservation_day = _safe_parse_date(reservation_date, date.today())
    if _is_shop_holiday(shop, reservation_day) or _is_staff_holiday(staff, reservation_day):
        ctx = build_shop_booking_context(shop_id, request, "選択した日はこのスタッフの休日のため予約できません。")
        return templates.TemplateResponse(
            request=request,
            name="shop/index.html",
            context=ctx,
            status_code=400,
        )
    start_dt = datetime.strptime(start_time, "%H:%M")
    end_dt = (start_dt + timedelta(minutes=int(menu.get("duration", 60)))).strftime("%H:%M")
    reservation_preview = {
        "customer_name": customer_name,
        "phone": phone,
        "email": email,
        "receive_email": 1 if receive_email == '1' else 0,
        "staff_id": staff_id,
        "staff_name": staff["name"],
        "menu_id": menu_id,
        "menu_name": menu["name"],
        "price": menu.get("price", 0),
        "duration": menu.get("duration", 60),
        "reservation_date": reservation_date,
        "start_time": start_time,
        "end_time": end_dt,
    }
    return templates.TemplateResponse(
        request=request,
        name="shop/confirm.html",
        context={"shop": shop, "shop_id": shop_id, "reservation_preview": reservation_preview},
    )


@app.post("/shop/{shop_id}/edit")
def shop_edit_redirect(
    shop_id: str,
    reservation_date: str = Form(""),
    start_time: str = Form(""),
    customer_name: str = Form(""),
    phone: str = Form(""),
    email: str = Form(""),
    receive_email: str = Form('1'),
    staff_id: str = Form(""),
    menu_id: str = Form(""),
    week_start: str = Form(""),
):
    week_start_part = f"&week_start={week_start}" if week_start else ""
    url = f"/shop/{shop_id}?reservation_date={reservation_date}&start_time={start_time}&customer_name={customer_name}&phone={phone}&email={email}&receive_email={receive_email}&staff_id={staff_id}&menu_id={menu_id}{week_start_part}#reserve-form"
    return RedirectResponse(url, status_code=303)


@app.post("/shop/{shop_id}/reserve", response_class=HTMLResponse)
def shop_reserve(
    request: Request,
    shop_id: str,
    customer_name: str = Form(...),
    phone: str = Form(""),
    email: str = Form(""),
    receive_email: str = Form("1"),
    staff_id: int = Form(...),
    menu_id: int = Form(...),
    reservation_date: str = Form(...),
    start_time: str = Form(...),
    line_user_id: str = Form(""),
):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    subscription = get_shop_subscription(shop_id) or {'status': 'active', 'plan_name': 'Free', 'show_ads': False}
    shop = _build_shop_with_visible_staff(shop, subscription)
    staff = next((s for s in shop.get("staff_list", []) if int(s["id"]) == int(staff_id)), None)
    menu = next((m for m in shop.get("menus", []) if int(m["id"]) == int(menu_id)), None)
    if not staff or not menu:
        ctx = build_shop_booking_context(shop_id, request, "スタッフまたはメニューが見つかりません。")
        return templates.TemplateResponse(
            request=request,
            name="shop/index.html",
            context=ctx,
            status_code=400,
        )
    if not _staff_allows_menu(staff, menu_id):
        ctx = build_shop_booking_context(shop_id, request, "選択したスタッフではこのメニューを予約できません。")
        return templates.TemplateResponse(
            request=request,
            name="shop/index.html",
            context=ctx,
            status_code=400,
        )
    reservation_day = _safe_parse_date(reservation_date, date.today())
    if _is_shop_holiday(shop, reservation_day) or _is_staff_holiday(staff, reservation_day):
        ctx = build_shop_booking_context(shop_id, request, "選択した日はこのスタッフの休日のため予約できません。")
        return templates.TemplateResponse(
            request=request,
            name="shop/index.html",
            context=ctx,
            status_code=400,
        )
    customer = find_customer(shop_id, customer_name, phone, email) or create_customer(shop_id, customer_name, phone, email)
    updated_customer = update_customer_contact(shop_id, int(customer['id']), customer_name, phone, email)
    if updated_customer is not None:
        customer = updated_customer
    resolved_line_user_id = (
        str(line_user_id or "").strip()
        or str(request.session.get("line_user_id") or "").strip()
    )
    if resolved_line_user_id:
        update_customer_line_user_id(shop_id, int(customer["id"]), resolved_line_user_id)
        customer = get_customer_by_id(shop_id, int(customer["id"])) or customer
    start_dt = datetime.strptime(start_time, "%H:%M")
    duration = int(menu.get("duration", 60))
    end_time = (start_dt + timedelta(minutes=duration)).strftime("%H:%M")
    reservation = create_reservation(
        shop_id=shop_id,
        customer_id=int(customer["id"]),
        customer_name=customer_name,
        customer_email=email,
        receive_email=1 if receive_email == '1' else 0,
        staff_id=int(staff_id),
        staff_name=staff["name"],
        menu_id=int(menu_id),
        menu_name=menu["name"],
        duration=duration,
        price=int(menu.get("price", 0)),
        reservation_date=reservation_date,
        start_time=start_time,
        end_time=end_time,
        source="web",
    )
    _record_audit_log(
        request,
        actor_type="guest",
        actor_id=email or phone or customer_name,
        actor_name=customer_name,
        action="reservation_create",
        shop_id=shop_id,
        target_type="reservation",
        target_id=int(reservation.get("id") or 0),
        target_label=customer_name,
        detail={"source": "web"},
    )
    if email and receive_email == '1':
        _send_reservation_mail(to_email=email, shop=shop, reservation_date=reservation_date, start_time=start_time, reply_to_email=str(shop.get('reply_to_email') or ''))
    if resolved_line_user_id:
        line_settings = get_shop_line_settings(shop_id)
        access_token = str(line_settings.get("line_channel_access_token") or "").strip()
        if access_token:
            send_line_message(
                access_token,
                resolved_line_user_id,
                build_reservation_line_message(shop, reservation),
            )
    return templates.TemplateResponse(
        request=request,
        name="shop/complete.html",
        context={"shop": shop, "shop_id": shop_id, "customer": customer, "reservation": reservation},
    )


def _member_default_next(shop_id: str) -> str:
    return f"/shop/{shop_id}"


def _member_redirect_target(shop_id: str, next_url: str | None) -> str:
    target = str(next_url or '').strip()
    if target.startswith('/'):
        return target
    return _member_default_next(shop_id)


def _get_logged_in_member(request: Request, shop_id: str) -> dict | None:
    member_id = request.session.get('member_logged_in_id')
    member_shop_id = str(request.session.get('member_logged_in_shop_id') or '')
    if not member_id or member_shop_id != shop_id:
        return None
    try:
        return get_member_by_id(shop_id, int(member_id))
    except (TypeError, ValueError):
        return None


def _login_member_session(request: Request, shop_id: str, member: dict) -> None:
    request.session['member_logged_in_id'] = int(member['id'])
    request.session['member_logged_in_shop_id'] = shop_id
    request.session['member_logged_in_name'] = str(member.get('name') or '')
    request.session['member_logged_in_phone_normalized'] = str(member.get('phone_normalized') or normalize_member_phone(str(member.get('phone') or '')) or '')


def _get_member_for_shop_session(request: Request, shop_id: str) -> dict | None:
    member = _get_logged_in_member(request, shop_id)
    if member is not None:
        return member
    normalized_phone = normalize_member_phone(str(request.session.get('member_logged_in_phone_normalized') or ''))
    if not normalized_phone:
        return None
    cross_shop_member = get_member_by_phone_normalized(shop_id, normalized_phone)
    if cross_shop_member is not None:
        _login_member_session(request, shop_id, cross_shop_member)
    return cross_shop_member


def _format_admin_subscription_status_label(subscription: dict | None) -> str:
    status = str((subscription or {}).get("status") or "").strip().lower()
    if status == "active":
        started_at_raw = str((subscription or {}).get("started_at") or "").strip()
        remaining_days = 0
        if started_at_raw:
            parsed_started_at = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    parsed_started_at = datetime.strptime(started_at_raw[:19], fmt) if fmt != "%Y-%m-%d" else datetime.strptime(started_at_raw[:10], fmt)
                    break
                except ValueError:
                    continue
            if parsed_started_at is not None:
                renewal_date = parsed_started_at.date() + timedelta(days=30)
                remaining_days = max((renewal_date - date.today()).days, 0)
        return f"契約中(自動更新まであと{remaining_days}日)"
    if status == "trial":
        return "トライアル中"
    if status == "canceled":
        return "解約済み"
    return status or "未設定"


def _chat_limit_for_subscription(subscription: dict | None) -> int | None:
    plan_code = str((subscription or {}).get('plan_code') or '').strip().lower()
    if plan_code == 'free':
        return 100
    return None


def _logout_member_session(request: Request) -> None:
    request.session.pop('member_logged_in_id', None)
    request.session.pop('member_logged_in_shop_id', None)
    request.session.pop('member_logged_in_name', None)
    request.session.pop('member_logged_in_phone_normalized', None)


def _serialize_chat_message(message: dict[str, object]) -> dict[str, object]:
    sender_type = str(message.get('sender_type') or '')
    return {
        'id': int(message.get('id') or 0),
        'sender_type': sender_type,
        'body': str(message.get('body') or ''),
        'created_at': _format_chat_datetime(message.get('created_at')),
        'role_label': 'あなた' if sender_type == 'member' else '店舗',
    }


def _serialize_unread_chat_item(item: dict[str, object]) -> dict[str, object]:
    serialized = dict(item)
    serialized['latest_created_at'] = _format_chat_datetime(item.get('latest_created_at'))
    return serialized



@app.get("/admin/register/{shop_id}", response_class=HTMLResponse)
def admin_staff_register_page(request: Request, shop_id: str, error_message: str = "", success_message: str = ""):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop(normalized_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="見つかりません")

    return templates.TemplateResponse(
        request=request,
        name="admin/register.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": normalized_shop_id,
            "error_message": error_message,
            "success_message": success_message,
            "staff_list": shop.get("staff_list", []),
            "menus": shop.get("menus", []),
        },
    )


@app.post("/admin/register/{shop_id}")
def admin_staff_register_submit(
    request: Request,
    shop_id: str,
    name: str = Form(...),
    menu_ids: list[str] = Form([]),
    holiday_dates: str = Form(""),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop(normalized_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="見つかりません")

    staff_name = (name or "").strip()
    if not staff_name:
        return admin_staff_register_page(request, normalized_shop_id, error_message="スタッフ名を入力してください。")

    normalized_menu_ids: list[int] = []
    for item in menu_ids or []:
        try:
            normalized_menu_ids.append(int(item))
        except (TypeError, ValueError):
            continue

    subscription = get_shop_subscription(normalized_shop_id) or {}
    staff_list = list(shop.get("staff_list", []))
    visible_staff_list = _get_visible_staff_list(shop, subscription)
    if len(visible_staff_list) != len(staff_list):
        return RedirectResponse(f"/admin/{normalized_shop_id}/staff-info?error=現在のプランではこれ以上スタッフを追加できません。", status_code=303)
    staff_limit = _get_staff_limit_for_subscription(subscription)
    if staff_limit is not None and len(staff_list) >= int(staff_limit):
        return RedirectResponse(f"/admin/{normalized_shop_id}/staff-info?error=現在のプランではスタッフは{staff_limit}人までです。", status_code=303)
    staff_list.append({
        "name": staff_name,
        "menu_ids": normalized_menu_ids,
        "photo_url": '',
        "default_avatar": 'male',
    })
    update_shop_staff_list(normalized_shop_id, staff_list)
    return RedirectResponse(f"/admin/{normalized_shop_id}/staff-info?saved=スタッフを登録しました。", status_code=303)



@app.get("/admin/{shop_id}/staff-info/{staff_id}/edit", response_class=HTMLResponse)
def admin_staff_edit_page(request: Request, shop_id: str, staff_id: int, error_message: str = ""):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop(normalized_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="見つかりません")

    subscription = get_shop_subscription(normalized_shop_id) or {}
    shop = _build_shop_with_visible_staff(shop, subscription)
    raw_staff_list = list(shop.get("staff_list", []))
    visible_staff_list, visible_target_staff = _get_visible_staff_or_404(shop, subscription, staff_id)
    target_index = next((index for index, staff in enumerate(raw_staff_list) if int(staff.get("id") or 0) == int(staff_id)), None)
    if target_index is None:
        raise HTTPException(status_code=404, detail="スタッフが見つかりません")

    target_staff = dict(visible_target_staff)
    selected_menu_ids = {int(menu_id) for menu_id in target_staff.get("menu_ids", []) if str(menu_id).strip()}
    template_name = "admin/tool/staff_edit.html" if shop.get("admin_ui_mode") == "tool" else "admin/staff_edit.html"
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": normalized_shop_id,
            "staff": target_staff,
            "menus": shop.get("menus", []),
            "selected_menu_ids": selected_menu_ids,
            "selected_holiday_dates": sorted(_get_staff_holiday_dates(target_staff)),
            "error_message": error_message,
            "active_page": "staff_info",
        },
    )


@app.post("/admin/{shop_id}/staff-info/{staff_id}/edit")
def admin_staff_edit_submit(
    request: Request,
    shop_id: str,
    staff_id: int,
    name: str = Form(...),
    menu_ids: list[str] = Form([]),
    holiday_dates: str = Form(""),
    default_avatar: str = Form('male'),
    photo: UploadFile | None = File(None),
):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop(normalized_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="見つかりません")

    staff_name = (name or "").strip()
    if not staff_name:
        return admin_staff_edit_page(request, normalized_shop_id, staff_id, error_message="スタッフ名を入力してください。")

    normalized_menu_ids: list[int] = []
    for item in menu_ids or []:
        try:
            normalized_menu_ids.append(int(item))
        except (TypeError, ValueError):
            continue

    normalized_holiday_dates: list[str] = sorted({
        parsed.date().isoformat()
        for parsed in [
            (lambda value: datetime.strptime(value.strip(), '%Y-%m-%d') if value.strip() else None)(item)
            for item in str(holiday_dates or '').split(',')
        ]
        if parsed is not None
    })

    normalized_default_avatar = _normalize_staff_default_avatar(default_avatar)

    staff_list = list(shop.get("staff_list", []))
    updated = False
    for index, staff in enumerate(staff_list):
        if int(staff.get("id") or 0) != int(staff_id):
            continue
        next_photo_url = str(staff.get('photo_url') or '').strip()
        if photo is not None and str(photo.filename or '').strip():
            if next_photo_url:
                _delete_staff_photo_file(next_photo_url)
            next_photo_url = _save_staff_photo_file(normalized_shop_id, staff_id, photo)
        staff_list[index] = {
            **staff,
            "name": staff_name,
            "menu_ids": normalized_menu_ids,
            "holiday_dates": normalized_holiday_dates,
            "photo_url": next_photo_url,
            "default_avatar": normalized_default_avatar,
        }
        updated = True
        break

    if not updated:
        raise HTTPException(status_code=404, detail="スタッフが見つかりません")

    update_shop_staff_list(normalized_shop_id, staff_list)
    return RedirectResponse(f"/admin/{normalized_shop_id}/staff-info?saved=スタッフ情報を更新しました。", status_code=303)



@app.get("/admin/{shop_id}/staff/{staff_id}", response_class=HTMLResponse)
def admin_staff_schedule_page(request: Request, shop_id: str, staff_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop(normalized_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

    subscription = get_shop_subscription(normalized_shop_id) or {}
    shop = _build_shop_with_visible_staff(shop, subscription)
    staff_list, target_staff = _get_visible_staff_or_404(shop, subscription, staff_id)

    reservations = _attach_staff_avatar_to_reservations(get_reservations(normalized_shop_id), shop.get('staff_list', []))
    customers = get_customers(normalized_shop_id)
    admin_users = get_admin_users(normalized_shop_id)
    current_admin_name = request.session.get("store_logged_in_admin_name") or (admin_users[0].get("name") if admin_users else "")

    month_value = request.query_params.get("month") or date.today().strftime("%Y-%m")
    try:
        current_month = datetime.strptime(month_value, "%Y-%m").date().replace(day=1)
    except ValueError:
        current_month = date.today().replace(day=1)
    calendar_month_value = current_month.strftime("%Y-%m")
    selected_date_obj = _safe_parse_date(request.query_params.get("date"), date.today())
    week_start = _normalize_week_start(_safe_parse_date(request.query_params.get("week_start"), selected_date_obj))

    selected_day_schedule = sorted(
        [
            r for r in reservations
            if str(r.get("reservation_date") or "") == selected_date_obj.isoformat()
            and str(r.get("staff_id") or "").strip() == str(staff_id)
        ],
        key=lambda x: (str(x.get("start_time") or ""), int(x.get("id") or 0)),
    )
    selected_day_count = len([r for r in selected_day_schedule if str(r.get("status") or "") != "キャンセル"])

    week_days, time_slots, weekly_rows = _build_week_availability_matrix(
        shop=shop,
        reservations=reservations,
        selected_date=selected_date_obj,
        week_start=week_start,
        staff_id=staff_id,
    )
    template_name = "admin/tool/staff_schedule.html" if shop.get("admin_ui_mode") == "tool" else "admin/staff_schedule.html"
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": normalized_shop_id,
            "subscription": subscription,
            "customers": customers,
            "today": date.today().isoformat(),
            "today_reservations": [
                r for r in reservations
                if str(r.get("reservation_date") or "") == date.today().isoformat()
            ],
            "staff_list": staff_list,
            "staff": target_staff,
            "current_staff_id": int(staff_id),
            "current_admin_name": current_admin_name,
            "selected_date": selected_date_obj.isoformat(),
            "selected_day_schedule": selected_day_schedule,
            "selected_day_count": selected_day_count,
            "week_days": week_days,
            "time_slots": time_slots,
            "weekly_rows": weekly_rows,
            "week_start": week_start.isoformat(),
            "prev_week_start": (week_start - timedelta(days=7)).isoformat(),
            "next_week_start": (week_start + timedelta(days=7)).isoformat(),
            "calendar_month_value": calendar_month_value,
            "active_page": "staff",
        },
    )


@app.get("/admin/{shop_id}/staff-info", response_class=HTMLResponse)
def admin_staff_info_page(request: Request, shop_id: str):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect

    normalized_shop_id = (shop_id or "").strip().lower()
    shop = get_shop(normalized_shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="見つかりません")

    reservations = get_reservations(normalized_shop_id)
    customers = get_customers(normalized_shop_id)
    admin_users = get_admin_users(normalized_shop_id)
    subscription = get_shop_subscription(normalized_shop_id) or {}
    shop = _build_shop_with_visible_staff(shop, subscription)
    reservations = _attach_staff_avatar_to_reservations(reservations, shop.get('staff_list', []))
    current_admin_name = request.session.get("store_logged_in_admin_name") or (admin_users[0].get("name") if admin_users else "")
    menu_lookup = {str(item.get("id")): item.get("name") for item in shop.get("menus", [])}
    staff_list = []
    for staff in shop.get("staff_list", []):
        names = [menu_lookup.get(str(menu_id)) for menu_id in staff.get("menu_ids", [])]
        staff_list.append({
            **staff,
            "menu_names": [name for name in names if name],
        })

    template_name = "admin/tool/staff_info.html" if shop.get("admin_ui_mode") == "tool" else "admin/staff_info.html"
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "request": request,
            "shop": shop,
            "shop_id": normalized_shop_id,
            "staff_list": staff_list,
            "menus": shop.get("menus", []),
            "customers": customers,
            "today_reservations": [item for item in reservations if str(item.get("reservation_date") or "") == date.today().isoformat()],
            "subscription": subscription,
            "current_admin_name": current_admin_name,
            "active_page": "staff_info",
            "success_message": request.query_params.get("saved", ""),
            "error_message": request.query_params.get("error", ""),
        },
    )


def _send_member_registration_verification_mail(*, to_email: str, code: str, shop: dict) -> bool:
    mail_settings = _get_mail_runtime_settings()
    smtp_user = str(mail_settings.get('smtp_user') or '').strip()
    smtp_password = str(mail_settings.get('smtp_password') or '').strip()
    from_email = str(mail_settings.get('from_email') or '').strip()
    if not to_email or not from_email or not smtp_user or not smtp_password:
        print('[_send_member_registration_verification_mail] missing SMTP settings')
        return False

    shop_name = str(shop.get('shop_name') or 'らくばい').strip() or 'らくばい'
    subject = f"【{shop_name}】会員登録確認コード"
    body = (
        f"{shop_name} の会員登録確認コードです。\n\n"
        f"確認コード: {code}\n\n"
        "このコードの有効期限は10分です。\n"
        "このメールに心当たりがない場合は、このまま破棄してください。\n"
    )

    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = formataddr((str(mail_settings.get('from_name') or shop_name), from_email))
    msg['To'] = to_email

    try:
        with smtplib.SMTP(str(mail_settings.get('smtp_host') or 'smtp.gmail.com'), int(mail_settings.get('smtp_port') or 587), timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
        return True
    except Exception as exc:
        print(f"[_send_member_registration_verification_mail] failed: {exc}")
        return False

@app.get("/member/{shop_id}/login", response_class=HTMLResponse)
def member_login_page(request: Request, shop_id: str, next: str | None = None, error: str | None = None):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    target_url = _member_redirect_target(shop_id, next)
    member = _get_member_for_shop_session(request, shop_id)
    if member is not None:
        return RedirectResponse(url=target_url or f"/member/{shop_id}/mypage", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="member/login.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "next_url": target_url,
            "error": error or "",
        },
    )


@app.post("/member/{shop_id}/login")
def member_login_submit(request: Request, shop_id: str, phone: str = Form(...), password: str = Form(...), next_url: str = Form("")):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    member = authenticate_member(shop_id, phone, password)
    if member is None:
        from urllib.parse import quote
        _record_audit_log(
            request,
            actor_type="member",
            actor_id=normalize_member_phone(phone),
            action="login",
            shop_id=shop_id,
            status="failure",
            detail={"phone": normalize_member_phone(phone)},
        )
        target = quote(_member_redirect_target(shop_id, next_url), safe='')
        return RedirectResponse(url=f"/member/{shop_id}/login?next={target}&error=電話番号またはパスワードが違います", status_code=303)
    target_url = _member_redirect_target(shop_id, next_url)
    line_user_id_for_link = extract_line_user_id_from_next_url(str(target_url or next_url or ""))

    _login_member_session(request, shop_id, member)
    if line_user_id_for_link:
        complete_line_member_link_after_registration(shop_id, member, line_user_id_for_link)
    _record_audit_log(
        request,
        actor_type="member",
        actor_id=int(member['id']),
        actor_name=str(member.get('name') or ''),
        action="login",
        shop_id=shop_id,
        target_type="member",
        target_id=int(member['id']),
        target_label=str(member.get('name') or ''),
    )
    return RedirectResponse(url=_member_redirect_target(shop_id, next_url or f"/member/{shop_id}/mypage"), status_code=303)



def complete_line_member_link_after_registration(shop_id: str, member: dict, line_user_id: str) -> None:
    """会員登録/ログイン後に、顧客とLINE user_idを確実に紐づけます。"""
    clean_line_user_id = str(line_user_id or "").strip()
    if not clean_line_user_id:
        return
    try:
        ensure_customer_line_user_id_schema()
        member_customer_id = int((member or {}).get("customer_id") or 0)
        if member_customer_id:
            update_customer_line_user_id(shop_id, member_customer_id, clean_line_user_id)
        else:
            linked_customer = ensure_line_customer_for_reservation(
                shop_id,
                clean_line_user_id,
                str((member or {}).get("name") or ""),
                str((member or {}).get("phone") or (member or {}).get("phone_normalized") or ""),
            )
            member_customer_id = int(linked_customer.get("id") or 0)
            try:
                with get_connection() as conn:
                    conn.execute(
                        "UPDATE members SET customer_id = ? WHERE shop_id = ? AND id = ?",
                        (member_customer_id, str(shop_id or "").strip(), int((member or {}).get("id") or 0)),
                    )
                    conn.commit()
            except Exception as exc:
                print("member customer_id backfill error:", repr(exc))

        settings = get_shop_line_settings(shop_id)
        token = str(settings.get("line_channel_access_token") or "").strip()
        if token:
            send_line_message(
                token,
                clean_line_user_id,
                "会員登録が完了し、このLINEと会員情報を紐づけました。\n予約を続ける場合は「予約」と送信してください。",
            )
        print("LINE member linked:", shop_id, clean_line_user_id, member_customer_id)
    except Exception as exc:
        print("complete line member link error:", repr(exc))


@app.get("/member/{shop_id}/line-register-complete", response_class=HTMLResponse)
def member_line_register_complete_page(request: Request, shop_id: str, line_user_id: str = ""):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

    member = _get_member_for_shop_session(request, shop_id)
    if member is not None and str(line_user_id or "").strip():
        complete_line_member_link_after_registration(shop_id, member, line_user_id)

    settings = get_shop_line_settings(shop_id)
    line_official_url = str(settings.get("line_official_url") or "").strip()
    shop_name = str((shop or {}).get("shop_name") or "店舗")
    line_button = f'<a class="btn" href="{line_official_url}">LINEに戻る</a>' if line_official_url else '<p class="sub">この画面を閉じてLINEアプリに戻ってください。</p>'
    html = f"""
<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>LINE連携完了</title>
  <style>
    body {{ font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background:#f6f7f9; margin:0; padding:24px; color:#111827; }}
    .card {{ max-width:520px; margin:32px auto; background:white; border-radius:18px; padding:24px; box-shadow:0 10px 30px rgba(15,23,42,.08); }}
    h1 {{ font-size:22px; margin:0 0 12px; }}
    p {{ line-height:1.8; }}
    .btn {{ display:block; text-align:center; background:#06c755; color:white; text-decoration:none; border-radius:12px; padding:14px 16px; font-weight:700; margin-top:16px; }}
    .sub {{ color:#6b7280; font-size:14px; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>LINE連携が完了しました</h1>
    <p>{shop_name}の会員情報とLINEを紐づけました。</p>
    <p>予約を続ける場合は、LINEに戻って <strong>「予約」</strong> と送信してください。</p>
    {line_button}
  </div>
</body>
</html>
"""
    return HTMLResponse(html)

@app.get("/member/{shop_id}/register", response_class=HTMLResponse)
def member_register_page(request: Request, shop_id: str, next: str | None = None, error: str | None = None):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    return templates.TemplateResponse(
        request=request,
        name="member/register.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "next_url": _member_redirect_target(shop_id, next),
            "error": error or "",
        },
    )


@app.post("/member/{shop_id}/register")
def member_register_submit(
    request: Request,
    shop_id: str,
    name: str = Form(...),
    phone: str = Form(...),
    password: str = Form(...),
    email: str = Form(''),
    next_url: str = Form(''),
    agree_terms: str | None = Form(None),
):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

    from urllib.parse import quote

    target_url = _member_redirect_target(shop_id, next_url)
    target = quote(target_url, safe='')
    normalized_email = (email or '').strip().lower()

    if not agree_terms:
        return RedirectResponse(url=f"/member/{shop_id}/register?next={target}&error=利用規約等への同意が必要です。", status_code=303)
    if not normalized_email:
        return RedirectResponse(url=f"/member/{shop_id}/register?next={target}&error=メールアドレスを入力してください。", status_code=303)
    if '@' not in normalized_email:
        return RedirectResponse(url=f"/member/{shop_id}/register?next={target}&error=メールアドレスの形式が正しくありません。", status_code=303)

    code = f"{secrets.randbelow(1_000_000):06d}"

    try:
        verification = create_member_registration_verification(
            shop_id=shop_id,
            name=(name or '').strip(),
            phone=(phone or '').strip(),
            password=password or '',
            email=normalized_email,
            code=code,
            next_url=target_url,
        )
    except ValueError as exc:
        return RedirectResponse(url=f"/member/{shop_id}/register?next={target}&error={quote(str(exc), safe='')}", status_code=303)

    sent = _send_member_registration_verification_mail(to_email=normalized_email, code=code, shop=shop)
    if not sent:
        return RedirectResponse(url=f"/member/{shop_id}/register?next={target}&error=確認コードメールの送信に失敗しました。時間をおいて再度お試しください。", status_code=303)

    verify_url = f"/member/{shop_id}/register/verify?token={quote(str(verification.get('token') or ''), safe='')}&next={target}"
    return RedirectResponse(url=verify_url, status_code=303)


@app.get("/member/{shop_id}/register/verify", response_class=HTMLResponse)
def member_register_verify_page(request: Request, shop_id: str, token: str, next: str | None = None, error: str | None = None):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    verification = get_member_registration_verification(shop_id, token)
    if verification is None:
        from urllib.parse import quote

        target = quote(_member_redirect_target(shop_id, next), safe='')
        return RedirectResponse(url=f"/member/{shop_id}/register?next={target}&error=確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。", status_code=303)

    return templates.TemplateResponse(
        request=request,
        name="member/register_verify.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "token": token,
            "next_url": _member_redirect_target(shop_id, next or str(verification.get('next_url') or '')),
            "pending_email": str(verification.get('email') or ''),
            "error": error or "",
        },
    )


@app.post("/member/{shop_id}/register/verify")
def member_register_verify_submit(
    request: Request,
    shop_id: str,
    token: str = Form(...),
    code: str = Form(...),
    next_url: str = Form(''),
):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

    from urllib.parse import quote

    target_url = _member_redirect_target(shop_id, next_url)
    verification = get_member_registration_verification(shop_id, token)
    if verification is None:
        target = quote(target_url, safe='')
        return RedirectResponse(url=f"/member/{shop_id}/register?next={target}&error=確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。", status_code=303)

    normalized_code = ''.join(ch for ch in str(code or '') if ch.isdigit())
    verify_target = quote(str(verification.get('next_url') or target_url), safe='')
    if len(normalized_code) != 6:
        return RedirectResponse(
            url=f"/member/{shop_id}/register/verify?token={quote(token, safe='')}&next={verify_target}&error=確認コードは6桁の数字で入力してください。",
            status_code=303,
        )

    try:
        verified = verify_member_registration_code(shop_id, token, normalized_code)
        if verified is None:
            return RedirectResponse(
                url=f"/member/{shop_id}/register/verify?token={quote(token, safe='')}&next={verify_target}&error=確認コードが正しくありません。",
                status_code=303,
            )
        member = consume_member_registration_verification(shop_id, token)
        line_user_id = extract_line_user_id_from_next_url(str(verification.get('next_url') or target_url))
        if line_user_id:
            complete_line_member_link_after_registration(shop_id, member, line_user_id)
    except ValueError as exc:
        return RedirectResponse(
            url=f"/member/{shop_id}/register/verify?token={quote(token, safe='')}&next={verify_target}&error={quote(str(exc), safe='')}",
            status_code=303,
        )

    _login_member_session(request, shop_id, member)
    _record_audit_log(
        request,
        actor_type="member",
        actor_id=int(member['id']),
        actor_name=str(member.get('name') or ''),
        action="member_register",
        shop_id=shop_id,
        target_type="member",
        target_id=int(member['id']),
        target_label=str(member.get('name') or ''),
    )
    _record_audit_log(
        request,
        actor_type="member",
        actor_id=int(member['id']),
        actor_name=str(member.get('name') or ''),
        action="login",
        shop_id=shop_id,
        target_type="member",
        target_id=int(member['id']),
        target_label=str(member.get('name') or ''),
    )
    return RedirectResponse(url=_member_redirect_target(shop_id, next_url or f"/member/{shop_id}/mypage"), status_code=303)


@app.get("/member/{shop_id}/logout")
def member_logout(request: Request, shop_id: str):
    member_id = request.session.get('member_logged_in_id')
    member_name = str(request.session.get('member_logged_in_name') or '')
    if member_id:
        _record_audit_log(
            request,
            actor_type="member",
            actor_id=int(member_id),
            actor_name=member_name,
            action="logout",
            shop_id=shop_id,
            target_type="member",
            target_id=int(member_id),
            target_label=member_name,
        )
    _logout_member_session(request)
    return RedirectResponse(url=f"/member/{shop_id}/login", status_code=303)


@app.get("/member/{shop_id}/mypage", response_class=HTMLResponse)
def member_dashboard_page(request: Request, shop_id: str):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    member = _get_member_for_shop_session(request, shop_id)
    if member is None:
        return RedirectResponse(url=f"/member/{shop_id}/login?next=/member/{shop_id}/mypage", status_code=303)
    reservations = get_member_all_reservations(str(member.get('phone_normalized') or member.get('phone') or ''))
    completed_reservations = [item for item in reservations if str(item.get('status') or '') == '来店済み']
    member_photos = [
        {**photo, "image_url": _normalize_customer_photo_url(str(photo.get("image_url") or ""))}
        for photo in (get_customer_photos(shop_id, int(member.get('customer_id') or 0)) if member.get('customer_id') else [])
    ]
    linked_shops = get_member_linked_shops(str(member.get('phone_normalized') or member.get('phone') or ''))
    member_unread_items = [_serialize_unread_chat_item(item) for item in get_member_unread_chat_summary(str(member.get('phone_normalized') or member.get('phone') or ''))]
    active_customer_id = int(member.get('customer_id') or 0)
    chat_messages = [_serialize_chat_message(item) for item in list_chat_messages(shop_id, active_customer_id, limit=200, member_id=int(member['id']))] if active_customer_id else []
    if active_customer_id:
        mark_chat_messages_read_for_member(shop_id, active_customer_id, int(member['id']))
    shop_subscription = get_shop_subscription(shop_id)
    chat_limit = _chat_limit_for_subscription(shop_subscription)
    shop_chat_sent_this_month = count_shop_chat_messages_in_month(shop_id)
    return templates.TemplateResponse(
        request=request,
        name="member/dashboard.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "member": member,
            "reservations": reservations[:10],
            "completed_reservations": completed_reservations[:10],
            "reservation_count": len(reservations),
            "member_photos": member_photos,
            "chat_messages": chat_messages,
            "linked_shops": linked_shops,
            "member_unread_items": member_unread_items,
            "member_chat_customer_id": active_customer_id,
            "member_chat_enabled": bool(active_customer_id),
            "member_chat_limit": chat_limit,
            "member_chat_sent_this_month": shop_chat_sent_this_month,
            "member_chat_remaining": None if chat_limit is None else max(chat_limit - shop_chat_sent_this_month, 0),
        },
    )


@app.get("/member/{shop_id}/unsubscribe/shop", response_class=HTMLResponse)
def member_unsubscribe_shop_page(request: Request, shop_id: str):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    member = _get_member_for_shop_session(request, shop_id)
    if member is None:
        return RedirectResponse(url=f"/member/{shop_id}/login?next=/member/{shop_id}/unsubscribe/shop", status_code=303)
    linked_shops = get_member_linked_shops(str(member.get('phone_normalized') or member.get('phone') or ''))
    return templates.TemplateResponse(
        request=request,
        name="member/unsubscribe.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "member": member,
            "unsubscribe_mode": "shop",
            "linked_shops": linked_shops,
        },
    )


@app.post("/member/{shop_id}/unsubscribe/shop")
def member_unsubscribe_shop_submit(request: Request, shop_id: str):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    member = _get_member_for_shop_session(request, shop_id)
    if member is None:
        return RedirectResponse(url=f"/member/{shop_id}/login?next=/member/{shop_id}/unsubscribe/shop", status_code=303)
    deactivate_member(shop_id, int(member['id']))
    _record_audit_log(
        request,
        actor_type="member",
        actor_id=int(member['id']),
        actor_name=str(member.get('name') or ''),
        action="member_unsubscribe_shop",
        shop_id=shop_id,
        target_type="member",
        target_id=int(member['id']),
        target_label=str(member.get('name') or ''),
    )
    _record_audit_log(
        request,
        actor_type="member",
        actor_id=int(member['id']),
        actor_name=str(member.get('name') or ''),
        action="logout",
        shop_id=shop_id,
        target_type="member",
        target_id=int(member['id']),
        target_label=str(member.get('name') or ''),
    )
    _logout_member_session(request)
    return templates.TemplateResponse(
        request=request,
        name="member/unsubscribe_complete.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "mode": "shop",
        },
    )


@app.get("/member/{shop_id}/unsubscribe/all", response_class=HTMLResponse)
def member_unsubscribe_all_page(request: Request, shop_id: str):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    member = _get_member_for_shop_session(request, shop_id)
    if member is None:
        return RedirectResponse(url=f"/member/{shop_id}/login?next=/member/{shop_id}/unsubscribe/all", status_code=303)
    linked_shops = get_member_linked_shops(str(member.get('phone_normalized') or member.get('phone') or ''))
    return templates.TemplateResponse(
        request=request,
        name="member/unsubscribe.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "member": member,
            "unsubscribe_mode": "all",
            "linked_shops": linked_shops,
        },
    )


@app.post("/member/{shop_id}/unsubscribe/all")
def member_unsubscribe_all_submit(request: Request, shop_id: str):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    member = _get_member_for_shop_session(request, shop_id)
    if member is None:
        return RedirectResponse(url=f"/member/{shop_id}/login?next=/member/{shop_id}/unsubscribe/all", status_code=303)
    deactivate_members_by_phone(str(member.get('phone_normalized') or member.get('phone') or ''))
    _record_audit_log(
        request,
        actor_type="member",
        actor_id=int(member['id']),
        actor_name=str(member.get('name') or ''),
        action="member_unsubscribe_all",
        shop_id=shop_id,
        target_type="member",
        target_id=int(member['id']),
        target_label=str(member.get('name') or ''),
    )
    _record_audit_log(
        request,
        actor_type="member",
        actor_id=int(member['id']),
        actor_name=str(member.get('name') or ''),
        action="logout",
        shop_id=shop_id,
        target_type="member",
        target_id=int(member['id']),
        target_label=str(member.get('name') or ''),
    )
    _logout_member_session(request)
    return templates.TemplateResponse(
        request=request,
        name="member/unsubscribe_complete.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "mode": "all",
        },
    )


@app.get("/member/{shop_id}/chat/messages")
def member_chat_messages_api(request: Request, shop_id: str):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    member = _get_member_for_shop_session(request, shop_id)
    if member is None:
        raise HTTPException(status_code=401, detail="ログインしてください")
    customer_id = int(member.get('customer_id') or 0)
    if not customer_id:
        return JSONResponse({"ok": True, "messages": [], "unread_count": 0})
    mark_chat_messages_read_for_member(shop_id, customer_id, int(member['id']))
    messages = [_serialize_chat_message(item) for item in list_chat_messages(shop_id, customer_id, limit=200, member_id=int(member['id']))]
    return JSONResponse({"ok": True, "messages": messages, "unread_count": 0})


@app.post("/member/{shop_id}/chat/messages")
def member_chat_send_api(request: Request, shop_id: str, message: str = Form(...)):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    member = _get_member_for_shop_session(request, shop_id)
    if member is None:
        raise HTTPException(status_code=401, detail="ログインしてください")
    customer_id = int(member.get('customer_id') or 0)
    if not customer_id:
        return JSONResponse({"ok": False, "error": "顧客データが見つかりません。"}, status_code=400)
    try:
        created_message = create_chat_message(shop_id, customer_id, int(member['id']), 'member', message)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    _record_audit_log(
        request,
        actor_type="member",
        actor_id=int(member['id']),
        actor_name=str(member.get('name') or ''),
        action="chat_send",
        shop_id=shop_id,
        target_type="customer",
        target_id=customer_id,
        target_label=str(member.get('name') or ''),
        detail={
            "sender_type": "member",
            "chat_message_id": int(created_message.get("id") or 0),
            "customer_id": customer_id,
        },
    )
    messages = [_serialize_chat_message(item) for item in list_chat_messages(shop_id, customer_id, limit=200, member_id=int(member['id']))]
    return JSONResponse({"ok": True, "messages": messages})


@app.get("/admin/{shop_id}/customers/{customer_id}/chat/messages")
def admin_chat_messages_api(request: Request, shop_id: str, customer_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        raise HTTPException(status_code=401, detail="ログインしてください")
    customer = get_customer_by_id(shop_id, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="顧客が見つかりません")
    active_member_id = get_latest_chat_member_id(shop_id, customer_id)
    mark_chat_messages_read_for_admin(shop_id, customer_id)
    messages = [_serialize_chat_message(item) for item in list_chat_messages(shop_id, customer_id, limit=200, member_id=active_member_id)]
    subscription = get_shop_subscription(shop_id)
    chat_limit = _chat_limit_for_subscription(subscription)
    sent_count = count_shop_chat_messages_in_month(shop_id)
    return JSONResponse({"ok": True, "messages": messages, "unread_count": 0, "sent_this_month": sent_count, "remaining": None if chat_limit is None else max(chat_limit - sent_count, 0), "limit": chat_limit})


@app.post("/admin/{shop_id}/customers/{customer_id}/chat/messages")
def admin_chat_send_api(request: Request, shop_id: str, customer_id: int, message: str = Form(...)):
    redirect = require_store_login(request, shop_id)
    if redirect:
        raise HTTPException(status_code=401, detail="ログインしてください")
    customer = get_customer_by_id(shop_id, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="顧客が見つかりません")
    active_member_id = get_latest_chat_member_id(shop_id, customer_id)
    linked_member = get_member_by_id(shop_id, active_member_id) if active_member_id else get_member_by_customer_id(shop_id, customer_id)
    subscription = get_shop_subscription(shop_id)
    chat_limit = _chat_limit_for_subscription(subscription)
    sent_count = count_shop_chat_messages_in_month(shop_id)
    if chat_limit is not None and sent_count >= chat_limit:
        return JSONResponse({"ok": False, "error": f"このプランではチャット送信は月{chat_limit}通までです。"}, status_code=400)
    try:
        created_message = create_chat_message(shop_id, customer_id, int(linked_member['id']) if linked_member else None, 'staff', message)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    _record_audit_log(
        request,
        actor_type="store_admin",
        actor_id=str(request.session.get("store_logged_in_login_id") or shop_id),
        actor_name=str(request.session.get("store_logged_in_admin_name") or ""),
        action="chat_send",
        shop_id=shop_id,
        target_type="customer",
        target_id=customer_id,
        target_label=str((customer or {}).get("name") or ""),
        detail={
            "sender_type": "staff",
            "chat_message_id": int(created_message.get("id") or 0),
            "customer_id": customer_id,
        },
    )
    mark_chat_messages_read_for_admin(shop_id, customer_id)
    messages = [_serialize_chat_message(item) for item in list_chat_messages(shop_id, customer_id, limit=200, member_id=active_member_id or (int(linked_member['id']) if linked_member else None))]
    new_sent_count = count_shop_chat_messages_in_month(shop_id)
    return JSONResponse({"ok": True, "messages": messages, "sent_this_month": new_sent_count, "remaining": None if chat_limit is None else max(chat_limit - new_sent_count, 0), "limit": chat_limit})


@app.get("/site/{shop_id}", response_class=HTMLResponse)
def site_page(request: Request, shop_id: str):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    homepage = get_shop_homepage_settings(shop_id) or {}
    sections = get_shop_homepage_sections(shop_id)
    subscription = get_shop_subscription(shop_id) or {}
    theme = {
        "primary": homepage.get("primary_color") or shop.get("primary_color") or "#2563eb",
        "background": homepage.get("background_color") or "#f8fafc",
        "surface": homepage.get("surface_color") or "#ffffff",
        "accent": homepage.get("accent_color") or shop.get("accent_bg") or "#dbeafe",
        "text": homepage.get("text_color") or "#111827",
        "subtext": homepage.get("subtext_color") or "#6b7280",
        "font_family": homepage.get("font_family") or "-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif",
    }
    requested_month = request.query_params.get("calendar_month")
    calendar_year, calendar_month = parse_month_string(requested_month)
    month_start = date(calendar_year, calendar_month, 1)
    prev_year, prev_month = shift_month(calendar_year, calendar_month, -1)
    next_year, next_month = shift_month(calendar_year, calendar_month, 1)
    holiday_weekday = WEEKDAY_MAP.get(shop.get("holiday"))
    return templates.TemplateResponse(
        request=request,
        name="site/home.html",
        context={
            "request": request,
            "shop": shop,
            "shop_id": shop_id,
            "homepage": homepage,
            "sections": sections,
            "theme": theme,
            "subscription": subscription,
            "calendar_days": build_public_calendar_days(calendar_year, calendar_month, holiday_weekday),
            "calendar_month_label": month_start.strftime("%Y年%m月"),
            "calendar_month_value": month_start.strftime("%Y-%m"),
            "calendar_prev_month": f"{prev_year:04d}-{prev_month:02d}",
            "calendar_next_month": f"{next_year:04d}-{next_month:02d}",
            "calendar_base_path": request.url.path,
            "line_official_url": (get_shop_line_settings(shop_id) or {}).get("line_official_url",""),
        },
    )


@app.get("/p/{public_path}", response_class=HTMLResponse)
def public_homepage(request: Request, public_path: str):
    homepage = get_shop_homepage_by_public_path(public_path)
    if not homepage:
        raise HTTPException(status_code=404, detail="公開ページが見つかりません")
    return site_page(request, homepage["shop_id"])


@app.get("/samples", response_class=HTMLResponse)
def sample_catalog_page(request: Request, category: str | None = None, q: str | None = None):
    categories = get_sample_categories()
    all_samples = get_all_samples()
    selected_category = (category or "").strip()
    keyword = (q or "").strip()

    def _matches(sample: dict) -> bool:
        if selected_category and sample.get("category_code") != selected_category:
            return False
        if not keyword:
            return True
        haystack = " ".join([
            str(sample.get("name") or ""),
            str(sample.get("lead") or ""),
            str(sample.get("summary") or ""),
            str(sample.get("variant") or ""),
            str(sample.get("category_name") or ""),
            str((sample.get("copy") or {}).get("hero_title") or ""),
            str((sample.get("copy") or {}).get("hero_text") or ""),
        ]).lower()
        for token in [part.lower() for part in keyword.split() if part.strip()]:
            if token not in haystack:
                return False
        return True

    samples = [s for s in all_samples if _matches(s)]
    grouped_counts = {}
    for s in all_samples:
        code = s.get("category_code")
        grouped_counts[code] = grouped_counts.get(code, 0) + 1
    current_admin_shop_id = request.session.get("admin_shop_id") or ""
    current_admin_shop = get_shop(current_admin_shop_id) if current_admin_shop_id else None
    return templates.TemplateResponse(
        request=request,
        name="platform/sample_catalog.html",
        context={
            "categories": categories,
            "samples": samples,
            "selected_category": selected_category,
            "grouped_counts": grouped_counts,
            "current_admin_shop_id": current_admin_shop_id,
            "current_admin_shop": current_admin_shop,
            "search_query": keyword,
        },
    )


@app.get("/samples/{category_code}/{sample_code}", response_class=HTMLResponse)
def sample_preview_page(request: Request, category_code: str, sample_code: str):
    sample = get_sample(category_code, sample_code)
    if not sample:
        raise HTTPException(status_code=404, detail="サンプルが見つかりません")
    template_name = sample.get("template_file") or "platform/sample_showcase.html"
    current_admin_shop_id = request.session.get("admin_shop_id") or ""
    current_admin_shop = get_shop(current_admin_shop_id) if current_admin_shop_id else None
    return templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "sample": sample,
            "current_admin_shop_id": current_admin_shop_id,
            "current_admin_shop": current_admin_shop,
            "return_to": str(request.url),
        },
    )



def _staff_allows_menu(staff: dict | None, menu_id: int | str | None) -> bool:
    if not staff:
        return False
    allowed_menu_ids = staff.get("menu_ids") or []
    if not allowed_menu_ids:
        return True
    try:
        normalized_menu_id = int(menu_id)
    except (TypeError, ValueError):
        return False
    normalized_allowed_ids: set[int] = set()
    for item in allowed_menu_ids:
        try:
            normalized_allowed_ids.add(int(item))
        except (TypeError, ValueError):
            continue
    return normalized_menu_id in normalized_allowed_ids


@app.get("/admin/api/shops")
def audit_api_shops(request: Request):
    _require_audit_api_token(request)
    shops = []
    for shop in get_all_shops_for_platform():
        shops.append({
            "shop_id": str(shop.get("shop_id") or ""),
            "shop_name": str(shop.get("shop_name") or ""),
        })
    return JSONResponse({"items": shops})


@app.get("/admin/api/members")
def audit_api_members(request: Request, shop_id: str):
    _require_audit_api_token(request)
    normalized_shop_id = (shop_id or "").strip().lower()
    if not normalized_shop_id:
        raise HTTPException(status_code=400, detail="shop_id は必須です")
    members = []
    for member in list_members_for_audit_api(normalized_shop_id):
        members.append({
            "member_id": int(member.get("id") or 0),
            "shop_id": str(member.get("shop_id") or normalized_shop_id),
            "name": str(member.get("name") or ""),
            "phone": str(member.get("phone") or ""),
            "email": str(member.get("email") or ""),
            "is_active": int(member.get("is_active") or 0),
            "created_at": str(member.get("created_at") or ""),
            "updated_at": str(member.get("updated_at") or ""),
        })
    return JSONResponse({"items": members})


@app.get("/admin/api/audit-logs")
def audit_api_logs(
    request: Request,
    shop_id: str = "",
    member_id: int | None = None,
    from_: str = "",
    to: str = "",
    limit: int = 500,
    offset: int = 0,
):
    _require_audit_api_token(request)
    normalized_shop_id = (shop_id or "").strip().lower()
    date_from = _normalize_api_datetime(from_, end_of_day=False)
    date_to = _normalize_api_datetime(to, end_of_day=True)
    rows = list_audit_logs_for_api(
        shop_id=normalized_shop_id,
        member_id=member_id,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )
    return JSONResponse(
        {
            "items": [_serialize_audit_log_row(row) for row in rows],
            "filters": {
                "shop_id": normalized_shop_id,
                "member_id": member_id,
                "from": date_from,
                "to": date_to,
                "limit": max(1, min(int(limit or 500), 1000)),
                "offset": max(0, int(offset or 0)),
            },
        }
    )


@app.get("/admin/api/audit-log-chat-detail")
def audit_api_chat_detail(
    request: Request,
    shop_id: str,
    customer_id: int,
    chat_message_id: int,
):
    _require_audit_api_token(request)
    normalized_shop_id = (shop_id or "").strip().lower()
    if not normalized_shop_id:
        raise HTTPException(status_code=400, detail="shop_id は必須です")
    if int(customer_id or 0) <= 0:
        raise HTTPException(status_code=400, detail="customer_id は必須です")
    if int(chat_message_id or 0) <= 0:
        raise HTTPException(status_code=400, detail="chat_message_id は必須です")

    messages = list_chat_messages(normalized_shop_id, int(customer_id), limit=500)
    target = next((item for item in messages if int(item.get("id") or 0) == int(chat_message_id)), None)
    if target is None:
        raise HTTPException(status_code=404, detail="チャットメッセージが見つかりません")

    return JSONResponse(
        {
            "item": {
                "id": int(target.get("id") or 0),
                "shop_id": str(target.get("shop_id") or normalized_shop_id),
                "customer_id": int(target.get("customer_id") or 0),
                "member_id": int(target.get("member_id") or 0) if target.get("member_id") is not None else None,
                "sender_type": str(target.get("sender_type") or ""),
                "body": str(target.get("body") or ""),
                "is_read": int(target.get("is_read") or 0),
                "created_at": _format_chat_datetime(target.get("created_at")),
            }
        }
    )


@app.get("/admin/api/shop-detail")
def audit_api_shop_detail(request: Request, shop_id: str):
    _require_audit_api_token(request)
    detail = get_shop_detail_for_audit_api(shop_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    return JSONResponse({"item": detail})


@app.get("/admin/api/member-detail")
def audit_api_member_detail(request: Request, shop_id: str, member_id: int):
    _require_audit_api_token(request)
    detail = get_member_detail_for_audit_api(shop_id, member_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="会員が見つかりません")
    return JSONResponse({"item": detail})


@app.post("/admin/api/shop-update")
async def audit_api_shop_update(request: Request):
    _require_audit_api_token(request)
    payload = await request.json()
    shop_id = _pick(payload, "shop_id").lower()
    if not shop_id:
        raise HTTPException(status_code=400, detail="shop_id は必須です")
    before = get_shop_detail_for_audit_api(shop_id)
    if before is None:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    try:
        after = update_shop_for_audit_api(
            shop_id,
            shop_name=_pick(payload, "shop_name", str(before.get("shop_name") or "")),
            phone=_pick(payload, "phone", str(before.get("phone") or "")),
            address=_pick(payload, "address", str(before.get("address") or "")),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _record_audit_log(
        request,
        actor_type="audit_tool",
        actor_id="desktop",
        actor_name="desktop_app",
        action="shop_update",
        shop_id=shop_id,
        target_type="shop",
        target_id=shop_id,
        target_label=str(after.get("shop_name") or shop_id),
        status="success",
        detail={"before": before, "after": after},
    )
    return JSONResponse({"item": after})


@app.post("/admin/api/member-update")
async def audit_api_member_update(request: Request):
    _require_audit_api_token(request)
    payload = await request.json()
    shop_id = _pick(payload, "shop_id").lower()
    member_id_raw = _pick(payload, "member_id")
    if not shop_id or not member_id_raw:
        raise HTTPException(status_code=400, detail="shop_id と member_id は必須です")
    member_id = int(member_id_raw)
    before = get_member_detail_for_audit_api(shop_id, member_id)
    if before is None:
        raise HTTPException(status_code=404, detail="会員が見つかりません")
    try:
        after = update_member_for_audit_api(
            shop_id,
            member_id,
            name=_pick(payload, "name", str(before.get("name") or "")),
            phone=_pick(payload, "phone", str(before.get("phone") or "")),
            email=_pick(payload, "email", str(before.get("email") or "")),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _record_audit_log(
        request,
        actor_type="audit_tool",
        actor_id="desktop",
        actor_name="desktop_app",
        action="member_update",
        shop_id=shop_id,
        target_type="member",
        target_id=str(member_id),
        target_label=str(after.get("name") or member_id),
        status="success",
        detail={"before": before, "after": after},
    )
    return JSONResponse({"item": after})




@app.post("/admin/api/restore")
async def audit_api_restore(request: Request):
    _require_audit_api_token(request)
    payload = await request.json()
    target_type = _pick(payload, "target_type").lower()
    shop_id = _pick(payload, "shop_id").lower()
    admin_password = _pick(payload, "admin_password")
    reason = _pick(payload, "reason")
    _require_audit_admin_password(admin_password)
    if target_type == "shop":
        before = get_shop_detail_for_audit_api(shop_id)
        if before is None:
            raise HTTPException(status_code=404, detail="店舗が見つかりません")
        after = restore_shop_for_audit_api(shop_id)
        _record_audit_log(
            request,
            actor_type="audit_tool",
            actor_id="desktop",
            actor_name="desktop_app",
            action="shop_restore",
            shop_id=shop_id,
            target_type="shop",
            target_id=shop_id,
            target_label=str(after.get("shop_name") or shop_id),
            status="success",
            detail={"reason": reason, "before": before, "after": after, "password_reauth": True},
        )
        return JSONResponse({"item": after})
    if target_type == "member":
        member_id_raw = _pick(payload, "member_id")
        if not member_id_raw:
            raise HTTPException(status_code=400, detail="member_id は必須です")
        member_id = int(member_id_raw)
        before = get_member_detail_for_audit_api(shop_id, member_id)
        if before is None:
            raise HTTPException(status_code=404, detail="会員が見つかりません")
        after = restore_member_for_audit_api(shop_id, member_id)
        _record_audit_log(
            request,
            actor_type="audit_tool",
            actor_id="desktop",
            actor_name="desktop_app",
            action="member_restore",
            shop_id=shop_id,
            target_type="member",
            target_id=str(member_id),
            target_label=str(after.get("name") or member_id),
            status="success",
            detail={"reason": reason, "before": before, "after": after, "password_reauth": True},
        )
        return JSONResponse({"item": after})
    raise HTTPException(status_code=400, detail="target_type は shop または member を指定してください")


@app.post("/admin/api/force-cancel")
async def audit_api_force_cancel(request: Request):
    _require_audit_api_token(request)
    payload = await request.json()
    target_type = _pick(payload, "target_type").lower()
    shop_id = _pick(payload, "shop_id").lower()
    admin_password = _pick(payload, "admin_password")
    reason = _pick(payload, "reason")
    _require_audit_admin_password(admin_password)
    if target_type == "shop":
        before = get_shop_detail_for_audit_api(shop_id)
        if before is None:
            raise HTTPException(status_code=404, detail="店舗が見つかりません")
        after = force_cancel_shop_for_audit_api(shop_id)
        _record_audit_log(
            request,
            actor_type="audit_tool",
            actor_id="desktop",
            actor_name="desktop_app",
            action="shop_force_cancel",
            shop_id=shop_id,
            target_type="shop",
            target_id=shop_id,
            target_label=str(after.get("shop_name") or shop_id),
            status="success",
            detail={"reason": reason, "before": before, "after": after, "password_reauth": True},
        )
        return JSONResponse({"item": after})
    if target_type == "member":
        member_id_raw = _pick(payload, "member_id")
        if not member_id_raw:
            raise HTTPException(status_code=400, detail="member_id は必須です")
        member_id = int(member_id_raw)
        before = get_member_detail_for_audit_api(shop_id, member_id)
        if before is None:
            raise HTTPException(status_code=404, detail="会員が見つかりません")
        after = force_cancel_member_for_audit_api(shop_id, member_id)
        _record_audit_log(
            request,
            actor_type="audit_tool",
            actor_id="desktop",
            actor_name="desktop_app",
            action="member_force_cancel",
            shop_id=shop_id,
            target_type="member",
            target_id=str(member_id),
            target_label=str(after.get("name") or member_id),
            status="success",
            detail={"reason": reason, "before": before, "after": after, "password_reauth": True},
        )
        return JSONResponse({"item": after})
    raise HTTPException(status_code=400, detail="target_type は shop または member を指定してください")

async def line_settings(request: Request, shop_id: str):
    with get_connection() as conn:
        shop = conn.execute(
            "SELECT * FROM shops WHERE shop_id = ? LIMIT 1",
            (shop_id,)
        ).fetchone()

    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

    return templates.TemplateResponse("admin/line_settings.html", {
        "request": request,
        "shop": shop,
        "shop_id": shop_id,
    })


async def save_line_settings(request: Request, shop_id: str):
    form = await request.form()

    with get_connection() as conn:
        shop = conn.execute(
            "SELECT shop_id FROM shops WHERE shop_id = ? LIMIT 1",
            (shop_id,)
        ).fetchone()

        if not shop:
            raise HTTPException(status_code=404, detail="店舗が見つかりません")

        conn.execute("""
            UPDATE shops SET
                line_mode = ?,
                line_channel_access_token = ?,
                line_channel_secret = ?,
                line_liff_id = ?,
                line_official_url = ?,
                line_webhook_enabled = ?
            WHERE shop_id = ?
        """, (
            form.get("line_mode") or "off",
            form.get("line_channel_access_token") or "",
            form.get("line_channel_secret") or "",
            form.get("line_liff_id") or "",
            form.get("line_official_url") or "",
            1 if form.get("line_webhook_enabled") else 0,
            shop_id,
        ))
        conn.commit()

    return RedirectResponse(f"/admin/{shop_id}/line-settings", status_code=303)

