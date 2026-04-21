from __future__ import annotations

from dotenv import load_dotenv

import calendar
from datetime import date, datetime, timedelta, timezone
import os
import smtplib
from urllib.parse import urlparse

import boto3
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
import secrets
import re
import unicodedata
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
    init_db,
    get_shop,
    get_all_shops_for_platform,
    get_shop_management_data,
    get_plans,
    update_shop_basic_info,
    update_shop_staff_list,
    update_shop_subscription,
    create_shop_with_owner,
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
)
from app.runtime_data import (
    get_platform_admin,
    get_sample_categories,
    get_all_samples,
    get_sample,
)
from app.routers.admin import router as admin_router
from app.routers import admin_patch


Path("data/uploads/shops").mkdir(parents=True, exist_ok=True)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="reservation-app-secret")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/uploads", StaticFiles(directory="data/uploads"), name="uploads")

templates = Jinja2Templates(directory="app/templates")
templates.env.globals["is_premium_subscription"] = lambda subscription: _is_premium_subscription(subscription)
PLATFORM_ADMIN = get_platform_admin()
JST = ZoneInfo("Asia/Tokyo")


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
    shop_data["staff_list"] = _get_visible_staff_list(shop_data, subscription)
    return shop_data


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
    bucket = _first_env("S3_BUCKET", "AWS_S3_BUCKET", "AWS_BUCKET", "BUCKET_NAME", "S3_BUCKET_NAME") or "reserve-site-images-001"
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
        s3.put_object(Bucket=bucket, Key=key, Body=file_bytes, **extra_args)

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
        })
        seen_staff_keys.add(staff_key)

    for staff_key, meta in reservation_staff_meta.items():
        if staff_key in seen_staff_keys:
            continue
        merged_staff_list.append({
            "id": meta.get("id"),
            "name": meta.get("name"),
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
        'error_message': error_message,
    }


@app.on_event("startup")
def startup():
    init_db()


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
        return RedirectResponse("/platform/shops", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="platform/login.html",
        context={"error_message": "ログインIDまたはパスワードが違います。"},
        status_code=400,
    )


@app.post("/platform/logout")
def platform_logout(request: Request):
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
    if plan_id:
        update_shop_subscription(shop_id, plan_id=plan_id, status=subscription_status)
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
def signup_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="shop_signup.html",
        context={"shops": get_all_shops_for_platform(), "error_message": ""},
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
    shop_id = (login_id or "").strip().lower()
    shop_name = (shop_name or "").strip()
    owner_name = (owner_name or "").strip()
    phone = (phone or "").strip()
    email = (email or "").strip()
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

    try:
        create_shop_with_owner(
            shop_id=shop_id,
            shop_name=shop_name,
            owner_name=owner_name,
            login_id=login_id,
            password=password,
            phone=phone,
            reply_to_email=email,
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
    update_customer(shop_id, customer_id, name, (phone or "").strip(), (email or "").strip().lower())
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
    add_customer_note(shop_id, customer_id, title, content)
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}?saved=追加情報を保存しました", status_code=303)


@app.post("/admin/{shop_id}/customers/{customer_id}/notes/{note_id}/delete")
def admin_delete_customer_note_route(request: Request, shop_id: str, customer_id: int, note_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    delete_customer_note(shop_id, customer_id, note_id)
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
    add_customer_photo(shop_id, customer_id, image_url)
    return RedirectResponse(f"/admin/{shop_id}/customers/{customer_id}?saved=写真を保存しました", status_code=303)


@app.post("/admin/{shop_id}/customers/{customer_id}/photos/{photo_id}/delete")
def admin_delete_customer_photo_route(request: Request, shop_id: str, customer_id: int, photo_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    deleted = delete_customer_photo(shop_id, customer_id, photo_id)
    if deleted is not None:
        _delete_customer_photo_file(str(deleted.get("image_url") or ""))
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
    request.session.pop("store_logged_in_shop_id", None)
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
    create_customer(shop_id, name, phone, email)
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
    create_reservation(
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
    return RedirectResponse(f"/admin/{shop_id}", status_code=303)


@app.post("/admin/{shop_id}/reservations/{reservation_id}/done")
def admin_mark_reservation_done(request: Request, shop_id: str, reservation_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    update_reservation_status(shop_id, reservation_id, "来店済み")
    return RedirectResponse(f"/admin/{shop_id}", status_code=303)


@app.post("/admin/{shop_id}/reservations/{reservation_id}/cancel")
def admin_mark_reservation_cancel(request: Request, shop_id: str, reservation_id: int):
    redirect = require_store_login(request, shop_id)
    if redirect:
        return redirect
    update_reservation_status(shop_id, reservation_id, "キャンセル")
    return RedirectResponse(f"/admin/{shop_id}", status_code=303)


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
    return JSONResponse({"ok": True})


@app.get("/shop/{shop_id}", response_class=HTMLResponse)
def shop_page(request: Request, shop_id: str):
    context = build_shop_booking_context(shop_id, request)
    return templates.TemplateResponse(
        request=request,
        name="shop/index.html",
        context=context,
    )


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
    if email and receive_email == '1':
        _send_reservation_mail(to_email=email, shop=shop, reservation_date=reservation_date, start_time=start_time, reply_to_email=str(shop.get('reply_to_email') or ''))
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

    staff_list = list(shop.get("staff_list", []))
    updated = False
    for index, staff in enumerate(staff_list):
        if int(staff.get("id") or 0) != int(staff_id):
            continue
        staff_list[index] = {
            **staff,
            "name": staff_name,
            "menu_ids": normalized_menu_ids,
            "holiday_dates": normalized_holiday_dates,
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
    staff_list, target_staff = _get_visible_staff_or_404(shop, subscription, staff_id)
    shop = _build_shop_with_visible_staff(shop, subscription)

    reservations = get_reservations(normalized_shop_id)
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
        target = quote(_member_redirect_target(shop_id, next_url), safe='')
        return RedirectResponse(url=f"/member/{shop_id}/login?next={target}&error=電話番号またはパスワードが違います", status_code=303)
    _login_member_session(request, shop_id, member)
    return RedirectResponse(url=_member_redirect_target(shop_id, next_url or f"/member/{shop_id}/mypage"), status_code=303)


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
    except ValueError as exc:
        return RedirectResponse(
            url=f"/member/{shop_id}/register/verify?token={quote(token, safe='')}&next={verify_target}&error={quote(str(exc), safe='')}",
            status_code=303,
        )

    _login_member_session(request, shop_id, member)
    return RedirectResponse(url=_member_redirect_target(shop_id, next_url or f"/member/{shop_id}/mypage"), status_code=303)


@app.get("/member/{shop_id}/logout")
def member_logout(request: Request, shop_id: str):
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
        create_chat_message(shop_id, customer_id, int(member['id']), 'member', message)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
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
        create_chat_message(shop_id, customer_id, int(linked_member['id']) if linked_member else None, 'staff', message)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
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


