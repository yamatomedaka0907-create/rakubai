from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import calendar
from datetime import date, datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
import secrets
import re

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
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
    update_reservation_status,
    authenticate_admin_user,
    get_admin_users,
    get_shop_subscription,
    get_system_mail_settings,
    update_system_mail_settings,
    get_shop_homepage_settings,
    get_shop_homepage_sections,
    get_shop_homepage_by_public_path,
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
PLATFORM_ADMIN = get_platform_admin()


WEEKDAY_MAP = {"月曜日": 0, "火曜日": 1, "水曜日": 2, "木曜日": 3, "金曜日": 4, "土曜日": 5, "日曜日": 6}


def _get_customer_photo_policy(subscription: dict | None) -> dict:
    label = str((subscription or {}).get("plan_name") or "現在のプラン")
    return {"enabled": True, "label": label, "max_photos": None}


def _save_customer_photo_file(shop_id: str, customer_id: int, upload: UploadFile) -> str:
    suffix = Path(upload.filename or "photo.jpg").suffix or ".jpg"
    customer_dir = Path("data/uploads/shops") / shop_id / "customers" / str(customer_id)
    customer_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{secrets.token_hex(4)}{suffix.lower()}"
    save_path = customer_dir / filename
    with save_path.open("wb") as fh:
        fh.write(upload.file.read())
    relative = save_path.relative_to(Path("data"))
    return "/uploads/" + str(relative).replace("\\", "/")


def _delete_local_upload_from_url(image_url: str) -> None:
    if not image_url:
        return
    cleaned = image_url.split("?", 1)[0]
    if not cleaned.startswith("/uploads/"):
        return
    target = Path("data") / cleaned.lstrip("/")
    try:
        if target.exists():
            target.unlink()
    except OSError:
        pass


def _safe_parse_date(value: str | None, fallback: date | None = None) -> date:
    if value:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            pass
    return fallback or date.today()


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


def _build_week_availability_matrix(*, shop: dict, reservations: list[dict], selected_date: date) -> tuple[list[dict], list[str], list[dict]]:
    start_of_week = selected_date - timedelta(days=((selected_date.weekday() + 1) % 7))
    holiday_idx = WEEKDAY_MAP.get(str(shop.get('holiday') or ''))
    week_days: list[dict] = []
    for offset in range(7):
        current_day = start_of_week + timedelta(days=offset)
        week_days.append({
            'date': current_day.isoformat(),
            'label': f"{current_day.month}/{current_day.day}",
            'weekday': '日月火水木金土'[offset],
            'is_today': current_day == date.today(),
            'is_holiday': holiday_idx is not None and current_day.weekday() == holiday_idx,
        })

    time_slots = _build_half_hour_slots(shop.get('business_hours'))
    active_reservations = [r for r in reservations if str(r.get('status') or '') != 'キャンセル']
    reserved_keys: set[tuple[str, str]] = set()
    for item in active_reservations:
        reservation_date = str(item.get('reservation_date') or '')
        start_time = str(item.get('start_time') or '')[:5]
        if reservation_date and start_time:
            reserved_keys.add((reservation_date, start_time))

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
            })
        weekly_rows.append({'time': slot, 'cells': cells})

    return week_days, time_slots, weekly_rows

def _send_reservation_mail(*, to_email: str, shop: dict, reservation_date: str, start_time: str, reply_to_email: str = '') -> None:
    settings = get_system_mail_settings()
    smtp_user = settings.get('smtp_username') or settings.get('from_email')
    smtp_password = settings.get('smtp_password')
    from_email = settings.get('from_email')
    if not to_email or not from_email or not smtp_user or not smtp_password:
        return

    subject = f"【{shop.get('shop_name', '店舗')}】ご予約を受け付けました"
    body = f"""{shop.get('shop_name', '店舗')} のご予約ありがとうございます。

店舗名: {shop.get('shop_name', '')}
ご予約日時: {reservation_date} {start_time}

ご来店をお待ちしております。"""
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = formataddr((settings.get('from_name') or '予約システム', from_email))
    msg['To'] = to_email
    if reply_to_email:
        msg['Reply-To'] = reply_to_email
    try:
        with smtplib.SMTP(settings.get('smtp_host') or 'smtp.gmail.com', int(settings.get('smtp_port') or '587')) as smtp:
            smtp.starttls()
            smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
    except Exception:
        pass


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
    logged_in_shop_id = str(request.session.get("store_logged_in_shop_id") or "").strip().lower()
    if logged_in_shop_id != (shop_id or "").strip().lower():
        return RedirectResponse("/store-login", status_code=303)
    return None


def build_shop_booking_context(shop_id: str, request: Request, error_message: str = ""):
    shop = get_shop(shop_id)
    if shop is None:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")

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

    holiday_idx = WEEKDAY_MAP.get(str(shop.get("holiday") or ""))
    today = date.today()
    reservations = [r for r in get_reservations(shop_id) if str(r.get('status')) != 'キャンセル']

    cal = calendar.Calendar(firstweekday=6)
    days = []
    for week in cal.monthdatescalendar(current_month.year, current_month.month):
        for day in week:
            is_current = day.month == current_month.month
            is_closed = holiday_idx is not None and day.weekday() == holiday_idx
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

    selected_menu = next((m for m in shop.get('menus', []) if str(m.get('id')) == str(selected_menu_id)), None)
    selected_staff = next((s for s in shop.get('staff_list', []) if str(s.get('id')) == str(selected_staff_id)), None)

    available_slots = []
    selected_slot = None
    if selected_menu and selected_staff:
        duration = int(selected_menu.get('duration', 60) or 60)
        current_dt = datetime.now()
        selected_reservations = [r for r in reservations if r.get('reservation_date') == selected_date and str(r.get('staff_id')) == str(selected_staff_id)]
        for start_time in _build_time_slots():
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
                'is_available': not slot_is_past and not slot_is_conflict,
                'is_conflict': slot_is_conflict,
            }
            if selected_start_time == start_time:
                selected_slot = slot
            available_slots.append(slot)

    form_data = {
        'customer_name': request.query_params.get('customer_name', ''),
        'phone': request.query_params.get('phone', ''),
        'email': request.query_params.get('email', ''),
        'receive_email': request.query_params.get('receive_email', '1'),
    }

    prev_month = (current_month.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m')
    next_month = (current_month.replace(day=28) + timedelta(days=4)).replace(day=1).strftime('%Y-%m')

    return {
        'request': request,
        'shop': shop,
        'shop_id': shop_id,
        'subscription': {'status': 'active', 'plan_name': 'Free', 'show_ads': False},
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
        'selected_date_is_holiday': (selected_date_obj.weekday() == holiday_idx) if holiday_idx is not None else False,
        'form_data': form_data,
        'error_message': error_message,
    }


@app.on_event("startup")
def startup():
    init_db()


@app.get("/", response_class=HTMLResponse)
def top_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="top.html",
        context={"request": request},
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
    success_message = request.query_params.get("saved", "")
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
            "current_admin_name": request.session.get("store_logged_in_admin_name") or (admin_users[0].get("name") if admin_users else ""),
            "active_page": "settings",
            "success_message": success_message,
            "message": success_message,
        },
    )


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
    )
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
            "available_plans": available_plans,
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
    selected_date = _safe_parse_date(filter_date, date.today()).isoformat()
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
        selected_date=_safe_parse_date(selected_date, today_obj),
    )
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
            "error_message": error_message,
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
    customer_items = customers
    if keyword:
        lowered = keyword.lower()
        customer_items = [
            item for item in customers
            if lowered in str(item.get("name") or "").lower()
            or lowered in str(item.get("phone") or "").lower()
            or lowered in str(item.get("email") or "").lower()
        ]
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
            "keyword": keyword,
            "today": date.today().isoformat(),
            "reservations": reservations,
            "admin_users": admin_users,
            "subscription": subscription,
            "available_plans": available_plans,
            "current_admin_name": current_admin_name,
            "error_message": error_message,
            "success_message": request.query_params.get("saved", ""),
            "active_page": "customers",
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
    customer_photos = get_customer_photos(shop_id, customer_id)
    customer_photo_policy = _get_customer_photo_policy(subscription)
    max_photos = customer_photo_policy.get("max_photos")
    customer_photo_remaining = None if max_photos is None else max(int(max_photos) - len(customer_photos), 0)
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
        _delete_local_upload_from_url(str(photo.get("image_url") or ""))
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
        _delete_local_upload_from_url(str(deleted.get("image_url") or ""))
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
    normalized_status = (status or "").strip().lower()
    if normalized_status not in {"active", "trial", "canceled"}:
        return admin_page(request, shop_id, error_message="契約状態の指定が正しくありません。")
    update_shop_subscription((shop_id or "").strip().lower(), plan_id, normalized_status)
    return RedirectResponse(f"/admin/{(shop_id or '').strip().lower()}", status_code=303)


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
):
    url = f"/shop/{shop_id}?reservation_date={reservation_date}&start_time={start_time}&customer_name={customer_name}&phone={phone}&email={email}&receive_email={receive_email}&staff_id={staff_id}&menu_id={menu_id}#reserve-form"
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


@app.get("/site/{shop_id}", response_class=HTMLResponse)
def site_page(request: Request, shop_id: str):
    shop = get_shop(shop_id)
    if not shop:
        raise HTTPException(status_code=404, detail="店舗が見つかりません")
    homepage = get_shop_homepage_settings(shop_id) or {}
    sections = get_shop_homepage_sections(shop_id)
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


