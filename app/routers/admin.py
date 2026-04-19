from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.db import (
    create_admin_password_reset_token,
    find_admin_login_id_by_shop_email,
    get_shop,
    get_valid_admin_password_reset_token,
    mark_admin_password_reset_token_used,
    update_admin_user_password,
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/forgot-id", response_class=HTMLResponse)
def forgot_id_page(request: Request):
    return templates.TemplateResponse(
        "admin/forgot_id.html",
        {"request": request, "message": None, "login_id": None, "shop_id": "", "email": ""},
    )


@router.post("/forgot-id", response_class=HTMLResponse)
def forgot_id_submit(
    request: Request,
    shop_id: str = Form(...),
    email: str = Form(...),
):
    login_id = find_admin_login_id_by_shop_email(shop_id, email)
    message = "入力内容を確認しました。"
    return templates.TemplateResponse(
        "admin/forgot_id.html",
        {
            "request": request,
            "message": message,
            "login_id": login_id,
            "shop_id": shop_id,
            "email": email,
        },
    )


@router.get("/forgot-password", response_class=HTMLResponse)
def forgot_password_page(request: Request):
    return templates.TemplateResponse(
        "admin/forgot_password.html",
        {"request": request, "message": None, "debug_reset_url": None, "shop_id": "", "login_id": "", "email": ""},
    )


@router.post("/forgot-password", response_class=HTMLResponse)
def forgot_password_submit(
    request: Request,
    shop_id: str = Form(...),
    login_id: str = Form(...),
    email: str = Form(...),
):
    debug_reset_url = None
    shop = get_shop(shop_id)
    registered_email = str((shop or {}).get("reply_to_email") or "").strip().lower()
    if shop and registered_email and registered_email == email.strip().lower():
        token = create_admin_password_reset_token(shop_id, login_id)
        debug_reset_url = f"/admin/reset-password?token={token}"

    return templates.TemplateResponse(
        "admin/forgot_password.html",
        {
            "request": request,
            "message": "送信処理を受け付けました。",
            "debug_reset_url": debug_reset_url,
            "shop_id": shop_id,
            "login_id": login_id,
            "email": email,
        },
    )


@router.get("/reset-password", response_class=HTMLResponse)
def reset_password_page(request: Request, token: str):
    token_row = get_valid_admin_password_reset_token(token)
    if token_row is None:
        return templates.TemplateResponse(
            "admin/reset_password.html",
            {"request": request, "token": "", "error_message": "無効または期限切れのリンクです。", "message": None},
            status_code=400,
        )
    return templates.TemplateResponse(
        "admin/reset_password.html",
        {"request": request, "token": token, "error_message": None, "message": None},
    )


@router.post("/reset-password", response_class=HTMLResponse)
def reset_password_submit(
    request: Request,
    token: str = Form(...),
    password: str = Form(...),
    password_confirm: str = Form(...),
):
    if password != password_confirm:
        return templates.TemplateResponse(
            "admin/reset_password.html",
            {"request": request, "token": token, "error_message": "確認用パスワードが一致しません。", "message": None},
            status_code=400,
        )
    if len(password) < 6:
        return templates.TemplateResponse(
            "admin/reset_password.html",
            {"request": request, "token": token, "error_message": "パスワードは6文字以上で入力してください。", "message": None},
            status_code=400,
        )

    token_row = get_valid_admin_password_reset_token(token)
    if token_row is None:
        return templates.TemplateResponse(
            "admin/reset_password.html",
            {"request": request, "token": "", "error_message": "無効または期限切れのリンクです。", "message": None},
            status_code=400,
        )

    ok = update_admin_user_password(str(token_row["shop_id"]), str(token_row["login_id"]), password)
    if not ok:
        return templates.TemplateResponse(
            "admin/reset_password.html",
            {"request": request, "token": token, "error_message": "パスワード更新に失敗しました。", "message": None},
            status_code=400,
        )

    mark_admin_password_reset_token_used(token)
    return templates.TemplateResponse(
        "admin/reset_password.html",
        {"request": request, "token": "", "error_message": None, "message": "パスワードを更新しました。"},
    )
