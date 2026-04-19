from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.db import (
    create_admin_password_reset_token,
    get_shop,
    get_owner_admin_user,
    get_valid_admin_password_reset_token,
    mark_admin_password_reset_token_used,
    update_admin_user_password,
    get_all_shops,
)

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/forgot-id", response_class=HTMLResponse)
def forgot_id_page(request: Request):
    return templates.TemplateResponse(
        "admin/forgot_id.html",
        {
            "request": request,
            "message": None,
            "error_message": None,
            "login_id": None,
            "email": "",
            "phone": "",
        },
    )


@router.post("/forgot-id", response_class=HTMLResponse)
def forgot_id_submit(
    request: Request,
    email: str = Form(...),
    phone: str = Form(...),
):
    normalized_email = (email or "").strip().lower()
    normalized_phone = (phone or "").strip()

    login_id = None
    for shop in get_all_shops():
        shop_email = str(shop.get("reply_to_email") or "").strip().lower()
        shop_phone = str(shop.get("phone") or "").strip()
        if shop_email == normalized_email and shop_phone == normalized_phone:
            owner = get_owner_admin_user(str(shop.get("shop_id") or ""))
            if owner:
                login_id = str(owner.get("login_id") or "")
                break

    if not login_id:
        return templates.TemplateResponse(
            "admin/forgot_id.html",
            {
                "request": request,
                "message": None,
                "error_message": "登録メールアドレスまたは登録電話番号が正しくありません。",
                "login_id": None,
                "email": email,
                "phone": phone,
            },
            status_code=400,
        )

    return templates.TemplateResponse(
        "admin/forgot_id.html",
        {
            "request": request,
            "message": "確認しました",
            "error_message": None,
            "login_id": login_id,
            "email": email,
            "phone": phone,
        },
    )


@router.get("/forgot-password", response_class=HTMLResponse)
def forgot_password_page(request: Request):
    return templates.TemplateResponse(
        "admin/forgot_password.html",
        {
            "request": request,
            "message": None,
            "error_message": None,
            "debug_reset_url": None,
            "shop_id": "",
            "email": "",
        },
    )


@router.post("/forgot-password", response_class=HTMLResponse)
def forgot_password_submit(
    request: Request,
    shop_id: str = Form(...),
    email: str = Form(...),
):
    normalized_shop_id = (shop_id or "").strip().lower()
    normalized_email = (email or "").strip().lower()

    shop = get_shop(normalized_shop_id)
    owner = get_owner_admin_user(normalized_shop_id) if shop else None
    shop_email = str((shop or {}).get("reply_to_email") or "").strip().lower()

    if not shop or not owner or shop_email != normalized_email:
        return templates.TemplateResponse(
            "admin/forgot_password.html",
            {
                "request": request,
                "message": None,
                "error_message": "ログインID（店舗ID）または登録メールアドレスが正しくありません。",
                "debug_reset_url": None,
                "shop_id": shop_id,
                "email": email,
            },
            status_code=400,
        )

    token = create_admin_password_reset_token(
        normalized_shop_id,
        str(owner.get("login_id") or ""),
    )
    debug_reset_url = f"/admin/reset-password?token={token}"

    return templates.TemplateResponse(
        "admin/forgot_password.html",
        {
            "request": request,
            "message": "送信しました",
            "error_message": None,
            "debug_reset_url": debug_reset_url,
            "shop_id": shop_id,
            "email": email,
        },
    )


@router.get("/reset-password", response_class=HTMLResponse)
def reset_password_page(request: Request, token: str):
    token_data = get_valid_admin_password_reset_token(token)

    if not token_data:
        return templates.TemplateResponse(
            "admin/reset_password.html",
            {"request": request, "error": "無効なトークン"},
            status_code=400,
        )

    return templates.TemplateResponse(
        "admin/reset_password.html",
        {"request": request, "token": token},
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
            {"request": request, "error": "パスワード不一致", "token": token},
            status_code=400,
        )

    token_data = get_valid_admin_password_reset_token(token)
    if not token_data:
        return templates.TemplateResponse(
            "admin/reset_password.html",
            {"request": request, "error": "無効なトークン"},
            status_code=400,
        )

    update_admin_user_password(
        token_data["shop_id"],
        token_data["login_id"],
        password,
    )
    mark_admin_password_reset_token_used(token)

    return templates.TemplateResponse(
        "admin/reset_password.html",
        {"request": request, "message": "更新完了"},
    )
