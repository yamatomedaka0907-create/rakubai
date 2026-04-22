from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.runtime_data import get_shops

SHOPS = get_shops()

DEFAULT_PLANS = [
    {
        'code': 'free',
        'name': 'フリー',
        'monthly_price': 0,
        'show_ads': 1,
        'max_staff': 3,
        'max_customers': 50,
        'max_reservations_per_month': 50,
        'can_use_line': 1,
        'can_use_reports': 0,
        'is_active': 1,
        'sort_order': 10,
    },
    {
        'code': 'basic',
        'name': 'ベーシック',
        'monthly_price': 4980,
        'show_ads': 0,
        'max_staff': 5,
        'max_customers': 500,
        'max_reservations_per_month': 300,
        'can_use_line': 1,
        'can_use_reports': 1,
        'is_active': 1,
        'sort_order': 20,
    },
    {
        'code': 'standard',
        'name': 'スタンダード',
        'monthly_price': 9800,
        'show_ads': 0,
        'max_staff': 15,
        'max_customers': 3000,
        'max_reservations_per_month': 2000,
        'can_use_line': 1,
        'can_use_reports': 1,
        'is_active': 1,
        'sort_order': 30,
    },
    {
        'code': 'pro',
        'name': 'プロ',
        'monthly_price': 19800,
        'show_ads': 0,
        'max_staff': 999,
        'max_customers': 999999,
        'max_reservations_per_month': 999999,
        'can_use_line': 1,
        'can_use_reports': 1,
        'is_active': 1,
        'sort_order': 40,
    },
]

DEFAULT_HOMEPAGE_TEMPLATES = [
    {'code': 'clean-light', 'name': 'Clean Light', 'description': '白基調でシンプルな定番テンプレート', 'theme_json': json.dumps({'preset_key':'clean-light','hero_style': 'classic', 'section_style': 'soft', 'button_style': 'solid', 'surface': '#ffffff', 'background': '#f7f7fb', 'primary': '#1f6feb', 'accent': '#dbeafe', 'text': '#111827', 'subtext': '#6b7280'})},
    {'code': 'salon-soft', 'name': 'Salon Soft', 'description': 'やわらかい配色のサロン向けテンプレート', 'theme_json': json.dumps({'preset_key':'salon-soft','hero_style': 'soft', 'section_style': 'soft', 'button_style': 'pill', 'surface': '#fffdfb', 'background': '#fff7f2', 'primary': '#c26d5f', 'accent': '#fde7de', 'text': '#2f241f', 'subtext': '#7a675d'})},
    {'code': 'luxury-dark', 'name': 'Luxury Dark', 'description': '濃色で高級感を出すテンプレート', 'theme_json': json.dumps({'preset_key':'luxury-dark','hero_style': 'dark', 'section_style': 'dark', 'button_style': 'solid', 'surface': '#171717', 'background': '#0b0b0b', 'primary': '#d4af37', 'accent': '#2a2415', 'text': '#f5f5f5', 'subtext': '#c9c9c9'})},
    {'code': 'natural-green', 'name': 'Natural Green', 'description': 'ナチュラル系の落ち着いたテンプレート', 'theme_json': json.dumps({'preset_key':'natural-green','hero_style': 'split', 'section_style': 'soft', 'button_style': 'pill', 'surface': '#fbfdf9', 'background': '#f3f8ef', 'primary': '#4f7b4a', 'accent': '#dfead7', 'text': '#1f2f1f', 'subtext': '#5d6d5d'})},
    {'code': 'modern-blue', 'name': 'Modern Blue', 'description': '少し今っぽい青系テンプレート', 'theme_json': json.dumps({'preset_key':'modern-blue','hero_style': 'gradient', 'section_style': 'card', 'button_style': 'solid', 'surface': '#ffffff', 'background': '#eff6ff', 'primary': '#2563eb', 'accent': '#dbeafe', 'text': '#0f172a', 'subtext': '#475569'})},
    {'code': 'warm-beige', 'name': 'Warm Beige', 'description': 'ベージュ基調のやさしいテンプレート', 'theme_json': json.dumps({'preset_key':'warm-beige','hero_style': 'soft', 'section_style': 'card', 'button_style': 'pill', 'surface': '#fffdfa', 'background': '#f6f0e8', 'primary': '#a0673d', 'accent': '#ead9c8', 'text': '#33241a', 'subtext': '#7a6758'})},
    {'code': 'mono-minimal', 'name': 'Mono Minimal', 'description': 'モノトーンでミニマルなテンプレート', 'theme_json': json.dumps({'preset_key':'mono-minimal','hero_style': 'classic', 'section_style': 'line', 'button_style': 'solid', 'surface': '#ffffff', 'background': '#f3f4f6', 'primary': '#111827', 'accent': '#e5e7eb', 'text': '#111827', 'subtext': '#6b7280'})},
    {'code': 'rose-premium', 'name': 'Rose Premium', 'description': '女性向けに寄せた上品なテンプレート', 'theme_json': json.dumps({'preset_key':'rose-premium','hero_style': 'gradient', 'section_style': 'soft', 'button_style': 'pill', 'surface': '#fffaff', 'background': '#fff1f7', 'primary': '#b84d79', 'accent': '#ffd7e6', 'text': '#3b1024', 'subtext': '#7f4b61'})},
    {'code': 'forest-premium', 'name': 'Forest Premium', 'description': '深めのグリーンで落ち着いたテンプレート', 'theme_json': json.dumps({'preset_key':'forest-premium','hero_style': 'dark', 'section_style': 'card', 'button_style': 'solid', 'surface': '#12221c', 'background': '#0c1713', 'primary': '#7bc49b', 'accent': '#1f3b31', 'text': '#eefcf4', 'subtext': '#b8d7c7'})},
    {'code': 'sunset-coral', 'name': 'Sunset Coral', 'description': 'コーラル系で親しみやすいテンプレート', 'theme_json': json.dumps({'preset_key':'sunset-coral','hero_style': 'split', 'section_style': 'card', 'button_style': 'pill', 'surface': '#fffdfb', 'background': '#fff3ee', 'primary': '#ef6c57', 'accent': '#ffd8ce', 'text': '#3e1d18', 'subtext': '#7f5c54'})},
]

from app.config_paths import DB_PATH


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing = {row['name'] for row in rows}
    if column_name in existing:
        return

    normalized_ddl = ' '.join(str(ddl).split())
    upper_ddl = normalized_ddl.upper()

    if 'DEFAULT CURRENT_TIMESTAMP' in upper_ddl:
        fallback_ddl = normalized_ddl.replace('DEFAULT CURRENT_TIMESTAMP', "DEFAULT ''")
        fallback_ddl = fallback_ddl.replace('default CURRENT_TIMESTAMP', "DEFAULT ''")
        fallback_ddl = fallback_ddl.replace('Default CURRENT_TIMESTAMP', "DEFAULT ''")
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {fallback_ddl}")
        conn.execute(
            f"UPDATE {table_name} SET {column_name} = CURRENT_TIMESTAMP "
            f"WHERE {column_name} IS NULL OR {column_name} = ''"
        )
        return

    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {normalized_ddl}")


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def hash_password(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), 100_000)
    return f'{salt}${digest.hex()}'


def verify_password(password: str, password_hash: str) -> bool:
    if not password_hash or '$' not in password_hash:
        return False
    salt, saved = password_hash.split('$', 1)
    digest = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt.encode('utf-8'), 100_000).hex()
    return secrets.compare_digest(saved, digest)


def init_db() -> None:
    with get_connection() as conn:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS shops (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL UNIQUE,
                shop_name TEXT NOT NULL,
                catch_copy TEXT DEFAULT '',
                description TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                address TEXT DEFAULT '',
                business_hours TEXT DEFAULT '10:00〜19:00',
                holiday TEXT DEFAULT '火曜日',
                primary_color TEXT DEFAULT '#2ec4b6',
                primary_dark TEXT DEFAULT '#159a90',
                accent_bg TEXT DEFAULT '#f7fffe',
                heading_bg_color TEXT DEFAULT '#ff6f91',
                staff_list_json TEXT NOT NULL DEFAULT '[]',
                menus_json TEXT NOT NULL DEFAULT '[]',
                admin_ui_mode TEXT NOT NULL DEFAULT 'web',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        _ensure_column(conn, 'shops', 'parent_shop_id', "parent_shop_id TEXT DEFAULT ''")
        _ensure_column(conn, 'shops', 'is_child_shop', 'is_child_shop INTEGER NOT NULL DEFAULT 0')

        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                name TEXT NOT NULL,
                phone TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS customer_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                customer_id INTEGER NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                content TEXT NOT NULL DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(customer_id) REFERENCES customers(id)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS customer_photos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                customer_id INTEGER NOT NULL,
                image_url TEXT NOT NULL DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(customer_id) REFERENCES customers(id)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS reservations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                customer_id INTEGER NOT NULL,
                customer_name TEXT NOT NULL,
                staff_id INTEGER NOT NULL,
                staff_name TEXT NOT NULL,
                menu_id INTEGER NOT NULL,
                menu_name TEXT NOT NULL,
                duration INTEGER NOT NULL,
                price INTEGER NOT NULL,
                reservation_date TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '予約済み',
                source TEXT NOT NULL DEFAULT 'admin',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(customer_id) REFERENCES customers(id)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS system_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT DEFAULT ''
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                monthly_price INTEGER NOT NULL DEFAULT 0,
                show_ads INTEGER NOT NULL DEFAULT 0,
                max_staff INTEGER,
                max_customers INTEGER,
                max_reservations_per_month INTEGER,
                can_use_line INTEGER NOT NULL DEFAULT 1,
                can_use_reports INTEGER NOT NULL DEFAULT 1,
                is_active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 100
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL UNIQUE,
                plan_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                started_at TEXT DEFAULT CURRENT_TIMESTAMP,
                expires_at TEXT,
                FOREIGN KEY(plan_id) REFERENCES plans(id)
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS admin_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                name TEXT NOT NULL,
                login_id TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                is_owner INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(shop_id, login_id)
            )
            '''
        )


        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS admin_password_reset_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL UNIQUE,
                shop_id TEXT NOT NULL,
                login_id TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used_at TEXT DEFAULT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )


        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                customer_id INTEGER,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                phone_normalized TEXT NOT NULL,
                email TEXT DEFAULT '',
                email_reminder_enabled INTEGER NOT NULL DEFAULT 0,
                password_hash TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(shop_id, phone_normalized),
                FOREIGN KEY(customer_id) REFERENCES customers(id)
            )
            '''
        )

        _ensure_column(conn, 'members', 'customer_id', 'customer_id INTEGER')
        _ensure_column(conn, 'members', 'email', "email TEXT DEFAULT ''")
        _ensure_column(conn, 'members', 'email_reminder_enabled', 'email_reminder_enabled INTEGER NOT NULL DEFAULT 0')
        _ensure_column(conn, 'members', 'updated_at', "updated_at TEXT DEFAULT CURRENT_TIMESTAMP")


        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS member_registration_verifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL UNIQUE,
                shop_id TEXT NOT NULL,
                name TEXT NOT NULL,
                phone TEXT NOT NULL,
                phone_normalized TEXT NOT NULL,
                email TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                verification_code TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                verified_at TEXT DEFAULT NULL,
                next_url TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        _ensure_column(conn, 'member_registration_verifications', 'next_url', "next_url TEXT DEFAULT ''")

        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS shop_registration_verifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL UNIQUE,
                shop_id TEXT NOT NULL,
                shop_name TEXT NOT NULL,
                owner_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                email TEXT NOT NULL,
                login_id TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                verification_code TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                verified_at TEXT DEFAULT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                customer_id INTEGER NOT NULL,
                member_id INTEGER,
                sender_type TEXT NOT NULL,
                body TEXT NOT NULL DEFAULT '',
                is_read INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(customer_id) REFERENCES customers(id),
                FOREIGN KEY(member_id) REFERENCES members(id)
            )
            '''
        )
        conn.execute('CREATE INDEX IF NOT EXISTS idx_chat_messages_shop_customer_created ON chat_messages(shop_id, customer_id, created_at, id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_chat_messages_shop_read ON chat_messages(shop_id, is_read, sender_type, created_at, id)')

        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT DEFAULT '',
                actor_type TEXT NOT NULL,
                actor_id TEXT NOT NULL DEFAULT '',
                actor_name TEXT NOT NULL DEFAULT '',
                action TEXT NOT NULL,
                target_type TEXT NOT NULL DEFAULT '',
                target_id TEXT NOT NULL DEFAULT '',
                target_label TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'success',
                method TEXT NOT NULL DEFAULT '',
                path TEXT NOT NULL DEFAULT '',
                ip_address TEXT NOT NULL DEFAULT '',
                user_agent TEXT NOT NULL DEFAULT '',
                detail_json TEXT NOT NULL DEFAULT '{}',
                occurred_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute('CREATE INDEX IF NOT EXISTS idx_audit_logs_occurred_at ON audit_logs(occurred_at, id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_audit_logs_actor ON audit_logs(actor_type, actor_id, occurred_at, id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_audit_logs_shop ON audit_logs(shop_id, occurred_at, id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action, occurred_at, id)')

        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS homepage_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                description TEXT DEFAULT '',
                theme_json TEXT NOT NULL DEFAULT '{}',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS shop_homepage_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL UNIQUE,
                template_id INTEGER,
                site_title TEXT DEFAULT '',
                hero_title TEXT DEFAULT '',
                hero_subtitle TEXT DEFAULT '',
                about_text TEXT DEFAULT '',
                menu_intro TEXT DEFAULT '',
                menu_items_json TEXT NOT NULL DEFAULT '[]',
                gallery_images_json TEXT NOT NULL DEFAULT '[]',
                feature_items_json TEXT NOT NULL DEFAULT '[]',
                news_items_json TEXT NOT NULL DEFAULT '[]',
                access_info TEXT DEFAULT '',
                reserve_button_label TEXT DEFAULT 'LINEで予約する',
                reserve_button_url TEXT DEFAULT '',
                public_path TEXT DEFAULT '',
                is_published INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(template_id) REFERENCES homepage_templates(id)
            )
            '''
        )

        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS shop_homepage_sections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_id TEXT NOT NULL,
                section_type TEXT NOT NULL DEFAULT 'text',
                title TEXT DEFAULT '',
                subtitle TEXT DEFAULT '',
                body_text TEXT DEFAULT '',
                image_url TEXT DEFAULT '',
                button_label TEXT DEFAULT '',
                button_url TEXT DEFAULT '',
                items_json TEXT NOT NULL DEFAULT '[]',
                sort_order INTEGER NOT NULL DEFAULT 100,
                is_visible INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
        _ensure_column(conn, 'shop_homepage_settings', 'logo_image_url', "logo_image_url TEXT DEFAULT ''")
        _ensure_column(conn, 'shop_homepage_settings', 'hero_image_url', "hero_image_url TEXT DEFAULT ''")
        _ensure_column(conn, 'shop_homepage_settings', 'hero_align', "hero_align TEXT DEFAULT 'left'")
        _ensure_column(conn, 'shop_homepage_settings', 'primary_color', "primary_color TEXT DEFAULT ''")
        _ensure_column(conn, 'shop_homepage_settings', 'background_color', "background_color TEXT DEFAULT ''")
        _ensure_column(conn, 'shop_homepage_settings', 'surface_color', "surface_color TEXT DEFAULT ''")
        _ensure_column(conn, 'shop_homepage_settings', 'text_color', "text_color TEXT DEFAULT ''")
        _ensure_column(conn, 'shop_homepage_settings', 'subtext_color', "subtext_color TEXT DEFAULT ''")
        _ensure_column(conn, 'shop_homepage_settings', 'font_family', "font_family TEXT DEFAULT ''")
        _ensure_column(conn, 'shop_homepage_settings', 'custom_css', "custom_css TEXT DEFAULT ''")
        conn.execute('CREATE INDEX IF NOT EXISTS idx_customers_shop_id ON customers(shop_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_customer_notes_shop_customer ON customer_notes(shop_id, customer_id, id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_customer_photos_shop_customer ON customer_photos(shop_id, customer_id, id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_reservations_shop_date ON reservations(shop_id, reservation_date, start_time)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_shop_id ON subscriptions(shop_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_admin_users_shop_id ON admin_users(shop_id, login_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_members_shop_phone ON members(shop_id, phone_normalized)')

        _ensure_column(conn, 'shops', 'reply_to_email', "reply_to_email TEXT DEFAULT ''")
        _ensure_column(conn, 'shops', 'heading_bg_color', "heading_bg_color TEXT DEFAULT '#ff6f91'")
        _ensure_column(conn, 'shops', 'admin_ui_mode', "admin_ui_mode TEXT NOT NULL DEFAULT 'web'")
        _ensure_column(conn, 'customers', 'email', "email TEXT DEFAULT ''")
        _ensure_column(conn, 'reservations', 'customer_email', "customer_email TEXT DEFAULT ''")
        _ensure_column(conn, 'reservations', 'receive_email', "receive_email INTEGER NOT NULL DEFAULT 0")

        for shop_id, shop in SHOPS.items():
            conn.execute(
                '''
                INSERT INTO shops (
                    shop_id, shop_name, catch_copy, description, phone, address, business_hours, holiday,
                    primary_color, primary_dark, accent_bg, heading_bg_color, staff_list_json, menus_json, admin_ui_mode
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(shop_id) DO NOTHING
                ''',
                (
                    shop_id,
                    shop.get('shop_name', ''),
                    shop.get('catch_copy', ''),
                    shop.get('description', ''),
                    shop.get('phone', ''),
                    shop.get('address', ''),
                    shop.get('business_hours', '10:00〜19:00'),
                    shop.get('holiday', '火曜日'),
                    shop.get('primary_color', '#2ec4b6'),
                    shop.get('primary_dark', '#159a90'),
                    shop.get('accent_bg', '#f7fffe'),
                    shop.get('heading_bg_color', '#ff6f91'),
                    json.dumps(shop.get('staff_list', []), ensure_ascii=False),
                    json.dumps(shop.get('menus', []), ensure_ascii=False),
                    shop.get('admin_ui_mode', 'web'),
                ),
            )

        for plan in DEFAULT_PLANS:
            conn.execute(
                '''
                INSERT INTO plans (
                    code, name, monthly_price, show_ads, max_staff, max_customers,
                    max_reservations_per_month, can_use_line, can_use_reports, is_active, sort_order
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    name = excluded.name,
                    monthly_price = excluded.monthly_price,
                    show_ads = excluded.show_ads,
                    max_staff = excluded.max_staff,
                    max_customers = excluded.max_customers,
                    max_reservations_per_month = excluded.max_reservations_per_month,
                    can_use_line = excluded.can_use_line,
                    can_use_reports = excluded.can_use_reports,
                    is_active = excluded.is_active,
                    sort_order = excluded.sort_order
                ''',
                (
                    plan['code'],
                    plan['name'],
                    plan['monthly_price'],
                    plan['show_ads'],
                    plan['max_staff'],
                    plan['max_customers'],
                    plan['max_reservations_per_month'],
                    plan['can_use_line'],
                    plan['can_use_reports'],
                    plan['is_active'],
                    plan['sort_order'],
                ),
            )


        free_plan_id = conn.execute("SELECT id FROM plans WHERE code = 'free' LIMIT 1").fetchone()['id']
        existing_shop_rows = conn.execute('SELECT shop_id FROM shops').fetchall()
        existing_shop_ids = {row['shop_id'] for row in existing_shop_rows}
        for shop_id in existing_shop_ids:
            conn.execute(
                '''
                INSERT INTO subscriptions (shop_id, plan_id, status)
                VALUES (?, ?, 'active')
                ON CONFLICT(shop_id) DO NOTHING
                ''',
                (shop_id, free_plan_id),
            )

        for shop_id, shop in SHOPS.items():
            login_id = shop.get('admin_login_id')
            password = shop.get('admin_password')
            if not login_id or not password:
                continue
            row = conn.execute(
                '''
                SELECT id
                FROM admin_users
                WHERE shop_id = ? AND login_id = ?
                LIMIT 1
                ''',
                (shop_id, login_id),
            ).fetchone()
            if row is None:
                conn.execute(
                    '''
                    INSERT INTO admin_users (shop_id, name, login_id, password_hash, is_owner, is_active)
                    VALUES (?, ?, ?, ?, 1, 1)
                    ''',
                    (shop_id, '初期管理ユーザー', login_id, hash_password(password)),
                )

        for template in DEFAULT_HOMEPAGE_TEMPLATES:
            conn.execute(
                '''
                INSERT INTO homepage_templates (code, name, description, theme_json, is_active)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(code) DO UPDATE SET
                    name = excluded.name,
                    description = excluded.description,
                    theme_json = excluded.theme_json
                ''',
                (
                    template['code'],
                    template['name'],
                    template['description'],
                    template['theme_json'],
                ),
            )

        first_template_row = conn.execute('SELECT id FROM homepage_templates ORDER BY id ASC LIMIT 1').fetchone()
        if first_template_row is not None:
            default_template_id = first_template_row['id']
            shop_rows = conn.execute('SELECT shop_id, shop_name, catch_copy, description, address, phone, business_hours FROM shops').fetchall()
            for row in shop_rows:
                conn.execute(
                    '''
                    INSERT INTO shop_homepage_settings (
                        shop_id, template_id, site_title, hero_title, hero_subtitle,
                        about_text, menu_intro, menu_items_json, gallery_images_json,
                        feature_items_json, news_items_json, access_info, reserve_button_label,
                        reserve_button_url, public_path, is_published
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(shop_id) DO NOTHING
                    ''',
                    (
                        row['shop_id'],
                        default_template_id,
                        row['shop_name'],
                        row['shop_name'],
                        row['catch_copy'] or 'LINE予約にすぐつながる店舗ページです。',
                        row['description'] or '店舗紹介文をここに入力できます。',
                        'おすすめメニュー',
                        json.dumps([
                            {'title': 'カット', 'price': '¥4,000', 'description': '髪質と骨格に合わせて仕上げます。'},
                            {'title': 'カット+カラー', 'price': '¥9,000', 'description': '人気の定番メニューです。'},
                            {'title': 'トリートメント', 'price': '¥3,500', 'description': 'ダメージを整えて艶を出します。'},
                        ], ensure_ascii=False),
                        json.dumps([
                            {'label': '外観', 'url': 'https://images.unsplash.com/photo-1560066984-138dadb4c035?auto=format&fit=crop&w=1200&q=80'},
                            {'label': '店内', 'url': 'https://images.unsplash.com/photo-1521590832167-7bcbfaa6381f?auto=format&fit=crop&w=1200&q=80'},
                            {'label': 'スタイル', 'url': 'https://images.unsplash.com/photo-1522337660859-02fbefca4702?auto=format&fit=crop&w=1200&q=80'},
                        ], ensure_ascii=False),
                        json.dumps([
                            {'title': 'LINE予約対応', 'description': '予約導線を分かりやすく表示できます。'},
                            {'title': 'メニュー掲載', 'description': '主要メニューをそのままホームページに掲載できます。'},
                            {'title': 'スマホ対応', 'description': 'iPhoneでも見やすい公開ページです。'},
                        ], ensure_ascii=False),
                        json.dumps([
                            {'date': '2026-04-01', 'title': 'ホームページ機能を公開しました。'},
                            {'date': '2026-04-10', 'title': 'LINE予約ボタンを設置できます。'},
                        ], ensure_ascii=False),
                        '\n'.join(filter(None, [row['address'], row['phone'], row['business_hours']])),
                        'LINEで予約する',
                        f"/shop/{row['shop_id']}",
                        row['shop_id'],
                    ),
                )

        for row in conn.execute('SELECT shop_id, hero_title, hero_subtitle, about_text, access_info, reserve_button_label, reserve_button_url, menu_items_json, gallery_images_json, feature_items_json, news_items_json FROM shop_homepage_settings').fetchall():
            section_count = conn.execute('SELECT COUNT(1) AS count FROM shop_homepage_sections WHERE shop_id = ?', (row['shop_id'],)).fetchone()['count']
            if section_count:
                continue
            conn.execute(
                '''
                INSERT INTO shop_homepage_sections (
                    shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
                ) VALUES (?, 'hero', ?, ?, '', '', ?, ?, '[]', 10, 1, CURRENT_TIMESTAMP)
                ''',
                (row['shop_id'], row['hero_title'], row['hero_subtitle'], row['reserve_button_label'], row['reserve_button_url']),
            )
            conn.execute(
                '''
                INSERT INTO shop_homepage_sections (
                    shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
                ) VALUES (?, 'about', 'About', '', ?, '', '', '', '[]', 20, 1, CURRENT_TIMESTAMP)
                ''',
                (row['shop_id'], row['about_text']),
            )
            conn.execute(
                '''
                INSERT INTO shop_homepage_sections (
                    shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
                ) VALUES (?, 'menu', 'Menu', '', '', '', '', '', ?, 30, 1, CURRENT_TIMESTAMP)
                ''',
                (row['shop_id'], row['menu_items_json']),
            )
            conn.execute(
                '''
                INSERT INTO shop_homepage_sections (
                    shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
                ) VALUES (?, 'features', 'Features', '', '', '', '', '', ?, 40, 1, CURRENT_TIMESTAMP)
                ''',
                (row['shop_id'], row['feature_items_json']),
            )
            conn.execute(
                '''
                INSERT INTO shop_homepage_sections (
                    shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
                ) VALUES (?, 'gallery', 'Gallery', '', '', '', '', '', ?, 50, 1, CURRENT_TIMESTAMP)
                ''',
                (row['shop_id'], row['gallery_images_json']),
            )
            conn.execute(
                '''
                INSERT INTO shop_homepage_sections (
                    shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
                ) VALUES (?, 'news', 'News', '', '', '', '', '', ?, 60, 1, CURRENT_TIMESTAMP)
                ''',
                (row['shop_id'], row['news_items_json']),
            )
            conn.execute(
                '''
                INSERT INTO shop_homepage_sections (
                    shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
                ) VALUES (?, 'contact', 'Access', '', ?, '', ?, ?, '[]', 70, 1, CURRENT_TIMESTAMP)
                ''',
                (row['shop_id'], row['access_info'], row['reserve_button_label'], row['reserve_button_url']),
            )
        conn.commit()


def _deserialize_theme_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    item = dict(row)
    try:
        item['theme'] = json.loads(item.pop('theme_json') or '{}')
    except json.JSONDecodeError:
        item['theme'] = {}
    return item


def _deserialize_homepage_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    item = dict(row)
    for key in ('menu_items_json', 'gallery_images_json', 'feature_items_json', 'news_items_json'):
        try:
            item[key[:-5]] = json.loads(item.pop(key) or '[]')
        except json.JSONDecodeError:
            item[key[:-5]] = []
    item.setdefault('logo_image_url', '')
    item.setdefault('hero_image_url', '')
    item.setdefault('hero_align', 'left')
    item.setdefault('primary_color', '')
    item.setdefault('background_color', '')
    item.setdefault('surface_color', '')
    item.setdefault('text_color', '')
    item.setdefault('subtext_color', '')
    item.setdefault('font_family', '')
    item.setdefault('custom_css', '')
    return item


def _deserialize_shop_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    shop = dict(row)
    try:
        shop['staff_list'] = json.loads(shop.pop('staff_list_json', '[]') or '[]')
    except json.JSONDecodeError:
        shop['staff_list'] = []
    try:
        shop['menus'] = json.loads(shop.pop('menus_json', '[]') or '[]')
    except json.JSONDecodeError:
        shop['menus'] = []
    return shop


def get_shop(shop_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                shop_id, shop_name, catch_copy, description, phone, address, business_hours, holiday,
                primary_color, primary_dark, accent_bg, heading_bg_color, reply_to_email, admin_ui_mode, staff_list_json, menus_json, created_at
            FROM shops
            WHERE shop_id = ?
            LIMIT 1
            ''',
            (shop_id,),
        ).fetchone()
    return _deserialize_shop_row(row)


def get_shop_ui_mode(shop_id: str) -> str:
    shop = get_shop(shop_id)
    return (shop or {}).get('admin_ui_mode') or 'web'


def get_all_shops() -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT
                shop_id, shop_name, catch_copy, description, phone, address, business_hours, holiday,
                primary_color, primary_dark, accent_bg, heading_bg_color, reply_to_email, admin_ui_mode, staff_list_json, menus_json, created_at
            FROM shops
            ORDER BY id ASC
            '''
        ).fetchall()
    return [_deserialize_shop_row(row) for row in rows]




def get_homepage_templates(active_only: bool = False) -> list[dict[str, Any]]:
    query = 'SELECT id, code, name, description, theme_json, is_active, created_at FROM homepage_templates'
    params: tuple[Any, ...] = ()
    if active_only:
        query += ' WHERE is_active = 1'
    query += ' ORDER BY id ASC'
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [_deserialize_theme_row(row) for row in rows]


def get_homepage_template(template_id: int) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute('SELECT id, code, name, description, theme_json, is_active, created_at FROM homepage_templates WHERE id = ?', (template_id,)).fetchone()
    return _deserialize_theme_row(row)


def create_homepage_template(*, code: str, name: str, description: str = '', theme: dict[str, Any] | None = None, is_active: int = 1) -> dict[str, Any]:
    normalized_code = code.strip().lower().replace(' ', '-')
    if not normalized_code:
        raise ValueError('テンプレートコードを入力してください。')
    allowed = set('abcdefghijklmnopqrstuvwxyz0123456789-')
    if any(ch not in allowed for ch in normalized_code):
        raise ValueError('テンプレートコードは半角英小文字・数字・ハイフンのみで入力してください。')
    theme = theme or {}
    with get_connection() as conn:
        cur = conn.execute(
            'INSERT INTO homepage_templates (code, name, description, theme_json, is_active) VALUES (?, ?, ?, ?, ?)',
            (normalized_code, name.strip(), description.strip(), json.dumps(theme, ensure_ascii=False), is_active),
        )
        conn.commit()
        template_id = cur.lastrowid
    created = get_homepage_template(int(template_id))
    if created is None:
        raise RuntimeError('テンプレートの保存に失敗しました。')
    return created


def get_shop_homepage_settings(shop_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                s.shop_id,
                s.template_id,
                s.site_title,
                s.hero_title,
                s.hero_subtitle,
                s.about_text,
                s.menu_intro,
                s.menu_items_json,
                s.gallery_images_json,
                s.feature_items_json,
                s.news_items_json,
                s.access_info,
                s.reserve_button_label,
                s.reserve_button_url,
                s.public_path,
                s.is_published,
                s.logo_image_url,
                s.hero_image_url,
                s.hero_align,
                s.primary_color,
                s.background_color,
                s.surface_color,
                s.text_color,
                s.subtext_color,
                s.font_family,
                s.custom_css,
                s.updated_at
            FROM shop_homepage_settings s
            WHERE s.shop_id = ?
            LIMIT 1
            ''',
            (shop_id,),
        ).fetchone()
    return _deserialize_homepage_row(row)


def upsert_shop_homepage_settings(
    shop_id: str,
    *,
    template_id: int,
    site_title: str,
    hero_title: str,
    hero_subtitle: str,
    about_text: str,
    menu_intro: str,
    menu_items: list[dict[str, Any]],
    gallery_images: list[dict[str, Any]],
    feature_items: list[dict[str, Any]],
    news_items: list[dict[str, Any]],
    access_info: str,
    reserve_button_label: str,
    reserve_button_url: str,
    public_path: str,
    is_published: int,
    logo_image_url: str = '',
    hero_image_url: str = '',
    hero_align: str = 'left',
    primary_color: str = '',
    background_color: str = '',
    surface_color: str = '',
    text_color: str = '',
    subtext_color: str = '',
    font_family: str = '',
    custom_css: str = '',
) -> None:
    public_path = (public_path or shop_id).strip().strip('/') or shop_id
    with get_connection() as conn:
        conn.execute(
            '''
            INSERT INTO shop_homepage_settings (
                shop_id, template_id, site_title, hero_title, hero_subtitle, about_text, menu_intro,
                menu_items_json, gallery_images_json, feature_items_json, news_items_json,
                access_info, reserve_button_label, reserve_button_url, public_path, is_published,
                logo_image_url, hero_image_url, hero_align, primary_color, background_color, surface_color,
                text_color, subtext_color, font_family, custom_css, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(shop_id) DO UPDATE SET
                template_id=excluded.template_id,
                site_title=excluded.site_title,
                hero_title=excluded.hero_title,
                hero_subtitle=excluded.hero_subtitle,
                about_text=excluded.about_text,
                menu_intro=excluded.menu_intro,
                menu_items_json=excluded.menu_items_json,
                gallery_images_json=excluded.gallery_images_json,
                feature_items_json=excluded.feature_items_json,
                news_items_json=excluded.news_items_json,
                access_info=excluded.access_info,
                reserve_button_label=excluded.reserve_button_label,
                reserve_button_url=excluded.reserve_button_url,
                public_path=excluded.public_path,
                is_published=excluded.is_published,
                logo_image_url=excluded.logo_image_url,
                hero_image_url=excluded.hero_image_url,
                hero_align=excluded.hero_align,
                primary_color=excluded.primary_color,
                background_color=excluded.background_color,
                surface_color=excluded.surface_color,
                text_color=excluded.text_color,
                subtext_color=excluded.subtext_color,
                font_family=excluded.font_family,
                custom_css=excluded.custom_css,
                updated_at=CURRENT_TIMESTAMP
            ''',
            (
                shop_id, template_id, site_title.strip(), hero_title.strip(), hero_subtitle.strip(),
                about_text.strip(), menu_intro.strip(), json.dumps(menu_items, ensure_ascii=False),
                json.dumps(gallery_images, ensure_ascii=False), json.dumps(feature_items, ensure_ascii=False),
                json.dumps(news_items, ensure_ascii=False), access_info.strip(), reserve_button_label.strip(),
                reserve_button_url.strip(), public_path, int(bool(is_published)), logo_image_url.strip(),
                hero_image_url.strip(), (hero_align or 'left').strip(), primary_color.strip(), background_color.strip(),
                surface_color.strip(), text_color.strip(), subtext_color.strip(), font_family.strip(), custom_css.strip(),
            ),
        )
        conn.commit()


def get_shop_homepage_by_public_path(public_path: str) -> dict[str, Any] | None:
    normalized = public_path.strip().strip('/')
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                s.shop_id,
                s.template_id,
                s.site_title,
                s.hero_title,
                s.hero_subtitle,
                s.about_text,
                s.menu_intro,
                s.menu_items_json,
                s.gallery_images_json,
                s.feature_items_json,
                s.news_items_json,
                s.access_info,
                s.reserve_button_label,
                s.reserve_button_url,
                s.public_path,
                s.is_published,
                s.logo_image_url,
                s.hero_image_url,
                s.hero_align,
                s.primary_color,
                s.background_color,
                s.surface_color,
                s.text_color,
                s.subtext_color,
                s.font_family,
                s.custom_css,
                s.updated_at
            FROM shop_homepage_settings s
            WHERE s.public_path = ? AND s.is_published = 1
            LIMIT 1
            ''',
            (normalized,),
        ).fetchone()
    return _deserialize_homepage_row(row)


def get_shop_homepage_sections(shop_id: str) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT id, shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
            FROM shop_homepage_sections
            WHERE shop_id = ?
            ORDER BY sort_order ASC, id ASC
            ''',
            (shop_id,),
        ).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        try:
            item['items'] = json.loads(item.pop('items_json') or '[]')
        except json.JSONDecodeError:
            item['items'] = []
        items.append(item)
    return items


def _normalize_section_items(section_type: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for item in items:
        if not any(str(value or '').strip() for value in item.values()):
            continue
        if section_type == 'menu':
            normalized.append({'title': item.get('title', ''), 'price': item.get('price', ''), 'description': item.get('description', '')})
        elif section_type == 'gallery':
            normalized.append({'label': item.get('label', ''), 'url': item.get('url', '')})
        elif section_type == 'news':
            normalized.append({'date': item.get('date', ''), 'title': item.get('title', '')})
        else:
            normalized.append({'title': item.get('title', ''), 'description': item.get('description', '')})
    return normalized


def create_shop_homepage_section(
    shop_id: str,
    *,
    section_type: str,
    title: str = '',
    subtitle: str = '',
    body_text: str = '',
    image_url: str = '',
    button_label: str = '',
    button_url: str = '',
    items: list[dict[str, Any]] | None = None,
    sort_order: int = 100,
    is_visible: int = 1,
) -> None:
    with get_connection() as conn:
        conn.execute(
            '''
            INSERT INTO shop_homepage_sections (
                shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''',
            (
                shop_id, section_type.strip(), title.strip(), subtitle.strip(), body_text.strip(), image_url.strip(),
                button_label.strip(), button_url.strip(), json.dumps(_normalize_section_items(section_type, items or []), ensure_ascii=False),
                int(sort_order), int(bool(is_visible)),
            ),
        )
        conn.commit()


def update_shop_homepage_section(
    shop_id: str,
    section_id: int,
    *,
    section_type: str,
    title: str = '',
    subtitle: str = '',
    body_text: str = '',
    image_url: str = '',
    button_label: str = '',
    button_url: str = '',
    items: list[dict[str, Any]] | None = None,
    sort_order: int = 100,
    is_visible: int = 1,
) -> None:
    with get_connection() as conn:
        conn.execute(
            '''
            UPDATE shop_homepage_sections
            SET section_type = ?, title = ?, subtitle = ?, body_text = ?, image_url = ?, button_label = ?, button_url = ?,
                items_json = ?, sort_order = ?, is_visible = ?, updated_at = CURRENT_TIMESTAMP
            WHERE shop_id = ? AND id = ?
            ''',
            (
                section_type.strip(), title.strip(), subtitle.strip(), body_text.strip(), image_url.strip(), button_label.strip(),
                button_url.strip(), json.dumps(_normalize_section_items(section_type, items or []), ensure_ascii=False),
                int(sort_order), int(bool(is_visible)), shop_id, section_id,
            ),
        )
        conn.commit()




def patch_shop_homepage_settings(shop_id: str, **fields) -> None:
    allowed = {
        "template_id", "site_title", "hero_title", "hero_subtitle", "about_text", "menu_intro",
        "access_info", "reserve_button_label", "reserve_button_url", "public_path", "is_published",
        "logo_image_url", "hero_image_url", "hero_align", "primary_color", "background_color",
        "surface_color", "text_color", "subtext_color", "font_family", "custom_css",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    current = get_shop_homepage_settings(shop_id) or {}
    merged = {**current, **updates}
    upsert_shop_homepage_settings(
        shop_id,
        template_id=int(merged.get("template_id") or current.get("template_id") or 0),
        site_title=str(merged.get("site_title") or ""),
        hero_title=str(merged.get("hero_title") or ""),
        hero_subtitle=str(merged.get("hero_subtitle") or ""),
        about_text=str(merged.get("about_text") or ""),
        menu_intro=str(merged.get("menu_intro") or ""),
        menu_items=current.get("menu_items") or [],
        gallery_images=current.get("gallery_images") or [],
        feature_items=current.get("feature_items") or [],
        news_items=current.get("news_items") or [],
        access_info=str(merged.get("access_info") or ""),
        reserve_button_label=str(merged.get("reserve_button_label") or ""),
        reserve_button_url=str(merged.get("reserve_button_url") or ""),
        public_path=str(merged.get("public_path") or shop_id),
        is_published=int(bool(merged.get("is_published", current.get("is_published", 1)))),
        logo_image_url=str(merged.get("logo_image_url") or ""),
        hero_image_url=str(merged.get("hero_image_url") or ""),
        hero_align=str(merged.get("hero_align") or current.get("hero_align") or "left"),
        primary_color=str(merged.get("primary_color") or ""),
        background_color=str(merged.get("background_color") or ""),
        surface_color=str(merged.get("surface_color") or ""),
        text_color=str(merged.get("text_color") or ""),
        subtext_color=str(merged.get("subtext_color") or ""),
        font_family=str(merged.get("font_family") or ""),
        custom_css=str(merged.get("custom_css") or ""),
    )


def patch_shop_homepage_section(shop_id: str, section_id: int, **fields) -> None:
    current = next((item for item in get_shop_homepage_sections(shop_id) if int(item.get("id")) == int(section_id)), None)
    if current is None:
        raise ValueError("セクションが見つかりません。")
    merged = {**current, **fields}
    update_shop_homepage_section(
        shop_id,
        section_id,
        section_type=str(merged.get("section_type") or current.get("section_type") or "text"),
        title=str(merged.get("title") or ""),
        subtitle=str(merged.get("subtitle") or ""),
        body_text=str(merged.get("body_text") or ""),
        image_url=str(merged.get("image_url") or ""),
        button_label=str(merged.get("button_label") or ""),
        button_url=str(merged.get("button_url") or ""),
        items=merged.get("items") or [],
        sort_order=int(merged.get("sort_order") or current.get("sort_order") or 100),
        is_visible=int(bool(merged.get("is_visible", current.get("is_visible", 1)))),
    )


def reorder_shop_homepage_sections(shop_id: str, ordered_ids: list[int]) -> None:
    with get_connection() as conn:
        for idx, section_id in enumerate(ordered_ids, start=1):
            conn.execute(
                "UPDATE shop_homepage_sections SET sort_order = ?, updated_at = CURRENT_TIMESTAMP WHERE shop_id = ? AND id = ?",
                (idx * 10, shop_id, int(section_id)),
            )
        conn.commit()

def delete_shop_homepage_section(shop_id: str, section_id: int) -> None:
    with get_connection() as conn:
        conn.execute('DELETE FROM shop_homepage_sections WHERE shop_id = ? AND id = ?', (shop_id, section_id))
        conn.commit()

def replace_shop_homepage_sections(shop_id: str, sections: list[dict[str, Any]]) -> None:
    with get_connection() as conn:
        conn.execute('DELETE FROM shop_homepage_sections WHERE shop_id = ?', (shop_id,))
        for section in sections:
            conn.execute(
                '''
                INSERT INTO shop_homepage_sections (
                    shop_id, section_type, title, subtitle, body_text, image_url, button_label, button_url, items_json, sort_order, is_visible, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''',
                (
                    shop_id,
                    section.get('section_type', 'text'),
                    section.get('title', ''),
                    section.get('subtitle', ''),
                    section.get('body_text', ''),
                    section.get('image_url', ''),
                    section.get('button_label', ''),
                    section.get('button_url', ''),
                    json.dumps(section.get('items', []), ensure_ascii=False),
                    int(section.get('sort_order', 100)),
                    1 if section.get('is_visible', True) else 0,
                ),
            )
        conn.commit()


def get_shop_homepage_by_public_path(public_path: str) -> dict[str, Any] | None:
    normalized = public_path.strip().strip('/')
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                s.shop_id,
                s.template_id,
                s.site_title,
                s.hero_title,
                s.hero_subtitle,
                s.about_text,
                s.menu_intro,
                s.menu_items_json,
                s.gallery_images_json,
                s.feature_items_json,
                s.news_items_json,
                s.access_info,
                s.reserve_button_label,
                s.reserve_button_url,
                s.public_path,
                s.is_published,
                s.updated_at
            FROM shop_homepage_settings s
            WHERE s.public_path = ? AND s.is_published = 1
            LIMIT 1
            ''',
            (normalized,),
        ).fetchone()
    return _deserialize_homepage_row(row)
def shop_exists(shop_id: str) -> bool:
    with get_connection() as conn:
        row = conn.execute('SELECT 1 FROM shops WHERE shop_id = ? LIMIT 1', (shop_id,)).fetchone()
    return row is not None


def create_shop_with_owner(
    *,
    shop_id: str,
    shop_name: str,
    owner_name: str,
    login_id: str,
    password: str = '',
    password_hash: str = '',
    plan_code: str = 'free',
    catch_copy: str = 'LINE予約もできる、やさしく使いやすい店舗予約システム',
    description: str = '新しく登録された店舗です。管理画面から店舗情報を編集してください。',
    phone: str = '',
    address: str = '',
    business_hours: str = '10:00〜19:00',
    holiday: str = '火曜日',
    reply_to_email: str = '',
) -> dict[str, Any]:
    normalized_shop_id = shop_id.strip().lower()
    if not normalized_shop_id:
        raise ValueError('店舗IDを入力してください。')
    allowed = set('abcdefghijklmnopqrstuvwxyz0123456789-')
    if any(ch not in allowed for ch in normalized_shop_id):
        raise ValueError('店舗IDは半角英小文字・数字・ハイフンのみで入力してください。')
    if shop_exists(normalized_shop_id):
        raise ValueError('この店舗IDはすでに使われています。')

    plan = get_plan_by_code(plan_code)
    if plan is None:
        raise ValueError('初期プランが見つかりません。')

    default_staff = [
        {'id': 1, 'name': owner_name or 'オーナー'},
    ]
    default_menus = [
        {'id': 1, 'name': 'カット', 'duration': 60, 'price': 4000, 'description': '基本のカットメニューです。'},
        {'id': 2, 'name': 'カット+カラー', 'duration': 120, 'price': 9000, 'description': '人気の定番メニューです。'},
    ]

    with get_connection() as conn:
        conn.execute(
            '''
            INSERT INTO shops (
                shop_id, shop_name, catch_copy, description, phone, address, business_hours, holiday,
                primary_color, primary_dark, accent_bg, heading_bg_color, reply_to_email, staff_list_json, menus_json, admin_ui_mode
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                normalized_shop_id, shop_name.strip(), catch_copy.strip(), description.strip(), phone.strip(),
                address.strip(), business_hours.strip(), holiday.strip() or '火曜日',
                '#2ec4b6', '#159a90', '#f7fffe', '#ff6f91', reply_to_email.strip().lower(),
                json.dumps(default_staff, ensure_ascii=False),
                json.dumps(default_menus, ensure_ascii=False),
                'web',
            ),
        )
        conn.execute(
            '''
            INSERT INTO subscriptions (shop_id, plan_id, status)
            VALUES (?, ?, 'active')
            ''',
            (normalized_shop_id, plan['id']),
        )
        conn.execute(
            '''
            INSERT INTO admin_users (shop_id, name, login_id, password_hash, is_owner, is_active)
            VALUES (?, ?, ?, ?, 1, 1)
            ''',
            (normalized_shop_id, owner_name.strip(), login_id.strip(), password_hash.strip() or hash_password(password)),
        )
        conn.commit()

    created = get_shop(normalized_shop_id)
    if created is None:
        raise RuntimeError('店舗登録に失敗しました。')
    return created


# customers

def get_customers(shop_id: str) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT id, shop_id, name, phone, email, created_at
            FROM customers
            WHERE shop_id = ?
            ORDER BY id ASC
            ''',
            (shop_id,),
        ).fetchall()
    return _rows_to_dicts(rows)


def get_member_customer_ids(shop_id: str) -> set[int]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT customer_id
            FROM members
            WHERE shop_id = ? AND is_active = 1 AND customer_id IS NOT NULL
            """,
            (shop_id,),
        ).fetchall()
    return {int(row[0]) for row in rows if row[0] is not None}


def get_customer_by_id(shop_id: str, customer_id: int) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, shop_id, name, phone, email, created_at
            FROM customers
            WHERE shop_id = ? AND id = ?
            ''',
            (shop_id, customer_id),
        ).fetchone()
    return dict(row) if row else None


def find_customer(shop_id: str, customer_name: str, phone: str, email: str = '') -> dict[str, Any] | None:
    normalized_email = (email or '').strip().lower()
    with get_connection() as conn:
        if normalized_email:
            row = conn.execute(
                """
                SELECT id, shop_id, name, phone, email, created_at
                FROM customers
                WHERE shop_id = ? AND email = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (shop_id, normalized_email),
            ).fetchone()
            if row:
                return dict(row)

        if phone:
            row = conn.execute(
                """
                SELECT id, shop_id, name, phone, email, created_at
                FROM customers
                WHERE shop_id = ? AND phone = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (shop_id, phone),
            ).fetchone()
            if row:
                return dict(row)

        row = conn.execute(
            """
            SELECT id, shop_id, name, phone, email, created_at
            FROM customers
            WHERE shop_id = ? AND name = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (shop_id, customer_name),
        ).fetchone()
    return dict(row) if row else None


def create_customer(shop_id: str, name: str, phone: str = '', email: str = '') -> dict[str, Any]:
    normalized_email = (email or '').strip().lower()
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO customers (shop_id, name, phone, email)
            VALUES (?, ?, ?, ?)
            """,
            (shop_id, name, phone, normalized_email),
        )
        customer_id = cursor.lastrowid
        conn.commit()

    customer = get_customer_by_id(shop_id, int(customer_id))
    if customer is None:
        raise RuntimeError('顧客の保存に失敗しました。')
    return customer


def update_customer(shop_id: str, customer_id: int, name: str, phone: str = '', email: str = '') -> dict[str, Any] | None:
    normalized_email = (email or '').strip().lower()
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE customers
            SET name = ?, phone = ?, email = ?
            WHERE shop_id = ? AND id = ?
            """,
            (name, phone, normalized_email, shop_id, customer_id),
        )
        conn.commit()
    return get_customer_by_id(shop_id, customer_id)


def update_customer_contact(shop_id: str, customer_id: int, name: str = '', phone: str = '', email: str = '') -> dict[str, Any] | None:
    customer = get_customer_by_id(shop_id, customer_id)
    if customer is None:
        return None
    next_name = (name or '').strip() or customer.get('name') or ''
    next_phone = (phone or '').strip() or customer.get('phone') or ''
    next_email = (email or '').strip().lower() or customer.get('email') or ''
    return update_customer(shop_id, customer_id, next_name, next_phone, next_email)


def get_customer_notes(shop_id: str, customer_id: int) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT id, shop_id, customer_id, title, content, created_at
            FROM customer_notes
            WHERE shop_id = ? AND customer_id = ?
            ORDER BY id DESC
            ''',
            (shop_id, customer_id),
        ).fetchall()
    return _rows_to_dicts(rows)


def add_customer_note(shop_id: str, customer_id: int, title: str, content: str) -> dict[str, Any]:
    with get_connection() as conn:
        cursor = conn.execute(
            '''
            INSERT INTO customer_notes (shop_id, customer_id, title, content)
            VALUES (?, ?, ?, ?)
            ''',
            (shop_id, customer_id, title, content),
        )
        note_id = cursor.lastrowid
        conn.commit()
        row = conn.execute(
            '''
            SELECT id, shop_id, customer_id, title, content, created_at
            FROM customer_notes
            WHERE id = ? AND shop_id = ? AND customer_id = ?
            ''',
            (note_id, shop_id, customer_id),
        ).fetchone()
    if row is None:
        raise RuntimeError('顧客メモの保存に失敗しました。')
    return dict(row)


def delete_customer_note(shop_id: str, customer_id: int, note_id: int) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, shop_id, customer_id, title, content, created_at
            FROM customer_notes
            WHERE id = ? AND shop_id = ? AND customer_id = ?
            ''',
            (note_id, shop_id, customer_id),
        ).fetchone()
        if row is None:
            return None
        conn.execute('DELETE FROM customer_notes WHERE id = ? AND shop_id = ? AND customer_id = ?', (note_id, shop_id, customer_id))
        conn.commit()
    return dict(row)


def get_customer_photos(shop_id: str, customer_id: int) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT id, shop_id, customer_id, image_url, created_at
            FROM customer_photos
            WHERE shop_id = ? AND customer_id = ?
            ORDER BY id DESC
            ''',
            (shop_id, customer_id),
        ).fetchall()
    return _rows_to_dicts(rows)


def get_customer_photo_by_id(shop_id: str, customer_id: int, photo_id: int) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, shop_id, customer_id, image_url, created_at
            FROM customer_photos
            WHERE id = ? AND shop_id = ? AND customer_id = ?
            ''',
            (photo_id, shop_id, customer_id),
        ).fetchone()
    return dict(row) if row else None


def add_customer_photo(shop_id: str, customer_id: int, image_url: str) -> dict[str, Any]:
    with get_connection() as conn:
        cursor = conn.execute(
            '''
            INSERT INTO customer_photos (shop_id, customer_id, image_url)
            VALUES (?, ?, ?)
            ''',
            (shop_id, customer_id, image_url),
        )
        photo_id = cursor.lastrowid
        conn.commit()
    photo = get_customer_photo_by_id(shop_id, customer_id, int(photo_id))
    if photo is None:
        raise RuntimeError('顧客写真の保存に失敗しました。')
    return photo


def delete_customer_photo(shop_id: str, customer_id: int, photo_id: int) -> dict[str, Any] | None:
    photo = get_customer_photo_by_id(shop_id, customer_id, photo_id)
    if photo is None:
        return None
    with get_connection() as conn:
        conn.execute('DELETE FROM customer_photos WHERE id = ? AND shop_id = ? AND customer_id = ?', (photo_id, shop_id, customer_id))
        conn.commit()
    return photo


def delete_customer(shop_id: str, customer_id: int) -> dict[str, Any] | None:
    customer = get_customer_by_id(shop_id, customer_id)
    if customer is None:
        return None
    with get_connection() as conn:
        conn.execute('DELETE FROM customer_notes WHERE shop_id = ? AND customer_id = ?', (shop_id, customer_id))
        conn.execute('DELETE FROM customer_photos WHERE shop_id = ? AND customer_id = ?', (shop_id, customer_id))
        conn.execute('DELETE FROM reservations WHERE shop_id = ? AND customer_id = ?', (shop_id, customer_id))
        conn.execute('DELETE FROM customers WHERE shop_id = ? AND id = ?', (shop_id, customer_id))
        conn.commit()
    return customer


# reservations

def get_reservations(shop_id: str) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT
                id,
                shop_id,
                customer_id,
                customer_name,
                customer_email,
                receive_email,
                staff_id,
                staff_name,
                menu_id,
                menu_name,
                duration,
                price,
                reservation_date,
                start_time,
                end_time,
                status,
                source,
                created_at
            FROM reservations
            WHERE shop_id = ?
            ORDER BY reservation_date ASC, start_time ASC, id ASC
            ''',
            (shop_id,),
        ).fetchall()
    return _rows_to_dicts(rows)


def create_reservation(
    shop_id: str,
    customer_id: int,
    customer_name: str,
    customer_email: str,
    receive_email: int,
    staff_id: int,
    staff_name: str,
    menu_id: int,
    menu_name: str,
    duration: int,
    price: int,
    reservation_date: str,
    start_time: str,
    end_time: str,
    status: str = '予約済み',
    source: str = 'admin',
) -> dict[str, Any]:
    with get_connection() as conn:
        cursor = conn.execute(
            '''
            INSERT INTO reservations (
                shop_id,
                customer_id,
                customer_name,
                customer_email,
                receive_email,
                staff_id,
                staff_name,
                menu_id,
                menu_name,
                duration,
                price,
                reservation_date,
                start_time,
                end_time,
                status,
                source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                shop_id,
                customer_id,
                customer_name,
                customer_email,
                receive_email,
                staff_id,
                staff_name,
                menu_id,
                menu_name,
                duration,
                price,
                reservation_date,
                start_time,
                end_time,
                status,
                source,
            ),
        )
        reservation_id = cursor.lastrowid
        conn.commit()

    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                id,
                shop_id,
                customer_id,
                customer_name,
                customer_email,
                receive_email,
                staff_id,
                staff_name,
                menu_id,
                menu_name,
                duration,
                price,
                reservation_date,
                start_time,
                end_time,
                status,
                source,
                created_at
            FROM reservations
            WHERE id = ? AND shop_id = ?
            ''',
            (reservation_id, shop_id),
        ).fetchone()
    if row is None:
        raise RuntimeError('予約の保存に失敗しました。')
    return dict(row)


def update_reservation_status(shop_id: str, reservation_id: int, status: str) -> None:
    with get_connection() as conn:
        conn.execute(
            '''
            UPDATE reservations
            SET status = ?
            WHERE shop_id = ? AND id = ?
            ''',
            (status, shop_id, reservation_id),
        )
        conn.commit()



def get_child_shops(parent_shop_id: str) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT
                s.shop_id,
                s.shop_name,
                s.phone,
                s.address,
                s.business_hours,
                s.holiday,
                s.reply_to_email,
                s.parent_shop_id,
                s.is_child_shop,
                s.created_at
            FROM shops s
            WHERE s.parent_shop_id = ? AND s.is_child_shop = 1
            ORDER BY s.created_at ASC, s.shop_id ASC
            ''',
            (parent_shop_id,),
        ).fetchall()
    return [_deserialize_shop_row(row) for row in rows]


def get_parent_shop(shop_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT parent_shop_id
            FROM shops
            WHERE shop_id = ? AND is_child_shop = 1
            LIMIT 1
            ''',
            (shop_id,),
        ).fetchone()
    if not row or not row['parent_shop_id']:
        return None
    return get_shop_management_data(str(row['parent_shop_id']))


def create_child_shop_under_parent(
    *,
    parent_shop_id: str,
    child_shop_id: str,
    child_shop_name: str,
    password: str,
) -> dict[str, Any]:
    normalized_parent_shop_id = parent_shop_id.strip().lower()
    normalized_child_shop_id = child_shop_id.strip().lower()
    if not normalized_parent_shop_id:
        raise ValueError('親店舗IDが不正です。')
    if not normalized_child_shop_id:
        raise ValueError('子店舗IDを入力してください。')
    allowed = set('abcdefghijklmnopqrstuvwxyz0123456789-')
    if any(ch not in allowed for ch in normalized_child_shop_id):
        raise ValueError('子店舗IDは半角英小文字・数字・ハイフンのみで入力してください。')
    if shop_exists(normalized_child_shop_id):
        raise ValueError('この子店舗IDはすでに使われています。')
    parent_shop = get_shop_management_data(normalized_parent_shop_id)
    if parent_shop is None:
        raise ValueError('親店舗が見つかりません。')
    child_shop_name = child_shop_name.strip()
    if not child_shop_name:
        raise ValueError('子店舗名を入力してください。')
    if len(password.strip()) < 4:
        raise ValueError('子店舗パスワードは4文字以上で入力してください。')

    with get_connection() as conn:
        conn.execute(
            '''
            INSERT INTO shops (
                shop_id, shop_name, catch_copy, description, phone, address, business_hours, holiday,
                primary_color, primary_dark, accent_bg, heading_bg_color, reply_to_email,
                staff_list_json, menus_json, admin_ui_mode, parent_shop_id, is_child_shop
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ''',
            (
                normalized_child_shop_id,
                child_shop_name,
                '',
                '',
                '',
                '',
                parent_shop.get('business_hours', '10:00〜19:00'),
                parent_shop.get('holiday', '火曜日'),
                parent_shop.get('primary_color', '#2ec4b6'),
                parent_shop.get('primary_dark', '#159a90'),
                parent_shop.get('accent_bg', '#f7fffe'),
                parent_shop.get('heading_bg_color', '#ff6f91'),
                '',
                json.dumps([], ensure_ascii=False),
                json.dumps([], ensure_ascii=False),
                parent_shop.get('admin_ui_mode', 'web'),
                normalized_parent_shop_id,
            ),
        )
        conn.execute(
            '''
            INSERT INTO admin_users (shop_id, name, login_id, password_hash, is_owner, is_active)
            VALUES (?, ?, ?, ?, 1, 1)
            ''',
            (normalized_child_shop_id, child_shop_name, normalized_child_shop_id, hash_password(password.strip())),
        )
        conn.commit()

    created = get_shop_management_data(normalized_child_shop_id)
    if created is None:
        raise RuntimeError('子店舗の作成に失敗しました。')
    return created


def delete_shop_subscription(shop_id: str) -> None:
    normalized_shop_id = (shop_id or '').strip().lower()
    if not normalized_shop_id:
        return
    with get_connection() as conn:
        conn.execute('DELETE FROM subscriptions WHERE shop_id = ?', (normalized_shop_id,))
        conn.commit()


# plans / subscriptions

def get_plans(active_only: bool = False) -> list[dict[str, Any]]:
    query = '''
        SELECT
            id, code, name, monthly_price, show_ads, max_staff, max_customers,
            max_reservations_per_month, can_use_line, can_use_reports, is_active, sort_order
        FROM plans
    '''
    if active_only:
        query += ' WHERE is_active = 1'
    query += ' ORDER BY sort_order ASC, id ASC'
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
    return _rows_to_dicts(rows)


def get_plan_by_code(code: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                id, code, name, monthly_price, show_ads, max_staff, max_customers,
                max_reservations_per_month, can_use_line, can_use_reports, is_active, sort_order
            FROM plans
            WHERE code = ?
            LIMIT 1
            ''',
            (code,),
        ).fetchone()
    return dict(row) if row else None


def get_shop_subscription(shop_id: str) -> dict[str, Any] | None:
    target_shop_id = (shop_id or '').strip().lower()
    with get_connection() as conn:
        shop_row = conn.execute(
            '''
            SELECT parent_shop_id, is_child_shop
            FROM shops
            WHERE shop_id = ?
            LIMIT 1
            ''',
            (target_shop_id,),
        ).fetchone()
        if shop_row and int(shop_row['is_child_shop'] or 0) == 1 and str(shop_row['parent_shop_id'] or '').strip():
            target_shop_id = str(shop_row['parent_shop_id']).strip().lower()

        row = conn.execute(
            '''
            SELECT
                s.id,
                ? AS shop_id,
                s.plan_id,
                s.status,
                s.started_at,
                s.expires_at,
                p.code AS plan_code,
                p.name AS plan_name,
                p.monthly_price,
                p.show_ads,
                p.max_staff,
                p.max_customers,
                p.max_reservations_per_month,
                p.can_use_line,
                p.can_use_reports,
                p.is_active
            FROM subscriptions s
            JOIN plans p ON p.id = s.plan_id
            WHERE s.shop_id = ?
            LIMIT 1
            ''',
            (shop_id, target_shop_id),
        ).fetchone()
    return dict(row) if row else None


def ensure_shop_subscription(shop_id: str, default_plan_code: str = 'free') -> dict[str, Any]:
    current = get_shop_subscription(shop_id)
    if current is not None:
        return current

    plan = get_plan_by_code(default_plan_code)
    if plan is None:
        raise RuntimeError('初期プランが見つかりません。')

    with get_connection() as conn:
        conn.execute(
            '''
            INSERT INTO subscriptions (shop_id, plan_id, status)
            VALUES (?, ?, 'active')
            ON CONFLICT(shop_id) DO NOTHING
            ''',
            (shop_id, plan['id']),
        )
        conn.commit()
    created = get_shop_subscription(shop_id)
    if created is None:
        raise RuntimeError('店舗プランの初期化に失敗しました。')
    return created




def get_all_shops_for_platform() -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT
                s.shop_id,
                s.shop_name,
                s.phone,
                s.address,
                s.business_hours,
                s.holiday,
                s.reply_to_email,
                s.created_at,
                CASE WHEN COALESCE(s.is_child_shop, 0) = 1 THEN NULL ELSE p.id END AS plan_id,
                CASE WHEN COALESCE(s.is_child_shop, 0) = 1 THEN '' ELSE COALESCE(p.code, '') END AS plan_code,
                CASE WHEN COALESCE(s.is_child_shop, 0) = 1 THEN '' ELSE COALESCE(p.name, '未設定') END AS plan_name,
                CASE WHEN COALESCE(s.is_child_shop, 0) = 1 THEN '' ELSE COALESCE(sub.status, 'inactive') END AS subscription_status,
                COUNT(DISTINCT au.id) AS admin_user_count,
                COUNT(DISTINCT c.id) AS customer_count,
                COUNT(DISTINCT r.id) AS reservation_count
            FROM shops s
            LEFT JOIN subscriptions sub ON sub.shop_id = s.shop_id AND COALESCE(s.is_child_shop, 0) = 0
            LEFT JOIN plans p ON p.id = sub.plan_id
            LEFT JOIN admin_users au ON au.shop_id = s.shop_id
            LEFT JOIN customers c ON c.shop_id = s.shop_id
            LEFT JOIN reservations r ON r.shop_id = s.shop_id
            GROUP BY
                s.shop_id, s.shop_name, s.phone, s.address, s.business_hours, s.holiday, s.reply_to_email, s.created_at,
                p.id, p.code, p.name, sub.status
            ORDER BY s.created_at DESC, s.shop_id ASC
            '''
        ).fetchall()
    return _rows_to_dicts(rows)


def get_shop_management_data(shop_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                s.shop_id,
                s.shop_name,
                s.catch_copy,
                s.description,
                s.phone,
                s.address,
                s.business_hours,
                s.holiday,
                s.primary_color,
                s.primary_dark,
                s.accent_bg,
                s.heading_bg_color,
                s.reply_to_email,
                s.admin_ui_mode,
                s.staff_list_json,
                s.menus_json,
                s.parent_shop_id,
                s.is_child_shop,
                s.created_at,
                CASE WHEN COALESCE(s.is_child_shop, 0) = 1 THEN NULL ELSE p.id END AS plan_id,
                CASE WHEN COALESCE(s.is_child_shop, 0) = 1 THEN '' ELSE COALESCE(p.code, '') END AS plan_code,
                CASE WHEN COALESCE(s.is_child_shop, 0) = 1 THEN '' ELSE COALESCE(p.name, '未設定') END AS plan_name,
                CASE WHEN COALESCE(s.is_child_shop, 0) = 1 THEN '' ELSE COALESCE(sub.status, 'inactive') END AS subscription_status
            FROM shops s
            LEFT JOIN subscriptions sub ON sub.shop_id = s.shop_id AND COALESCE(s.is_child_shop, 0) = 0
            LEFT JOIN plans p ON p.id = sub.plan_id
            WHERE s.shop_id = ?
            LIMIT 1
            ''',
            (shop_id,),
        ).fetchone()
    return _deserialize_shop_row(row) if row else None


def _normalize_shop_menus(menus: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, menu in enumerate(menus or [], start=1):
        name = str(menu.get('name', '')).strip()
        duration_raw = str(menu.get('duration', '')).strip()
        price_raw = str(menu.get('price', '')).strip()
        description = str(menu.get('description', '')).strip()
        if not any([name, duration_raw, price_raw, description]):
            continue
        if not name:
            continue
        try:
            duration = max(0, int(duration_raw or 0))
        except (TypeError, ValueError):
            duration = 0
        try:
            price = max(0, int(price_raw or 0))
        except (TypeError, ValueError):
            price = 0
        menu_id = menu.get('id', index)
        try:
            menu_id = int(menu_id)
        except (TypeError, ValueError):
            menu_id = index
        normalized.append({
            'id': menu_id,
            'name': name,
            'duration': duration,
            'price': price,
            'description': description,
        })

    for idx, item in enumerate(normalized, start=1):
        item['id'] = idx
    return normalized


def _normalize_shop_staff_list(staff_list: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, staff in enumerate(staff_list or [], start=1):
        name = str(staff.get('name', '')).strip()
        if not name:
            continue
        staff_id = staff.get('id', index)
        try:
            staff_id = int(staff_id)
        except (TypeError, ValueError):
            staff_id = index
        menu_ids_raw = staff.get('menu_ids', [])
        if isinstance(menu_ids_raw, str):
            menu_ids_raw = [item.strip() for item in menu_ids_raw.split(',') if item.strip()]
        menu_ids: list[int] = []
        for item in menu_ids_raw or []:
            try:
                menu_ids.append(int(item))
            except (TypeError, ValueError):
                continue
        holiday_dates_raw = staff.get('holiday_dates', [])
        if isinstance(holiday_dates_raw, str):
            holiday_dates_raw = [item.strip() for item in holiday_dates_raw.split(',') if item.strip()]
        holiday_dates: list[str] = []
        for item in holiday_dates_raw or []:
            value = str(item or '').strip()
            if not value:
                continue
            try:
                holiday_dates.append(datetime.strptime(value, '%Y-%m-%d').date().isoformat())
            except ValueError:
                continue

        normalized.append({
            'id': staff_id,
            'name': name,
            'menu_ids': menu_ids,
            'holiday_dates': sorted(set(holiday_dates)),
        })

    for idx, item in enumerate(normalized, start=1):
        item['id'] = idx
    return normalized


def update_shop_basic_info(
    shop_id: str,
    *,
    shop_name: str,
    phone: str = '',
    address: str = '',
    business_hours: str = '10:00〜19:00',
    holiday: str = '火曜日',
    catch_copy: str = '',
    description: str = '',
    reply_to_email: str = '',
    admin_ui_mode: str = 'web',
    primary_color: str = '#2ec4b6',
    primary_dark: str = '#159a90',
    accent_bg: str = '#f7fffe',
    heading_bg_color: str = '#ff6f91',
    menus: list[dict[str, Any]] | None = None,
) -> None:
    normalized_menus = _normalize_shop_menus(menus)
    with get_connection() as conn:
        conn.execute(
            '''
            UPDATE shops
            SET
                shop_name = ?,
                phone = ?,
                address = ?,
                business_hours = ?,
                holiday = ?,
                catch_copy = ?,
                description = ?,                reply_to_email = ?,
                admin_ui_mode = ?,
                primary_color = ?,
                primary_dark = ?,
                accent_bg = ?,
                heading_bg_color = ?,
                menus_json = ?
            WHERE shop_id = ?
            ''',
            (
                shop_name.strip(),
                phone.strip(),
                address.strip(),
                business_hours.strip() or '10:00〜19:00',
                holiday.strip() or '火曜日',
                catch_copy.strip(),
                description.strip(),
                reply_to_email.strip(),
                'tool' if admin_ui_mode == 'tool' else 'web',
                primary_color.strip() or '#2ec4b6',
                primary_dark.strip() or '#159a90',
                accent_bg.strip() or '#f7fffe',
                heading_bg_color.strip() or '#ff6f91',
                json.dumps(normalized_menus, ensure_ascii=False),
                shop_id,
            ),
        )
        conn.commit()

def update_shop_staff_list(shop_id: str, staff_list: list[dict[str, Any]] | None = None) -> None:
    normalized_staff_list = _normalize_shop_staff_list(staff_list)
    with get_connection() as conn:
        conn.execute(
            '''
            UPDATE shops
            SET staff_list_json = ?
            WHERE shop_id = ?
            ''',
            (json.dumps(normalized_staff_list, ensure_ascii=False), shop_id),
        )
        conn.commit()


def update_shop_subscription(shop_id: str, plan_id: int, status: str = 'active') -> None:
    with get_connection() as conn:
        conn.execute(
            '''
            INSERT INTO subscriptions (shop_id, plan_id, status)
            VALUES (?, ?, ?)
            ON CONFLICT(shop_id) DO UPDATE SET
                plan_id = excluded.plan_id,
                status = excluded.status,
                started_at = CURRENT_TIMESTAMP
            ''',
            (shop_id, plan_id, status),
        )
        conn.commit()


# admin users

def get_admin_users(shop_id: str) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT id, shop_id, name, login_id, is_owner, is_active, created_at
            FROM admin_users
            WHERE shop_id = ?
            ORDER BY id ASC
            ''',
            (shop_id,),
        ).fetchall()
    return _rows_to_dicts(rows)


def get_admin_user_by_login_id(shop_id: str, login_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, shop_id, name, login_id, password_hash, is_owner, is_active, created_at
            FROM admin_users
            WHERE shop_id = ? AND login_id = ?
            LIMIT 1
            ''',
            (shop_id, login_id),
        ).fetchone()
    return dict(row) if row else None


def authenticate_admin_user(shop_id: str, login_id: str, password: str) -> dict[str, Any] | None:
    user = get_admin_user_by_login_id(shop_id, login_id)
    if user is None or not user.get('is_active'):
        return None
    if not verify_password(password, str(user.get('password_hash') or '')):
        return None
    return user


def create_admin_user(shop_id: str, name: str, login_id: str, password: str, is_owner: int = 0) -> dict[str, Any]:
    with get_connection() as conn:
        existing = conn.execute(
            'SELECT id FROM admin_users WHERE shop_id = ? AND login_id = ? LIMIT 1',
            (shop_id, login_id),
        ).fetchone()
        if existing:
            raise ValueError('このログインIDはすでに使われています。')

        cursor = conn.execute(
            '''
            INSERT INTO admin_users (shop_id, name, login_id, password_hash, is_owner, is_active)
            VALUES (?, ?, ?, ?, ?, 1)
            ''',
            (shop_id, name, login_id, hash_password(password), int(bool(is_owner))),
        )
        user_id = cursor.lastrowid
        conn.commit()

    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, shop_id, name, login_id, is_owner, is_active, created_at
            FROM admin_users
            WHERE id = ?
            ''',
            (user_id,),
        ).fetchone()
    if row is None:
        raise RuntimeError('ユーザー登録に失敗しました。')
    return dict(row)


def get_owner_admin_user(shop_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, shop_id, name, login_id, is_owner, is_active, created_at
            FROM admin_users
            WHERE shop_id = ? AND is_owner = 1 AND is_active = 1
            ORDER BY id ASC
            LIMIT 1
            """,
            (shop_id,),
        ).fetchone()
    return dict(row) if row else None


def find_admin_login_id_by_shop_email(shop_id: str, email: str) -> str | None:
    normalized_email = (email or '').strip().lower()
    if not normalized_email:
        return None
    shop = get_shop(shop_id)
    if shop is None:
        return None
    registered = str(shop.get('reply_to_email') or '').strip().lower()
    if not registered or registered != normalized_email:
        return None
    owner = get_owner_admin_user(shop_id)
    if owner is None:
        return None
    return str(owner.get('login_id') or '')


def create_admin_password_reset_token(shop_id: str, login_id: str, expires_minutes: int = 30) -> str:
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.utcnow() + timedelta(minutes=expires_minutes)).isoformat(timespec='seconds')
    with get_connection() as conn:
        conn.execute(
            'INSERT INTO admin_password_reset_tokens (token, shop_id, login_id, expires_at) VALUES (?, ?, ?, ?)',
            (token, shop_id, login_id, expires_at),
        )
        conn.commit()
    return token


def get_valid_admin_password_reset_token(token: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, token, shop_id, login_id, expires_at, used_at, created_at
            FROM admin_password_reset_tokens
            WHERE token = ?
            LIMIT 1
            ''',
            (token,),
        ).fetchone()
    if row is None:
        return None
    item = dict(row)
    if item.get('used_at'):
        return None
    expires_at = str(item.get('expires_at') or '')
    try:
        if datetime.utcnow() > datetime.fromisoformat(expires_at):
            return None
    except ValueError:
        return None
    return item


def mark_admin_password_reset_token_used(token: str) -> None:
    with get_connection() as conn:
        conn.execute(
            'UPDATE admin_password_reset_tokens SET used_at = CURRENT_TIMESTAMP WHERE token = ?',
            (token,),
        )
        conn.commit()


def update_admin_user_password(shop_id: str, login_id: str, new_password: str) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            'UPDATE admin_users SET password_hash = ? WHERE shop_id = ? AND login_id = ?',
            (hash_password(new_password), shop_id, login_id),
        )
        conn.commit()
    return bool(cursor.rowcount)


# system settings

def get_system_setting(setting_key: str, default: str = '') -> str:
    with get_connection() as conn:
        row = conn.execute('SELECT setting_value FROM system_settings WHERE setting_key = ? LIMIT 1', (setting_key,)).fetchone()
    return str(row['setting_value']) if row and row['setting_value'] is not None else default


def set_system_setting(setting_key: str, setting_value: str) -> None:
    with get_connection() as conn:
        conn.execute(
            '''
            INSERT INTO system_settings (setting_key, setting_value)
            VALUES (?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET setting_value = excluded.setting_value
            ''',
            (setting_key, setting_value),
        )
        conn.commit()


def get_system_mail_settings() -> dict[str, str]:
    return {
        'from_email': get_system_setting('mail_from_email', ''),
        'from_name': get_system_setting('mail_from_name', '予約システム'),
        'smtp_host': get_system_setting('mail_smtp_host', 'smtp.gmail.com'),
        'smtp_port': get_system_setting('mail_smtp_port', '587'),
        'smtp_username': get_system_setting('mail_smtp_username', ''),
        'smtp_password': get_system_setting('mail_smtp_password', ''),
    }


def update_system_mail_settings(*, from_email: str, from_name: str = '予約システム', smtp_host: str = 'smtp.gmail.com', smtp_port: str = '587', smtp_username: str = '', smtp_password: str = '') -> None:
    set_system_setting('mail_from_email', from_email.strip())
    set_system_setting('mail_from_name', from_name.strip() or '予約システム')
    set_system_setting('mail_smtp_host', smtp_host.strip() or 'smtp.gmail.com')
    set_system_setting('mail_smtp_port', str(smtp_port).strip() or '587')
    set_system_setting('mail_smtp_username', smtp_username.strip())
    set_system_setting('mail_smtp_password', smtp_password.strip())



def normalize_member_phone(phone: str) -> str:
    value = ''.join(ch for ch in str(phone or '').strip() if ch.isdigit())
    if value.startswith('81') and len(value) >= 11:
        value = '0' + value[2:]
    return value


def get_member_by_phone(shop_id: str, phone: str) -> dict[str, Any] | None:
    normalized_phone = normalize_member_phone(phone)
    if not normalized_phone:
        return None
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, shop_id, customer_id, name, phone, phone_normalized, email,
                   email_reminder_enabled, password_hash, is_active, created_at, updated_at
            FROM members
            WHERE shop_id = ? AND phone_normalized = ?
            LIMIT 1
            ''',
            (shop_id, normalized_phone),
        ).fetchone()
    return dict(row) if row else None


def get_member_by_id(shop_id: str, member_id: int) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, shop_id, customer_id, name, phone, phone_normalized, email,
                   email_reminder_enabled, password_hash, is_active, created_at, updated_at
            FROM members
            WHERE shop_id = ? AND id = ?
            LIMIT 1
            ''',
            (shop_id, member_id),
        ).fetchone()
    return dict(row) if row else None


def create_member(shop_id: str, name: str, phone: str, password: str, email: str = '') -> dict[str, Any]:
    normalized_phone = normalize_member_phone(phone)
    normalized_email = (email or '').strip().lower()
    if not name.strip():
        raise ValueError('お名前を入力してください。')
    if not normalized_phone:
        raise ValueError('電話番号を入力してください。')
    if len(password or '') < 4:
        raise ValueError('パスワードは4文字以上で入力してください。')

    existing = get_member_by_phone(shop_id, normalized_phone)
    if existing is not None:
        raise ValueError('この電話番号はすでに会員登録されています。')

    customer = find_customer(shop_id, name.strip(), normalized_phone, normalized_email)
    if customer is None:
        customer = create_customer(shop_id, name.strip(), normalized_phone, normalized_email)
    else:
        customer = update_customer_contact(shop_id, int(customer['id']), name.strip(), normalized_phone, normalized_email) or customer

    with get_connection() as conn:
        cursor = conn.execute(
            '''
            INSERT INTO members (
                shop_id, customer_id, name, phone, phone_normalized, email,
                email_reminder_enabled, password_hash, is_active, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
            ''',
            (
                shop_id,
                int(customer['id']) if customer else None,
                name.strip(),
                normalized_phone,
                normalized_phone,
                normalized_email,
                1 if normalized_email else 0,
                hash_password(password),
            ),
        )
        member_id = cursor.lastrowid
        conn.commit()
    member = get_member_by_id(shop_id, int(member_id))
    if member is None:
        raise RuntimeError('会員登録に失敗しました。')
    return member



def create_member_registration_verification(
    *,
    shop_id: str,
    name: str,
    phone: str,
    password: str,
    email: str,
    code: str,
    next_url: str = '',
) -> dict[str, Any]:
    normalized_name = (name or '').strip()
    normalized_phone = normalize_member_phone(phone)
    normalized_email = (email or '').strip().lower()
    normalized_code = ''.join(ch for ch in str(code or '') if ch.isdigit())

    if not normalized_name:
        raise ValueError('お名前を入力してください。')
    if not normalized_phone:
        raise ValueError('電話番号を入力してください。')
    if len(password or '') < 4:
        raise ValueError('パスワードは4文字以上で入力してください。')
    if not normalized_email:
        raise ValueError('メールアドレスを入力してください。')
    if '@' not in normalized_email:
        raise ValueError('メールアドレスの形式が正しくありません。')
    if len(normalized_code) != 6:
        raise ValueError('確認コードの生成に失敗しました。')

    existing = get_member_by_phone(shop_id, normalized_phone)
    if existing is not None:
        raise ValueError('この電話番号はすでに会員登録されています。')

    token = secrets.token_urlsafe(24)
    password_hash = hash_password(password)
    expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat()

    with get_connection() as conn:
        conn.execute(
            'DELETE FROM member_registration_verifications WHERE shop_id = ? AND (phone_normalized = ? OR email = ?)',
            (shop_id, normalized_phone, normalized_email),
        )
        conn.execute(
            '''
            INSERT INTO member_registration_verifications (
                token, shop_id, name, phone, phone_normalized, email, password_hash, verification_code, expires_at, next_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                token,
                shop_id,
                normalized_name,
                normalized_phone,
                normalized_phone,
                normalized_email,
                password_hash,
                normalized_code,
                expires_at,
                (next_url or '').strip(),
            ),
        )
        conn.commit()

    verification = get_member_registration_verification(shop_id, token)
    if verification is None:
        raise RuntimeError('確認コードの保存に失敗しました。')
    return verification


def get_member_registration_verification(shop_id: str, token: str) -> dict[str, Any] | None:
    normalized_token = (token or '').strip()
    if not normalized_token:
        return None
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                id, token, shop_id, name, phone, phone_normalized, email, password_hash,
                verification_code, expires_at, verified_at, next_url, created_at
            FROM member_registration_verifications
            WHERE shop_id = ? AND token = ?
            LIMIT 1
            ''',
            (shop_id, normalized_token),
        ).fetchone()
    if row is None:
        return None
    item = dict(row)
    expires_at = str(item.get('expires_at') or '').strip()
    verified_at = str(item.get('verified_at') or '').strip()
    if verified_at:
        return None
    try:
        if expires_at and datetime.fromisoformat(expires_at) < datetime.utcnow():
            return None
    except ValueError:
        return None
    return item


def _get_member_registration_verification_including_verified(shop_id: str, token: str) -> dict[str, Any] | None:
    normalized_token = (token or '').strip()
    if not normalized_token:
        return None
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                id, token, shop_id, name, phone, phone_normalized, email, password_hash,
                verification_code, expires_at, verified_at, next_url, created_at
            FROM member_registration_verifications
            WHERE shop_id = ? AND token = ?
            LIMIT 1
            ''',
            (shop_id, normalized_token),
        ).fetchone()
    return dict(row) if row else None


def verify_member_registration_code(shop_id: str, token: str, code: str) -> dict[str, Any] | None:
    verification = get_member_registration_verification(shop_id, token)
    if verification is None:
        raise ValueError('確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。')
    normalized_code = ''.join(ch for ch in str(code or '') if ch.isdigit())
    if str(verification.get('verification_code') or '') != normalized_code:
        return None
    with get_connection() as conn:
        conn.execute(
            'UPDATE member_registration_verifications SET verified_at = CURRENT_TIMESTAMP WHERE shop_id = ? AND token = ?',
            (shop_id, token),
        )
        conn.commit()
    return _get_member_registration_verification_including_verified(shop_id, token)


def consume_member_registration_verification(shop_id: str, token: str) -> dict[str, Any]:
    verification = _get_member_registration_verification_including_verified(shop_id, token)
    if verification is None:
        raise ValueError('確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。')
    if not str(verification.get('verified_at') or '').strip():
        raise ValueError('確認コードが未認証です。')

    try:
        expires_at = datetime.fromisoformat(str(verification.get('expires_at') or '').strip())
    except ValueError as exc:
        raise ValueError('確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。') from exc
    if expires_at < datetime.utcnow():
        raise ValueError('確認コードの有効期限が切れました。最初からやり直してください。')

    normalized_phone = str(verification.get('phone_normalized') or '').strip()
    existing = get_member_by_phone(shop_id, normalized_phone)
    if existing is not None:
        raise ValueError('この電話番号はすでに会員登録されています。')

    normalized_name = str(verification.get('name') or '').strip()
    normalized_email = str(verification.get('email') or '').strip().lower()
    customer = find_customer(shop_id, normalized_name, normalized_phone, normalized_email)
    if customer is None:
        customer = create_customer(shop_id, normalized_name, normalized_phone, normalized_email)
    else:
        customer = update_customer_contact(shop_id, int(customer['id']), normalized_name, normalized_phone, normalized_email) or customer

    with get_connection() as conn:
        cursor = conn.execute(
            '''
            INSERT INTO members (
                shop_id, customer_id, name, phone, phone_normalized, email,
                email_reminder_enabled, password_hash, is_active, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
            ''',
            (
                shop_id,
                int(customer['id']) if customer else None,
                normalized_name,
                normalized_phone,
                normalized_phone,
                normalized_email,
                1,
                str(verification.get('password_hash') or ''),
            ),
        )
        member_id = cursor.lastrowid
        conn.execute('DELETE FROM member_registration_verifications WHERE shop_id = ? AND token = ?', (shop_id, token))
        conn.commit()

    member = get_member_by_id(shop_id, int(member_id))
    if member is None:
        raise RuntimeError('会員登録に失敗しました。')
    return member





def create_shop_registration_verification(
    *,
    shop_name: str,
    owner_name: str,
    phone: str,
    email: str,
    login_id: str,
    password: str,
    code: str,
) -> dict[str, Any]:
    normalized_shop_name = (shop_name or '').strip()
    normalized_owner_name = (owner_name or '').strip()
    normalized_phone = (phone or '').strip()
    normalized_email = (email or '').strip().lower()
    normalized_login_id = (login_id or '').strip().lower()
    normalized_code = ''.join(ch for ch in str(code or '') if ch.isdigit())

    if not normalized_shop_name:
        raise ValueError('店舗名を入力してください。')
    if not normalized_owner_name:
        raise ValueError('管理者名を入力してください。')
    if not normalized_phone:
        raise ValueError('電話番号を入力してください。')
    if not normalized_email:
        raise ValueError('メールアドレスを入力してください。')
    if '@' not in normalized_email:
        raise ValueError('メールアドレスの形式が正しくありません。')
    if len(normalized_login_id) < 4:
        raise ValueError('ログインIDは4文字以上で入力してください。')
    allowed = set('abcdefghijklmnopqrstuvwxyz0123456789-')
    if any(ch not in allowed for ch in normalized_login_id):
        raise ValueError('ログインIDは半角英小文字・数字・ハイフンのみで入力してください。')
    if len(password or '') < 6:
        raise ValueError('パスワードは6文字以上で入力してください。')
    if len(normalized_code) != 6:
        raise ValueError('確認コードの生成に失敗しました。')
    if shop_exists(normalized_login_id):
        raise ValueError('この店舗IDはすでに使われています。')

    token = secrets.token_urlsafe(24)
    password_hash = hash_password(password)
    expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat()

    with get_connection() as conn:
        conn.execute(
            'DELETE FROM shop_registration_verifications WHERE shop_id = ? OR email = ? OR login_id = ?',
            (normalized_login_id, normalized_email, normalized_login_id),
        )
        conn.execute(
            '''
            INSERT INTO shop_registration_verifications (
                token, shop_id, shop_name, owner_name, phone, email, login_id, password_hash, verification_code, expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                token,
                normalized_login_id,
                normalized_shop_name,
                normalized_owner_name,
                normalized_phone,
                normalized_email,
                normalized_login_id,
                password_hash,
                normalized_code,
                expires_at,
            ),
        )
        conn.commit()

    verification = get_shop_registration_verification(normalized_login_id, token)
    if verification is None:
        raise RuntimeError('確認コードの保存に失敗しました。')
    return verification


def get_shop_registration_verification(shop_id: str, token: str) -> dict[str, Any] | None:
    normalized_shop_id = (shop_id or '').strip().lower()
    normalized_token = (token or '').strip()
    if not normalized_shop_id or not normalized_token:
        return None
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                id, token, shop_id, shop_name, owner_name, phone, email, login_id, password_hash,
                verification_code, expires_at, verified_at, created_at
            FROM shop_registration_verifications
            WHERE shop_id = ? AND token = ?
            LIMIT 1
            ''',
            (normalized_shop_id, normalized_token),
        ).fetchone()
    if row is None:
        return None
    item = dict(row)
    expires_at = str(item.get('expires_at') or '').strip()
    verified_at = str(item.get('verified_at') or '').strip()
    if verified_at:
        return None
    try:
        if expires_at and datetime.fromisoformat(expires_at) < datetime.utcnow():
            return None
    except ValueError:
        return None
    return item


def _get_shop_registration_verification_including_verified(shop_id: str, token: str) -> dict[str, Any] | None:
    normalized_shop_id = (shop_id or '').strip().lower()
    normalized_token = (token or '').strip()
    if not normalized_shop_id or not normalized_token:
        return None
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT
                id, token, shop_id, shop_name, owner_name, phone, email, login_id, password_hash,
                verification_code, expires_at, verified_at, created_at
            FROM shop_registration_verifications
            WHERE shop_id = ? AND token = ?
            LIMIT 1
            ''',
            (normalized_shop_id, normalized_token),
        ).fetchone()
    return dict(row) if row else None


def verify_shop_registration_code(shop_id: str, token: str, code: str) -> dict[str, Any] | None:
    verification = get_shop_registration_verification(shop_id, token)
    if verification is None:
        raise ValueError('確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。')
    normalized_code = ''.join(ch for ch in str(code or '') if ch.isdigit())
    if str(verification.get('verification_code') or '') != normalized_code:
        return None
    with get_connection() as conn:
        conn.execute(
            'UPDATE shop_registration_verifications SET verified_at = CURRENT_TIMESTAMP WHERE shop_id = ? AND token = ?',
            ((shop_id or '').strip().lower(), token),
        )
        conn.commit()
    return _get_shop_registration_verification_including_verified(shop_id, token)


def consume_shop_registration_verification(shop_id: str, token: str) -> dict[str, Any]:
    verification = _get_shop_registration_verification_including_verified(shop_id, token)
    if verification is None:
        raise ValueError('確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。')
    if not str(verification.get('verified_at') or '').strip():
        raise ValueError('確認コードが未認証です。')

    try:
        expires_at = datetime.fromisoformat(str(verification.get('expires_at') or '').strip())
    except ValueError as exc:
        raise ValueError('確認コードの有効期限が切れたか、無効になりました。最初からやり直してください。') from exc
    if expires_at < datetime.utcnow():
        raise ValueError('確認コードの有効期限が切れました。最初からやり直してください。')

    normalized_shop_id = str(verification.get('shop_id') or '').strip().lower()
    if shop_exists(normalized_shop_id):
        raise ValueError('この店舗IDはすでに使われています。')

    created = create_shop_with_owner(
        shop_id=normalized_shop_id,
        shop_name=str(verification.get('shop_name') or '').strip(),
        owner_name=str(verification.get('owner_name') or '').strip(),
        login_id=str(verification.get('login_id') or '').strip(),
        password_hash=str(verification.get('password_hash') or '').strip(),
        phone=str(verification.get('phone') or '').strip(),
        reply_to_email=str(verification.get('email') or '').strip().lower(),
        password='',
    )

    with get_connection() as conn:
        conn.execute('DELETE FROM shop_registration_verifications WHERE shop_id = ? AND token = ?', (normalized_shop_id, token))
        conn.commit()

    return created
def authenticate_member(shop_id: str, phone: str, password: str) -> dict[str, Any] | None:
    member = get_member_by_phone(shop_id, phone)
    if member is None or not int(member.get('is_active') or 0):
        return None
    if not verify_password(password, str(member.get('password_hash') or '')):
        return None
    if not member.get('customer_id'):
        customer = find_customer(shop_id, str(member.get('name') or ''), str(member.get('phone') or ''), str(member.get('email') or ''))
        if customer is None:
            customer = create_customer(shop_id, str(member.get('name') or ''), str(member.get('phone') or ''), str(member.get('email') or ''))
        with get_connection() as conn:
            conn.execute(
                'UPDATE members SET customer_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND shop_id = ?',
                (int(customer['id']), int(member['id']), shop_id),
            )
            conn.commit()
        member = get_member_by_id(shop_id, int(member['id'])) or member
    return member


def get_member_reservations(shop_id: str, member_id: int) -> list[dict[str, Any]]:
    member = get_member_by_id(shop_id, member_id)
    if member is None or not member.get('customer_id'):
        return []
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT
                r.id,
                r.shop_id,
                r.customer_id,
                r.customer_name,
                r.customer_email,
                r.receive_email,
                r.staff_id,
                r.staff_name,
                r.menu_id,
                r.menu_name,
                r.duration,
                r.price,
                r.reservation_date,
                r.start_time,
                r.end_time,
                r.status,
                r.source,
                r.created_at,
                s.shop_name
            FROM reservations r
            JOIN shops s ON s.shop_id = r.shop_id
            WHERE r.shop_id = ? AND r.customer_id = ?
            ORDER BY r.reservation_date DESC, r.start_time DESC, r.id DESC
            ''',
            (shop_id, int(member['customer_id'])),
        ).fetchall()
    return _rows_to_dicts(rows)


def get_member_all_reservations(phone_or_normalized: str) -> list[dict[str, Any]]:
    normalized_phone = normalize_member_phone(phone_or_normalized)
    if not normalized_phone:
        return []
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT
                r.id,
                r.shop_id,
                r.customer_id,
                r.customer_name,
                r.customer_email,
                r.receive_email,
                r.staff_id,
                r.staff_name,
                r.menu_id,
                r.menu_name,
                r.duration,
                r.price,
                r.reservation_date,
                r.start_time,
                r.end_time,
                r.status,
                r.source,
                r.created_at,
                s.shop_name
            FROM members m
            JOIN reservations r ON r.shop_id = m.shop_id AND r.customer_id = m.customer_id
            JOIN shops s ON s.shop_id = r.shop_id
            WHERE m.phone_normalized = ? AND m.is_active = 1 AND m.customer_id IS NOT NULL
            ORDER BY r.reservation_date DESC, r.start_time DESC, r.id DESC
            ''',
            (normalized_phone,),
        ).fetchall()
    return _rows_to_dicts(rows)


# chat

def get_member_by_customer_id(shop_id: str, customer_id: int) -> dict[str, Any] | None:
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, shop_id, customer_id, name, phone, phone_normalized, email,
                   email_reminder_enabled, password_hash, is_active, created_at, updated_at
            FROM members
            WHERE shop_id = ? AND customer_id = ? AND is_active = 1
            ORDER BY id ASC
            LIMIT 1
            ''',
            (shop_id, customer_id),
        ).fetchone()
    return dict(row) if row else None


def get_member_by_phone_normalized(shop_id: str, phone_normalized: str) -> dict[str, Any] | None:
    normalized_phone = normalize_member_phone(phone_normalized)
    if not normalized_phone:
        return None
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT id, shop_id, customer_id, name, phone, phone_normalized, email,
                   email_reminder_enabled, password_hash, is_active, created_at, updated_at
            FROM members
            WHERE shop_id = ? AND phone_normalized = ? AND is_active = 1
            ORDER BY id ASC
            LIMIT 1
            ''',
            (shop_id, normalized_phone),
        ).fetchone()
    return dict(row) if row else None


def get_member_linked_shops(phone_or_normalized: str) -> list[dict[str, Any]]:
    normalized_phone = normalize_member_phone(phone_or_normalized)
    if not normalized_phone:
        return []
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT m.id AS member_id, m.shop_id, m.customer_id, m.name AS member_name, m.phone,
                   s.shop_name, s.primary_color, s.primary_dark, s.accent_bg,
                   c.name AS customer_name
            FROM members m
            JOIN shops s ON s.shop_id = m.shop_id
            LEFT JOIN customers c ON c.id = m.customer_id AND c.shop_id = m.shop_id
            WHERE m.phone_normalized = ? AND m.is_active = 1
            ORDER BY s.shop_name COLLATE NOCASE ASC, m.id ASC
            ''',
            (normalized_phone,),
        ).fetchall()
    return _rows_to_dicts(rows)


def deactivate_member(shop_id: str, member_id: int) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            '''
            UPDATE members
            SET is_active = 0, updated_at = CURRENT_TIMESTAMP
            WHERE shop_id = ? AND id = ? AND is_active = 1
            ''',
            (shop_id, int(member_id)),
        )
        conn.commit()
    return bool(cursor.rowcount)


def deactivate_members_by_phone(phone_or_normalized: str) -> int:
    normalized_phone = normalize_member_phone(phone_or_normalized)
    if not normalized_phone:
        return 0
    with get_connection() as conn:
        cursor = conn.execute(
            '''
            UPDATE members
            SET is_active = 0, updated_at = CURRENT_TIMESTAMP
            WHERE phone_normalized = ? AND is_active = 1
            ''',
            (normalized_phone,),
        )
        conn.commit()
    return int(cursor.rowcount or 0)


def list_chat_messages(shop_id: str, customer_id: int, limit: int = 200, member_id: int | None = None) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit or 200), 500))
    params: list[Any] = [shop_id, customer_id]
    member_clause = ''
    if member_id is not None:
        member_clause = ' AND member_id = ?'
        params.append(int(member_id))
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, shop_id, customer_id, member_id, sender_type, body, is_read, created_at
            FROM chat_messages
            WHERE shop_id = ? AND customer_id = ?{member_clause}
            ORDER BY id ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return _rows_to_dicts(rows)


def get_latest_chat_member_id(shop_id: str, customer_id: int) -> int | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT member_id
            FROM chat_messages
            WHERE shop_id = ? AND customer_id = ? AND member_id IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (shop_id, customer_id),
        ).fetchone()
    if not row:
        return None
    try:
        return int(row['member_id'])
    except (TypeError, ValueError, KeyError):
        return None


def create_chat_message(shop_id: str, customer_id: int, member_id: int | None, sender_type: str, body: str) -> dict[str, Any]:
    cleaned = str(body or '').strip()
    if not cleaned:
        raise ValueError('メッセージを入力してください。')
    sender = str(sender_type or '').strip().lower()
    if sender not in {'member', 'staff'}:
        raise ValueError('送信種別が不正です。')
    with get_connection() as conn:
        cursor = conn.execute(
            '''
            INSERT INTO chat_messages (shop_id, customer_id, member_id, sender_type, body, is_read)
            VALUES (?, ?, ?, ?, ?, 0)
            ''',
            (shop_id, customer_id, member_id, sender, cleaned),
        )
        message_id = int(cursor.lastrowid)
        conn.commit()
        row = conn.execute(
            '''
            SELECT id, shop_id, customer_id, member_id, sender_type, body, is_read, created_at
            FROM chat_messages
            WHERE id = ?
            LIMIT 1
            ''',
            (message_id,),
        ).fetchone()
    if not row:
        raise RuntimeError('メッセージの保存に失敗しました。')
    return dict(row)



def purge_old_audit_logs(retention_days: int = 90) -> int:
    cutoff = (datetime.utcnow() - timedelta(days=max(int(retention_days or 90), 1))).strftime('%Y-%m-%d %H:%M:%S')
    with get_connection() as conn:
        cursor = conn.execute('DELETE FROM audit_logs WHERE occurred_at < ?', (cutoff,))
        return int(cursor.rowcount or 0)


def create_audit_log(
    *,
    actor_type: str,
    action: str,
    actor_id: str = '',
    actor_name: str = '',
    shop_id: str = '',
    target_type: str = '',
    target_id: str = '',
    target_label: str = '',
    status: str = 'success',
    method: str = '',
    path: str = '',
    ip_address: str = '',
    user_agent: str = '',
    detail: dict[str, Any] | None = None,
    retention_days: int = 90,
) -> dict[str, Any]:
    detail_json = json.dumps(detail or {}, ensure_ascii=False)
    with get_connection() as conn:
        cursor = conn.execute(
            '''
            INSERT INTO audit_logs (
                shop_id, actor_type, actor_id, actor_name, action,
                target_type, target_id, target_label, status,
                method, path, ip_address, user_agent, detail_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                (shop_id or '').strip(),
                (actor_type or '').strip(),
                (actor_id or '').strip(),
                (actor_name or '').strip(),
                (action or '').strip(),
                (target_type or '').strip(),
                str(target_id or '').strip(),
                (target_label or '').strip(),
                (status or 'success').strip(),
                (method or '').strip().upper(),
                (path or '').strip(),
                (ip_address or '').strip(),
                (user_agent or '').strip(),
                detail_json,
            ),
        )
        log_id = int(cursor.lastrowid or 0)
        row = conn.execute('SELECT * FROM audit_logs WHERE id = ?', (log_id,)).fetchone()
    try:
        purge_old_audit_logs(retention_days=retention_days)
    except Exception:
        pass
    return dict(row) if row else {}



def list_members_for_audit_api(shop_id: str) -> list[dict[str, Any]]:
    normalized_shop_id = (shop_id or '').strip().lower()
    if not normalized_shop_id:
        return []
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT id, shop_id, customer_id, name, phone, email, is_active, created_at, updated_at
            FROM members
            WHERE shop_id = ?
            ORDER BY is_active DESC, name COLLATE NOCASE ASC, id ASC
            ''',
            (normalized_shop_id,),
        ).fetchall()
    return [dict(row) for row in rows]



def list_audit_logs_for_api(
    *,
    shop_id: str = '',
    member_id: int | None = None,
    date_from: str = '',
    date_to: str = '',
    limit: int = 500,
    offset: int = 0,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    normalized_shop_id = (shop_id or '').strip().lower()
    if normalized_shop_id:
        clauses.append('al.shop_id = ?')
        params.append(normalized_shop_id)
    if member_id is not None:
        clauses.append("al.actor_type = 'member'")
        clauses.append('CAST(al.actor_id AS INTEGER) = ?')
        params.append(int(member_id))
    normalized_from = (date_from or '').strip()
    if normalized_from:
        clauses.append('al.occurred_at >= ?')
        params.append(normalized_from)
    normalized_to = (date_to or '').strip()
    if normalized_to:
        clauses.append('al.occurred_at <= ?')
        params.append(normalized_to)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    safe_limit = max(1, min(int(limit or 500), 1000))
    safe_offset = max(0, int(offset or 0))
    with get_connection() as conn:
        rows = conn.execute(
            f'''
            SELECT
                al.id,
                al.occurred_at,
                al.shop_id,
                s.shop_name,
                al.actor_type,
                al.actor_id,
                al.actor_name,
                m.name AS member_name,
                al.action,
                al.target_type,
                al.target_id,
                al.target_label,
                al.status,
                al.method,
                al.path,
                al.ip_address,
                al.user_agent,
                al.detail_json
            FROM audit_logs al
            LEFT JOIN shops s ON s.shop_id = al.shop_id
            LEFT JOIN members m ON al.actor_type = 'member' AND CAST(m.id AS TEXT) = al.actor_id AND m.shop_id = al.shop_id
            {where_sql}
            ORDER BY al.occurred_at DESC, al.id DESC
            LIMIT ? OFFSET ?
            ''',
            (*params, safe_limit, safe_offset),
        ).fetchall()
    return [dict(row) for row in rows]



def get_shop_detail_for_audit_api(shop_id: str) -> dict[str, Any] | None:
    normalized_shop_id = (shop_id or '').strip().lower()
    if not normalized_shop_id:
        return None
    shop = get_shop_management_data(normalized_shop_id)
    if shop is None:
        return None
    subscription = get_shop_subscription(normalized_shop_id) or {}
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM members WHERE shop_id = ? AND is_active = 1) AS active_member_count,
                (SELECT COUNT(*) FROM members WHERE shop_id = ?) AS total_member_count,
                (SELECT COUNT(*) FROM admin_users WHERE shop_id = ? AND is_active = 1) AS active_admin_count,
                (SELECT COUNT(*) FROM customers WHERE shop_id = ?) AS customer_count,
                (SELECT COUNT(*) FROM reservations WHERE shop_id = ?) AS reservation_count
            """,
            (normalized_shop_id, normalized_shop_id, normalized_shop_id, normalized_shop_id, normalized_shop_id),
        ).fetchone()
    detail = dict(shop)
    detail['subscription_status'] = str(subscription.get('status') or detail.get('subscription_status') or '')
    detail['plan_name'] = str(subscription.get('plan_name') or detail.get('plan_name') or '')
    if row:
        detail.update(dict(row))
    return detail


def get_member_detail_for_audit_api(shop_id: str, member_id: int) -> dict[str, Any] | None:
    normalized_shop_id = (shop_id or '').strip().lower()
    normalized_member_id = int(member_id)
    member = get_member_by_id(normalized_shop_id, normalized_member_id)
    if member is None:
        return None
    customer = None
    customer_id = member.get('customer_id')
    if customer_id:
        customer = get_customer_by_id(normalized_shop_id, int(customer_id))
    detail = dict(member)
    detail['member_id'] = normalized_member_id
    detail['customer'] = customer
    with get_connection() as conn:
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM reservations WHERE shop_id = ? AND customer_id = ?) AS reservation_count,
                (SELECT COUNT(*) FROM chat_messages WHERE shop_id = ? AND member_id = ?) AS chat_count
            """,
            (
                normalized_shop_id,
                int(customer_id) if customer_id else -1,
                normalized_shop_id,
                normalized_member_id,
            ),
        ).fetchone()
    if counts:
        detail.update(dict(counts))
    return detail


def update_shop_for_audit_api(
    shop_id: str,
    *,
    shop_name: str,
    phone: str = '',
    address: str = '',
) -> dict[str, Any]:
    normalized_shop_id = (shop_id or '').strip().lower()
    current = get_shop_management_data(normalized_shop_id)
    if current is None:
        raise ValueError('店舗が見つかりません。')
    update_shop_basic_info(
        normalized_shop_id,
        shop_name=(shop_name or current.get('shop_name') or '').strip(),
        phone=(phone or '').strip(),
        address=(address or '').strip(),
        business_hours=str(current.get('business_hours') or '10:00〜19:00'),
        holiday=str(current.get('holiday') or '火曜日'),
        catch_copy=str(current.get('catch_copy') or ''),
        description=str(current.get('description') or ''),
        reply_to_email=str(current.get('reply_to_email') or ''),
        admin_ui_mode=str(current.get('admin_ui_mode') or 'web'),
        primary_color=str(current.get('primary_color') or '#2ec4b6'),
        primary_dark=str(current.get('primary_dark') or '#159a90'),
        accent_bg=str(current.get('accent_bg') or '#f7fffe'),
        heading_bg_color=str(current.get('heading_bg_color') or '#ff6f91'),
        menus=current.get('menus') or current.get('menus_json') or [],
    )
    return get_shop_detail_for_audit_api(normalized_shop_id) or {}


def update_member_for_audit_api(
    shop_id: str,
    member_id: int,
    *,
    name: str,
    phone: str,
    email: str = '',
) -> dict[str, Any]:
    normalized_shop_id = (shop_id or '').strip().lower()
    current = get_member_by_id(normalized_shop_id, int(member_id))
    if current is None:
        raise ValueError('会員が見つかりません。')
    cleaned_name = str(name or '').strip()
    normalized_phone = normalize_member_phone(phone)
    normalized_email = str(email or '').strip().lower()
    if not cleaned_name:
        raise ValueError('名前を入力してください。')
    if not normalized_phone:
        raise ValueError('電話番号を入力してください。')
    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT id FROM members
            WHERE shop_id = ? AND phone_normalized = ? AND id <> ?
            LIMIT 1
            """,
            (normalized_shop_id, normalized_phone, int(member_id)),
        ).fetchone()
        if existing:
            raise ValueError('この電話番号は別会員で使用されています。')
        conn.execute(
            """
            UPDATE members
            SET name = ?,
                phone = ?,
                phone_normalized = ?,
                email = ?,
                email_reminder_enabled = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE shop_id = ? AND id = ?
            """,
            (
                cleaned_name,
                normalized_phone,
                normalized_phone,
                normalized_email,
                1 if normalized_email else 0,
                normalized_shop_id,
                int(member_id),
            ),
        )
        customer_id = current.get('customer_id')
        if customer_id:
            conn.execute(
                """
                UPDATE customers
                SET name = ?, phone = ?, email = ?
                WHERE shop_id = ? AND id = ?
                """,
                (cleaned_name, normalized_phone, normalized_email, normalized_shop_id, int(customer_id)),
            )
        conn.commit()
    return get_member_detail_for_audit_api(normalized_shop_id, int(member_id)) or {}


def force_cancel_member_for_audit_api(shop_id: str, member_id: int) -> dict[str, Any]:
    normalized_shop_id = (shop_id or '').strip().lower()
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE members
            SET is_active = 0, updated_at = CURRENT_TIMESTAMP
            WHERE shop_id = ? AND id = ?
            """,
            (normalized_shop_id, int(member_id)),
        )
        conn.commit()
    return get_member_detail_for_audit_api(normalized_shop_id, int(member_id)) or {}


def force_cancel_shop_for_audit_api(shop_id: str) -> dict[str, Any]:
    normalized_shop_id = (shop_id or '').strip().lower()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO subscriptions (shop_id, plan_id, status)
            VALUES (?, COALESCE((SELECT plan_id FROM subscriptions WHERE shop_id = ?), 1), 'cancelled')
            ON CONFLICT(shop_id) DO UPDATE SET
                status = 'cancelled'
            """,
            (normalized_shop_id, normalized_shop_id),
        )
        conn.execute('UPDATE admin_users SET is_active = 0 WHERE shop_id = ?', (normalized_shop_id,))
        conn.execute('UPDATE members SET is_active = 0, updated_at = CURRENT_TIMESTAMP WHERE shop_id = ?', (normalized_shop_id,))
        conn.commit()
    return get_shop_detail_for_audit_api(normalized_shop_id) or {}


def restore_member_for_audit_api(shop_id: str, member_id: int) -> dict[str, Any]:
    normalized_shop_id = (shop_id or '').strip().lower()
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE members
            SET is_active = 1, updated_at = CURRENT_TIMESTAMP
            WHERE shop_id = ? AND id = ?
            """,
            (normalized_shop_id, int(member_id)),
        )
        conn.commit()
    return get_member_detail_for_audit_api(normalized_shop_id, int(member_id)) or {}


def restore_shop_for_audit_api(shop_id: str) -> dict[str, Any]:
    normalized_shop_id = (shop_id or '').strip().lower()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO subscriptions (shop_id, plan_id, status)
            VALUES (?, COALESCE((SELECT plan_id FROM subscriptions WHERE shop_id = ?), 1), 'active')
            ON CONFLICT(shop_id) DO UPDATE SET
                status = 'active'
            """,
            (normalized_shop_id, normalized_shop_id),
        )
        conn.execute('UPDATE admin_users SET is_active = 1 WHERE shop_id = ?', (normalized_shop_id,))
        conn.execute('UPDATE members SET is_active = 1, updated_at = CURRENT_TIMESTAMP WHERE shop_id = ?', (normalized_shop_id,))
        conn.commit()
    return get_shop_detail_for_audit_api(normalized_shop_id) or {}

def mark_chat_messages_read_for_admin(shop_id: str, customer_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            '''
            UPDATE chat_messages
            SET is_read = 1
            WHERE shop_id = ? AND customer_id = ? AND sender_type = 'member' AND is_read = 0
            ''',
            (shop_id, customer_id),
        )
        conn.commit()


def mark_chat_messages_read_for_member(shop_id: str, customer_id: int, member_id: int | None = None) -> None:
    params = [shop_id, customer_id]
    member_clause = ''
    if member_id is not None:
        member_clause = ' AND member_id = ?'
        params.append(int(member_id))
    with get_connection() as conn:
        conn.execute(
            f'''
            UPDATE chat_messages
            SET is_read = 1
            WHERE shop_id = ? AND customer_id = ? AND sender_type = 'staff' AND is_read = 0{member_clause}
            ''',
            tuple(params),
        )
        conn.commit()


def count_member_chat_messages_in_month(shop_id: str, customer_id: int, year_month: str | None = None) -> int:
    target = (year_month or datetime.now().strftime('%Y-%m')).strip()
    like_value = f'{target}-%'
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT COUNT(*) AS count_value
            FROM chat_messages
            WHERE shop_id = ? AND customer_id = ? AND sender_type = 'member' AND created_at LIKE ?
            ''',
            (shop_id, customer_id, like_value),
        ).fetchone()
    return int((row or {'count_value': 0})['count_value'] or 0)


def count_shop_chat_messages_in_month(shop_id: str, year_month: str | None = None) -> int:
    target = (year_month or datetime.now().strftime('%Y-%m')).strip()
    like_value = f'{target}-%'
    with get_connection() as conn:
        row = conn.execute(
            '''
            SELECT COUNT(*) AS count_value
            FROM chat_messages
            WHERE shop_id = ? AND sender_type = 'staff' AND created_at LIKE ?
            ''',
            (shop_id, like_value),
        ).fetchone()
    return int((row or {'count_value': 0})['count_value'] or 0)


def get_admin_unread_chat_summary(shop_id: str) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT
                c.id AS customer_id,
                c.name AS customer_name,
                c.phone AS customer_phone,
                MAX(cm.created_at) AS latest_created_at,
                COUNT(CASE WHEN cm.sender_type = 'member' AND cm.is_read = 0 THEN 1 END) AS unread_count,
                (
                    SELECT body
                    FROM chat_messages cm2
                    WHERE cm2.shop_id = cm.shop_id AND cm2.customer_id = cm.customer_id
                    ORDER BY cm2.id DESC
                    LIMIT 1
                ) AS latest_body
            FROM chat_messages cm
            JOIN customers c ON c.id = cm.customer_id AND c.shop_id = cm.shop_id
            WHERE cm.shop_id = ?
            GROUP BY c.id, c.name, c.phone
            HAVING unread_count > 0
            ORDER BY latest_created_at DESC, c.id DESC
            ''',
            (shop_id,),
        ).fetchall()
    return _rows_to_dicts(rows)


def get_member_unread_chat_summary(phone_or_normalized: str) -> list[dict[str, Any]]:
    normalized_phone = normalize_member_phone(phone_or_normalized)
    if not normalized_phone:
        return []
    with get_connection() as conn:
        rows = conn.execute(
            '''
            SELECT
                m.shop_id,
                m.id AS member_id,
                m.customer_id,
                s.shop_name,
                COUNT(CASE WHEN cm.sender_type = 'staff' AND cm.is_read = 0 THEN 1 END) AS unread_count,
                MAX(cm.created_at) AS latest_created_at,
                (
                    SELECT body
                    FROM chat_messages cm2
                    WHERE cm2.shop_id = m.shop_id AND cm2.customer_id = m.customer_id
                    ORDER BY cm2.id DESC
                    LIMIT 1
                ) AS latest_body
            FROM members m
            JOIN shops s ON s.shop_id = m.shop_id
            LEFT JOIN chat_messages cm ON cm.shop_id = m.shop_id AND cm.customer_id = m.customer_id
            WHERE m.phone_normalized = ? AND m.is_active = 1 AND m.customer_id IS NOT NULL
            GROUP BY m.shop_id, m.id, m.customer_id, s.shop_name
            HAVING unread_count > 0
            ORDER BY latest_created_at DESC, s.shop_name COLLATE NOCASE ASC
            ''',
            (normalized_phone,),
        ).fetchall()
    return _rows_to_dicts(rows)
