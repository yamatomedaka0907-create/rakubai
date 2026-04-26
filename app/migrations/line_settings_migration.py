"""
LINE連携設定用のDBマイグレーション。
既存 shops テーブルに店舗ごとのLINE設定カラムを追加します。
既に存在する場合は何もしません。
"""

LINE_SETTING_COLUMNS = {
    "line_mode": "TEXT DEFAULT 'off'",
    "line_channel_access_token": "TEXT",
    "line_channel_secret": "TEXT",
    "line_liff_id": "TEXT",
    "line_official_url": "TEXT",
    "line_webhook_enabled": "INTEGER DEFAULT 0",
}


def ensure_line_setting_columns(conn):
    cursor = conn.cursor()
    existing_columns = {
        row[1]
        for row in cursor.execute("PRAGMA table_info(shops)").fetchall()
    }

    for column_name, column_type in LINE_SETTING_COLUMNS.items():
        if column_name not in existing_columns:
            cursor.execute(
                f"ALTER TABLE shops ADD COLUMN {column_name} {column_type}"
            )

    conn.commit()
