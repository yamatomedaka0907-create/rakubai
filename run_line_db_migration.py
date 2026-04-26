from app.db import get_connection
from app.migrations.line_settings_migration import ensure_line_setting_columns


def main():
    conn = get_connection()
    try:
        ensure_line_setting_columns(conn)
        print("LINE設定カラムを shops テーブルに追加しました。")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
