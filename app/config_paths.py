from __future__ import annotations

import os
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent

APP_DATA_DIR = PROJECT_ROOT / "data"
RUNTIME_DATA_DIR = Path(os.getenv("DATA_DIR", str(APP_DATA_DIR)))

DB_PATH = RUNTIME_DATA_DIR / "db" / "salon.db"
SHOPS_PATH = APP_DATA_DIR / "bootstrap" / "shops.json"
PLATFORM_ADMIN_PATH = APP_DATA_DIR / "bootstrap" / "platform_admin.json"
SAMPLE_SHOWCASE_PATH = APP_DATA_DIR / "samples" / "sample_showcase.json"
UPLOADS_DIR = RUNTIME_DATA_DIR / "uploads"
SHOP_UPLOADS_DIR = UPLOADS_DIR / "shops"
