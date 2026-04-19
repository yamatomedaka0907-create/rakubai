from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"

DB_PATH = DATA_DIR / "db" / "salon.db"
SHOPS_PATH = DATA_DIR / "bootstrap" / "shops.json"
PLATFORM_ADMIN_PATH = DATA_DIR / "bootstrap" / "platform_admin.json"
SAMPLE_SHOWCASE_PATH = DATA_DIR / "samples" / "sample_showcase.json"
UPLOADS_DIR = DATA_DIR / "uploads"
SHOP_UPLOADS_DIR = UPLOADS_DIR / "shops"
