from __future__ import annotations
import json
from copy import deepcopy
from pathlib import Path
from .config_paths import SHOPS_PATH, PLATFORM_ADMIN_PATH, SAMPLE_SHOWCASE_PATH

def _load_json(path: Path, default):
    if not path.exists():
        return deepcopy(default)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_shops():
    return deepcopy(_load_json(SHOPS_PATH, {}))

def save_shops(data):
    _save_json(SHOPS_PATH, data)

def get_platform_admin():
    return deepcopy(_load_json(PLATFORM_ADMIN_PATH, {"id": "platform-admin", "name": "運営管理者", "login_id": "platform_admin", "password": "platform123"}))

def get_sample_showcase_payload():
    return deepcopy(_load_json(SAMPLE_SHOWCASE_PATH, {"categories": [], "samples": []}))

def get_sample_categories():
    return get_sample_showcase_payload().get("categories", [])

def get_all_samples():
    return get_sample_showcase_payload().get("samples", [])

def get_sample(category_code: str, sample_code: str):
    for sample in get_all_samples():
        if sample.get("category_code") == category_code and sample.get("code") == sample_code:
            return deepcopy(sample)
    return None
