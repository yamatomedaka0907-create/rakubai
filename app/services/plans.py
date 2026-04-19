from __future__ import annotations

from typing import Any


def has_feature(plan: dict[str, Any] | None, feature_name: str) -> bool:
    if not plan:
        return False
    return bool(plan.get(feature_name, 0))


def within_limit(plan: dict[str, Any] | None, current_count: int, limit_field: str) -> bool:
    if not plan:
        return True
    limit_value = plan.get(limit_field)
    if limit_value in (None, 0):
        return True
    return current_count < int(limit_value)


def get_plan_label(plan: dict[str, Any] | None) -> str:
    if not plan:
        return '未設定'
    return str(plan.get('name') or plan.get('code') or '未設定')
