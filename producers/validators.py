from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any


class ValidationError(ValueError):
    """Raised when a normalized market event fails business validation."""


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    errors: tuple[str, ...] = ()

    def raise_for_errors(self) -> None:
        if not self.is_valid:
            raise ValidationError("; ".join(self.errors))


def validate_market_event(event: dict[str, Any]) -> ValidationResult:
    event_type = event.get("event_type")
    if event_type == "trade":
        return validate_trade_event(event)
    if event_type == "kline":
        return validate_kline_event(event)
    if event_type == "ticker":
        return validate_ticker_event(event)
    return ValidationResult(False, (f"unsupported event_type: {event_type}",))


def validate_trade_event(event: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []

    require_non_empty(event, "symbol", errors)
    require_parseable_timestamp(event, "event_time", errors)
    require_present(event, "trade_id", errors)
    require_decimal_gt_zero(event, "price", errors)
    require_decimal_gt_zero(event, "quantity", errors)

    return ValidationResult(not errors, tuple(errors))


def validate_kline_event(event: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []

    require_non_empty(event, "symbol", errors)
    require_parseable_timestamp(event, "kline_start_time", errors)
    require_decimal(event, "open_price", errors)
    require_decimal(event, "high_price", errors)
    require_decimal(event, "low_price", errors)
    require_decimal(event, "close_price", errors)
    require_decimal_gte_zero(event, "volume", errors)

    return ValidationResult(not errors, tuple(errors))


def validate_ticker_event(event: dict[str, Any]) -> ValidationResult:
    errors: list[str] = []

    require_non_empty(event, "symbol", errors)
    require_parseable_timestamp(event, "event_time", errors)
    require_decimal_gt_zero(event, "last_price", errors)
    require_decimal_gte_zero(event, "volume", errors)

    return ValidationResult(not errors, tuple(errors))


def require_present(event: dict[str, Any], field: str, errors: list[str]) -> None:
    if field not in event or event[field] is None:
        errors.append(f"{field} is missing")


def require_non_empty(event: dict[str, Any], field: str, errors: list[str]) -> None:
    value = event.get(field)
    if value is None or str(value).strip() == "":
        errors.append(f"{field} is missing")


def require_parseable_timestamp(event: dict[str, Any], field: str, errors: list[str]) -> None:
    value = event.get(field)
    if value is None or str(value).strip() == "":
        errors.append(f"{field} is missing")
        return

    try:
        parse_utc_timestamp(str(value))
    except ValueError:
        errors.append(f"{field} is not a valid UTC timestamp")


def require_decimal(event: dict[str, Any], field: str, errors: list[str]) -> None:
    value = event.get(field)
    if value is None or str(value).strip() == "":
        errors.append(f"{field} is missing")
        return

    try:
        Decimal(str(value))
    except InvalidOperation:
        errors.append(f"{field} is not numeric")


def require_decimal_gt_zero(event: dict[str, Any], field: str, errors: list[str]) -> None:
    value = event.get(field)
    if value is None or str(value).strip() == "":
        errors.append(f"{field} is missing")
        return

    try:
        decimal_value = Decimal(str(value))
    except InvalidOperation:
        errors.append(f"{field} is not numeric")
        return

    if decimal_value <= 0:
        errors.append(f"{field} must be greater than zero")


def require_decimal_gte_zero(event: dict[str, Any], field: str, errors: list[str]) -> None:
    value = event.get(field)
    if value is None or str(value).strip() == "":
        errors.append(f"{field} is missing")
        return

    try:
        decimal_value = Decimal(str(value))
    except InvalidOperation:
        errors.append(f"{field} is not numeric")
        return

    if decimal_value < 0:
        errors.append(f"{field} must be greater than or equal to zero")


def parse_utc_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return parsed
