"""Shared broker-realistic bracket price rounding."""
from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, InvalidOperation
from typing import Optional


PENNY = Decimal("0.01")


def _decimal_price(price: float | int | str | None) -> Optional[Decimal]:
    if price is None:
        return None
    try:
        return Decimal(str(price))
    except (InvalidOperation, ValueError):
        return None


def _round_to_tick(price: float | int | str | None, mode: str,
                   tick: Decimal = PENNY) -> Optional[float]:
    value = _decimal_price(price)
    if value is None:
        return None
    units = value / tick
    rounding = ROUND_CEILING if mode == "ceil" else ROUND_FLOOR
    return float((units.to_integral_value(rounding=rounding) * tick).quantize(tick))


def round_bracket_price(side: str, kind: str, price: float | int | str | None) -> Optional[float]:
    """Round stock bracket prices to the penny in the fill-friendly direction.

    LONG exits are sells, so TP/SL prices round down. SHORT exits are buys, so
    TP/SL prices round up. This keeps Step 2 from scoring sub-penny touches that
    Live cannot submit or realistically fill through broker brackets.
    """
    side_u = str(side or "").upper()
    if side_u == "LONG":
        return _round_to_tick(price, "floor")
    if side_u == "SHORT":
        return _round_to_tick(price, "ceil")
    value = _decimal_price(price)
    return float(value) if value is not None else None


def round_exit_brackets(side: str, sl: float | int | str | None,
                        tp: float | int | str | None) -> tuple[Optional[float], Optional[float]]:
    return round_bracket_price(side, "sl", sl), round_bracket_price(side, "tp", tp)
