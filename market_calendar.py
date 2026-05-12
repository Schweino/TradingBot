from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional


def _parse_day(day: str | date) -> date:
    if isinstance(day, date):
        return day
    return datetime.strptime(str(day), '%Y-%m-%d').date()


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    cur = date(year, month, 1)
    while cur.weekday() != weekday:
        cur += timedelta(days=1)
    return cur + timedelta(days=7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cur = date(year, 12, 31)
    else:
        cur = date(year, month + 1, 1) - timedelta(days=1)
    while cur.weekday() != weekday:
        cur -= timedelta(days=1)
    return cur


def _easter(year: int) -> date:
    # Anonymous Gregorian algorithm.
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def market_holidays(year: int) -> dict[date, str]:
    holidays = {
        _observed(date(year, 1, 1)): "New Year's Day",
        _nth_weekday(year, 1, 0, 3): 'Martin Luther King Jr. Day',
        _nth_weekday(year, 2, 0, 3): "Washington's Birthday",
        _easter(year) - timedelta(days=2): 'Good Friday',
        _last_weekday(year, 5, 0): 'Memorial Day',
        _observed(date(year, 6, 19)): 'Juneteenth National Independence Day',
        _observed(date(year, 7, 4)): 'Independence Day',
        _nth_weekday(year, 9, 0, 1): 'Labor Day',
        _nth_weekday(year, 11, 3, 4): 'Thanksgiving Day',
        _observed(date(year, 12, 25)): 'Christmas Day',
    }
    # If New Year's Day for the following year is observed on Dec 31, include it
    # in the current year's holiday set.
    next_new_year = _observed(date(year + 1, 1, 1))
    if next_new_year.year == year:
        holidays[next_new_year] = "New Year's Day observed"
    return holidays


def market_calendar_status(day: str | date) -> dict:
    d = _parse_day(day)
    if d.weekday() >= 5:
        return {
            'day': d.isoformat(),
            'is_trading_day': False,
            'reason': 'weekend',
        }
    holidays = market_holidays(d.year)
    if d in holidays:
        return {
            'day': d.isoformat(),
            'is_trading_day': False,
            'reason': holidays[d],
        }
    return {
        'day': d.isoformat(),
        'is_trading_day': True,
        'reason': 'regular_session',
    }


def is_trading_day(day: str | date) -> bool:
    return bool(market_calendar_status(day).get('is_trading_day'))


def main(argv: Optional[list[str]] = None) -> int:
    import argparse
    import json

    ap = argparse.ArgumentParser(description='Local US equity market-day calendar check.')
    ap.add_argument('day')
    args = ap.parse_args(argv)
    print(json.dumps(market_calendar_status(args.day), indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
