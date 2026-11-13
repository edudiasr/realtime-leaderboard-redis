"""Nomes das chaves no Redis. Ficam todos aqui pra não ter string solta espalhada pelo código."""

from __future__ import annotations

from datetime import date, datetime, timezone

METRICS = ("kills", "headshots", "longest")
PERIODS = ("all", "day", "week")

# quanto tempo os rankings do dia e da semana ficam guardados depois de abertos
DAY_TTL = 8 * 24 * 3600
WEEK_TTL = 35 * 24 * 3600

CHANNEL = "lb:updates"
PLAYERS = "lb:players"
EVENTS = "lb:events"
SUMMARY_CACHE = "cache:summary"


def today() -> date:
    return datetime.now(timezone.utc).date()


def period_suffix(period: str, day: date) -> str:
    if period == "all":
        return "all"
    if period == "day":
        return f"d:{day.isoformat()}"
    if period == "week":
        year, week, _ = day.isocalendar()
        return f"w:{year}-W{week:02d}"
    raise ValueError(f"período inválido: {period}")


def board(metric: str, period: str = "all", day: date | None = None) -> str:
    if metric not in METRICS:
        raise ValueError(f"métrica inválida: {metric}")
    return f"lb:{metric}:{period_suffix(period, day or today())}"


def player(player_id: str) -> str:
    return f"player:{player_id}"


def rate_limit(client: str) -> str:
    return f"ratelimit:{client}"
