"""Acesso ao Redis: rankings, perfil de jogador, resumo com cache e eventos ao vivo."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone

from redis.asyncio import BlockingConnectionPool, Redis

from . import keys
from .scripts import RECORD_KILL, SLIDING_WINDOW

SUMMARY_TTL = 30  # segundos


def connect(url: str, max_connections: int = 50) -> Redis:
    """Pool que espera conexão livre em vez de dar erro. O pool padrão do redis-py 8 levanta
    "Too many connections" num pico de requisições (achei isso no teste de 200 kills simultâneas)."""
    pool = BlockingConnectionPool.from_url(url, max_connections=max_connections, timeout=5, decode_responses=True)
    return Redis(connection_pool=pool)


@dataclass(frozen=True)
class Kill:
    killer_id: str
    killer_name: str
    victim_id: str
    victim_name: str
    weapon: str
    distance: float
    headshot: bool


@dataclass(frozen=True)
class Entry:
    rank: int
    player_id: str
    name: str
    score: float


class LeaderboardStore:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._record = redis.register_script(RECORD_KILL)
        self._limit = redis.register_script(SLIDING_WINDOW)

    async def record_kill(self, kill: Kill, day: date | None = None) -> int:
        day = day or keys.today()
        boards = [keys.board(m, p, day) for m in keys.METRICS for p in keys.PERIODS]
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        event = json.dumps({"type": "kill", "at": now, **asdict(kill)}, ensure_ascii=False)
        score = await self._record(
            keys=[*boards, keys.player(kill.killer_id), keys.player(kill.victim_id),
                  keys.EVENTS, keys.PLAYERS, keys.SUMMARY_CACHE],
            args=[kill.killer_id, kill.killer_name, kill.victim_id, kill.victim_name,
                  int(kill.headshot), round(kill.distance, 1), keys.DAY_TTL, keys.WEEK_TTL, now,
                  event, keys.CHANNEL],
        )
        return int(float(score))

    async def top(self, metric: str, period: str = "all", limit: int = 10, day: date | None = None) -> list[Entry]:
        rows = await self.redis.zrevrange(keys.board(metric, period, day), 0, limit - 1, withscores=True)
        return await self._entries(rows, first_rank=1)

    async def around(self, player_id: str, metric: str, period: str = "all", span: int = 2,
                     day: date | None = None) -> list[Entry] | None:
        key = keys.board(metric, period, day)
        rank = await self.redis.zrevrank(key, player_id)
        if rank is None:
            return None
        start = max(0, rank - span)
        rows = await self.redis.zrevrange(key, start, rank + span, withscores=True)
        return await self._entries(rows, first_rank=start + 1)

    async def player(self, player_id: str) -> dict | None:
        data = await self.redis.hgetall(keys.player(player_id))
        if not data:
            return None
        async with self.redis.pipeline(transaction=False) as pipe:
            for metric in keys.METRICS:
                pipe.zrevrank(keys.board(metric), player_id)
                pipe.zscore(keys.board(metric), player_id)
            ranks = await pipe.execute()
        return {
            "id": player_id,
            "name": data.get("name", player_id),
            "kills": int(data.get("kills", 0)),
            "deaths": int(data.get("deaths", 0)),
            "last_seen": data.get("last_seen"),
            "ranks": {
                metric: {"rank": None if r is None else r + 1, "score": s}
                for metric, r, s in zip(keys.METRICS, ranks[0::2], ranks[1::2])
            },
        }

    async def summary(self) -> tuple[dict, bool]:
        """Resumo do servidor, com cache-aside: devolve (dados, veio_do_cache).
        Cada kill nova apaga o cache dentro do script, então nunca fica desatualizado."""
        cached = await self.redis.get(keys.SUMMARY_CACHE)
        if cached:
            return json.loads(cached), True

        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.get(keys.EVENTS)
            pipe.scard(keys.PLAYERS)
            pipe.zrevrange(keys.board("kills", "day"), 0, 0, withscores=True)
            pipe.zrevrange(keys.board("longest"), 0, 0, withscores=True)
            events, players, top_today, longest = await pipe.execute()

        async def named(rows):
            if not rows:
                return None
            pid, score = rows[0]
            name = await self.redis.hget(keys.player(pid), "name")
            return {"player_id": pid, "name": name or pid, "score": score}

        data = {
            "events": int(events or 0),
            "players": players,
            "top_killer_today": await named(top_today),
            "longest_shot": await named(longest),
        }
        await self.redis.set(keys.SUMMARY_CACHE, json.dumps(data), ex=SUMMARY_TTL)
        return data, False

    async def allow(self, client: str, limit: int, window_ms: int) -> tuple[bool, int, int]:
        """Janela deslizante: (liberado, requisições na janela, ms até liberar)."""
        ok, count, retry = await self._limit(keys=[keys.rate_limit(client)],
                                             args=[window_ms, limit, uuid.uuid4().hex])
        return bool(ok), int(count), int(retry)

    async def _entries(self, rows: list[tuple[str, float]], first_rank: int) -> list[Entry]:
        if not rows:
            return []
        async with self.redis.pipeline(transaction=False) as pipe:
            for player_id, _ in rows:
                pipe.hget(keys.player(player_id), "name")
            names = await pipe.execute()
        return [
            Entry(rank=first_rank + i, player_id=pid, name=name or pid, score=score)
            for i, ((pid, score), name) in enumerate(zip(rows, names))
        ]
