"""API HTTP. Rodar com: uvicorn leaderboard.api:app"""

from __future__ import annotations

import asyncio
import os
import secrets
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from . import keys
from .store import Kill, LeaderboardStore, connect

Metric = Literal["kills", "headshots", "longest"]
Period = Literal["all", "day", "week"]

REDIS_URL = os.environ.get("LB_REDIS_URL", "redis://127.0.0.1:6379/0")
API_KEY = os.environ.get("LB_API_KEY", "dev-key")
RATE_LIMIT = int(os.environ.get("LB_RATE_LIMIT", "120"))        # requisições
RATE_WINDOW_MS = int(os.environ.get("LB_RATE_WINDOW_MS", "60000"))  # por minuto
STATIC = Path(__file__).parent / "static"


class KillIn(BaseModel):
    killer_id: str = Field(min_length=1, max_length=64)
    killer_name: str = Field(min_length=1, max_length=64)
    victim_id: str = Field(min_length=1, max_length=64)
    victim_name: str = Field(min_length=1, max_length=64)
    weapon: str = Field(min_length=1, max_length=64)
    distance: float = Field(ge=0, le=5000)
    headshot: bool = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = connect(REDIS_URL)
    app.state.store = LeaderboardStore(app.state.redis)
    yield
    await app.state.redis.aclose()


app = FastAPI(title="Realtime Leaderboard", lifespan=lifespan)


def store(request: Request) -> LeaderboardStore:
    return request.app.state.store


async def require_key(response: Response, x_api_key: str | None = Header(default=None),
                      lb: LeaderboardStore = Depends(store)) -> None:
    if not x_api_key or not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(401, "X-Api-Key inválida")
    ok, count, retry_ms = await lb.allow(x_api_key, RATE_LIMIT, RATE_WINDOW_MS)
    response.headers["X-RateLimit-Limit"] = str(RATE_LIMIT)
    response.headers["X-RateLimit-Remaining"] = str(max(0, RATE_LIMIT - count))
    if not ok:
        raise HTTPException(429, "limite de requisições atingido",
                            headers={"Retry-After": str(max(1, retry_ms // 1000))})


@app.post("/events/kill", status_code=201, dependencies=[Depends(require_key)])
async def post_kill(kill: KillIn, lb: LeaderboardStore = Depends(store)):
    if kill.killer_id == kill.victim_id:
        raise HTTPException(422, "matador e vítima são o mesmo jogador")
    kills = await lb.record_kill(Kill(**kill.model_dump()))
    return {"killer_id": kill.killer_id, "kills": kills}


@app.get("/boards/{metric}")
async def get_board(metric: Metric, period: Period = "all", limit: int = Query(10, ge=1, le=100),
                    lb: LeaderboardStore = Depends(store)):
    return [asdict(e) for e in await lb.top(metric, period, limit)]


@app.get("/players/{player_id}")
async def get_player(player_id: str, lb: LeaderboardStore = Depends(store)):
    data = await lb.player(player_id)
    if data is None:
        raise HTTPException(404, "jogador não encontrado")
    return data


@app.get("/players/{player_id}/around")
async def get_around(player_id: str, metric: Metric = "kills", period: Period = "all",
                     span: int = Query(2, ge=1, le=10), lb: LeaderboardStore = Depends(store)):
    rows = await lb.around(player_id, metric, period, span)
    if rows is None:
        raise HTTPException(404, "jogador fora desse ranking")
    return [asdict(e) for e in rows]


@app.get("/summary")
async def get_summary(response: Response, lb: LeaderboardStore = Depends(store)):
    data, hit = await lb.summary()
    response.headers["X-Cache"] = "HIT" if hit else "MISS"
    return data


@app.get("/stream")
async def stream(request: Request):
    """Server-Sent Events: cada kill publicada no canal do Redis chega aqui na hora."""
    redis: Redis = request.app.state.redis

    async def events():
        pubsub = redis.pubsub()
        await pubsub.subscribe(keys.CHANNEL)
        try:
            yield "retry: 3000\n\n"
            while not await request.is_disconnected():
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=15)
                # comentário a cada 15 s mantém a conexão aberta em proxy que derruba conexão parada
                yield f"data: {message['data']}\n\n" if message else ": ping\n\n"
        finally:
            await pubsub.unsubscribe(keys.CHANNEL)
            await pubsub.aclose()

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/", include_in_schema=False)
async def page():
    return FileResponse(STATIC / "index.html")


@app.get("/health", include_in_schema=False)
async def health(request: Request):
    await asyncio.wait_for(request.app.state.redis.ping(), timeout=2)
    return {"status": "ok"}
