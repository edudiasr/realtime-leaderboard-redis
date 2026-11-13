import asyncio

import pytest
from fastapi.testclient import TestClient
from redis.asyncio import Redis

from leaderboard import api

from .conftest import TEST_URL

KEY = {"X-Api-Key": "chave-de-teste"}


def body(killer="p1", victim="p2", **extra):
    return {"killer_id": killer, "killer_name": killer.upper(), "victim_id": victim, "victim_name": victim.upper(),
            "weapon": "AKM", "distance": 120.0, "headshot": False, **extra}


@pytest.fixture
def client(monkeypatch):
    async def flush():
        redis = Redis.from_url(TEST_URL)
        await redis.flushdb()
        await redis.aclose()

    asyncio.run(flush())
    monkeypatch.setattr(api, "API_KEY", "chave-de-teste")
    with TestClient(api.app) as c:
        yield c


def test_posting_a_kill_needs_the_api_key(client):
    assert client.post("/events/kill", json=body()).status_code == 401
    assert client.post("/events/kill", json=body(), headers={"X-Api-Key": "errada"}).status_code == 401
    r = client.post("/events/kill", json=body(), headers=KEY)
    assert r.status_code == 201
    assert r.json() == {"killer_id": "p1", "kills": 1}
    assert r.headers["X-RateLimit-Remaining"] == str(api.RATE_LIMIT - 1)


def test_invalid_events_are_rejected(client):
    assert client.post("/events/kill", json=body(victim="p1"), headers=KEY).status_code == 422
    assert client.post("/events/kill", json=body(distance=-5), headers=KEY).status_code == 422
    assert client.post("/events/kill", json={"killer_id": "p1"}, headers=KEY).status_code == 422


def test_rate_limit_returns_429_with_retry_after(client, monkeypatch):
    monkeypatch.setattr(api, "RATE_LIMIT", 3)
    codes = [client.post("/events/kill", json=body(), headers=KEY).status_code for _ in range(4)]
    assert codes == [201, 201, 201, 429]
    r = client.post("/events/kill", json=body(), headers=KEY)
    assert int(r.headers["Retry-After"]) >= 1


def test_boards_players_and_around(client):
    for killer in ("p1", "p1", "p3"):
        client.post("/events/kill", json=body(killer=killer), headers=KEY)

    board = client.get("/boards/kills?period=day").json()
    assert [(r["rank"], r["player_id"], r["score"]) for r in board] == [(1, "p1", 2.0), (2, "p3", 1.0)]

    player = client.get("/players/p2").json()
    assert player["deaths"] == 3

    around = client.get("/players/p3/around?span=1").json()
    assert [r["player_id"] for r in around] == ["p1", "p3"]

    assert client.get("/players/ninguem").status_code == 404
    assert client.get("/boards/dano").status_code == 422
    assert client.get("/boards/kills?limit=500").status_code == 422


def test_summary_reports_cache_hits(client):
    client.post("/events/kill", json=body(), headers=KEY)
    assert client.get("/summary").headers["X-Cache"] == "MISS"
    assert client.get("/summary").headers["X-Cache"] == "HIT"
    client.post("/events/kill", json=body(), headers=KEY)
    r = client.get("/summary")
    assert r.headers["X-Cache"] == "MISS"
    assert r.json()["events"] == 2


def test_live_page_is_served(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "EventSource" in r.text
