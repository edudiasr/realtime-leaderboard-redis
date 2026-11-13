import asyncio
import os

import pytest
from redis.asyncio import Redis

# banco 15 do Redis só pros testes, apagado antes de cada um
TEST_URL = os.environ.get("LB_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")
os.environ["LB_REDIS_URL"] = TEST_URL
os.environ["LB_API_KEY"] = "chave-de-teste"

from leaderboard.store import LeaderboardStore, connect  # noqa: E402


@pytest.fixture
def lb():
    """Roda uma função async com um store limpo: lb(lambda store, redis: ...)."""

    async def runner(fn):
        redis: Redis = connect(TEST_URL)
        try:
            await redis.flushdb()
            return await fn(LeaderboardStore(redis), redis)
        finally:
            await redis.aclose()

    return lambda fn: asyncio.run(runner(fn))
