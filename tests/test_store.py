import asyncio
import json
from datetime import date

from leaderboard import keys
from leaderboard.store import Kill

DAY = date(2026, 11, 13)


def kill(killer="p1", victim="p2", distance=100.0, headshot=False, weapon="AKM"):
    names = {"p1": "Lobo", "p2": "Corvo", "p3": "Raposa", "p4": "Tatu"}
    return Kill(killer, names.get(killer, killer), victim, names.get(victim, victim), weapon, distance, headshot)


def test_kill_updates_every_period_and_only_counts_headshots_when_they_happen(lb):
    async def t(store, redis):
        await store.record_kill(kill(headshot=True), DAY)
        await store.record_kill(kill(), DAY)
        for period in keys.PERIODS:
            assert await redis.zscore(keys.board("kills", period, DAY), "p1") == 2
            assert await redis.zscore(keys.board("headshots", period, DAY), "p1") == 1

    lb(t)


def test_longest_shot_keeps_the_record(lb):
    async def t(store, redis):
        await store.record_kill(kill(distance=420.5), DAY)
        await store.record_kill(kill(distance=90.0), DAY)
        assert await redis.zscore(keys.board("longest", "all", DAY), "p1") == 420.5

    lb(t)


def test_daily_and_weekly_boards_expire_but_all_time_does_not(lb):
    async def t(store, redis):
        await store.record_kill(kill(), DAY)
        day_ttl = await redis.ttl(keys.board("kills", "day", DAY))
        week_ttl = await redis.ttl(keys.board("kills", "week", DAY))
        assert 0 < day_ttl <= keys.DAY_TTL
        assert keys.DAY_TTL < week_ttl <= keys.WEEK_TTL
        assert await redis.ttl(keys.board("kills", "all", DAY)) == -1

    lb(t)


def test_expiry_is_not_renewed_by_new_kills(lb):
    async def t(store, redis):
        await store.record_kill(kill(), DAY)
        await redis.expire(keys.board("kills", "day", DAY), 100)
        await store.record_kill(kill(), DAY)
        assert await redis.ttl(keys.board("kills", "day", DAY)) <= 100

    lb(t)


def test_top_comes_ordered_with_names(lb):
    async def t(store, redis):
        for killer, n in (("p1", 3), ("p2", 5), ("p3", 1)):
            for _ in range(n):
                await store.record_kill(kill(killer=killer, victim="p4"), DAY)
        top = await store.top("kills", "day", 3, DAY)
        assert [(e.rank, e.name, e.score) for e in top] == [(1, "Corvo", 5), (2, "Lobo", 3), (3, "Raposa", 1)]

    lb(t)


def test_around_shows_who_is_just_above_and_below(lb):
    async def t(store, redis):
        for i in range(10):
            await redis.zadd(keys.board("kills"), {f"x{i}": i * 10})
        rows = await store.around("x5", "kills", span=2)
        assert [(e.rank, e.player_id) for e in rows] == [(3, "x7"), (4, "x6"), (5, "x5"), (6, "x4"), (7, "x3")]
        top = await store.around("x9", "kills", span=2)
        assert [e.rank for e in top] == [1, 2, 3]
        assert await store.around("ninguem", "kills") is None

    lb(t)


def test_player_profile_has_counts_and_ranks(lb):
    async def t(store, redis):
        await store.record_kill(kill(killer="p1", victim="p2", distance=300, headshot=True), DAY)
        await store.record_kill(kill(killer="p2", victim="p1"), DAY)
        await store.record_kill(kill(killer="p1", victim="p3"), DAY)
        profile = await store.player("p1")
        assert profile["name"] == "Lobo"
        assert (profile["kills"], profile["deaths"]) == (2, 1)
        assert profile["ranks"]["kills"] == {"rank": 1, "score": 2.0}
        assert await store.player("ninguem") is None

    lb(t)


def test_summary_is_cached_and_a_new_kill_invalidates_it(lb):
    async def t(store, redis):
        await store.record_kill(kill(distance=250), DAY)
        first, hit1 = await store.summary()
        second, hit2 = await store.summary()
        assert (hit1, hit2) == (False, True)
        assert first == second
        assert first["events"] == 1 and first["players"] == 2
        assert first["longest_shot"]["name"] == "Lobo"

        await store.record_kill(kill(), DAY)
        third, hit3 = await store.summary()
        assert hit3 is False
        assert third["events"] == 2

    lb(t)


def test_each_kill_is_published_to_the_live_channel(lb):
    async def t(store, redis):
        pubsub = redis.pubsub()
        await pubsub.subscribe(keys.CHANNEL)
        await pubsub.get_message(timeout=1)  # confirmação da inscrição
        await store.record_kill(kill(weapon="Mosin9130", headshot=True), DAY)
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2)
        event = json.loads(message["data"])
        assert (event["killer_name"], event["weapon"], event["headshot"]) == ("Lobo", "Mosin9130", True)
        await pubsub.aclose()

    lb(t)


def test_concurrent_kills_are_never_lost(lb):
    async def t(store, redis):
        await asyncio.gather(*(store.record_kill(kill(), DAY) for _ in range(200)))
        assert await redis.zscore(keys.board("kills"), "p1") == 200
        assert int(await redis.hget(keys.player("p2"), "deaths")) == 200
        assert int(await redis.get(keys.EVENTS)) == 200

    lb(t)


def test_rate_limit_blocks_after_the_limit_and_says_when_to_retry(lb):
    async def t(store, redis):
        results = [await store.allow("cliente", limit=5, window_ms=10_000) for _ in range(6)]
        assert [ok for ok, _, _ in results] == [True] * 5 + [False]
        assert 0 < results[-1][2] <= 10_000

    lb(t)


def test_rate_limit_holds_under_concurrency(lb):
    async def t(store, redis):
        results = await asyncio.gather(*(store.allow("cliente", limit=10, window_ms=10_000) for _ in range(50)))
        assert sum(ok for ok, _, _ in results) == 10

    lb(t)


def test_rate_limit_window_slides(lb):
    async def t(store, redis):
        for _ in range(3):
            assert (await store.allow("c", limit=3, window_ms=300))[0]
        assert not (await store.allow("c", limit=3, window_ms=300))[0]
        await asyncio.sleep(0.35)
        assert (await store.allow("c", limit=3, window_ms=300))[0]

    lb(t)
