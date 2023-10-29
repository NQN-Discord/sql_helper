from typing import Optional, List

import pytest
from hypothesis import given, settings, HealthCheck, assume
from hypothesis.strategies import lists, sampled_from, booleans

from sql_helper import SQLEmoji
from .base import *


@pytest.mark.asyncio
async def test_get_emote_like_no_emote(postgres):
    postgres.cur.fetchall.side_effect = [
        []
    ]
    rtn = await postgres.get_emote_like(emote_id=123)
    assert rtn is None
    assert postgres.cur.fetchall.call_count == 1


@pytest.mark.asyncio
async def test_get_emote_like_default(postgres):
    postgres.cur.fetchall.side_effect = [
        [get_sql_emoji(123)]
    ]
    rtn = await postgres.get_emote_like(emote_id=123)
    assert rtn == get_sql_emoji(123)
    assert postgres.cur.fetchall.call_count == 1


@pytest.mark.asyncio
async def test_get_emote_like_unusable_no_secondary(postgres):
    postgres.cur.fetchall.side_effect = [
        [get_sql_emoji(123, usable=False)],
        []
    ]
    rtn = await postgres.get_emote_like(emote_id=123)
    assert rtn is None
    assert postgres.cur.fetchall.call_count == 2


@pytest.mark.asyncio
async def test_get_emote_like_unusable_with_secondary(postgres):
    postgres.cur.fetchall.side_effect = [
        [get_sql_emoji(123, usable=False)],
        [get_sql_emoji(234)]
    ]
    rtn = await postgres.get_emote_like(emote_id=123)
    assert rtn == get_sql_emoji(234)
    assert postgres.cur.fetchall.call_count == 2


@pytest.mark.asyncio
async def test_get_emote_like_no_guild_no_secondary(postgres):
    postgres.cur.fetchall.side_effect = [
        [get_sql_emoji(123, guild_id=None)],
        [get_sql_emoji(234)]
    ]
    rtn = await postgres.get_emote_like(emote_id=123)
    assert rtn == get_sql_emoji(234)
    assert postgres.cur.fetchall.call_count == 2


@pytest.mark.asyncio
async def test_get_emote_like_no_require_guild(postgres):
    postgres.cur.fetchall.side_effect = [
        [get_sql_emoji(123, guild_id=None)],
    ]
    rtn = await postgres.get_emote_like(emote_id=123, require_guild=False)
    assert rtn == get_sql_emoji(123, guild_id=None)
    assert postgres.cur.fetchall.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("side_effect, expected", [
    ([None], None),
    (
        [get_sql_emoji(1, name="test", usable=True)],
        get_sql_emoji(1, name="test", usable=True)
    ),
    (
        [
            get_sql_emoji(2, name="Test", usable=True),
            get_sql_emoji(1, name="test", usable=True),
        ],
        get_sql_emoji(1, name="test", usable=True)
    ),
    (
        [
            get_sql_emoji(2, name="Test", usable=True),
            get_sql_emoji(3, name="test", usable=False),
            get_sql_emoji(1, name="test", usable=True),
        ],
        get_sql_emoji(1, name="test", usable=True)
    ),
    (
        [
            get_sql_emoji(2, name="Test", usable=True),
            get_sql_emoji(3, name="test", usable=False),
            None
        ],
        get_sql_emoji(2, name="Test", usable=True)
    ),
    (
        [
            get_sql_emoji(2, name="Test", usable=False),
            get_sql_emoji(3, name="test", usable=True),
        ],
        get_sql_emoji(3, name="test", usable=True)
    ),
    (
        [
            get_sql_emoji(2, name="test", usable=False),
            None,
            get_sql_emoji(4, name="Test", usable=True),
        ],
        get_sql_emoji(4, name="Test", usable=True)
    ),
    (
        [
            get_sql_emoji(2, name="test", usable=False),
            None,
            None,
            get_sql_emoji(4, name="test", usable=True),
        ],
        get_sql_emoji(4, name="test", usable=True)
    ),
])
async def test_case_insensitive_get_emote(postgres, side_effect: List[Optional[SQLEmoji]], expected: Optional[SQLEmoji]):
    side_effect = [None if i is None else [i] for i in side_effect]
    postgres.cur.fetchall.side_effect = side_effect
    rtn = await postgres._case_insensitive_get_emote(query_where="", emote_name="test", parameters={})
    assert rtn == expected
    assert postgres.cur.fetchall.call_count == len(side_effect)


@pytest.mark.asyncio
@given(
    lists(sqlemoji_strategy(name=sampled_from(["test", "Test", "TEST"]))),
    sampled_from(["test", "Test", "TEST"])
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
async def test_case_insensitive_get_emote_never_unusable(postgres, emote_pool: List[SQLEmoji], emote_name: str):
    sql_query = None

    def _execute_side_effect(query: str, parameters):
        nonlocal sql_query
        sql_query = query

    def _fetchall_side_effect():
        is_case_sensitive = "lower(" not in sql_query
        force_usable = "usable=true" in sql_query
        for emote in emote_pool:
            if is_case_sensitive and emote.name != emote_name:
                continue
            if force_usable and not emote.usable:
                continue
            return [emote]
        return None

    postgres.cur.execute.side_effect = _execute_side_effect
    postgres.cur.fetchall.side_effect = _fetchall_side_effect

    rtn: Optional[SQLEmoji] = await postgres._case_insensitive_get_emote(query_where="", emote_name=emote_name, parameters={})

    if rtn is None:
        assert all(emote.usable is False for emote in emote_pool)
    else:
        assert rtn in emote_pool
        assert rtn.usable
        if rtn.name != emote_name:
            assert not any(emote.name == emote_name and emote.usable for emote in emote_pool)


@pytest.mark.asyncio
@given(
    sqlemoji_strategy(
        emote_hash=sampled_from(["1", "2", "3", "4"])
    ),
    lists(sqlemoji_strategy(
        emote_hash=sampled_from(["1", "2", "3", "4"])
    )),
    booleans(),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
async def test_get_emote_like(postgres, to_find: SQLEmoji, emote_pool: List[SQLEmoji], add_to_pool: bool):
    sql_query = None
    parameters_ = {}

    def _execute_side_effect(query: str, parameters):
        nonlocal sql_query, parameters_
        sql_query, parameters_ = query, parameters

    def _fetchall_side_effect():
        force_emote_hash = "emote_hash=%(emote_hash)s" in sql_query
        force_usable = "usable=true" in sql_query
        force_emote_id = "emote_id=%(emote_id)s" in sql_query
        for emote in emote_pool:
            if force_emote_hash and emote.emote_hash != parameters_["emote_hash"]:
                continue
            if force_usable and not emote.usable:
                continue
            if force_emote_id and emote.emote_id != parameters_["emote_id"]:
                continue
            return [emote]
        return None

    postgres.cur.execute.side_effect = _execute_side_effect
    postgres.cur.fetchall.side_effect = _fetchall_side_effect

    if add_to_pool:
        emote_pool.append(to_find)
    else:
        assume(not any(emote.emote_id == to_find.emote_id for emote in emote_pool))

    rtn: Optional[SQLEmoji] = await postgres.get_emote_like(to_find.emote_id)
    if not add_to_pool:
        # If the emote id we're looking for isn't in the pool, we never find anything sharing its hash
        assert rtn is None
    elif rtn is None:
        assert not any(emote.usable for emote in emote_pool if emote.emote_hash == to_find.emote_hash)
    else:
        assert rtn.emote_hash == to_find.emote_hash
        assert rtn.usable
