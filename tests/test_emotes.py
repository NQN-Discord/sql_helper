import pytest

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
async def test_get_emote_like_force_change(postgres):
    postgres.cur.fetchall.side_effect = [
        [get_sql_emoji(123)],
        [get_sql_emoji(234)]
    ]
    rtn = await postgres.get_emote_like(emote_id=123, force_change=True)
    assert rtn == get_sql_emoji(234)
    assert postgres.cur.fetchall.call_count == 2


@pytest.mark.asyncio
async def test_get_emote_like_force_change_no_guild(postgres):
    postgres.cur.fetchall.side_effect = [
        [get_sql_emoji(123)],
        [get_sql_emoji(234)]
    ]
    rtn = await postgres.get_emote_like(emote_id=123, force_change=True, require_guild=False)
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
