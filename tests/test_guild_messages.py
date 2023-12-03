from typing import Optional, Any

import pytest
from hypothesis import given, HealthCheck, settings, assume
from hypothesis.strategies import integers, none, one_of

from .base import *


@pytest.mark.asyncio
async def test_get_guild_messages_no_filter(postgres):
    with pytest.raises(NotImplementedError):
        await postgres.get_guild_messages(no_results=0)


@pytest.mark.asyncio
@given(
    message_id=optional_id_strategy,
    guild_id=optional_id_strategy,
    channel_id=optional_id_strategy,
    user_id=optional_id_strategy,
    after=optional_id_strategy,
    offset=one_of(none(), integers(min_value=0, max_value=10)),
    no_results=one_of(none(), integers(min_value=0, max_value=10)),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
async def test_get_guild_messages(
    postgres,
    message_id: Optional[int],
    guild_id: Optional[int],
    channel_id: Optional[int],
    user_id: Optional[int],
    after: Optional[int],
    offset: Optional[int],
    no_results: Optional[int],
):
    assume(
        message_id is not None
        or guild_id is not None
        or channel_id is not None
        or user_id is not None
    )

    def _execute_side_effect(query: str, parameters):
        _assert_sql_if_not_none(offset, "OFFSET %(offset)s", query)
        _assert_sql_if_not_none(no_results, "LIMIT %(limit)s", query)
        _assert_sql_if_not_none(after, "message_id > %(after_id)s", query)
        _assert_sql_if_not_none(message_id, "message_id=%(message_id)s", query)
        if message_id is None:
            _assert_sql_if_not_none(channel_id, "channel_id=%(channel_id)s", query)
            _assert_sql_if_not_none(user_id, "user_id=%(user_id)s", query)
            if channel_id is None:
                _assert_sql_if_not_none(guild_id, "guild_id=%(guild_id)s", query)
        assert parameters == {
            "message_id": message_id,
            "guild_id": guild_id,
            "channel_id": channel_id,
            "user_id": user_id,
            "after_id": after,
            "offset": offset,
            "limit": no_results,
        }

    postgres.cur.execute.side_effect = _execute_side_effect

    await postgres.get_guild_messages(
        message_id=message_id,
        guild_id=guild_id,
        channel_id=channel_id,
        user_id=user_id,
        after=after,
        offset=offset,
        no_results=no_results,
    )


def _assert_sql_if_not_none(value: Optional[Any], sql: str, query: str):
    if value is None:
        assert sql not in query
    else:
        assert sql in query
