from typing import Optional

from mock import MagicMock, AsyncMock
from hypothesis.strategies import builds, integers, booleans, text
import pytest

from sql_helper import SQLEmoji
from sql_helper.connection import PostgresConnection

__all__ = ["get_guild", "get_emoji", "postgres", "get_sql_emoji", "sqlemoji_strategy"]


sqlemoji_strategy = lambda emote_id=integers(), name=text(), emote_hash=text(): builds(
    SQLEmoji,
    emote_id=emote_id,
    emote_hash=emote_hash,
    usable=booleans(),
    animated=booleans(),
    emote_sha=text(),
    guild_id=integers(),
    name=name,
    has_roles=booleans()
)


@pytest.fixture
def get_guild():
    rtn = MagicMock()
    rtn.side_effect = lambda x: x
    return rtn


@pytest.fixture
def get_emoji():
    rtn = MagicMock()
    rtn.side_effect = lambda x: x
    return rtn


@pytest.fixture
def postgres(get_guild, get_emoji) -> PostgresConnection:
    postgres = PostgresConnection(MagicMock(), get_guild=get_guild, get_emoji=get_emoji)
    postgres.cur = AsyncMock()
    postgres.cur.return_value = None
    return postgres


def get_sql_emoji(emote_id: int, emote_hash: str = "abc", usable: bool = True, animated: bool = False, emote_sha: str = "cde", guild_id: Optional[int] = 123, name: str = "foo", has_roles: bool= False) -> SQLEmoji:
    return SQLEmoji(emote_id, emote_hash, usable, animated, emote_sha, guild_id, name, has_roles)
