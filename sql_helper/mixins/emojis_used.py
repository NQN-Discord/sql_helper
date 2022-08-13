from datetime import datetime
from typing import List, Tuple
from discord import PartialEmoji
from .._connection import _PostgresConnection


class EmojisUsedMixin(_PostgresConnection):
    async def add_used_emotes(self, to_cache: List[Tuple[int, PartialEmoji, datetime]]):
        params = {
            "times": [],
            "guild_ids": [],
            "names": [],
            "ids": [],
            "animateds": [],
        }

        for guild_id, emote, time in to_cache:
            params["guild_ids"].append(guild_id)
            params["times"].append(time)
            params["names"].append(emote.name)
            params["ids"].append(emote.id)
            params["animateds"].append(emote.animated)

        await self.cur.execute(
            f"INSERT INTO emotes_used (time, guild_id, name, emote_id, animated) VALUES (unnest(%(times)s), unnest(%(guild_ids)s), unnest(%(names)s), unnest(%(ids)s), unnest(%(animateds)s))",
            parameters=params
        )
