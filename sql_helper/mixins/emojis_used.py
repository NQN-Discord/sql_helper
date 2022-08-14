from datetime import datetime
from typing import List, Tuple, Optional
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

    async def get_recently_used_emote(self, guild_id: int, name: str) -> Optional[PartialEmoji]:
        await self.cur.execute(
            "SELECT first(animated, time), first(name, time), emote_id FROM emotes_used WHERE guild_id=%(guild_id)s and lower(\"name\")=%(name)s group by emote_id",
            parameters={"guild_id": guild_id, "name": name.lower()}
        )
        emotes = await _get_emotes(self.cur)
        if emotes:
            return next((emote for emote in emotes if emote.name == name), emotes[0])

    async def get_recently_used_emotes(self, guild_id: int, prefix: str, limit: int = 25) -> List[PartialEmoji]:
        if not prefix:
            await self.cur.execute(
                "select first(animated, time), first(name, time), emote_id from emotes_used where guild_id=%(guild_id)s group by emote_id limit %(limit)s",
                parameters={"guild_id": guild_id, "limit": limit}
            )
        else:
            # This one doesn't use an index for the whole thing. Should be OK though
            await self.cur.execute(
                "select first(animated, time), first(name, time), emote_id from emotes_used where guild_id=%(guild_id)s and starts_with(lower(\"name\"), %(prefix)s) group by emote_id limit %(limit)s",
                parameters={"guild_id": guild_id, "prefix": prefix, "limit": limit}
            )
        return await _get_emotes(self.cur)


async def _get_emotes(cur) -> List[PartialEmoji]:
    results = await cur.fetchall()
    return [
        PartialEmoji(animated=animated, name=name.rstrip(" "), id=id)
        for animated, name, id in results
    ]
