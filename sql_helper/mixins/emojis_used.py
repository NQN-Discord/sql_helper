from typing import List, Optional
from discord import PartialEmoji
from .._connection import _PostgresConnection


class EmojisUsedMixin(_PostgresConnection):
    async def add_used_emotes(self, guild_id: int, author_id: int, message_id: int, emotes: List[PartialEmoji], force_author: bool):
        names = []
        ids = []
        animateds = []
        for emote in emotes:
            names.append(emote.name)
            ids.append(emote.id)
            animateds.append(emote.animated)

        if force_author:
            on_constraint_do = "UPDATE SET author_id = excluded.author_id"
        else:
            on_constraint_do = "NOTHING"

        await self.cur.execute(
            f"INSERT INTO emotes_used (guild_id, author_id, message_id, name, emote_id, animated) VALUES (%(guild_id)s, %(author_id)s, %(message_id)s, unnest(%(names)s), unnest(%(emote_ids)s), unnest(%(animateds)s)) ON CONFLICT ON CONSTRAINT emotes_used_pk DO {on_constraint_do}",
            parameters={
                "guild_id": guild_id,
                "author_id": author_id,
                "message_id": message_id,
                "names": names,
                "emote_ids": ids,
                "animateds": animateds,
            }
        )

    async def get_used_guild_emote(self, guild_id: int, name: str, after: int) -> Optional[PartialEmoji]:
        await self.cur.execute(
            f"SELECT animated, \"name\", emote_id FROM emotes_used WHERE guild_id=%(guild_id)s AND name=%(name)s AND message_id > %(after)s ORDER BY message_id DESC LIMIT 1",
            parameters={"guild_id": guild_id, "name": name, "after": after}
        )

        results = await self.cur.fetchall()
        if results:
            return PartialEmoji(animated=results[0][0], name=results[0][1].rstrip(" "), id=results[0][2])
