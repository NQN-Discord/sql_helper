from typing import List, Optional, Tuple
from enum import Enum, auto

from discord import PartialEmoji

from .._connection import _PostgresConnection
from .aliases import _get_emotes


class Ordering(Enum):
    before = 1
    equal = 2
    after = 3


class BlockedEmojisMixin(_PostgresConnection):
    async def get_guilds_with_blocked_emojis(self) -> List[int]:
        await self.cur.execute(
            "SELECT distinct guild_id FROM blocked_emotes",
            parameters={}
        )
        results = await self.cur.fetchall()
        return [guild_id for guild_id, in results]

    async def has_blocked_emotes(self, guild_id: int, emote_ids: List[int]) -> bool:
        await self.cur.execute(
            """     
select
   exists(select emote_id from blocked_emotes where guild_id=%(guild_id)s and emote_id=any(%(emote_ids)s))
       or
   exists(select emote_id from blocked_emotes where guild_id=%(guild_id)s and emote_hash in (select emote_hash from emote_ids where emote_id=any(%(emote_ids)s)))
            """,
            parameters={"guild_id": guild_id, "emote_ids": emote_ids}
        )
        results = await self.cur.fetchall()
        return results[0][0]

    async def emotes_blocked(self, guild_id: int, emote_ids: List[int]) -> List[int]:
        await self.cur.execute(
            """
with guild_blocks as (
    select emote_id, emote_hash
    from blocked_emotes
    where guild_id = %(guild_id)s
)
(select emote_id from guild_blocks where emote_id = any(%(emote_ids)s))
union
(select emote_id from emote_ids where emote_id = any(%(emote_ids)s) and emote_hash in (select emote_hash from guild_blocks))
            """,
            parameters={"guild_id": guild_id, "emote_ids": emote_ids}
        )
        results = await self.cur.fetchall()
        return [guild_id for guild_id, in results]

    async def block_emotes(self, guild_id: int, emotes: List[PartialEmoji]):
        names = []
        ids = []
        animateds = []
        for emote in emotes:
            names.append(emote.name)
            ids.append(emote.id)
            animateds.append(emote.animated)

        await self.cur.execute(
            """
INSERT INTO blocked_emotes (guild_id, emote_id, name, animated, emote_hash) select guild_id, a.emote_id, "name", animated, emote_hash from ((select
        %(guild_id)s as guild_id,
        unnest(%(emote_ids)s) as emote_id,
        unnest(%(names)s) as "name",
        unnest(%(animateds)s) as animated
    ) a
    join
    (select gids.emote_id, emote_hash from (SELECT * from unnest(%(emote_ids)s) as emote_id) gids left join emote_ids on emote_ids.emote_id=gids.emote_id) b
    on a.emote_id = b.emote_id
) ON CONFLICT DO NOTHING
            """,
            parameters={
                "guild_id": guild_id,
                "names": names,
                "emote_ids": ids,
                "animateds": animateds,
            }
        )

    async def get_blocked_emotes(self, guild_id: int) -> List[PartialEmoji]:
        await self.cur.execute(
            "SELECT animated, \"name\", emote_id FROM blocked_emotes where guild_id = %(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        return await _get_emotes(self.cur)

    async def get_blocked_emotes_search(self, guild_id: int, name: str, ordering: Ordering) -> Tuple[int, List[PartialEmoji]]:
        await self.cur.execute(
            "SELECT count(*) FROM blocked_emotes where guild_id = %(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        count = await self.cur.fetchall()
        if ordering is Ordering.before:
            await self.cur.execute(
                f"SELECT animated, \"name\", emote_id FROM blocked_emotes where guild_id = %(guild_id)s and \"name\" < %(name)s order by \"name\" desc limit 10",
                parameters={"guild_id": guild_id, "name": name}
            )
        elif ordering is Ordering.equal:
            await self.cur.execute(
                f"SELECT animated, \"name\", emote_id FROM blocked_emotes where guild_id = %(guild_id)s and \"name\" >= %(name)s order by \"name\" limit 10",
                parameters={"guild_id": guild_id, "name": name}
            )
        elif ordering is Ordering.after:
            await self.cur.execute(
                f"SELECT animated, \"name\", emote_id FROM blocked_emotes where guild_id = %(guild_id)s and \"name\" > %(name)s order by \"name\" limit 10",
                parameters={"guild_id": guild_id, "name": name}
            )
        emotes = await _get_emotes(self.cur)
        if name and ordering is Ordering.before:
            emotes.reverse()
        return count[0][0], emotes

    async def get_blocked_emotes_with_prefix(self, guild_id: int, prefix: str) -> List[PartialEmoji]:
        await self.cur.execute(
            f"SELECT animated, \"name\", emote_id FROM blocked_emotes where guild_id = %(guild_id)s and starts_with(lower(\"name\"), %(prefix)s) order by \"name\" limit 25",
            parameters={"guild_id": guild_id, "prefix": prefix.lower()}
        )
        return await _get_emotes(self.cur)

    async def get_blocked_emote_by_name(self, guild_id: int, name: str) -> Optional[PartialEmoji]:
        await self.cur.execute(
            "SELECT animated, \"name\", emote_id FROM blocked_emotes where guild_id = %(guild_id)s and \"name\" = %(name)s limit 1",
            parameters={"guild_id": guild_id, "name": name}
        )
        emotes = await _get_emotes(self.cur)
        if emotes:
            return emotes[0]

    async def unblock_emote(self, guild_id: int, emote_id: int) -> bool:
        # Returns if the guild still has blocked emotes
        await self.cur.execute(
            "delete from blocked_emotes where guild_id=%(guild_id)s and emote_id=%(emote_id)s",
            parameters={"guild_id": guild_id, "emote_id": emote_id}
        )
        await self.cur.execute(
            "select exists(select emote_id from blocked_emotes where guild_id = %(guild_id)s)",
            parameters={"guild_id": guild_id}
        )
        results = await self.cur.fetchall()
        return results[0][0]

    async def share_hashes_with_blocked_emote(self, guild_id: int, emote_id: int, targets: List[int]) -> List[int]:
        await self.cur.execute(
            "select emote_id from emote_ids where emote_hash=(select emote_hash from blocked_emotes where guild_id=%(guild_id)s and emote_id=%(emote_id)s) and emote_id=ANY(%(targets)s)",
            parameters={"guild_id": guild_id, "emote_id": emote_id, "targets": targets}
        )
        results = await self.cur.fetchall()
        return [emote_id for emote_id, in results]

