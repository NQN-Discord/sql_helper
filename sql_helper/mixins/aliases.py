from typing import List, Optional, NoReturn

from discord import PartialEmoji

from .._connection import _PostgresConnection


class AliasesMixin(_PostgresConnection):
    async def get_user_aliases(self, user_id: int) -> List[PartialEmoji]:
        await self.cur.execute(
            "SELECT animated, \"name\", emote_id FROM aliases WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )
        return await _get_emotes(self.cur)

    async def get_user_aliases_after(self, user_id: int, name: str, limit: int) -> List[PartialEmoji]:
        await self.cur.execute(
            "SELECT animated, \"name\", emote_id FROM aliases WHERE user_id=%(user_id)s and name>%(name)s order by name limit %(limit)s",
            parameters={"user_id": user_id, "name": name, "limit": limit}
        )
        return await _get_emotes(self.cur)

    async def get_user_alias_name(self, user_id: int, name: str) -> Optional[PartialEmoji]:
        await self.cur.execute(
            "SELECT animated, \"name\", emote_id FROM aliases WHERE user_id=%(user_id)s and lower(\"name\")=%(name)s",
            parameters={"user_id": user_id, "name": name.lower()}
        )
        emotes = await _get_emotes(self.cur)
        if emotes:
            return next((emote for emote in emotes if emote.name == name), emotes[0])

    async def get_user_alias_name_with_guild(self, user_id: int, name: str, guild_id: int) -> Optional[PartialEmoji]:
        # Get an alias, but exclude where the current guild has has_roles set on the alias you're trying to use
        await self.cur.execute(
            "SELECT aliases.emote_id, aliases.animated, aliases.\"name\" from aliases join emote_ids on aliases.emote_id=emote_ids.emote_id where user_id=%(user_id)s and lower(aliases.\"name\")=%(name)s and "
            "emote_ids.emote_hash not in (select emote_hash from emote_ids where guild_id=%(guild_id)s and has_roles=true)",
            parameters={"user_id": user_id, "name": name.lower(), "guild_id": guild_id}
        )
        emotes = await _get_emotes(self.cur)
        if emotes:
            return next((emote for emote in emotes if emote.name == name), emotes[0])

    async def count_user_aliases(self, user_id: int) -> int:
        await self.cur.execute(
            "select count(*) from aliases where user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )
        results = await self.cur.fetchall()
        return results[0][0]

    async def user_has_aliases(self, user_id: int) -> bool:
        await self.cur.execute(
            "select 1 from aliases where user_id=%(user_id)s limit 1",
            parameters={"user_id": user_id}
        )
        return bool(await self.cur.fetchall())

    async def set_user_aliases(self, user_id: int, aliases: List[PartialEmoji]):
        if not aliases:
            return
        names = []
        ids = []
        animateds = []
        for emote in aliases:
            names.append(emote.name)
            ids.append(emote.id)
            animateds.append(emote.animated)

        await self.cur.execute(
            "INSERT INTO aliases (user_id, emote_id, animated, name) VALUES (%(user_id)s, unnest(%(emote_ids)s), unnest(%(animateds)s), unnest(%(names)s)) "
            "ON CONFLICT ON CONSTRAINT aliases_pk DO UPDATE SET emote_id = excluded.emote_id, animated = excluded.animated, \"name\" = excluded.name",
            parameters={
                "user_id": user_id,
                "names": names,
                "emote_ids": ids,
                "animateds": animateds,
            }
        )

    async def set_user_alias(self, user_id: int, alias: PartialEmoji) -> NoReturn:
        await self.cur.execute(
            "INSERT INTO aliases (user_id, emote_id, animated, name) VALUES (%(user_id)s, %(emote_id)s, %(animated)s, %(name)s) "
            "ON CONFLICT ON CONSTRAINT aliases_pk DO UPDATE SET emote_id = excluded.emote_id, animated = excluded.animated, \"name\" = excluded.name",
            parameters={
                "user_id": user_id,
                "name": alias.name,
                "emote_id": alias.id,
                "animated": alias.animated,
            }
        )

    async def delete_user_alias(self, user_id: int, name: str) -> NoReturn:
        await self.cur.execute(
            "DELETE FROM aliases WHERE user_id=%(user_id)s and \"name\"=%(name)s",
            parameters={"user_id": user_id, "name": name}
        )

    async def delete_all_user_aliases(self, user_id: int) -> NoReturn:
        await self.cur.execute(
            "DELETE FROM aliases WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )


async def _get_emotes(cur) -> List[PartialEmoji]:
    results = await cur.fetchall()
    return [
        PartialEmoji(animated=animated, name=name.rstrip(" "), id=id)
        for animated, name, id in results
    ]
