from typing import Dict, Optional, List, Set, Tuple
from discord import Emoji
from ..emoji import SQLEmoji, EmojiCounts

from .._connection import _PostgresConnection


class EmojisMixin(_PostgresConnection):
    async def get_emote_hashes(self, emote_ids: List[int]) -> Dict[str, int]:
        if not emote_ids:
            return {}
        await self.cur.execute(
            "SELECT emote_id, emote_hash FROM emote_ids WHERE emote_id IN (SELECT(UNNEST(%(emote_ids)s)))",
            parameters={"emote_ids": emote_ids}
        )
        emote_hashes = await self.cur.fetchall()
        return {emote_hash: emote_id for emote_id, emote_hash in emote_hashes}

    async def get_emote_hash(self, emote_id: int) -> Optional[str]:
        await self.cur.execute(
            "SELECT emote_hash FROM emote_ids WHERE emote_id=%(emote_id)s",
            parameters={"emote_id": emote_id}
        )
        emote_hash = await self.cur.fetchall()
        if emote_hash and emote_hash[0][0]:
            return emote_hash[0][0]

    async def get_synonyms_for_emote(self, emote_hash: str, limit: Optional[int] = 10) -> List[int]:
        if limit is None:
            await self.cur.execute(
                "SELECT emote_id FROM emote_ids WHERE emote_hash=%(emote_hash)s and usable=true and has_roles=false and manual_block=false",
                parameters={"emote_hash": emote_hash}
            )
        else:
            await self.cur.execute(
                "SELECT emote_id FROM emote_ids WHERE emote_hash=%(emote_hash)s and usable=true and has_roles=false and manual_block=false LIMIT %(limit)s",
                parameters={"emote_hash": emote_hash, "limit": limit}
            )

        results = await self.cur.fetchall()
        return [emote_id for emote_id, in results]

    async def is_emote_blocked(self, emote_id: int) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM emote_ids WHERE emote_id=%(emote_id)s and (has_roles=true or manual_block=true) LIMIT 1",
            parameters={"emote_id": emote_id}
        )
        results = await self.cur.fetchall()
        return bool(results)

    async def is_emote_usable(self, emote_id: int) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM emote_ids WHERE emote_id=%(emote_id)s and has_roles=false and manual_block=false and usable=true LIMIT 1",
            parameters={"emote_id": emote_id}
        )
        results = await self.cur.fetchall()
        return bool(results)

    async def purge_emojis(self, emote_ids: List[int]) -> Set[int]:
        await self.cur.execute(
            "SELECT emote_id FROM emote_ids WHERE emote_id=ANY(%(emote_ids)s) and guild_id is NULL",
            parameters={"emote_ids": emote_ids}
        )
        results = await self.cur.fetchall()
        if results:
            await self.cur.execute(
                "DELETE FROM emote_ids WHERE emote_id=ANY(%(emote_ids)s) and guild_id is NULL",
                parameters={"emote_ids": emote_ids}
            )
        return set(emote_id for emote_id, in results)

    async def share_hashes(self, emote_id_1: int, emote_id_2: int) -> bool:
        await self.cur.execute(
            "select 1 from emote_ids where emote_hash=(select emote_hash from emote_ids where emote_id = %(emote_id_1)s) and emote_id = %(emote_id_2)s LIMIT 1",
            parameters={"emote_id_1": emote_id_1, "emote_id_2": emote_id_2}
        )
        results = await self.cur.fetchall()
        return bool(results)

    async def get_emote_like(self, emote_id: int) -> Optional[Emoji]:
        # Get the emoji we're searching for
        await self.cur.execute(
            "SELECT emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles FROM emote_ids WHERE emote_id=%(emote_id)s LIMIT 1",
            parameters={"emote_id": emote_id}
        )
        results = await self.cur.fetchall()
        if not results:
            return None
        emote = SQLEmoji(*results[0])
        if not (emote.guild_id and emote.usable):
            # If we can't use this one, find one like it
            await self.cur.execute(
                "SELECT emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles FROM emote_ids WHERE emote_hash=%(emote_hash)s and guild_id is not null and usable=true LIMIT 1",
                parameters={"emote_hash": emote.emote_hash}
            )
            results = await self.cur.fetchall()
            if not results:
                return None
            emote = SQLEmoji(*results[0])
        return self._get_emoji(emote)

    async def set_emote_perceptual_data(self, emote_id: int, guild_id: int, emote_hash: str, emote_sha: str, animated: bool, usable: bool, name: Optional[str], has_roles: bool):
        await self.cur.execute(
            "INSERT INTO emote_ids (emote_id, emote_hash, usable, animated, emote_sha, guild_id, name, has_roles) VALUES (%(emote_id)s, %(emote_hash)s, %(usable)s, %(animated)s, %(emote_sha)s, %(guild_id)s, %(name)s, %(has_roles)s) ON CONFLICT (emote_id) DO UPDATE SET name=%(name)s, has_roles=%(has_roles)s, guild_id=coalesce(emote_ids.guild_id, %(guild_id)s)",
            parameters={
                "emote_id": emote_id,
                "emote_hash": emote_hash,
                "animated": animated,
                "emote_sha": emote_sha,
                "guild_id": guild_id,
                "usable": usable,
                "name": name,
                "has_roles": has_roles,
            }
        )

    async def set_emote_guild(self, emote_id: int, guild_id: Optional[int], usable: Optional[bool], has_roles: bool, name: Optional[str]):
        if usable is None:
            # We don't know if the emote is available or not
            await self.cur.execute(
                "UPDATE emote_ids SET guild_id=%(guild_id)s, name=%(name)s, has_roles=%(has_roles)s where emote_id=%(emote_id)s",
                parameters={"emote_id": emote_id, "guild_id": guild_id, "name": name, "has_roles": has_roles}
            )
        else:
            await self.cur.execute(
                "UPDATE emote_ids SET guild_id=%(guild_id)s, usable=%(usable)s, name=%(name)s, has_roles=%(has_roles)s where emote_id=%(emote_id)s",
                parameters={"emote_id": emote_id, "guild_id": guild_id, "usable": usable, "name": name, "has_roles": has_roles}
            )

    async def increment_guild_emote_score(self, emoji_guild_ids: List[Tuple[int, int]]):
        # Make sure the formula for the bitshifting gives -1, as 2 >> -1 = 0, which is what we want if we want to give
        # the first numbers 100% chance
        # select (-128)>>5+3; -> -1
        # select (-128)>>4+7 -> -1;
        await self.cur.execute(
            'UPDATE emote_ids SET score=((score::int)+1)::"char" '
            'where (guild_id, emote_id) in (SELECT * FROM unnest(%(emoji_guild_ids)s) as f(guild_id bigint, emote_id bigint)) and '
            'trunc(random() * (2<<(((score::int)>>5)+3))) = 0',
            parameters={"emoji_guild_ids": emoji_guild_ids}
        )

    async def clear_guild_emotes(self, guild_id: int):
        await self.cur.execute(
            "UPDATE emote_ids SET guild_id=null where guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )

    async def set_emote_usability(self, emote_id: int, usable: Optional[bool]):
        await self.cur.execute(
            "UPDATE emote_ids SET usable=%(usable)s where emote_id=%(emote_id)s",
            parameters={"emote_id": emote_id, "usable": usable}
        )

    async def set_emotes_unusuable(self, emote_ids: List[int]):
        await self.cur.execute(
            "UPDATE emote_ids SET usable=false where emote_id=ANY(%(emote_ids)s)",
            parameters={"emote_ids": emote_ids}
        )

    async def clear_guild_emojis(self, guild_id: int):
        await self.cur.execute(
            "UPDATE emote_ids SET guild_id=null where guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )

    async def clear_guilds_emojis(self, guild_ids: List[int]):
        await self.cur.execute(
            "UPDATE emote_ids SET guild_id=null where guild_id=ANY(%(guild_ids)s)",
            parameters={"guild_ids": guild_ids}
        )

    async def clear_guilds_from_emojis(self, emoji_ids: List[int]):
        await self.cur.execute(
            "UPDATE emote_ids SET guild_id=null where emote_id=ANY(%(emoji_ids)s)",
            parameters={"emoji_ids": emoji_ids}
        )

    async def get_guilds_with_emojis(self) -> Set[int]:
        await self.cur.execute(
            "SELECT guild_id FROM emote_ids WHERE guild_id IS NOT NULL",
            parameters={}
        )
        results = await self.cur.fetchall()
        return {guild_id for guild_id, in results}


    async def get_pack_emote(self, pack_name: str, emote_name: str) -> Optional[Emoji]:
        return await self._case_insensitive_get_emote(
            "guild_id=(select guild_id from packs where pack_name=%(pack_name)s)",
            emote_name,
            parameters={"pack_name": pack_name}
        )

    async def get_guild_emote(self, guild_id: int, emote_name: str) -> Optional[Emoji]:
        return await self._case_insensitive_get_emote(
            "guild_id=%(guild_id)s",
            emote_name,
            parameters={"guild_id": guild_id}
        )

    async def guild_emote_counts(self, guild_id: int) -> EmojiCounts:
        await self.cur.execute(
            "SELECT COUNT(*) filter(where not animated) as static, COUNT(*) filter(where animated) as animated FROM emote_ids where guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        counts = await self.cur.fetchall()
        return EmojiCounts(static=counts[0][0], animated=counts[0][1])

    async def get_mutual_guild_emote(self, user_id: int, emote_name: str) -> Optional[Emoji]:
        return await self._case_insensitive_get_emote(
            "guild_id in (select guild_id from members where user_id=%(user_id)s)",
            emote_name,
            parameters={"user_id": user_id}
        )

    async def get_pack_guild_emote(self, user_id: int, emote_name: str) -> Optional[Emoji]:
        return await self._case_insensitive_get_emote(
            "guild_id in (select guild_id from user_packs where user_id=%(user_id)s)",
            emote_name,
            parameters={"user_id": user_id}
        )

    async def get_guild_emotes(self, guild_id: int) -> List[Emoji]:
        return await self._get_emojis(
            "guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )

    async def get_mutual_guild_emotes(self, user_id: int) -> List[Emoji]:
        return await self._get_emojis(
            "guild_id in (select guild_id from members where user_id=%(user_id)s)",
            parameters={"user_id": user_id}
        )

    async def get_guilds_emotes(self, guild_ids: List[int]) -> List[Emoji]:
        return await self._get_emojis(
            "guild_id = ANY(%(guild_ids)s)",
            parameters={"guild_ids": guild_ids}
        )

    async def get_guilds_emotes_raw(self, guild_ids: List[int]) -> List:
        await self.cur.execute(
            f"select emote_id, guild_id, trim(name), has_roles from emote_ids where guild_id = ANY(%(guild_ids)s)",
            parameters={"guild_ids": guild_ids}
        )
        results = await self.cur.fetchall()
        return results

    async def get_guild_emotes_raw(self, guild_id: int) -> List:
        await self.cur.execute(
            f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where guild_id = %(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        results = await self.cur.fetchall()
        return [SQLEmoji(*i) for i in results]

    async def get_emotes_raw(self, emote_ids: List[int]) -> List:
        await self.cur.execute(
            f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where emote_id = ANY(%(emote_ids)s)",
            parameters={"emote_ids": emote_ids}
        )
        results = await self.cur.fetchall()
        return [SQLEmoji(*i) for i in results]

    async def get_emotes(self, emote_ids: List[int]) -> List[Emoji]:
        return await self._get_emojis(
            "emote_id = ANY(%(emote_ids)s) and guild_id is not NULL",
            parameters={"emote_ids": emote_ids}
        )

    async def get_pack_guild_emotes(self, user_id: int) -> List[Emoji]:
        return await self._get_emojis(
            "guild_id in (select guild_id from user_packs where user_id=%(user_id)s)",
            parameters={"user_id": user_id}
        )

    async def get_mega_emotes(self, guild_id: int, emote_name: str) -> List[Emoji]:
        await self.cur.execute(
            "select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where guild_id=%(guild_id)s and trim(name) ~ ('^' || %(emote_name)s || '_\d_\d$') and usable=true and has_roles=false and manual_block=false",
            parameters={"guild_id": guild_id, "emote_name": emote_name}
        )
        results = await self.cur.fetchall()
        return [self._get_emoji(SQLEmoji(*emote)) for emote in results]

    async def _get_emojis(self, query_where, parameters) -> List[Emoji]:
        await self.cur.execute(
            f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where {query_where} and usable=true and has_roles=false and manual_block=false",
            parameters=parameters
        )
        results = await self.cur.fetchall()
        emotes = (self._get_emoji(SQLEmoji(*i)) for i in results)
        return [emote for emote in emotes if emote]

    async def _case_insensitive_get_emote(self, query_where, emote_name: str, parameters) -> Optional[Emoji]:
        await self.cur.execute(
            f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where {query_where} and lower(trim(name))=lower(%(emote_name)s) and usable=true and has_roles=false and manual_block=false",
            parameters={**parameters, "emote_name": emote_name}
        )
        emote = await self._get_cur_emoji()
        if emote and emote.name != emote_name:
            await self.cur.execute(
                f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where {query_where} and trim(name)=%(emote_name)s and usable=true and has_roles=false and manual_block=false",
                parameters={**parameters, "emote_name": emote_name}
            )
            emote = await self._get_cur_emoji() or emote
        return emote

    async def _get_cur_emoji(self) -> Optional[Emoji]:
        results = await self.cur.fetchall()
        if not results:
            return None
        emote = SQLEmoji(*results[0])
        return self._get_emoji(emote)
