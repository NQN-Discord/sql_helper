from typing import Dict, Optional, List, Set, Tuple
from collections import Counter
from discord import Emoji
from ..emoji import SQLEmoji, EmojiCounts

from .._connection import _PostgresConnection


class EmojisMixin(_PostgresConnection):
    async def get_emote_hashes(self, emote_ids: List[int]) -> Dict[str, int]:
        if not emote_ids:
            return {}
        await self.cur.execute(
            "SELECT emote_id, emote_hash FROM emote_ids WHERE emote_id IN (SELECT(UNNEST(%(emote_ids)s)))",
            parameters={"emote_ids": emote_ids},
        )
        emote_hashes = await self.cur.fetchall()
        return {emote_hash: emote_id for emote_id, emote_hash in emote_hashes}

    async def get_emote_hash(self, emote_id: int) -> Optional[str]:
        await self.cur.execute(
            "SELECT emote_hash FROM emote_ids WHERE emote_id=%(emote_id)s",
            parameters={"emote_id": emote_id},
        )
        emote_hash = await self.cur.fetchall()
        if emote_hash and emote_hash[0][0]:
            return emote_hash[0][0]

    async def get_synonyms_for_emote(
        self, emote_hash: str, limit: Optional[int] = 10
    ) -> List[int]:
        if limit is None:
            await self.cur.execute(
                "SELECT emote_id FROM emote_ids WHERE emote_hash=%(emote_hash)s and usable=true and has_roles=false and manual_block=false",
                parameters={"emote_hash": emote_hash},
            )
        else:
            await self.cur.execute(
                "SELECT emote_id FROM emote_ids WHERE emote_hash=%(emote_hash)s and usable=true and has_roles=false and manual_block=false LIMIT %(limit)s",
                parameters={"emote_hash": emote_hash, "limit": limit},
            )

        results = await self.cur.fetchall()
        return [emote_id for emote_id, in results]

    async def alternative_emote_names(self, emote_id: int) -> List[str]:
        await self.cur.execute(
            """select trim("name") as name from (select "name", count(*) from emote_ids where emote_hash=(select emote_hash from emote_ids where emote_id=%(emote_id)s) group by "name" order by count desc) a where count > 10 and lower("name") not like 'emoji%%' limit 10""",
            parameters={"emote_id": emote_id},
        )
        results = await self.cur.fetchall()
        return [name for name, in results]

    async def is_emote_blocked(self, emote_id: int) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM emote_ids WHERE emote_id=%(emote_id)s and (has_roles=true or manual_block=true) LIMIT 1",
            parameters={"emote_id": emote_id},
        )
        results = await self.cur.fetchall()
        return bool(results)

    async def is_emote_usable(self, emote_id: int) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM emote_ids WHERE emote_id=%(emote_id)s and has_roles=false and manual_block=false and usable=true LIMIT 1",
            parameters={"emote_id": emote_id},
        )
        results = await self.cur.fetchall()
        return bool(results)

    async def purge_emojis(self, emote_ids: List[int]) -> Set[int]:
        await self.cur.execute(
            "SELECT emote_id FROM emote_ids WHERE emote_id=ANY(%(emote_ids)s) and guild_id is NULL",
            parameters={"emote_ids": emote_ids},
        )
        results = await self.cur.fetchall()
        if results:
            await self.cur.execute(
                "DELETE FROM emote_ids WHERE emote_id=ANY(%(emote_ids)s) and guild_id is NULL",
                parameters={"emote_ids": emote_ids},
            )
        return set(emote_id for emote_id, in results)

    async def share_hashes(self, emote_id_1: int, emote_id_2: int) -> bool:
        await self.cur.execute(
            "select 1 from emote_ids where emote_hash=(select emote_hash from emote_ids where emote_id = %(emote_id_1)s) and emote_id = %(emote_id_2)s LIMIT 1",
            parameters={"emote_id_1": emote_id_1, "emote_id_2": emote_id_2},
        )
        results = await self.cur.fetchall()
        return bool(results)

    async def get_emote_like(
        self, emote_id: int, *, require_guild: bool = True
    ) -> Optional[Emoji]:
        # Get the emoji we're searching for
        await self.cur.execute(
            "SELECT emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles FROM emote_ids WHERE emote_id=%(emote_id)s LIMIT 1",
            parameters={"emote_id": emote_id},
        )
        results = await self.cur.fetchall()
        if not results:
            return None
        emote = SQLEmoji(*results[0])
        if emote.usable:
            if emote.guild_id:
                return self._get_emoji(emote)
            elif require_guild:
                return self._sql_emoji_to_emoji(
                    await self._get_emote_like(emote, require_guild)
                )
            else:
                return self._get_emoji(emote)
        else:
            return self._sql_emoji_to_emoji(
                await self._get_emote_like(emote, require_guild)
            )

    async def _get_emote_like(
        self, emote: SQLEmoji, require_guild: bool
    ) -> Optional[SQLEmoji]:
        # At this point, we can't use the original emoji. Let's look for a similar one
        if require_guild:
            await self.cur.execute(
                "SELECT emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles FROM emote_ids WHERE emote_hash=%(emote_hash)s and guild_id is not null and usable=true LIMIT 1",
                parameters={"emote_hash": emote.emote_hash},
            )
        else:
            await self.cur.execute(
                "SELECT emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles FROM emote_ids WHERE emote_hash=%(emote_hash)s and usable=true LIMIT 1",
                parameters={"emote_hash": emote.emote_hash},
            )
        results = await self.cur.fetchall()
        if results:
            return SQLEmoji(*results[0])

    async def set_emote_perceptual_data(
        self,
        emote_id: int,
        guild_id: int,
        emote_hash: str,
        emote_sha: str,
        animated: bool,
        usable: bool,
        name: Optional[str],
        has_roles: bool,
    ):
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
            },
        )

    async def set_emote_guild(
        self,
        emote_id: int,
        guild_id: Optional[int],
        usable: Optional[bool],
        has_roles: bool,
        name: Optional[str],
    ):
        if usable is None:
            # We don't know if the emote is available or not
            await self.cur.execute(
                "UPDATE emote_ids SET guild_id=%(guild_id)s, name=%(name)s, has_roles=%(has_roles)s where emote_id=%(emote_id)s",
                parameters={
                    "emote_id": emote_id,
                    "guild_id": guild_id,
                    "name": name,
                    "has_roles": has_roles,
                },
            )
        else:
            await self.cur.execute(
                "UPDATE emote_ids SET guild_id=%(guild_id)s, usable=%(usable)s, name=%(name)s, has_roles=%(has_roles)s where emote_id=%(emote_id)s",
                parameters={
                    "emote_id": emote_id,
                    "guild_id": guild_id,
                    "usable": usable,
                    "name": name,
                    "has_roles": has_roles,
                },
            )

    async def increment_guild_emote_score(self, emoji_guild_ids: List[Tuple[int, int]]):
        # Make sure the formula for the bitshifting gives -1, as 2 >> -1 = 0, which is what we want if we want to give
        # the first numbers 100% chance
        # select (-128)>>5+3; -> -1
        # select (-128)>>4+7 -> -1;
        await self.cur.execute(
            'UPDATE emote_ids SET score=((score::int) + 1)::"char" '
            "where (guild_id, emote_id) in (SELECT * FROM unnest(%(emoji_guild_ids)s) as f(guild_id bigint, emote_id bigint)) and "
            "trunc(random() * (2<<(((score::int)>>5)+3))) = 0 and "
            "score::int != 127",  # POSITIVE 127 is max number here as signed
            parameters={"emoji_guild_ids": emoji_guild_ids},
        )

    async def decay_guild_emote_score(self):
        await self.cur.execute(
            'update emote_ids set score=(score::int - 1)::"char" '
            "where guild_id in (select guild_id from emote_ids where score::int > -28 and guild_id is not null) and "
            "score::int != -128",  # -128 is smallest number
            parameters={},
        )

    async def guild_emote_scores(self, guild_id: int) -> Counter:
        await self.cur.execute(
            "select emote_id, score::int + 128 from emote_ids where guild_id=%(guild_id)s order by score desc",
            parameters={"guild_id": guild_id},
        )
        emote_scores = await self.cur.fetchall()
        return Counter({k: v for k, v in emote_scores})

    async def emote_score(self, emote_id: int) -> int:
        await self.cur.execute(
            "select score::int + 128 from emote_ids where emote_id=%(emote_id)s",
            parameters={"emote_id": emote_id},
        )
        emote_score = await self.cur.fetchall()
        if not emote_score:
            return 0
        return emote_score[0][0]

    async def daily_scores_for_emotes(self, emote_id: int) -> int:
        await self.cur.execute(
            "select score::int + 128 as score from emote_ids where emote_hash=(select emote_hash from emote_ids where emote_id=%(emote_id)s) and score::int > -28",
            parameters={"emote_id": emote_id},
        )
        emote_scores = await self.cur.fetchall()
        return [score for score, in emote_scores]

    async def clear_guild_emotes(self, guild_id: int):
        await self.cur.execute(
            "UPDATE emote_ids SET guild_id=null where guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id},
        )

    async def set_emote_usability(self, emote_id: int, usable: Optional[bool]):
        await self.cur.execute(
            "UPDATE emote_ids SET usable=%(usable)s where emote_id=%(emote_id)s",
            parameters={"emote_id": emote_id, "usable": usable},
        )

    async def set_emotes_unusuable(self, emote_ids: List[int]):
        await self.cur.execute(
            "UPDATE emote_ids SET usable=false where emote_id=ANY(%(emote_ids)s)",
            parameters={"emote_ids": emote_ids},
        )

    async def clear_guild_emojis(self, guild_id: int):
        await self.cur.execute(
            "UPDATE emote_ids SET guild_id=null where guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id},
        )

    async def clear_guilds_emojis(self, guild_ids: List[int]):
        await self.cur.execute(
            "UPDATE emote_ids SET guild_id=null where guild_id=ANY(%(guild_ids)s)",
            parameters={"guild_ids": guild_ids},
        )

    async def clear_guilds_from_emojis(self, emoji_ids: List[int]):
        await self.cur.execute(
            "UPDATE emote_ids SET guild_id=null where emote_id=ANY(%(emoji_ids)s)",
            parameters={"emoji_ids": emoji_ids},
        )

    async def get_guilds_with_emojis(self) -> Set[int]:
        await self.cur.execute(
            "SELECT guild_id FROM emote_ids WHERE guild_id IS NOT NULL", parameters={}
        )
        results = await self.cur.fetchall()
        return {guild_id for guild_id, in results}

    async def get_pack_emote(self, pack_name: str, emote_name: str) -> Optional[Emoji]:
        return await self._case_insensitive_get_emote(
            "guild_id=(select guild_id from packs where pack_name=%(pack_name)s)",
            emote_name,
            parameters={"pack_name": pack_name},
        )

    async def get_guild_emote(self, guild_id: int, emote_name: str) -> Optional[Emoji]:
        return await self._case_insensitive_get_emote(
            "guild_id=%(guild_id)s", emote_name, parameters={"guild_id": guild_id}
        )

    async def guild_emote_counts(self, guild_id: int) -> EmojiCounts:
        await self.cur.execute(
            "SELECT COUNT(*) filter(where not animated) as static, COUNT(*) filter(where animated) as animated FROM emote_ids where guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id},
        )
        counts = await self.cur.fetchall()
        return EmojiCounts(static=counts[0][0], animated=counts[0][1])

    async def get_mutual_guild_emote(
        self, user_id: int, emote_name: str
    ) -> Optional[Emoji]:
        return await self._case_insensitive_get_emote(
            "guild_id in (select guild_id from members where user_id=%(user_id)s)",
            emote_name,
            parameters={"user_id": user_id},
        )

    async def get_pack_guild_emote(
        self, user_id: int, emote_name: str
    ) -> Optional[Emoji]:
        return await self._case_insensitive_get_emote(
            "guild_id in (select guild_id from user_packs where user_id=%(user_id)s)",
            emote_name,
            parameters={"user_id": user_id},
        )

    async def get_guild_emotes(
        self, guild_id: int, *, order_by_score: bool = False
    ) -> List[Emoji]:
        return await self._get_emojis(
            "guild_id=%(guild_id)s",
            "order by score desc" if order_by_score else "",
            parameters={"guild_id": guild_id},
        )

    async def get_mutual_guild_emotes(
        self, user_id: int, *, order_by_score: bool = False
    ) -> List[Emoji]:
        return await self._get_emojis(
            "guild_id in (select guild_id from members where user_id=%(user_id)s)",
            "order by score desc" if order_by_score else "",
            parameters={"user_id": user_id},
        )

    async def get_guilds_emotes(
        self, guild_ids: List[int], *, order_by_score: bool = False
    ) -> List[Emoji]:
        return await self._get_emojis(
            "guild_id = ANY(%(guild_ids)s)",
            "order by score desc" if order_by_score else "",
            parameters={"guild_ids": guild_ids},
        )

    async def get_guilds_emotes_raw(self, guild_ids: List[int]) -> List:
        await self.cur.execute(
            f"select emote_id, guild_id, trim(name), has_roles from emote_ids where guild_id = ANY(%(guild_ids)s)",
            parameters={"guild_ids": guild_ids},
        )
        results = await self.cur.fetchall()
        return results

    async def get_guild_emotes_raw(self, guild_id: int) -> List:
        await self.cur.execute(
            f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where guild_id = %(guild_id)s",
            parameters={"guild_id": guild_id},
        )
        results = await self.cur.fetchall()
        return [SQLEmoji(*i) for i in results]

    async def get_emotes_raw(self, emote_ids: List[int]) -> List:
        await self.cur.execute(
            f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where emote_id = ANY(%(emote_ids)s)",
            parameters={"emote_ids": emote_ids},
        )
        results = await self.cur.fetchall()
        return [SQLEmoji(*i) for i in results]

    async def get_emotes(self, emote_ids: List[int]) -> List[Emoji]:
        return await self._get_emojis(
            "emote_id = ANY(%(emote_ids)s) and guild_id is not NULL",
            parameters={"emote_ids": emote_ids},
        )

    async def get_pack_guild_emotes(self, user_id: int) -> List[Emoji]:
        return await self._get_emojis(
            "guild_id in (select guild_id from user_packs where user_id=%(user_id)s)",
            parameters={"user_id": user_id},
        )

    async def get_mega_emotes(self, guild_id: int, emote_name: str) -> List[Emoji]:
        await self.cur.execute(
            "select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where guild_id=%(guild_id)s and trim(name) ~ ('^' || %(emote_name)s || '_\d_\d$') and usable=true and has_roles=false and manual_block=false",
            parameters={"guild_id": guild_id, "emote_name": emote_name},
        )
        results = await self.cur.fetchall()
        return [self._get_emoji(SQLEmoji(*emote)) for emote in results]

    async def _get_emojis(
        self, query_where, query_suffix: str = "", *, parameters
    ) -> List[Emoji]:
        await self.cur.execute(
            f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where {query_where} and usable=true and has_roles=false and manual_block=false {query_suffix}",
            parameters=parameters,
        )
        results = await self.cur.fetchall()
        emotes = (self._get_emoji(SQLEmoji(*i)) for i in results)
        return [emote for emote in emotes if emote]

    async def _case_insensitive_get_emote(
        self, query_where, emote_name: str, parameters
    ) -> Optional[Emoji]:
        case_insensitive = await self._get_emote_with_where(
            f"{query_where} and lower(trim(name))=lower(%(emote_name)s)",
            emote_name,
            parameters,
        )
        # If the least sensitive query didn't get us anything, we're done.
        if case_insensitive is None:
            return
        usable_case_insensitive = case_insensitive if case_insensitive.usable else None

        # First, try case-sensitive
        if case_insensitive.name == emote_name:
            case_sensitive = case_insensitive
        else:
            case_sensitive = await self._get_emote_with_where(
                f"{query_where} and trim(name)=%(emote_name)s", emote_name, parameters
            )
        if case_sensitive is not None:
            assert case_sensitive.name == emote_name
            if case_sensitive.usable:
                return self._get_emoji(case_sensitive)
            # See if we can find a copy that's usable!
            case_sensitive = await self._get_emote_like(
                case_sensitive, require_guild=True
            )
            if case_sensitive is not None:
                return self._get_emoji(case_sensitive)
        # At this point, give up on case sensitivity. Let's see if we can find something usable that's insensitive.
        if usable_case_insensitive is None:
            usable_case_insensitive = await self._get_emote_with_where(
                f"{query_where} and lower(trim(name))=lower(%(emote_name)s) and usable=true",
                emote_name,
                parameters,
            )
        if usable_case_insensitive is None:
            # If not, well we found *something* insensitive earlier
            usable_case_insensitive = await self._get_emote_like(
                case_insensitive, require_guild=True
            )
        if usable_case_insensitive is not None:
            return self._get_emoji(usable_case_insensitive)

    async def _get_emote_with_where(
        self, query_where: str, emote_name: str, parameters
    ) -> Optional[SQLEmoji]:
        await self.cur.execute(
            f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name), has_roles from emote_ids where {query_where} and has_roles=false and manual_block=false LIMIT 1",
            parameters={**parameters, "emote_name": emote_name},
        )
        results = await self.cur.fetchall()
        if results:
            return SQLEmoji(*results[0])

    async def _get_cur_emoji(self) -> Optional[Emoji]:
        results = await self.cur.fetchall()
        if not results:
            return None
        emote = SQLEmoji(*results[0])
        return self._get_emoji(emote)

    def _sql_emoji_to_emoji(self, emote: Optional[SQLEmoji]) -> Optional[Emoji]:
        if emote is not None:
            return self._get_emoji(emote)
