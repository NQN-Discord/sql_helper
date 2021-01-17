from typing import Union, Awaitable, Optional, List, Tuple, Callable
from discord import User, Guild, Object, Emoji
from discord import Webhook as DiscordWebhook
from .guild_settings import GuildSettings
from .webhook import Webhook
from .pack import Pack
from .guild_feature import GuildFeature
from .premium_user import PremiumUser
from .emoji import SQLEmoji


class AsyncList:
    def __init__(self, contents: Awaitable):
        self.contents = contents

    def __await__(self):
        return self.contents.__await__()

    async def __aiter__(self):
        for i in await self:
            yield i


def async_list(f):
    return lambda *args, **kwargs: AsyncList(f(*args, **kwargs))


class PostgresConnection:
    def __init__(self, pool, get_guild: Callable[[int], Optional[Guild]], get_emoji: Callable[[SQLEmoji], Optional[Emoji]], profiler=None):
        self.pool = pool
        self.pool_acq = None
        self.conn = None
        self.cur_acq = None
        self.cur = None
        self._get_guild = get_guild
        self._get_emoji = get_emoji
        self.profiler = profiler

    @async_list
    async def mutual_guild_ids(self, user_id: Union[User, int]) -> AsyncList:
        if not isinstance(user_id, int):
            user_id = user_id.id
        await self.cur.execute(
            "SELECT guild_id FROM members WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )
        guilds = await self.cur.fetchall()
        return [g for g, in guilds]

    @async_list
    async def mutual_guilds(self, user_id: Union[User, int]) -> AsyncList:
        mutuals = []
        async for guild_id in self.mutual_guild_ids(user_id):
            guild = self._get_guild(guild_id)
            if guild:
                mutuals.append(guild)
        return mutuals

    async def in_guild(self, *, user_id: int, guild_id: int) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM members WHERE user_id=%(user_id)s AND guild_id=%(guild_id)s",
            parameters={"user_id": user_id, "guild_id": guild_id}
        )
        return bool(await self.cur.fetchall())

    @async_list
    async def alias_guilds(self) -> AsyncList:
        await self.cur.execute(
            "SELECT guild_id FROM guild_settings WHERE is_alias_server=true"
        )
        return [gid for gid, in await self.cur.fetchall()]

    @async_list
    async def guild_settings(self) -> AsyncList:
        await self.cur.execute(
            "SELECT guild_id, prefix, nitro_role, boost_channel, boost_role, audit_channel, enable_stickers, enable_nitro, enable_replies, enable_masked_links, is_alias_server, locale, enable_pings, max_guildwide_emotes FROM guild_settings"
        )
        results = await self.cur.fetchall()
        return [GuildSettings(*i) for i in results]

    async def get_guild_settings(self, guild_id: Union[Guild, int]) -> Optional[GuildSettings]:
        await self.cur.execute(
            "SELECT guild_id, prefix, nitro_role, boost_channel, boost_role, audit_channel, enable_stickers, enable_nitro, enable_replies, enable_masked_links, is_alias_server, locale, enable_pings, max_guildwide_emotes FROM guild_settings WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        results = await self.cur.fetchall()
        if not results:
            return None
        return GuildSettings(*results[0])

    async def get_guild_rank(self, max_emotes: int) -> int:
        await self.cur.execute(
            "SELECT count(*) FROM guild_settings WHERE max_guildwide_emotes > %(max_emotes)s",
            parameters={"max_emotes": max_emotes}
        )
        results = await self.cur.fetchall()
        return results[0][0]

    async def get_top_10_rank(self) -> List[Tuple[int, int]]:
        await self.cur.execute(
            "SELECT guild_id, max_guildwide_emotes FROM guild_settings ORDER BY max_guildwide_emotes DESC LIMIT 10"
        )
        return await self.cur.fetchall()

    async def set_user_guilds(self, user_id: int, guild_ids: List[int]):
        async with self.cur.begin():
            await self.cur.execute(
                "DELETE FROM members WHERE members.user_id = %(user_id)s",
                parameters={
                    "user_id": user_id
                }
            )
            for guild_id in guild_ids:
                await self.cur.execute(
                    "INSERT INTO members (guild_id, user_id) VALUES (%(guild_id)s, %(user_id)s)",
                    parameters={
                        "user_id": user_id,
                        "guild_id": guild_id
                    }
                )

    async def set_guild_settings(self, guild: GuildSettings):
        await self.cur.execute(
            "INSERT INTO guild_settings (guild_id, prefix, nitro_role, boost_channel, boost_role, audit_channel, enable_stickers, enable_nitro, enable_replies, enable_masked_links, is_alias_server, locale, enable_pings, max_guildwide_emotes)  VALUES "
            "(%(guild_id)s, %(prefix)s, %(nitro_role)s, %(boost_channel)s, %(boost_role)s, %(audit_channel)s, %(enable_stickers)s, %(enable_nitro)s, %(enable_replies)s, %(enable_masked_links)s, %(is_alias_server)s, %(locale)s, %(enable_pings)s, %(max_guildwide_emotes)s)"
            'ON CONFLICT (guild_id) DO UPDATE SET (prefix, nitro_role, boost_channel, boost_role, audit_channel, enable_stickers, enable_nitro, enable_replies, enable_masked_links, is_alias_server, "locale", enable_pings, max_guildwide_emotes) = '
            "(EXCLUDED.prefix, EXCLUDED.nitro_role, EXCLUDED.boost_channel, EXCLUDED.boost_role, EXCLUDED.audit_channel, EXCLUDED.enable_stickers, EXCLUDED.enable_nitro, EXCLUDED.enable_replies, EXCLUDED.enable_masked_links, EXCLUDED.is_alias_server, EXCLUDED.locale, EXCLUDED.enable_pings, EXCLUDED.max_guildwide_emotes)",
            parameters={slot: getattr(guild, slot) for slot in guild.__slots__}
        )

    async def delete_guild_settings(self, guild_id: int):
        await self.cur.execute(
            "DELETE FROM guild_settings WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )

    @async_list
    async def get_channel_webhooks(self, channel_id: int) -> AsyncList:
        await self.cur.execute(
            "SELECT * FROM webhooks WHERE channel_id=%(channel_id)s",
            parameters={"channel_id": channel_id}
        )
        results = await self.cur.fetchall()
        return [Webhook(*i) for i in results]

    async def set_channel_webhooks(self, channel_id: int, webhooks: List[DiscordWebhook]):
        async with self.cur.begin():
            await self.cur.execute(
                "DELETE FROM webhooks WHERE channel_id=%(channel_id)s",
                parameters={"channel_id": channel_id}
            )
            for webhook in webhooks:
                await self.cur.execute(
                    "INSERT INTO webhooks VALUES (%(webhook_id)s, %(guild_id)s, %(channel_id)s, %(token)s, %(name)s) "
                    "ON CONFLICT (webhook_id) DO UPDATE SET channel_id=excluded.channel_id, name=excluded.name",
                    parameters={
                        "webhook_id": webhook.id,
                        "guild_id": webhook.guild_id,
                        "channel_id": webhook.channel_id,
                        "token": webhook.token,
                        "name": webhook.name
                    }
                )

    @async_list
    async def pack_ids(self):
        await self.cur.execute("SELECT guild_id from packs")
        guilds = await self.cur.fetchall()
        return [g for g, in guilds]

    @async_list
    async def user_pack_ids(self, user_id: Union[User, int]) -> AsyncList:
        if not isinstance(user_id, int):
            user_id = user_id.id
        await self.cur.execute(
            "SELECT guild_id FROM user_packs WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )
        guilds = await self.cur.fetchall()
        return [g for g, in guilds]

    async def user_pack_count(self, user_id: Union[Object, int]) -> int:
        if not isinstance(user_id, int):
            user_id = user_id.id
        await self.cur.execute(
            "SELECT count(*) FROM user_packs WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )
        results = await self.cur.fetchall()
        return results[0][0]

    @async_list
    async def user_packs(self, user_id: Union[User, int]) -> AsyncList:
        if not isinstance(user_id, int):
            user_id = user_id.id
        await self.cur.execute(
            "SELECT packs.* FROM user_packs JOIN packs on packs.guild_id=user_packs.guild_id where user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )
        packs = await self.cur.fetchall()
        return [Pack(*p) for p in packs]

    @async_list
    async def pack_member_ids(self, guild_id: Union[Guild, int]) -> AsyncList:
        if not isinstance(guild_id, int):
            guild_id = guild_id.id
        await self.cur.execute(
            "SELECT user_id FROM user_packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        users = await self.cur.fetchall()
        return [g for g, in users]

    async def in_pack(self, *, user_id: int, guild_id: int) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM user_packs WHERE user_id=%(user_id)s AND guild_id=%(guild_id)s",
            parameters={"user_id": user_id, "guild_id": guild_id}
        )
        return bool(await self.cur.fetchall())

    async def pack_size(self, guild_id: int) -> int:
        await self.cur.execute(
            "SELECT COUNT(*) FROM user_packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        users = await self.cur.fetchall()
        return users[0][0]

    async def delete_pack(self, guild_id: int):
        await self.cur.execute(
            "DELETE FROM user_packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        await self.cur.execute(
            "DELETE FROM packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )

    async def create_update_pack(self, guild_id: int, name: str, public: bool):
        await self.cur.execute(
            "INSERT INTO packs VALUES (%(guild_id)s, %(pack_name)s, %(is_public)s) "
            "ON CONFLICT (guild_id) DO UPDATE SET pack_name=excluded.pack_name, is_public=excluded.is_public",
            parameters={
                "guild_id": guild_id,
                "pack_name": name,
                "is_public": public,
            }
        )

    async def get_pack_guild(self, guild_id: int) -> Optional[Pack]:
        await self.cur.execute(
            "SELECT * FROM packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        results = await self.cur.fetchall()
        if results:
            return Pack(*results[0])
        else:
            return

    async def get_pack_name(self, name: str) -> Optional[Pack]:
        await self.cur.execute(
            "SELECT * FROM packs WHERE pack_name=%(name)s",
            parameters={"name": name}
        )
        results = await self.cur.fetchall()
        if results:
            return Pack(*results[0])
        else:
            return

    async def join_pack(self, *, user_id: int, guild_id: int):
        await self.cur.execute(
            "INSERT INTO user_packs VALUES (%(user_id)s, %(guild_id)s)",
            parameters={"guild_id": guild_id, "user_id": user_id}
        )

    async def leave_pack(self, *, user_id: int, guild_id: int):
        await self.cur.execute(
            "DELETE FROM user_packs WHERE guild_id=%(guild_id)s AND user_id=%(user_id)s",
            parameters={"guild_id": guild_id, "user_id": user_id}
        )

    @async_list
    async def guild_features(self, *, guild_id: int) -> List[GuildFeature]:
        await self.cur.execute(
            "SELECT feature FROM guild_features WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        return [GuildFeature(feature) for feature, in await self.cur.fetchall()]

    async def add_guild_feature(self, *, guild_id: int, feature: GuildFeature):
        await self.cur.execute(
            "INSERT INTO guild_features VALUES(%(guild_id)s, %(feature)s)",
            parameters={"guild_id": guild_id, "feature": feature.value}
        )

    async def remove_guild_feature(self, *, guild_id: int, feature: GuildFeature):
        await self.cur.execute(
            "DELETE FROM guild_features WHERE guild_id=%(guild_id)s AND feature=%(feature)s",
            parameters={"guild_id": guild_id, "feature": feature.value}
        )

    async def set_premium_user(self, user: PremiumUser):
        await self.cur.execute(
            "INSERT INTO premium_users (patreon_id, discord_id, lifetime_support_cents, last_charge_date, last_charge_status, tokens, tokens_spent) VALUES "
            "(%(patreon_id)s, %(discord_id)s, %(lifetime_support_cents)s, %(last_charge_date)s, %(last_charge_status)s, %(tokens)s, %(tokens_spent)s)"
            'ON CONFLICT (patreon_id) DO UPDATE SET (discord_id, lifetime_support_cents, last_charge_date, last_charge_status, tokens, tokens_spent) = '
            "(EXCLUDED.discord_id, EXCLUDED.lifetime_support_cents, EXCLUDED.last_charge_date, EXCLUDED.last_charge_status, EXCLUDED.tokens, EXCLUDED.tokens_spent)",
            parameters={
                "patreon_id": user.patreon_id,
                "discord_id": user.discord_id,
                "lifetime_support_cents": user.lifetime_support_cents,
                "last_charge_date": user.last_charge_date,
                "last_charge_status": user.last_charge_status,
                "tokens": user.tokens,
                "tokens_spent": user.tokens_spent
            }
        )

    async def get_premium_user_patreon(self, patreon_id: str) -> Optional[PremiumUser]:
        await self.cur.execute(
            "SELECT * FROM premium_users WHERE patreon_id=%(patreon_id)s",
            parameters={"patreon_id": patreon_id}
        )
        results = await self.cur.fetchall()
        if not results:
            return None
        return PremiumUser(*results[0])

    async def get_premium_user_discord(self, discord_id: str) -> Optional[PremiumUser]:
        await self.cur.execute(
            "SELECT * FROM premium_users WHERE discord_id=%(discord_id)s",
            parameters={"discord_id": discord_id}
        )

        results = await self.cur.fetchall()
        if not results:
            return None
        return PremiumUser(*results[0])

    async def get_synonyms_for_emote(self, emote_hash: str, limit: Optional[int] = 10) -> List[int]:
        if limit is None:
            await self.cur.execute(
                "SELECT emote_id FROM emote_ids WHERE emote_hash=%(emote_hash)s and usable=true",
                parameters={"emote_hash": emote_hash}
            )
        else:
            await self.cur.execute(
                "SELECT emote_id FROM emote_ids WHERE emote_hash=%(emote_hash)s and usable=true LIMIT %(limit)s",
                parameters={"emote_hash": emote_hash, "limit": limit}
            )

        results = await self.cur.fetchall()
        return [emote_id for emote_id, in results]

    async def get_emote_like(self, emote_id: int) -> Optional[Emoji]:
        await self.cur.execute(
            "SELECT emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name) FROM emote_ids WHERE emote_id=%(emote_id)s LIMIT 1",
            parameters={"emote_id": emote_id}
        )
        results = await self.cur.fetchall()
        if not results:
            return None
        emote = SQLEmoji(*results[0])
        if not (emote.guild_id and emote.usable):
            await self.cur.execute(
                "SELECT emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name) FROM emote_ids WHERE emote_hash=%(emote_hash)s and guild_id is not null and usable=true LIMIT 1",
                parameters={"emote_hash": emote.emote_hash}
            )
            results = await self.cur.fetchall()
            if not results:
                return None
            emote = SQLEmoji(*results[0])
        return self._get_emoji(emote)

    async def set_emote_perceptual_data(self, emote_id: int, guild_id: int, emote_hash: str, emote_sha: str, animated: bool, usable: bool, name: Optional[str]):
        await self.cur.execute(
            "INSERT INTO emote_ids (emote_id, emote_hash, usable, animated, emote_sha, guild_id, name) VALUES (%(emote_id)s, %(emote_hash)s, %(usable)s, %(animated)s, %(emote_sha)s, %(guild_id)s, %(name)s) ON CONFLICT (emote_id) DO UPDATE SET name=%(name)s",
            parameters={
                "emote_id": emote_id,
                "emote_hash": emote_hash,
                "animated": animated,
                "emote_sha": emote_sha,
                "guild_id": guild_id,
                "usable": usable,
                "name": name
            }
        )

    async def set_emote_guild(self, emote_id: int, guild_id: Optional[int], usable: Optional[bool], name: Optional[str]):
        # Called when we leave a guild, or an emote is otherwise created
        if usable is None:
            # We don't know if the emote is available or not
            await self.cur.execute(
                "UPDATE emote_ids SET guild_id=%(guild_id)s, name=%(name)s where emote_id=%(emote_id)s",
                parameters={"emote_id": emote_id, "guild_id": guild_id, "name": name}
            )
        else:
            await self.cur.execute(
                "UPDATE emote_ids SET guild_id=%(guild_id)s, usable=%(usable)s, name=%(name)s where emote_id=%(emote_id)s",
                parameters={"emote_id": emote_id, "guild_id": guild_id, "usable": usable, "name": name}
            )

    async def set_emote_usability(self, emote_id: int, usable: Optional[bool]):
        await self.cur.execute(
            "UPDATE emote_ids SET usable=%(usable)s where emote_id=%(emote_id)s",
            parameters={"emote_id": emote_id, "usable": usable}
        )

    async def clear_guild_emojis(self, guild_id: int):
        await self.cur.execute(
            "UPDATE emote_ids SET guild_id=null where guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )

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

    async def _case_insensitive_get_emote(self, query_where, emote_name: str, parameters) -> Optional[Emoji]:
        await self.cur.execute(
            f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name) from emote_ids where {query_where} and lower(trim(name))=lower(%(emote_name)s) and usable=true",
            parameters={**parameters, "emote_name": emote_name}
        )
        emote = await self._get_cur_emoji()
        if emote and emote.name != emote_name:
            await self.cur.execute(
                f"select emote_id, emote_hash, usable, animated, emote_sha, guild_id, trim(name) from emote_ids where {query_where} and trim(name)=%(emote_name)s and usable=true",
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

    async def __aenter__(self):
        self.pool_acq = self.pool.acquire()
        self.conn = await self.pool_acq.__aenter__()
        self.cur_acq = self.conn.cursor()
        self.cur = await self.cur_acq.__aenter__()
        if self.profiler is not None:
            self.cur.execute = self.profiler(self.cur.execute)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cur_acq.__aexit__(exc_type, exc_val, exc_tb)
        rtn = await self.pool_acq.__aexit__(exc_type, exc_val, exc_tb)
        self.pool_acq = None
        self.conn = None
        self.cur_acq = None
        self.cur = None
        return rtn


class SQLConnection:
    def __init__(
            self,
            pool,
            get_guild: Optional[Callable[[int], Optional[Guild]]] = None,
            get_emoji: Optional[Callable[[SQLEmoji], Optional[Emoji]]] = None,
            profiler=None
    ):
        self.pool = pool
        self._get_guild = get_guild or (lambda id: None)
        self._get_emoji = get_emoji or (lambda emoji: None)
        self.profiler = profiler

    def __call__(self) -> PostgresConnection:
        return PostgresConnection(self.pool, self._get_guild, self._get_emoji, self.profiler)
