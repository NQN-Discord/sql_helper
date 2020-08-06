from typing import Union, Awaitable, Optional, List
from discord import User, Guild
from discord import Webhook as DiscordWebhook
from discord.ext.commands import Bot
from .guild_settings import GuildSettings
from .webhook import Webhook


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
    def __init__(self, pool, bot: Optional[Bot]):
        self.pool = pool
        self.pool_acq = None
        self.conn = None
        self.cur_acq = None
        self.cur = None
        self.bot = bot

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
            guild = self.bot.get_guild(guild_id)
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
            "SELECT * FROM guild_settings"
        )
        results = await self.cur.fetchall()
        return [GuildSettings(*i) for i in results]

    async def get_guild_settings(self, guild_id: Union[Guild, int]) -> Optional[GuildSettings]:
        await self.cur.execute(
            "SELECT * FROM guild_settings WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        results = await self.cur.fetchall()
        if not results:
            return None
        return GuildSettings(*results[0])

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
            "INSERT INTO guild_settings VALUES "
            "(%(guild_id)s, %(prefix)s, %(announcement_channel)s, %(boost_channel)s, %(boost_role)s, %(audit_channel)s, %(enable_stickers)s, %(enable_nitro)s, %(enable_replies)s, %(enable_masked_links)s, %(is_alias_server)s, %(locale)s)"
            'ON CONFLICT (guild_id) DO UPDATE SET (prefix, announcement_channel, boost_channel, boost_role, audit_channel, enable_stickers, enable_nitro, enable_replies, enable_masked_links, is_alias_server, "locale") = '
            "(EXCLUDED.prefix, EXCLUDED.announcement_channel, EXCLUDED.boost_channel, EXCLUDED.boost_role, EXCLUDED.audit_channel, EXCLUDED.enable_stickers, EXCLUDED.enable_nitro, EXCLUDED.enable_replies, EXCLUDED.enable_masked_links, EXCLUDED.is_alias_server, EXCLUDED.locale)",
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

    async def __aenter__(self):
        self.pool_acq = self.pool.acquire()
        self.conn = await self.pool_acq.__aenter__()
        self.cur_acq = self.conn.cursor()
        self.cur = await self.cur_acq.__aenter__()
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
    def __init__(self, pool, bot: Optional[Bot] = None):
        self.pool = pool
        self.bot = bot

    def __call__(self) -> PostgresConnection:
        return PostgresConnection(self.pool, self.bot)
