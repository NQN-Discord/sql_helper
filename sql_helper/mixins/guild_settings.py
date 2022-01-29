from typing import Union, Optional, List, Tuple
from discord import Guild
from dataclasses import fields
from ..guild_settings import GuildSettings

from ..async_list import AsyncList, async_list
from .._connection import _PostgresConnection


class GuildSettingsMixin(_PostgresConnection):
    @async_list
    async def alias_guilds(self) -> AsyncList:
        await self.cur.execute(
            "SELECT guild_id FROM guild_settings WHERE is_alias_server=true"
        )
        return [gid for gid, in await self.cur.fetchall()]

    @async_list
    async def personas_guilds(self) -> AsyncList:
        await self.cur.execute(
            "SELECT guild_id FROM guild_settings WHERE enable_personas=true"
        )
        return [gid for gid, in await self.cur.fetchall()]

    @async_list
    async def guild_settings(self) -> AsyncList:
        await self.cur.execute(
            "SELECT guild_id, prefix, locale, max_guildwide_emotes, nitro_role, boost_channel, boost_role, audit_channel, enable_stickers, enable_nitro, enable_replies, enable_masked_links, is_alias_server, enable_pings, enable_user_content, enable_personas, enable_dashboard_posting FROM guild_settings"
        )
        results = await self.cur.fetchall()
        return [GuildSettings(*i) for i in results]

    async def get_guild_settings(self, guild_id: Union[Guild, int]) -> Optional[GuildSettings]:
        await self.cur.execute(
            "SELECT guild_id, prefix, locale, max_guildwide_emotes, nitro_role, boost_channel, boost_role, audit_channel, enable_stickers, enable_nitro, enable_replies, enable_masked_links, is_alias_server, enable_pings, enable_user_content, enable_personas, enable_dashboard_posting FROM guild_settings WHERE guild_id=%(guild_id)s",
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

    async def set_guild_settings(self, guild_settings: GuildSettings):
        await self.cur.execute(
            "INSERT INTO guild_settings (guild_id, prefix, nitro_role, boost_channel, boost_role, audit_channel, enable_stickers, enable_nitro, enable_replies, enable_masked_links, is_alias_server, locale, enable_pings, max_guildwide_emotes, enable_user_content, enable_personas, enable_dashboard_posting)  VALUES "
            "(%(guild_id)s, %(prefix)s, %(nitro_role)s, %(boost_channel)s, %(boost_role)s, %(audit_channel)s, %(enable_stickers)s, %(enable_nitro)s, %(enable_replies)s, %(enable_masked_links)s, %(is_alias_server)s, %(locale)s, %(enable_pings)s, %(max_guildwide_emotes)s, %(enable_user_content)s, %(enable_personas)s, %(enable_dashboard_posting)s)"
            'ON CONFLICT (guild_id) DO UPDATE SET (prefix, nitro_role, boost_channel, boost_role, audit_channel, enable_stickers, enable_nitro, enable_replies, enable_masked_links, is_alias_server, "locale", enable_pings, max_guildwide_emotes, enable_user_content, enable_personas, enable_dashboard_posting) = '
            "(EXCLUDED.prefix, EXCLUDED.nitro_role, EXCLUDED.boost_channel, EXCLUDED.boost_role, EXCLUDED.audit_channel, EXCLUDED.enable_stickers, EXCLUDED.enable_nitro, EXCLUDED.enable_replies, EXCLUDED.enable_masked_links, EXCLUDED.is_alias_server, EXCLUDED.locale, EXCLUDED.enable_pings, EXCLUDED.max_guildwide_emotes, EXCLUDED.enable_user_content, EXCLUDED.enable_personas, EXCLUDED.enable_dashboard_posting)",
            parameters={field.name: getattr(guild_settings, field.name) for field in fields(guild_settings)}
        )

    async def delete_guild_settings(self, guild_id: int):
        await self.cur.execute(
            "DELETE FROM guild_settings WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
