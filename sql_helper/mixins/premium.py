from typing import Optional, List
from ..guild_feature import GuildFeature
from ..premium_user import PremiumUser

from ..async_list import async_list
from .._connection import _PostgresConnection


class PremiumMixin(_PostgresConnection):
    @async_list
    async def guild_features(self, *, guild_id: int) -> List[GuildFeature]:
        await self.cur.execute(
            "SELECT feature FROM guild_features WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id},
        )
        return [GuildFeature(feature) for feature, in await self.cur.fetchall()]

    async def add_guild_feature(
        self, *, guild_ids: List[int], features: List[GuildFeature]
    ):
        if guild_ids and features:
            await self.cur.execute(
                "INSERT INTO guild_features (SELECT * from unnest(%(guild_ids)s) as guild_id CROSS JOIN unnest(%(feature)s::guild_features_enum[]) as feature) "
                "ON CONFLICT (guild_id, feature) DO NOTHING",
                parameters={
                    "guild_ids": guild_ids,
                    "feature": [feature.value for feature in features],
                },
            )

    async def remove_guild_feature(
        self, *, guild_ids: List[int], features: List[GuildFeature]
    ):
        if guild_ids and features:
            await self.cur.execute(
                "DELETE FROM guild_features WHERE guild_id=ANY(%(guild_ids)s) AND feature=ANY(%(feature)s::guild_features_enum[])",
                parameters={
                    "guild_ids": guild_ids,
                    "feature": [feature.value for feature in features],
                },
            )
            if GuildFeature.USER_CONTENT in features:
                await self.cur.execute(
                    "UPDATE guild_settings SET enable_user_content=true WHERE guild_id=ANY(%(guild_ids)s)",
                    parameters={"guild_ids": guild_ids},
                )
            if GuildFeature.EMOTE_ROLES in features:
                await self.cur.execute(
                    "UPDATE guild_settings SET nitro_role=null WHERE guild_id=ANY(%(guild_ids)s)",
                    parameters={"guild_ids": guild_ids},
                )

    async def set_premium_user_discord_override(self, member_id: str, discord_id: int):
        await self.cur.execute(
            "UPDATE premium_users SET discord_override_id=%(discord_id)s WHERE pledge_id=%(member_id)s",
            parameters={"member_id": member_id, "discord_id": discord_id},
        )

    async def get_premium_user_discord_ids(self) -> List[int]:
        await self.cur.execute(
            "SELECT COALESCE(discord_override_id, discord_patreon_id) FROM premium_users where discord_patreon_id is not null or discord_override_id is not null",
            parameters={},
        )
        results = await self.cur.fetchall()
        return [user_id for user_id, in results]

    async def get_premium_guild_ids(self) -> List[int]:
        await self.cur.execute(
            "SELECT guild_id FROM guild_features where feature = 'premium'",
            parameters={},
        )
        results = await self.cur.fetchall()
        return [guild_id for guild_id, in results]

    async def update_premium_users(self, users: List[PremiumUser]):
        if not users:
            return
        patreon_ids = []
        discord_ids = []
        lifetime_support_cents = []
        last_charge_dates = []
        last_charge_status = []
        pledge_ids = []
        for user in users:
            patreon_ids.append(user.patreon_id)
            discord_ids.append(user.discord_id)
            lifetime_support_cents.append(user.lifetime_support_cents)
            last_charge_dates.append(user.last_charge_date)
            last_charge_status.append(
                user.last_charge_status and user.last_charge_status.value
            )
            pledge_ids.append(user.pledge_id)
        await self.cur.execute(
            "INSERT INTO premium_users (patreon_id, discord_patreon_id, lifetime_support_cents, last_charge_date, last_charge_status, tokens, tokens_spent, pledge_id) VALUES "
            "(unnest(%(patreon_ids)s), unnest(%(discord_ids)s::bigint[]), unnest(%(lifetime_support_cents)s), unnest(%(last_charge_dates)s), unnest(%(last_charge_status)s)::premium_last_charge_status_enum, 0, 0, unnest(%(pledge_ids)s))"
            "ON CONFLICT (patreon_id) DO UPDATE SET (discord_patreon_id, lifetime_support_cents, last_charge_date, last_charge_status, tokens, tokens_spent, pledge_id) = "
            "(EXCLUDED.discord_patreon_id, EXCLUDED.lifetime_support_cents, EXCLUDED.last_charge_date, EXCLUDED.last_charge_status, premium_users.tokens, premium_users.tokens_spent, EXCLUDED.pledge_id)",
            parameters={
                "patreon_ids": patreon_ids,
                "discord_ids": discord_ids,
                "lifetime_support_cents": lifetime_support_cents,
                "last_charge_dates": last_charge_dates,
                "last_charge_status": last_charge_status,
                "pledge_ids": pledge_ids,
            },
        )

    async def get_premium_user_patreon(self, member_id: str) -> Optional[PremiumUser]:
        await self.cur.execute(
            "SELECT * FROM premium_users WHERE pledge_id=%(pledge_id)s",
            parameters={"pledge_id": member_id},
        )
        results = await self.cur.fetchall()
        if not results:
            return None
        return PremiumUser(*results[0])

    async def get_premium_user_discord(self, discord_id: int) -> Optional[PremiumUser]:
        await self.cur.execute(
            "SELECT * FROM premium_users WHERE coalesce(discord_override_id, discord_patreon_id)=%(discord_id)s",
            parameters={"discord_id": discord_id},
        )

        results = await self.cur.fetchall()
        if not results:
            return None
        return PremiumUser(*results[0])

    async def get_premium_users(self) -> List[PremiumUser]:
        await self.cur.execute(
            "SELECT * FROM premium_users WHERE last_charge_status is not null",
            parameters={},
        )
        results = await self.cur.fetchall()
        return [PremiumUser(*i) for i in results]
