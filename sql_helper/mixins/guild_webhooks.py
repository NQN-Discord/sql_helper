from typing import List, Optional
from discord import Webhook as DiscordWebhook
from ..webhook import Webhook

from ..async_list import AsyncList, async_list
from .._connection import _PostgresConnection


class GuildWebhooksMixin(_PostgresConnection):
    @async_list
    async def get_channel_webhooks(self, channel_id: int) -> AsyncList:
        await self.cur.execute(
            "SELECT * FROM webhooks WHERE channel_id=%(channel_id)s",
            parameters={"channel_id": channel_id}
        )
        results = await self.cur.fetchall()
        return [Webhook(*i) for i in results]

    async def get_webhook_by_id(self, webhook_id: int) -> Optional[DiscordWebhook]:
        await self.cur.execute(
            "SELECT * FROM webhooks WHERE webhook_id=%(webhook_id)s",
            parameters={"webhook_id": webhook_id}
        )
        results = await self.cur.fetchall()
        if results:
            return Webhook(*results[0])

    async def set_channel_webhooks(self, channel_id: int, webhooks: List[DiscordWebhook], *, delete: bool = True):
        async with self.cur.begin():
            if delete:
                await self.cur.execute(
                    "DELETE FROM webhooks WHERE channel_id=%(channel_id)s",
                    parameters={"channel_id": channel_id}
                )
            for webhook in webhooks:
                try:
                    user_id = webhook.user_id
                except AttributeError:
                    user_id = webhook.user and webhook.user.id
                await self.cur.execute(
                    "INSERT INTO webhooks VALUES (%(webhook_id)s, %(guild_id)s, %(channel_id)s, %(token)s, %(name)s, %(user_id)s) "
                    "ON CONFLICT (webhook_id) DO UPDATE SET channel_id=excluded.channel_id, name=excluded.name",
                    parameters={
                        "webhook_id": webhook.id,
                        "guild_id": webhook.guild_id,
                        "channel_id": webhook.channel_id,
                        "user_id": user_id,
                        "token": webhook.token,
                        "name": webhook.name
                    }
                )
