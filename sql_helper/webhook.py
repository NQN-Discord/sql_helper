from typing import Optional
from collections import namedtuple


class Webhook(
    namedtuple(
        "Webhook", ["webhook_id", "guild_id", "channel_id", "token", "name", "user_id"]
    )
):
    index = "webhook"

    webhook_id: str
    guild_id: str
    channel_id: str
    user_id: Optional[str]

    token: str
    name: str

    @property
    def url(self) -> str:
        return f"discord.com/api/webhooks/{self.webhook_id}/{self.token}"

    def __eq__(self, other):
        if not isinstance(other, Webhook):
            return False
        return self.webhook_id == other.webhook_id

    def __hash__(self):
        return int(self.webhook_id)
