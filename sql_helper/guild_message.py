from dataclasses import dataclass


@dataclass()
class GuildMessage:
    guild_id: int
    channel_id: int
    message_id: int
    user_id: int
