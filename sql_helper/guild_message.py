from collections import namedtuple

GuildMessage = namedtuple("GuildMessage", ["message_id", "guild_id", "user_id", "channel_id", "content"])
