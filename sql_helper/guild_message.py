from collections import namedtuple

GuildMessage = namedtuple("GuildMessage", ["guild_id", "channel_id", "message_id", "user_id", "content"])
