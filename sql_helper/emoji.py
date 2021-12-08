from collections import namedtuple


SQLEmoji = namedtuple("SQLEmoji", ["emote_id", "emote_hash", "usable", "animated", "emote_sha", "guild_id", "name", "has_roles"])
EmojiCounts = namedtuple("EmojiCounts", ["static", "animated"])
