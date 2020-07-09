from typing import Optional
from collections import namedtuple


class GuildSettings(namedtuple("GuildSettings", [
    "guild_id", "prefix", "announcement_channel", "boost_channel", "boost_role", "audit_channel", "enable_stickers",
    "enable_nitro", "enable_replies", "enable_masked_links", "is_alias_server"
])):
    index = "guild_settings"

    prefix: str
    announcement_channel: Optional[int]
    boost_channel: Optional[int]
    boost_role: Optional[int]
    audit_channel: Optional[int]

    enable_stickers: Optional[bool]
    enable_nitro: Optional[bool]
    enable_replies: Optional[bool]
    enable_masked_links: Optional[bool]

    is_alias_server: Optional[bool]
