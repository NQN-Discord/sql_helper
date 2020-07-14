from typing import Optional


class GuildSettings:
    __slots__ = [
        "guild_id",
        "prefix",
        "announcement_channel",
        "boost_channel",
        "boost_role",
        "audit_channel",
        "enable_stickers",
        "enable_nitro",
        "enable_replies",
        "enable_masked_links",
        "is_alias_server"
    ]
    
    def __init__(
            self,
            guild_id,
            prefix,
            announcement_channel,
            boost_channel,
            boost_role,
            audit_channel,
            enable_stickers,
            enable_nitro,
            enable_replies,
            enable_masked_links,
            is_alias_server
    ):
        self.guild_id = guild_id
        self.prefix = prefix
        self.announcement_channel = announcement_channel
        self.boost_channel = boost_channel
        self.boost_role = boost_role
        self.audit_channel = audit_channel
        self.enable_stickers = self._parse_boolean(enable_stickers)
        self.enable_nitro = self._parse_boolean(enable_nitro)
        self.enable_replies = self._parse_boolean(enable_replies)
        self.enable_masked_links = self._parse_boolean(enable_masked_links)
        self.is_alias_server = self._parse_boolean(is_alias_server)

    def _parse_boolean(self, bool: Optional[bool], on_none: Optional[bool] = True):
        return on_none if bool is None else bool
