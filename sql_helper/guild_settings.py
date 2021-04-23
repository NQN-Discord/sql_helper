from typing import Optional


class GuildSettings:
    __slots__ = [
        "guild_id",
        "prefix",
        "nitro_role",
        "boost_channel",
        "boost_role",
        "audit_channel",
        "enable_stickers",
        "enable_nitro",
        "enable_replies",
        "enable_masked_links",
        "is_alias_server",
        "locale",
        "enable_pings",
        "max_guildwide_emotes",
        "enable_user_content",
        "enable_personas"
    ]
    
    def __init__(
            self,
            guild_id,
            prefix,
            nitro_role,
            boost_channel,
            boost_role,
            audit_channel,
            enable_stickers,
            enable_nitro,
            enable_replies,
            enable_masked_links,
            is_alias_server,
            locale,
            enable_pings,
            max_guildwide_emotes,
            enable_user_content,
            enable_personas
    ):
        self.guild_id = guild_id
        self.prefix = prefix
        self.nitro_role = nitro_role
        self.boost_channel = boost_channel
        self.boost_role = boost_role
        self.audit_channel = audit_channel
        self.enable_stickers = self._parse_boolean(enable_stickers)
        self.enable_nitro = self._parse_boolean(enable_nitro)
        self.enable_replies = self._parse_boolean(enable_replies)
        self.enable_masked_links = self._parse_boolean(enable_masked_links)
        self.enable_pings = self._parse_boolean(enable_pings)
        self.is_alias_server = self._parse_boolean(is_alias_server, on_none=False)
        self.locale = locale
        self.max_guildwide_emotes = max_guildwide_emotes
        self.enable_user_content = self._parse_boolean(enable_user_content)
        self.enable_personas = self._parse_boolean(enable_personas, on_none=False)

    def _parse_boolean(self, bool: Optional[bool], on_none: Optional[bool] = True):
        return on_none if bool is None else bool
