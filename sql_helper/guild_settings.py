from typing import Optional
from dataclasses import dataclass
from enum import Flag, auto


class SettingsFlags(Flag):
    stickers = auto()
    nitro = auto()
    replies = auto()
    masked_links = auto()
    alias_server = auto()
    pings = auto()
    user_content = auto()
    personas = auto()
    dashboard_posting = auto()
    phish_detection = auto()


DEFAULTS = (
    SettingsFlags.stickers |
    SettingsFlags.nitro |
    SettingsFlags.replies |
    SettingsFlags.masked_links |
    SettingsFlags.pings |
    SettingsFlags.user_content |
    SettingsFlags.dashboard_posting |
    SettingsFlags.phish_detection
)


@dataclass()
class GuildSettings:
    guild_id: int
    prefix: str = "!"
    locale: str = "en"
    max_guildwide_emotes: int = 10
    nitro_role: Optional[int] = None
    boost_channel: Optional[int] = None
    boost_role: Optional[int] = None
    audit_channel: Optional[int] = None
    enable_stickers: bool = True
    enable_nitro: bool = True
    enable_replies: bool = True
    enable_masked_links: bool = True
    is_alias_server: bool = False
    enable_pings: bool = True
    enable_user_content: bool = True
    enable_personas: bool = False
    enable_dashboard_posting: bool = True
    enable_phish_detection: bool = True
