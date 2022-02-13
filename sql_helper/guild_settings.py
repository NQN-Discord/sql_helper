from typing import Optional
from dataclasses import dataclass


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
