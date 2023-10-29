from typing import Optional, Callable
from discord import Guild, Emoji
from .emoji import SQLEmoji

from .mixins import *


class PostgresConnection(
    AliasesMixin,
    BlockedEmojisMixin,
    CommandMessagesMixin,
    EmojisMixin,
    EmojiHashesMixin,
    EmojisUsedMixin,
    GuildMembersMixin,
    GuildMessagesMixin,
    GuildSettingsMixin,
    GuildWebhooksMixin,
    PacksMixin,
    PersonasMixin,
    PremiumMixin,
):
    pass


class SQLConnection:
    def __init__(
        self,
        pool,
        get_guild: Optional[Callable[[int], Optional[Guild]]] = None,
        get_emoji: Optional[Callable[[SQLEmoji], Optional[Emoji]]] = None,
        profiler=None,
    ):
        self.pool = pool
        self._get_guild = get_guild or (lambda id: None)
        self._get_emoji = get_emoji or (lambda emoji: None)
        self.profiler = profiler

    def __call__(self) -> PostgresConnection:
        return PostgresConnection(
            self.pool, self._get_guild, self._get_emoji, self.profiler
        )
