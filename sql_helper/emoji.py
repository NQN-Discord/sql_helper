from collections import namedtuple
from typing import TypedDict

SQLEmoji = namedtuple(
    "SQLEmoji",
    [
        "emote_id",
        "emote_hash",
        "usable",
        "animated",
        "emote_sha",
        "guild_id",
        "name",
        "has_roles",
    ],
)
EmojiCounts = namedtuple("EmojiCounts", ["static", "animated"])


class EmotePerceptualHashData(TypedDict):
    id: int
    animated: bool
    sha: str
    perceptual: str
