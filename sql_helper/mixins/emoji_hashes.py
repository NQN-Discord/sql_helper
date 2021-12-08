from typing import List, Set

from .._connection import _PostgresConnection


class EmojiHashesMixin(_PostgresConnection):
    async def is_emote_hash_filtered(self, emote_hash: str) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM emote_hashes WHERE emote_hash=%(emote_hash)s and filtered=true",
            parameters={"emote_hash": emote_hash}
        )
        results = await self.cur.fetchall()
        return bool(results)

    async def get_all_emote_hashes_filtered(self, emote_hashes: List[str]) -> Set[str]:
        await self.cur.execute(
            "SELECT emote_hash FROM emote_hashes WHERE emote_hash=ANY(%(emote_hashes)s) and filtered=true",
            parameters={"emote_hashes": emote_hashes}
        )
        results = await self.cur.fetchall()
        return set(hash for hash, in results)

    async def mark_emote_hash_filtered(self, emote_hash: str):
        await self.cur.execute(
            "INSERT INTO emote_hashes (emote_hash, filtered) VALUES (%(emote_hash)s, true)",
            parameters={"emote_hash": emote_hash}
        )
