from typing import Optional, Callable
from discord import Guild, Emoji
from .emoji import SQLEmoji


class _PostgresConnection:
    def __init__(
        self,
        pool,
        get_guild: Callable[[int], Optional[Guild]],
        get_emoji: Callable[[SQLEmoji], Optional[Emoji]],
        profiler=None,
    ):
        self.pool = pool
        self.pool_acq = None
        self.conn = None
        self.cur_acq = None
        self.cur = None
        self._get_guild = get_guild
        self._get_emoji = get_emoji
        self.profiler = profiler

    async def __aenter__(self) -> "_PostgresConnection":
        self.pool_acq = self.pool.acquire()
        self.conn = await self.pool_acq.__aenter__()
        self.cur_acq = self.conn.cursor()
        self.cur = await self.cur_acq.__aenter__()
        if self.profiler is not None:
            self.cur.execute = self.profiler(self.cur.execute)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cur_acq.__aexit__(exc_type, exc_val, exc_tb)
        rtn = await self.pool_acq.__aexit__(exc_type, exc_val, exc_tb)
        self.pool_acq = None
        self.conn = None
        self.cur_acq = None
        self.cur = None
        return rtn
