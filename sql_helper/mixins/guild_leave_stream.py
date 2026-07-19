from typing import List

from ..async_list import async_list
from .._connection import _PostgresConnection


class GuildLeaveStreamMixin(_PostgresConnection):
    @async_list
    async def get_guild_leave_stream(self) -> List[str]:
        await self.cur.execute(
            "SELECT leave_data FROM guild_leave_stream",
        )
        rtn = [leave_data for leave_data, in await self.cur.fetchall()]
        await self.cur.execute("TRUNCATE guild_leave_stream")
        return rtn
