from typing import Union, Optional, List
from discord import User

from ..async_list import AsyncList, async_list
from .._connection import _PostgresConnection


class GuildMembersMixin(_PostgresConnection):
    @async_list
    async def mutual_guild_ids(self, user_id: Union[User, int]) -> AsyncList:
        if not isinstance(user_id, int):
            user_id = user_id.id
        await self.cur.execute(
            "SELECT guild_id FROM members WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id},
        )
        guilds = await self.cur.fetchall()
        return [g for g, in guilds]

    @async_list
    async def mutual_guilds(self, user_id: Union[User, int]) -> AsyncList:
        mutuals = []
        async for guild_id in self.mutual_guild_ids(user_id):
            guild = self._get_guild(guild_id)
            if guild:
                mutuals.append(guild)
        return mutuals

    async def in_guild(self, *, user_id: int, guild_id: int) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM members WHERE user_id=%(user_id)s AND guild_id=%(guild_id)s LIMIT 1",
            parameters={"user_id": user_id, "guild_id": guild_id},
        )
        return bool(await self.cur.fetchall())

    async def random_member(self, guild_id: int) -> Optional[int]:
        for i in range(10):
            await self.cur.execute(
                "SELECT user_id FROM members WHERE guild_id=%(guild_id)s AND random() < (SELECT 2::decimal/count(*) FROM members WHERE guild_id=%(guild_id)s) LIMIT 1",
                parameters={"guild_id": guild_id},
            )
            user_id = await self.cur.fetchall()
            if user_id:
                return user_id[0][0]
        return

    async def set_user_guilds(self, user_id: int, guild_ids: List[int]):
        async with self.cur.begin():
            await self.cur.execute(
                "DELETE FROM members WHERE members.user_id = %(user_id)s",
                parameters={"user_id": user_id},
            )
            for guild_id in guild_ids:
                await self.cur.execute(
                    "INSERT INTO members (guild_id, user_id) VALUES (%(guild_id)s, %(user_id)s)",
                    parameters={"user_id": user_id, "guild_id": guild_id},
                )
