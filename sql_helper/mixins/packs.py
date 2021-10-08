from typing import Union, Optional
from discord import User, Guild, Object
from ..pack import Pack

from ..async_list import AsyncList, async_list
from .._connection import _PostgresConnection


class PacksMixin(_PostgresConnection):

    @async_list
    async def pack_ids(self):
        await self.cur.execute("SELECT guild_id, pack_name from packs")
        return await self.cur.fetchall()

    @async_list
    async def user_pack_ids(self, user_id: Union[User, int]) -> AsyncList:
        if not isinstance(user_id, int):
            user_id = user_id.id
        await self.cur.execute(
            "SELECT guild_id FROM user_packs WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )
        guilds = await self.cur.fetchall()
        return [g for g, in guilds]

    @async_list
    async def get_top_packs_by_count(self) -> AsyncList:
        await self.cur.execute(
            "SELECT packs.* FROM (SELECT guild_id, COUNT(*) from user_packs GROUP BY guild_id ORDER BY count DESC LIMIT 25) a JOIN packs on a.guild_id=packs.guild_id",
            parameters={}
        )
        packs = await self.cur.fetchall()
        return [Pack(*p) for p in packs]

    async def user_pack_count(self, user_id: Union[Object, int]) -> int:
        if not isinstance(user_id, int):
            user_id = user_id.id
        await self.cur.execute(
            "SELECT count(*) FROM user_packs WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )
        results = await self.cur.fetchall()
        return results[0][0]

    @async_list
    async def user_packs(self, user_id: Union[User, int]) -> AsyncList:
        if not isinstance(user_id, int):
            user_id = user_id.id
        await self.cur.execute(
            "SELECT packs.* FROM user_packs JOIN packs on packs.guild_id=user_packs.guild_id where user_id=%(user_id)s",
            parameters={"user_id": user_id}
        )
        packs = await self.cur.fetchall()
        return [Pack(*p) for p in packs]

    @async_list
    async def pack_member_ids(self, guild_id: Union[Guild, int]) -> AsyncList:
        if not isinstance(guild_id, int):
            guild_id = guild_id.id
        await self.cur.execute(
            "SELECT user_id FROM user_packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        users = await self.cur.fetchall()
        return [g for g, in users]

    @async_list
    async def all_pack_size_emote_counts(self) -> AsyncList:
        await self.cur.execute(
            "SELECT guild_id, count(*) as user_count, (SELECT count(*) FROM emote_ids WHERE emote_ids.guild_id=user_packs.guild_id) as emote_count FROM user_packs GROUP BY guild_id",
            parameters={}
        )
        return await self.cur.fetchall()

    async def in_pack(self, *, user_id: int, guild_id: int) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM user_packs WHERE user_id=%(user_id)s AND guild_id=%(guild_id)s",
            parameters={"user_id": user_id, "guild_id": guild_id}
        )
        return bool(await self.cur.fetchall())

    async def pack_size(self, guild_id: int) -> int:
        await self.cur.execute(
            "SELECT COUNT(*) FROM user_packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        users = await self.cur.fetchall()
        return users[0][0]

    async def delete_pack(self, guild_id: int):
        await self.cur.execute(
            "DELETE FROM user_packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        await self.cur.execute(
            "DELETE FROM packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )

    async def create_update_pack(self, guild_id: int, name: str, public: bool):
        await self.cur.execute(
            "INSERT INTO packs VALUES (%(guild_id)s, %(pack_name)s, %(is_public)s) "
            "ON CONFLICT (guild_id) DO UPDATE SET pack_name=excluded.pack_name, is_public=excluded.is_public",
            parameters={
                "guild_id": guild_id,
                "pack_name": name,
                "is_public": public,
            }
        )

    async def get_pack_guild(self, guild_id: int) -> Optional[Pack]:
        await self.cur.execute(
            "SELECT * FROM packs WHERE guild_id=%(guild_id)s",
            parameters={"guild_id": guild_id}
        )
        results = await self.cur.fetchall()
        if results:
            return Pack(*results[0])
        else:
            return

    async def get_pack_name(self, name: str) -> Optional[Pack]:
        await self.cur.execute(
            "SELECT * FROM packs WHERE pack_name=%(name)s",
            parameters={"name": name}
        )
        results = await self.cur.fetchall()
        if results:
            return Pack(*results[0])
        else:
            return

    @async_list
    async def get_public_packs(self) -> AsyncList:
        await self.cur.execute(
            "SELECT * FROM packs WHERE is_public=true",
            parameters={}
        )
        results = await self.cur.fetchall()
        return [Pack(*i) for i in results]

    async def join_pack(self, *, user_id: int, guild_id: int):
        await self.cur.execute(
            "INSERT INTO user_packs VALUES (%(user_id)s, %(guild_id)s)",
            parameters={"guild_id": guild_id, "user_id": user_id}
        )

    async def leave_pack(self, *, user_id: int, guild_id: int):
        await self.cur.execute(
            "DELETE FROM user_packs WHERE guild_id=%(guild_id)s AND user_id=%(user_id)s",
            parameters={"guild_id": guild_id, "user_id": user_id}
        )
