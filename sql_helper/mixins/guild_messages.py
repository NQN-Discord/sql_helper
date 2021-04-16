from typing import Optional, List
from ..guild_message import GuildMessage

from ..async_list import async_list
from .._connection import _PostgresConnection


class GuildMessagesMixin(_PostgresConnection):

    async def add_guild_message(self, *, message_id: int, guild_id: int, channel_id: int, user_id: int, content: str):
        try:
            await self.cur.execute(
                "INSERT INTO guild_messages (guild_id, channel_id, message_id, user_id, content) VALUES (%(guild_id)s, %(channel_id)s, %(message_id)s, %(user_id)s, %(content)s) ON CONFLICT ON CONSTRAINT guild_messages_pk DO UPDATE SET content = %(content)s",
                parameters={
                    "guild_id": guild_id,
                    "channel_id": channel_id,
                    "message_id": message_id,
                    "user_id": user_id,
                    "content": content
                }
            )
        except ValueError:
            # A string literal cannot contain NUL (0x00) characters.
            pass

    async def add_guild_message_bulk(self, messages: List[GuildMessage]):
        guilds = []
        channels = []
        message_ids = []
        users = []
        contents = []
        for i in messages:
            guilds.append(i.guild_id)
            channels.append(i.channel_id)
            message_ids.append(i.message_id)
            users.append(i.user_id)
            contents.append(i.content)
        try:
            await self.cur.execute(
                "INSERT INTO guild_messages (guild_id, channel_id, message_id, user_id, content) VALUES (unnest(%(guild_id)s), unnest(%(channel_id)s), unnest(%(message_id)s), unnest(%(user_id)s), unnest(%(content)s)) ON CONFLICT ON CONSTRAINT guild_messages_pk DO UPDATE SET content = excluded.content",
                parameters={
                    "guild_id": guilds,
                    "channel_id": channels,
                    "message_id": message_ids,
                    "user_id": users,
                    "content": contents
                }
            )
        except ValueError:
            # A string literal cannot contain NUL (0x00) characters.
            for i in messages:
                try:
                    await self.add_guild_message(
                        message_id=i.message_id,
                        channel_id=i.channel_id,
                        guild_id=i.guild_id,
                        user_id=i.user_id,
                        content=i.content
                    )
                except ValueError:
                    pass

    async def delete_guild_message(self, *, message_id: int):
        await self.cur.execute(
            "DELETE FROM guild_messages where message_id=%(message_id)s",
            parameters={
                "message_id": message_id
            }
        )

    @async_list
    async def get_guild_messages(
            self,
            *,
            message_id: Optional[int] = None,
            guild_id: Optional[int] = None,
            channel_id: Optional[int] = None,
            user_id: Optional[int] = None,
            offset: Optional[int] = None,
            no_results: int
    ) -> List[GuildMessage]:
        await self._get_guild_message("guild_id, channel_id, message_id, user_id, content", "ORDER BY message_id DESC", message_id, guild_id, channel_id, user_id, offset, no_results)
        results = await self.cur.fetchall()
        return [GuildMessage(*i) for i in results]

    async def get_guild_message_count(
            self,
            *,
            message_id: Optional[int] = None,
            guild_id: Optional[int] = None,
            channel_id: Optional[int] = None,
            user_id: Optional[int] = None,
    ) -> int:
        await self._get_guild_message("count(*)", "", message_id, guild_id, channel_id, user_id, 0, 1)
        results = await self.cur.fetchall()
        return results[0][0]

    async def _get_guild_message(
            self,
            select: str,
            order: str,
            message_id: Optional[int] = None,
            guild_id: Optional[int] = None,
            channel_id: Optional[int] = None,
            user_id: Optional[int] = None,
            offset: Optional[int] = None,
            no_results: int = 1
    ):
        if message_id is not None:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE message_id=%(message_id)s LIMIT %(limit)s OFFSET %(offset)s",
                parameters={"message_id": message_id, "limit": no_results, "offset": offset}
            )
        elif channel_id and user_id:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE channel_id=%(channel_id)s AND user_id=%(user_id)s {order} LIMIT %(limit)s OFFSET %(offset)s",
                parameters={"channel_id": channel_id, "user_id": user_id, "limit": no_results, "offset": offset}
            )
        elif channel_id:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE channel_id=%(channel_id)s {order} LIMIT %(limit)s OFFSET %(offset)s",
                parameters={"channel_id": channel_id, "limit": no_results, "offset": offset}
            )
        elif guild_id and user_id:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE guild_id=%(guild_id)s AND user_id=%(user_id)s {order} LIMIT %(limit)s OFFSET %(offset)s",
                parameters={"guild_id": guild_id, "user_id": user_id, "limit": no_results, "offset": offset}
            )
        elif guild_id:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE guild_id=%(guild_id)s {order} LIMIT %(limit)s OFFSET %(offset)s",
                parameters={"guild_id": guild_id, "limit": no_results, "offset": offset}
            )
        else:
            raise NotImplementedError(f"Invalid combination of requests: message_id={message_id} guild_id={guild_id} channel_id={channel_id} user_id={user_id}")
