from typing import Optional, List, Iterable, Dict, Tuple
from ..guild_message import GuildMessage

from ..async_list import async_list
from .._connection import _PostgresConnection


class GuildMessagesMixin(_PostgresConnection):
    async def add_guild_message(
        self, *, message_id: int, guild_id: int, channel_id: int, user_id: int
    ):
        await self.cur.execute(
            "INSERT INTO guild_messages (guild_id, channel_id, message_id, user_id) VALUES (%(guild_id)s, %(channel_id)s, %(message_id)s, %(user_id)s) ON CONFLICT ON CONSTRAINT guild_messages_pk DO NOTHING",
            parameters={
                "guild_id": guild_id,
                "channel_id": channel_id,
                "message_id": message_id,
                "user_id": user_id,
            },
        )

    async def add_guild_message_bulk(self, messages: Iterable[GuildMessage]):
        guilds = []
        channels = []
        message_ids = []
        users = []
        for i in messages:
            guilds.append(i.guild_id)
            channels.append(i.channel_id)
            message_ids.append(i.message_id)
            users.append(i.user_id)
        await self.cur.execute(
            "INSERT INTO guild_messages (guild_id, channel_id, message_id, user_id) VALUES (unnest(%(guild_id)s), unnest(%(channel_id)s), unnest(%(message_id)s), unnest(%(user_id)s))",
            parameters={
                "guild_id": guilds,
                "channel_id": channels,
                "message_id": message_ids,
                "user_id": users,
            },
        )

    async def delete_guild_message(self, *, message_id: int):
        await self.cur.execute(
            "DELETE FROM guild_messages where message_id=%(message_id)s",
            parameters={"message_id": message_id},
        )

    @async_list
    async def get_guild_messages(
        self,
        *,
        message_id: Optional[int] = None,
        guild_id: Optional[int] = None,
        channel_id: Optional[int] = None,
        user_id: Optional[int] = None,
        after: Optional[int] = None,
        offset: Optional[int] = None,
        no_results: Optional[int],
    ) -> List[GuildMessage]:
        await self._get_guild_message(
            "guild_id, channel_id, message_id, user_id",
            "ORDER BY message_id DESC",
            message_id,
            guild_id,
            channel_id,
            user_id,
            after,
            offset,
            no_results,
        )
        results = await self.cur.fetchall()
        return [GuildMessage(*i) for i in results]

    @async_list
    async def get_guild_message_ids(
        self, guild_id: int, message_ids: List[int]
    ) -> List[Tuple[int, int]]:
        await self.cur.execute(
            f"SELECT message_id, user_id FROM guild_messages WHERE guild_id=%(guild_id)s and message_id=ANY(%(message_ids)s)",
            parameters={"guild_id": guild_id, "message_ids": message_ids},
        )
        return await self.cur.fetchall()

    async def get_guild_message_count(
        self,
        *,
        message_id: Optional[int] = None,
        guild_id: Optional[int] = None,
        channel_id: Optional[int] = None,
        user_id: Optional[int] = None,
    ) -> int:
        await self._get_guild_message(
            "count(*)", "", message_id, guild_id, channel_id, user_id, None, 0, 1
        )
        results = await self.cur.fetchall()
        return results[0][0]

    @async_list
    async def get_users_posted_since(self, since: int):
        await self.cur.execute(
            "select distinct user_id from guild_messages where message_id > %(message_id)s",
            parameters={"message_id": since},
        )
        results = await self.cur.fetchall()
        return (i[0] for i in results)

    async def user_has_posted(self, user_id: int) -> bool:
        await self.cur.execute(
            "select 1 from guild_messages where user_id = %(user_id)s LIMIT 1",
            parameters={"user_id": user_id},
        )
        results = await self.cur.fetchall()
        return bool(results)

    async def _get_guild_message(
        self,
        select: str,
        order: str,
        message_id: Optional[int] = None,
        guild_id: Optional[int] = None,
        channel_id: Optional[int] = None,
        user_id: Optional[int] = None,
        after_id: Optional[int] = None,
        offset: Optional[int] = None,
        no_results: Optional[int] = 1,
    ):
        params = {
            "message_id": message_id,
            "guild_id": guild_id,
            "channel_id": channel_id,
            "user_id": user_id,
            "after_id": after_id,
            "offset": offset,
            "limit": no_results,
        }
        if message_id is not None:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE message_id=%(message_id)s {_get_after_clause(after_id)} {_get_limit_clause(no_results, offset)}",
                parameters=params,
            )
        elif channel_id is not None and user_id is not None:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE channel_id=%(channel_id)s AND user_id=%(user_id)s {_get_after_clause(after_id)} {order} {_get_limit_clause(no_results, offset)}",
                parameters=params,
            )
        elif channel_id is not None:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE channel_id=%(channel_id)s {_get_after_clause(after_id)} {order} {_get_limit_clause(no_results, offset)}",
                parameters=params,
            )
        elif guild_id is not None and user_id is not None:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE guild_id=%(guild_id)s AND user_id=%(user_id)s {_get_after_clause(after_id)} {order} {_get_limit_clause(no_results, offset)}",
                parameters=params,
            )
        elif guild_id is not None:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE guild_id=%(guild_id)s {_get_after_clause(after_id)} {order} {_get_limit_clause(no_results, offset)}",
                parameters=params,
            )
        elif user_id is not None:
            await self.cur.execute(
                f"SELECT {select} FROM guild_messages WHERE user_id=%(user_id)s {_get_after_clause(after_id)} {order} {_get_limit_clause(no_results, offset)}",
                parameters=params,
            )
        else:
            raise NotImplementedError(
                f"Invalid combination of requests: message_id={message_id} guild_id={guild_id} channel_id={channel_id} user_id={user_id}"
            )


def _get_limit_clause(limit: Optional[int], offset: Optional[int]) -> str:
    rtn = []
    if limit is not None:
        rtn.append("LIMIT %(limit)s")
    if offset is not None:
        rtn.append("OFFSET %(offset)s")
    return " ".join(rtn)


def _get_after_clause(after: Optional[int]) -> str:
    if after is None:
        return ""
    return "AND message_id > %(after_id)s"
