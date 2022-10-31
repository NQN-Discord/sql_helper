from typing import Optional
from .._connection import _PostgresConnection


class CommandMessagesMixin(_PostgresConnection):
    async def add_command_message(self, *, message_id: int, user_id: int):
        await self.cur.execute(
            "INSERT INTO command_messages (message_id, user_id) VALUES (%(message_id)s, %(user_id)s)",
            parameters={
                "message_id": message_id,
                "user_id": user_id
            }
        )

    async def get_command_message_author(self, message_id: int) -> Optional[int]:
        await self.cur.execute(
            "SELECT user_id FROM command_messages WHERE message_id=%(message_id)s LIMIT 1",
            parameters={
                "message_id": message_id,
            }
        )
        results = await self.cur.fetchall()
        if results:
            return results[0][0]
        return bool(results)

