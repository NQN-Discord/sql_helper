from typing import Optional

from ..persona import Persona
from ..async_list import AsyncList, async_list
from .._connection import _PostgresConnection


class PersonasMixin(_PostgresConnection):
    @async_list
    async def personas(self, user_id: int) -> AsyncList:
        await self.cur.execute(
            "SELECT user_id, short_name, display_name, avatar_url FROM personas WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id},
        )
        personas = await self.cur.fetchall()
        return [Persona(*i) for i in personas]

    @async_list
    async def all_personas(self) -> AsyncList:
        await self.cur.execute(
            "SELECT user_id, short_name, display_name, avatar_url FROM personas"
        )
        personas = await self.cur.fetchall()
        return [Persona(*i) for i in personas]

    async def get_persona(self, user_id: int, name: str) -> Optional[Persona]:
        await self.cur.execute(
            "SELECT user_id, short_name, display_name, avatar_url FROM personas WHERE user_id=%(user_id)s and (short_name=%(name)s or display_name=%(name)s)",
            parameters={"user_id": user_id, "name": name},
        )
        personas = await self.cur.fetchall()
        if personas:
            if len(personas) != 1:
                personas.sort(key=lambda p: p[1] == name, reverse=True)
            return Persona(*personas[0])

    async def count_personas(self, user_id: int) -> int:
        await self.cur.execute(
            "select count(*) from personas where user_id=%(user_id)s",
            parameters={"user_id": user_id},
        )
        results = await self.cur.fetchall()
        return results[0][0]

    async def persona_exists(self, user_id: int, short_name: str) -> bool:
        await self.cur.execute(
            "SELECT 1 FROM personas WHERE user_id=%(user_id)s and short_name=%(name)s LIMIT 1",
            parameters={"user_id": user_id, "name": short_name},
        )
        return bool(await self.cur.fetchall())

    async def create_persona(self, persona: Persona):
        await self.cur.execute(
            "INSERT INTO personas (user_id, short_name, display_name, avatar_url) VALUES (%(user_id)s, %(short_name)s, %(display_name)s, %(avatar_url)s)",
            parameters={
                "user_id": persona.user_id,
                "short_name": persona.short_name,
                "display_name": persona.display_name,
                "avatar_url": persona.avatar_url,
            },
        )

    async def set_persona(self, original_name: str, persona: Persona):
        await self.cur.execute(
            "UPDATE personas SET user_id=%(user_id)s, short_name=%(short_name)s, display_name=%(display_name)s, avatar_url=%(avatar_url)s where user_id=%(user_id)s and short_name=%(original_name)s",
            parameters={
                "user_id": persona.user_id,
                "original_name": original_name,
                "short_name": persona.short_name,
                "display_name": persona.display_name,
                "avatar_url": persona.avatar_url,
            },
        )

    async def delete_persona(self, user_id: int, short_name: str):
        await self.cur.execute(
            "DELETE FROM personas WHERE user_id=%(user_id)s and short_name=%(short_name)s",
            parameters={"user_id": user_id, "short_name": short_name},
        )

    async def delete_all_personas(self, user_id: int):
        await self.cur.execute(
            "DELETE FROM personas WHERE user_id=%(user_id)s",
            parameters={"user_id": user_id},
        )
