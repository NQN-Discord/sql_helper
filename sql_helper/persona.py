from dataclasses import dataclass


@dataclass
class Persona:
    user_id: int
    short_name: str
    display_name: str
    avatar_url: str
