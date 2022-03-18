from collections import namedtuple


class Pack(namedtuple("Pack", ["guild_id", "name", "public"])):
    @property
    def _id(self):
        return self.guild_id
