from collections import namedtuple


class Sticker(namedtuple("Sticker", ["prefix", "suffix", "owner_id", "url"])):
    @property
    def name(self):
        return f"{self.prefix}.{self.suffix}"
