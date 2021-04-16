from typing import Awaitable


class AsyncList:
    def __init__(self, contents: Awaitable):
        self.contents = contents

    def __await__(self):
        return self.contents.__await__()

    async def __aiter__(self):
        for i in await self:
            yield i


def async_list(f):
    return lambda *args, **kwargs: AsyncList(f(*args, **kwargs))
