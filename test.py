import asyncio
from contextlib import asynccontextmanager

@asynccontextmanager
async def ydb_ctx():
    try:
        print('aaa')
        yield 1
    finally:
        print('bbb')

async def main():
    async with ydb_ctx():
        print('zzz')

if __name__ == '__main__':
    asyncio.run(main())