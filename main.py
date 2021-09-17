import asyncio
import asyncpg



async def run():
    conn = await asyncpg.connect(user='beksultan', password='beksultan', database='data', host='127.0.0.1',port='1433', server_settings='DESKTOP-OG81R5M\\SQLEXPRESS')
    values = await conn.fetch(
        'select *from famile'
    )
    print(values)
    await conn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
