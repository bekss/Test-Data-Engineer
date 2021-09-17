import asyncio
import asyncpg
import sqlalchemy as db
from conf import *


async def connect_to_database():
    """
    For connect to database
    :return:
    """

    conn = await asyncpg.connect(user='beksultan', password='beksultan', database='data', host='127.0.0.1',port='1433', server_settings='DESKTOP-OG81R5M\\SQLEXPRESS')
    values = await conn.fetch(
        'select *from famile'
    )
    conn_text = 'mysql://{}:{}@{}/{}'.format(user, password, host, db)
    await conn.close()

    engine = db.create_engine(conn_text)

loop = asyncio.get_event_loop()
loop.run_until_complete(connect_to_database())


