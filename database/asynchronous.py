from database.base import Database
import psycopg


class DatabaseAsync(Database):

    def __init__(self,
                 connection_url: str,
                 num_classes: int) -> None:

        self.connection_url = connection_url
        self.num_classes = num_classes

    def create_table(self):
        conn = psycopg.connect(self.connection_url)
        self._create_table(conn)
        conn.close()

    async def get_conn(self):
        return await psycopg.AsyncConnection.connect(self.connection_url)

    async def save_message(self, conn, prediction: dict):
        async with conn.cursor() as cur:
            await cur.execute(*self._build_insert(prediction))

        await conn.commit()

    async def close(self):
        await self.get_conn().close()
