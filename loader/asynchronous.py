import pandas as pd
import asyncio
from loader.naive import LoaderNaive
from database.asynchronous import DatabaseAsync


class LoaderAsync(LoaderNaive):

    def __init__(self,
                 db: DatabaseAsync) -> None:
        self.db = db

    async def load_async(self, df: pd.DataFrame):

        conn = await self.db.get_conn()

        async with conn:
            async with asyncio.TaskGroup() as tg:
                for _, row in df.iterrows():
                    tg.create_task(self.db.save_message(conn, row.to_dict()))
