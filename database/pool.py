from database.base import Database
from psycopg_pool import ConnectionPool


class DatabasePool(Database):

    def __init__(self,
                 connection_url: str,
                 num_classes: int,
                 num_threads: int) -> None:

        self.connection_url = connection_url
        self.num_classes = num_classes

        self.pool = ConnectionPool(connection_url,
                                   min_size=num_threads)

    def create_table(self):
        with self.pool.connection() as conn:
            self._create_table(conn)

    def save_message(self, prediction: dict):

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(*self._build_insert(prediction))
                conn.commit()

    def close(self):
        self.pool.close()
