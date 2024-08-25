import psycopg

from database.base import Database


class DatabaseNaive(Database):

    def __init__(self,
                 connection_url: str,
                 num_classes: int) -> None:

        super().__init__(connection_url, num_classes)
        self.conn = psycopg.connect(connection_url)

    def create_table(self):
        self._create_table(self.conn)

    def save_message(self, prediction: dict):

        with self.conn.cursor() as cur:
            cur.execute(*self._build_insert(prediction))

        self.conn.commit()

    def close(self):
        self.conn.close()
