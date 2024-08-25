from database.base import Database
import psycopg


class DatabaseLazy(Database):

    def __init__(self,
                 connection_url: str,
                 num_classes: int) -> None:

        super().__init__(connection_url, num_classes)
        self.conn = None

    def _get_conn(self):

        if self.conn is None:
            self.conn = psycopg.connect(self.connection_url)

        return self.conn

    def create_table(self):
        self._create_table(self._get_conn())

    def save_message(self, prediction: dict):

        conn = self._get_conn()

        with self.conn.cursor() as cur:
            cur.execute(*self._build_insert(prediction))

        conn.commit()

    def close(self):
        self._get_conn().close()
