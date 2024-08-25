from abc import ABC


class Database(ABC):

    def __init__(self,
                 connection_url: str,
                 num_classes: int) -> None:

        self.connection_url = connection_url
        self.num_classes = num_classes

    def _create_table(self, conn):

        with conn.cursor() as cur:

            classes = [f"class_{i} real" for i in range(self.num_classes)]

            cur.execute(f'''create table if not exists
                            predictions(id_client char(36) primary key,
                                        {", ".join(classes)})''')

            cur.execute('truncate table predictions')

        conn.commit()

    def _build_insert(self, prediction: dict) -> tuple[str, str]:
        values = (
            (prediction['id_client'],) +
            tuple(prediction[f'class_{i}'] for i in range(self.num_classes))
        )

        insert = f'''insert into predictions VALUES (%s{",%s"*self.num_classes})'''

        return insert, values

    def create_table(self):
        raise NotImplementedError()

    def save_message(self):
        raise NotImplementedError()
