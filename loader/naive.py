import pandas as pd
from database.naive import DatabaseNaive


class LoaderNaive():

    def __init__(self, db: DatabaseNaive) -> None:
        self.db = db

    def _make_slices(self, df: pd.DataFrame):

        num_rows = df.shape[0]
        num_batches = num_rows//self.batch_size
        remainder = num_rows % self.batch_size

        for i in range(num_batches):
            yield df.iloc[i*self.batch_size:(i+1)*self.batch_size]

        if remainder > 0:
            yield df[self.batch_size*num_batches:num_rows]

    def load(self, df: pd.DataFrame):

        for _, row in df.iterrows():
            self.db.save_message(row.to_dict())
