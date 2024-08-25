import pandas as pd
from loader.naive import LoaderNaive
from database.pool import DatabasePool
from multiprocessing.pool import ThreadPool


class LoaderMultiThread(LoaderNaive):

    def __init__(self,
                 batch_size: int,
                 db: DatabasePool,
                 num_threads: int) -> None:

        self.db = db
        self.batch_size = batch_size
        self.num_threads = num_threads

    def load_parallel(self, df: pd.DataFrame):

        dfs = self._make_slices(df)

        with ThreadPool(self.num_threads) as p:
            p.map(self.load, dfs)
