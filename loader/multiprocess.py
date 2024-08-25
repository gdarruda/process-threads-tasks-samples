import pandas as pd
from loader.naive import LoaderNaive
from database.lazy import DatabaseLazy
from multiprocessing.pool import Pool


class LoaderMultiProcess(LoaderNaive):

    def __init__(self,
                 batch_size: int,
                 num_threads: int,
                 db: DatabaseLazy) -> None:

        self.db = db
        self.batch_size = batch_size
        self.num_threads = num_threads

    def load_parallel(self, df: pd.DataFrame):

        dfs = self._make_slices(df)

        with Pool(self.num_threads) as p:
            p.map(self.load, dfs)
