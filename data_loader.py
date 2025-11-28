import time
import pandas as pd
import polars as pl
from memory_profiler import memory_usage


def load_data_pandas(file_path:str) -> pd.DataFrame:
    start = time.perf_counter()
    df = pd.read_csv(file_path, index_col="timestamp",parse_dates=True)
    end =  time.perf_counter()

    elapsted_time = end - start
    mem = memory_usage((pd.read_csv, (file_path,)), max_usage=True)
    return df, elapsted_time, mem


def load_data_polars(file_path:str) -> pl.DataFrame:
    start = time.perf_counter()
    df = (
        pl.read_csv(file_path, has_header=True, try_parse_dates=True)
        .sort("timestamp")
        .with_columns(pl.col("timestamp").alias("_index"))
    )
    end = time.perf_counter()

    elapsed_time = end - start
    mem= memory_usage((pl.read_csv, (file_path,)), max_usage=True)
    return df, elapsed_time, mem

if __name__ == "__main__":
    file_path = "./data/market_data-1.csv"
    _, elapsed_time, mem = load_data_pandas(file_path)
    print(elapsed_time)
    print(mem)

    _, time_polars, mem_polars = load_data_polars(file_path)
    print(time_polars)
    print(mem_polars)





