import pandas as pd
import polars as pl
import numpy as np
from data_loader import load_data_pandas, load_data_polars
import time


def rolling_metrics_pandas(df:pd.DataFrame, symbol:str, ts_cols:list, window=20) -> tuple[pl.DataFrame, float]:
    start = time.time()
    df_symbol = df[df["symbol"] == symbol].copy()
    df_symbol.sort_values("timestamp")

    for col in ts_cols:
        df_symbol[f"{col}_MA_{window}"] = (
            df_symbol[col].rolling(window=window).mean()
        )
        df_symbol[f"{col}_STD_{window}"] = (
            df_symbol[col].rolling(window=window).std(ddof=1)
        )
        df_symbol[f"{col}_rets"] = df_symbol[col].pct_change()
        df_symbol[f"{col}_rets_mean_{window}"] = (
            df_symbol[f"{col}_rets"].rolling(window=window).mean()
        )
        df_symbol[f"{col}_rets_std_{window}"] = (
            df_symbol[f"{col}_rets"].rolling(window=window).std(ddof=1)
        )
        df_symbol[f"{col}_sharpe_{window}"] = (
            df_symbol[f"{col}_rets_mean_{window}"] / df_symbol[f"{col}_rets_std_{window}"]
        )
        #annualized sharpe ratio
        df_symbol[f"{col}_sharpe_{window}"] *= np.sqrt(252)


    elapsed = time.time() - start
    return df_symbol, elapsed


def rolling_metrics_polars(df:pl.DataFrame, symbol:str, ts_cols:list, window=20) -> tuple[pl.DataFrame, float]:
    start = time.time()
    df_symbol = (
        df.filter(pl.col("symbol") == symbol).
        sort("timestamp")
    )

    for col in ts_cols:
        df_symbol = df_symbol.with_columns([
            pl.col(col).rolling_mean(window_size=window).alias(f"{col}_MA_{window}"),
            pl.col(col).rolling_std(window_size=window, ddof=1).alias(f"{col}_STD_{window}"),

            #returns
            pl.col(col).pct_change().alias(f"{col}_rets"),

            pl.col(col).pct_change().rolling_mean(window_size=window).alias(f"{col}_rets_mean_{window}"),
            pl.col(col).pct_change().rolling_std(window_size=window, ddof=1).alias(f"{col}_rets_std_{window}"),

            (
            pl.col(col).pct_change().rolling_mean(window_size=window)/ pl.col(col).pct_change().rolling_std(window_size=window, ddof=1)* np.sqrt(252)).
             alias(f"{col}_sharpe_{window}")
            ])

    time_elapsed = time.time() - start
    return df_symbol, time_elapsed

if __name__ == "__main__":
    symbol = "AAPL"
    file_path = "data/market_data-1.csv"
    df,_,_ = load_data_pandas(file_path)
    df , time_elapsed = rolling_metrics_pandas(df, symbol, ["price"], window = 20)
    print(df.tail())
    print(time_elapsed)

    symbol = "AAPL"
    df,_,_ = load_data_polars(file_path)
    df , time_elapsed = rolling_metrics_polars(df, symbol, ["price"], window = 20)
    print(df.tail())
    print(time_elapsed)

