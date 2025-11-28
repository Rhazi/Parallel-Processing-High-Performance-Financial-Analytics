import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from metrics import rolling_metrics_pandas, rolling_metrics_polars
from data_loader import load_data_pandas, load_data_polars

def plot_rolling_metrics(df: pd.DataFrame, window: int = 20, subsample_size : int =20):
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()

    if "timestamp" in df.columns:
        df = df.sort_values("timestamp").set_index("timestamp")

    df = df[window:subsample_size].copy()

    _, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)

    # Price & MA
    axes[0].plot(df.index, df["price"], color="tab:blue", label="Price")
    axes[0].plot(df.index, df[f"price_MA_{window}"], "--", color="tab:cyan", label=f"{window}-Day MA")
    axes[0].set_ylabel("Price")
    axes[0].legend(loc="upper left")

    # Volatility
    axes[1].plot(df.index, df[f"price_STD_{window}"], color="tab:orange", label="Volatility")
    axes[1].set_ylabel("Volatility")
    axes[1].legend(loc="upper left")

    # Sharpe
    axes[2].plot(df.index, df[f"price_sharpe_{window}"], color="tab:red", label="Sharpe Ratio")
    axes[2].set_ylabel("Sharpe (Annualized)")
    axes[2].legend(loc="upper left")

    plt.suptitle("Rolling Metrics Over Time")
    plt.xlabel("Date")
    plt.tight_layout()
    plt.show()


def measure_cpu_mem_exec_time(df: pd.DataFrame):
    fig, axs = plt.subplots(3, 1, figsize=(18, 8))

    axs[0].bar(df.index, df["Total Time"], color="tab:blue", label="Execution Time")
    axs[0].set_title("Execution Time")
    axs[0].set_ylabel("Seconds")
    axs[0].legend()

    axs[1].bar(df.index, df["CPU"], color="tab:orange", label="Average CPU Usage")
    axs[1].set_title("Average CPU Usage")
    axs[1].set_ylabel("CPU")
    axs[1].legend()

    axs[2].bar(df.index, df["Memory"], color="tab:red", label="Memory Usage(MB)")
    axs[2].set_title("Memory Usage(MB)")
    axs[2].set_ylabel("Memory(MB)")
    axs[2].legend()

    plt.show()

if __name__ == "__main__":
    symbol = 'AAPL'
    window = 20
    subsample_size = 1000
    file_path = "./data/market_data-1.csv"

    df_pandas, _, _ = load_data_pandas(file_path)
    df_pandas_metrics, elapsed_time = rolling_metrics_pandas(df_pandas, symbol, ['price'], window=window)
    print(f"Pandas elapsed time: {elapsed_time:.2f} seconds")
    plot_rolling_metrics(df_pandas_metrics, window=window, subsample_size=subsample_size)

    df_polars, _, _ = load_data_polars(file_path)
    df_polars_metrics, elapsed_time = rolling_metrics_polars(df_polars, symbol, ['price'], window=window)
    print(f"Polars elapsed time: {elapsed_time:.2f} seconds")
    plot_rolling_metrics(df_polars_metrics, window=window, subsample_size=subsample_size)