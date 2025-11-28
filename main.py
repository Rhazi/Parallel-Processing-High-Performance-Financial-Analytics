from data_loader import load_data_pandas, load_data_polars
from parallel import compute_metrics_threading, compute_metrics_multiprocessing, measure_cpu_mem_during
import pandas as pd
from reporting import measure_cpu_mem_exec_time

def main():
    window = 1000
    file_path = "./data/market_data-1.csv"

    ########______________________________________DATA INGESTION____________________________________###################
    print("[LOADING DATA USING PANDAS]...")
    df_pandas, _, _ = load_data_pandas(file_path)
    print("[LOADING DATA USING POLARS]...")
    df_polars, _, _ = load_data_polars(file_path)

    symbols = df_pandas["symbol"].unique().tolist()
    print(f"[LOADED {len(df_pandas)} ROWS FOR {len(symbols)}]")

    results = []

    ########_________________________________COMPUTATIONS USING PANDAS____________________________________###################

    print("[BEGINNING PANDAS THREADING]...")
    _, total_time, avg_cpu, avg_mem = measure_cpu_mem_during( compute_metrics_threading, df_pandas, symbols, "pandas", window)

    results.append(["Threading using Pandas", total_time, avg_cpu, avg_mem])
    print(f"[FINISHED OPERATION]: {total_time}s | CPU {avg_cpu}% | Memory {avg_mem} MB")

    print("[BEGINNING PANDAS MULTIPROCESSING]...")
    _, total_time, avg_cpu, avg_mem = measure_cpu_mem_during(compute_metrics_multiprocessing, df_pandas, symbols, "pandas", window)

    results.append(["Multiprocessing using Pandas", total_time, avg_cpu, avg_mem])
    print(f"[FINISHED OPERATION]: {total_time}s | CPU {avg_cpu}% | Memory {avg_mem} MB")

    ########_________________________________COMPUTATIONS USING POLARS_________________________###################

    print("[BEGINNING POLARS THREADING]...")
    _, total_time, avg_cpu, avg_mem = measure_cpu_mem_during( compute_metrics_threading, df_polars, symbols, "polars", window)

    results.append(["Threading using Polars", total_time, avg_cpu, avg_mem])
    print(f"[FINISHED OPERATION]: {total_time}s | CPU {avg_cpu}% | Memory {avg_mem} MB")

    print("[RUNNING POLARS MULTIPROCESSING]...")
    _, total_time, avg_cpu, avg_mem = measure_cpu_mem_during(compute_metrics_multiprocessing, df_polars, symbols, "polars", window
                                                             )
    results.append(["Multiprocessing (Polars)", total_time, avg_cpu, avg_mem])
    print(f"[FINISHED OPERATION]: {total_time}s | CPU {avg_cpu}% | Memory {avg_mem} MB")

    ############_________________________________REPORTING______________________________________###########################
    print("[LOADING PERFORMANCE REPORT]...")
    summary = pd.DataFrame(results, columns=["Method", "Total Time", "CPU", "Memory"]).set_index("Method")

    print(summary)
    measure_cpu_mem_exec_time(summary)
    print("[REPORT GENERATED]")


if __name__ == "__main__":
    main()