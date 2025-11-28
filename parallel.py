import time
from data_loader import load_data_polars, load_data_pandas
from metrics import rolling_metrics_pandas, rolling_metrics_polars
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import psutil
import threading

def compute_metrics_threading(df, symbols, lib="pandas", window=20):
    results={}

    with ThreadPoolExecutor(max_workers=len(symbols)) as executor:
        futures={}
        for s in symbols:
            if lib=="pandas":
                futures[executor.submit(rolling_metrics_pandas, df, s, ["price"], window)] = s
            elif lib=="polars":
                futures[executor.submit(rolling_metrics_polars, df, s, ["price"], window)] = s
            else:
                raise ValueError(f"only pandas and polars are valid libraries")

        for f in as_completed(futures):
            symbol = futures[f]
            result_df, elapsed = f.result()
            results[symbol] = result_df

    return results

def compute_metrics_multiprocessing(df, symbols, lib="pandas", window=20):
    results = {}
    with ProcessPoolExecutor(max_workers=len(symbols)) as executor:
        futures={}
        for s in symbols:
            if lib=="pandas":
                futures[executor.submit(rolling_metrics_pandas, df, s, ["price"], window)] = s
            elif lib == "polars":
                futures[executor.submit(rolling_metrics_polars, df, s, ["price"], window)] = s
            else:
                ValueError(f"only pandas and polars are valid libraries")

        for f in as_completed(futures):
            symbol = futures[f]
            result_df, time_elapsed = f.result()
            results[symbol] = result_df

        return results


def measure_cpu_mem_during(func, *args, **kwargs):
    process = psutil.Process()
    cpu_readings=[]
    mem_readings=[]

    def monitor():
        while not done[0]:
            cpu_readings.append(process.cpu_percent(interval=0.05))
            mem_readings.append(process.memory_info().rss/1e6)

    done = [False]

    monitor_thread=threading.Thread(target=monitor)
    monitor_thread.start()

    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    total_time = time.perf_counter() - start_time

    done[0] = True
    monitor_thread.join()

    avg_cpu = sum(cpu_readings) / len(cpu_readings)
    mem_usage = sum(mem_readings) / len(mem_readings)

    return result, total_time, avg_cpu, mem_usage

if __name__ == "__main__":
    window = 1000
    file_path = "./data/market_data-1.csv"

    df_pandas, _, _ = load_data_pandas(file_path)
    df_polars, _, _ = load_data_polars(file_path)
    symbols = df_pandas['symbol'].unique().tolist()

    # Threading (pandas)
    threading_results, total_time, avg_cpu, mem_usage = (
        measure_cpu_mem_during(
            compute_metrics_threading, df_pandas, symbols, lib="pandas", window=window)
    )
    print(f"Threading (Pandas) - Time: {total_time:.2f}s, Avg CPU: {avg_cpu:.1f}%, Mem Usage: {mem_usage:.2f}%")

    # Multiprocessing (pandas)
    multiprocessing_results, total_time, avg_cpu, mem_usage = (
        measure_cpu_mem_during(
            compute_metrics_multiprocessing,df_pandas, symbols, lib="pandas", window=window)
    )
    print(f"Multiprocessing (Pandas) - Time: {total_time:.2f}s, Avg CPU: {avg_cpu:.1f}%, Mem Usage: {mem_usage:.2f}%")

    # Threading (polars)
    threading_results_polars, total_time, avg_cpu, mem_usage = (
        measure_cpu_mem_during(
            compute_metrics_threading,df_polars, symbols, lib="polars", window=window)
    )
    print(f"Threading (Polars) - Time: {total_time:.2f}s, Avg CPU: {avg_cpu:.1f}%, Mem Usage: {mem_usage:.2f}%")

    # Multiprocessing (polars)
    multiprocessing_results_polars, total_time, avg_cpu, mem_usage = (
        measure_cpu_mem_during(
        compute_metrics_multiprocessing, df_polars, symbols, lib="polars", window=window)
    )
    print(f"Multiprocessing (Polars) - Time: {total_time:.2f}s, Avg CPU: {avg_cpu:.1f}%, Mem Usage: {mem_usage:.2f}%")



