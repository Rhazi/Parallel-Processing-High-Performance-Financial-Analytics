import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
from data_loader import load_data_pandas, load_data_polars
from metrics import rolling_metrics_pandas, rolling_metrics_polars
from parallel import compute_metrics_threading, compute_metrics_multiprocessing, measure_cpu_mem_during

def plot_rolling_metrics(df: pd.DataFrame, window: int=20, )