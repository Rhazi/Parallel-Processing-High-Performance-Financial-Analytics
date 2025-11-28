# Parallel Processing & High-Performance Financial Analytics

## Overview
This project demonstrates advanced parallel processing techniques and high-performance computing methods for financial analytics. It focuses on rolling metrics calculation, portfolio optimization, and multi-threaded data processing using modern Python libraries and design patterns.

## Setup Instructions

### Prerequisites
- Python 3.8 or higher

### Installation

1. **Clone the repository** (if not already done):
   ```bash
   git clone <repository-url>
   cd Parallel-Processing-High-Performance-Financial-Analytics/
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   
   Or install individually:
   ```bash
   pip install polars pandas memory-profiler pytest numpy
   ```

3. **Verify installation**:
   ```bash
   python -c "import polars, pandas, numpy; print('All dependencies installed successfully')"
   ```

4. **Run the main application**:
   ```bash
   python main.py
   ```

5. **Run tests**:
    Run the following command at root level of A7.
   ```bash
   pytest
   ```

## Module Descriptions

### Core Modules

#### `main.py`
- **Purpose**: Entry point for the parallel processing demonstration
- **Functionality**: Orchestrates data loading, parallel metric calculations, and performance comparisons
- **Features**:
  - Benchmarks serial vs parallel processing
  - Demonstrates various parallelization strategies
  - Generates performance reports

#### `data_loader.py`
- **Purpose**: High-performance data loading utilities
- **Features**:
  - Polars-based fast CSV reading
  - Pandas compatibility layer
  - Memory-efficient data streaming
  - Data validation and cleaning

#### `metrics.py`
- **Purpose**: Financial metrics calculation engine
- **Contains**:
  - Rolling window calculations (mean, volatility, Sharpe ratio)
  - Performance analytics (alpha, information ratio, drawdown)
- **Optimization**: Vectorized operations using NumPy and Polars
- **Parallelization**: Thread-safe implementations for concurrent processing

#### `parallel.py`
- **Purpose**: Parallel processing framework and utilities
- **Features**:
  - Thread pool management
  - Process pool implementations
  - Async/await patterns for I/O operations
  - Work distribution algorithms
- **Design Patterns**: Producer-Consumer, MapReduce
- **Performance**: Automatic CPU core detection and load balancing

#### `reporting.py`
- **Purpose**: Performance reporting and visualization
- **Features**:
  - Parallel processing performance comparisons
  - Memory usage analysis
  - Execution time benchmarking
- **Visualizations**: Performance charts, scalability analysis

### Configuration & Data

#### `data/`
- **Purpose**: Sample datasets and configuration files
- **Contents**:
  - Market data samples (CSV)
  - Benchmark datasets for testing
  - Configuration templates
- **Performance**: Optimized file formats for fast loading

## Key Features

### 1. **High-Performance Data Processing**
- **Polars Integration**: Ultra-fast DataFrame operations
- **Memory Efficiency**: Lazy evaluation and streaming processing
- **SIMD Optimization**: Vectorized calculations using modern CPU features

### 2. **Parallel Processing Strategies**
- **Thread-based Parallelism**: CPU-bound tasks using ThreadPoolExecutor
- **Process-based Parallelism**: Intensive computations using multiprocessing
- **Hybrid Approaches**: Combined strategies for optimal performance

### 3. **Financial Analytics**
- **Rolling Metrics**: Moving averages, volatility, correlation

### 4. **Scalability & Performance**
- **Automatic Scaling**: Adapts to available CPU cores
- **Memory Management**: Efficient memory usage patterns
- **Caching**: Intelligent result caching for repeated calculations
- **Profiling**: Built-in performance monitoring

## Usage Examples

### Basic Rolling Metrics
```python
from metrics import rolling_metrics_pandas
from data_loader import load_market_data

data = load_data_pandas("data/sample_prices.csv")
metrics = rolling_metrics_pandas(data['returns'], 'AAPL', ['price'])
```

## Development & Testing

### Running Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html
```

### Benchmarking
The project includes built-in benchmarking tools:
```python
from reporting import plot_rolling_metrics
from metrics import rolling_metrics_pandas
symbol = 'AAPL'
window = 20
subsample_size = 1000
file_path = "./data/market_data-1.csv"

df_pandas, _, _ = load_data_pandas(file_path)
df_pandas_metrics, elapsed_time = rolling_metrics_pandas(df_pandas, symbol, ['price'], window=window)
print(f"Pandas elapsed time: {elapsed_time:.2f} seconds")
plot_rolling_metrics(df_pandas_metrics, window=window, subsample_size=subsample_size)
```

## Dependencies

- **polars**: High-performance DataFrame library
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **memory-profiler**: Memory usage monitoring

## Contributing

1. Follow existing code structure and naming conventions
2. Add tests for new functionality in `tests/`
3. Update this README for new modules or features
4. Ensure all tests pass and performance doesn't regress
5. Profile new code for memory and CPU usage

## Performance Reports

The system generates detailed performance reports in `performance_report.md` including:
- Execution time comparisons
- Memory usage analysis
- Scalability metrics
- Optimization recommendations

## Author
- Mohammed Rhazi - University of Chicago (mrhazi@uchicago.edu)

---

For detailed implementation examples, advanced usage patterns, and performance optimization tips, refer to the source code documentation and the generated performance reports.
