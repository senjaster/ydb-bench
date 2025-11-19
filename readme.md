# YDB Bench - pgbench-like Workload Tool

A tool for running pgbench-like workloads against YDB databases.

## Installation

### Using pipx (Recommended)

```bash
# Install from local directory
pipx install .

# Or install in editable mode for development
pipx install -e .
```

### Using pip

```bash
# Activate virtual environment
source .venv/bin/activate

# Install the package
pip install -e .
```

## Configuration

The tool supports configuration via environment variables with CLI option overrides.

### Environment Variables

- `YDB_ENDPOINT` - YDB endpoint (e.g., `grpcs://ydb-host:2135`)
- `YDB_DATABASE` - Database path (e.g., `/Root/database`)
- `YDB_ROOT_CERT` - Path to root certificate file (optional)
- `YDB_USER` - Username for authentication (optional)
- `YDB_PASSWORD` - Password for authentication (optional)
- `YDB_PREFIX_PATH` - Folder name for tables (default: pgbench)

### Example Environment Setup

```bash
export YDB_ENDPOINT="grpcs://ydb-node-1.ydb-cluster.com:2135"
export YDB_DATABASE="/Root/database"
export YDB_ROOT_CERT="./ca.crt"
export YDB_USER="your_user"
export YDB_PASSWORD="your_password"
```

## Usage

### Initialize Database

Create tables and populate with test data:

```bash
# Using environment variables
ydb-bench init --scale 100

# Using CLI options (overrides environment variables)
ydb-bench init \
  --endpoint "grpcs://ydb-host:2135" \
  --database "/Root/database" \
  --ca-file "./ca.crt" \
  --user "root" \
  --password "your_password" \
  --scale 100
```

**Options:**
- `--scale` / `-s` - Number of branches to create (default: 100)
- `--prefix-path` - Folder name for tables (default: pgbench)

### Run Workload

Execute pgbench-like transactions:

```bash
# Using environment variables
ydb-bench run --jobs 100 --transactions 1000

# Using CLI options
ydb-bench run \
  --endpoint "grpcs://ydb-host:2135" \
  --database "/Root/database" \
  --ca-file "./ca.crt" \
  --user "root" \
  --password "your_password" \
  --jobs 100 \
  --transactions 1000

# With multiple client processes
ydb-bench run --processes 4 --jobs 25 --transactions 1000

# Using single session mode
ydb-bench run --jobs 10 --transactions 100 --single-session

# Using custom SQL script
ydb-bench run --jobs 10 --transactions 100 --file custom_script.sql
```

**Options:**
- `--processes` - Number of parallel client processes (default: 1)
- `--jobs` / `-j` - Number of parallel jobs per process (default: 1). Each job uses separate connection.
- `--transactions` / `-t` - Number of transactions each job runs (default: 100)
- `--single-session` - Use single persistent session per job instead of requesting session from pool each time
- `--file` / `-f` - Path to file containing SQL script to execute

## Architecture

The application is organized as a Python package:

```
src/ydb_bench/
├── __init__.py          - Package initialization
├── __main__.py          - Entry point for python -m ydb_bench
├── cli.py               - Click CLI implementation
├── runner.py            - YDB connection management and workload orchestration
├── initializer.py       - Database table creation and data population
├── job.py               - Transaction execution logic
├── base_executor.py     - Base class for executors
├── metrics.py           - Metrics collection and reporting
└── constants.py         - Constants and default values
```

## Execution Modes

### Pooled Mode (Default)
Uses YDB's session pool with automatic retry logic. Best for most workloads.

### Single Session Mode
Uses a single acquired session for all operations. Useful for testing session behavior.

```bash
ydb-bench run --single-session --jobs 10 --transactions 100
```

## Multiprocessing 

When `--processes` is set to 1 (default), the tool runs in single-process mode using async/await for concurrency.

When `--processes` is greater than 1, the tool uses Python's multiprocessing to run multiple processes in parallel, each with its own set of async jobs. 

## Connection count

Each job uses it's own connection.
Each process has it's own set of jobs
Therefore Number of parallel connections is equal to (jobs * processes).
Generally it is recommended to raise jobs until python process CPU consumption reaches 90%. Practical job count limit is somewhere between 10 and 100 depending on cluster performance (better performance - less jobs). To rise connection count further one should increase process count.


## Examples

```bash
# Initialize with 50 branches
ydb-bench init --scale 50

# Run workload with 10 jobs, 500 transactions each
ydb-bench run --jobs 10 --transactions 500

# Run workload with 4 client processes, 25 jobs per process
ydb-bench run --client 4 --jobs 25 --transactions 1000

# Run with custom script
ydb-bench run --jobs 10 --transactions 100 --file my_script.sql