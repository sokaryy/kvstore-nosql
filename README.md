# Key-Value Store (noSQL Final Project)

A persistent key-value store over TCP (HTTP), with replication, indexes, and tests/benchmarks.

## Features

- **Core**: `Set(key, value)`, `Get(key)`, `Delete(key)`, `BulkSet([(key, value), ...])`
- **Persistence**: Write-Ahead Log (WAL) with fsync; data survives restarts
- **Transport**: HTTP over TCP (default port 8765)
- **Client**: Python class `KVClient` with `Get`, `Set`, `Delete`, `BulkSet`
- **Durability**: WAL written and fsync'd before apply; optimized for 100% durability
- **ACID**: Atomic bulk set (single WAL record); concurrent bulk writes don't corrupt each other
- **Debug**: `debug_flaky` (e.g. 0.01) on Set/BulkSet: random skip of in-memory apply (WAL still written) to simulate fsync/crash; replay recovers
- **Replication**: Cluster of 3 (1 primary, 2 secondaries); primary replicates to secondaries; failover via `/promote_to_primary`
- **Indexes** (optional `--index`): Full-text search on values; TF-IDF embedding similarity search
- **Master-less**: All 3 nodes accept writes and replicate to each other (last-write-wins)

## Quick Start

```bash
# Install
pip install -r requirements.txt

# Run server (single node)
python -m kvstore.server --port 8765 --data-dir data

# Run with indexes (full-text + embedding)
python -m kvstore.server --port 8765 --data-dir data --index

# Client
from kvstore.client import KVClient
c = KVClient("http://127.0.0.1:8765")
c.set("k", "v")
c.get("k")   # "v"
c.delete("k")
c.bulk_set([("a", 1), ("b", 2)])
c.search("hello")           # full-text (server with --index)
c.search_similar("query", top_k=5)  # embedding (server with --index)
```

## Cluster (Primary + Secondaries)

```bash
# Node 0: primary, replicates to 8766 and 8767
python -m kvstore.cluster_server --port 8765 --data-dir d0 --role primary --peers "http://127.0.0.1:8766,http://127.0.0.1:8767"

# Node 1 & 2: secondaries
python -m kvstore.cluster_server --port 8766 --data-dir d1 --role secondary --peers ""
python -m kvstore.cluster_server --port 8767 --data-dir d2 --role secondary --peers ""
```

Use `kvstore.cluster_runner.run_cluster(base_port=8765)` to start all 3; `elect_primary(base_port)` to promote a secondary after primary dies.

## Master-less

```python
from kvstore.masterless_runner import run_masterless_cluster
procs = run_masterless_cluster(base_port=8770, data_dirs=[...])
# Any node accepts writes; replicates to the other two
```

## Tests

All tests use the `KVClient`.

```bash
pytest tests/ -v
```

- **Common**: Set then Get; Set then Delete then Get; Get without setting; Set then Set (same key) then Get; Set then graceful exit then Get; BulkSet then Gets
- **ACID**: Concurrent bulk set touching same keys (no corruption); bulk set then SIGKILL restart (all or nothing)
- **Debug flaky**: Set/BulkSet with `debug_flaky=1.0`; after restart, replay recovers
- **Replication**: Primary accepts writes; secondaries have data; failover (kill primary, elect new primary, writes work)
- **Indexes**: Full-text search; embedding similarity search
- **Master-less**: Write to any node; data on all nodes

## Benchmarks

```bash
# Write throughput vs pre-populated data (writes/sec)
python benchmarks/bench_throughput.py

# Durability: writer thread + killer thread; restart and check lost keys
python benchmarks/bench_durability.py
```

- **Throughput**: Measures writes/sec with 0, 1k, 5k, 10k, 20k pre-populated keys
- **Durability**: One thread adds data and records acknowledged keys; another kills the DB randomly (SIGKILL where available); restart and report which acknowledged keys are lost (target: 0 lost)

## Killing the server (tests/benchmarks)

- Use `process.send_signal(signal.SIGKILL)` (or `os.kill(pid, 9)`) where supported; on Windows use `process.kill()`.

## Project layout

```
kvstore/
  __init__.py
  store.py          # KVStore + WAL
  server.py         # HTTP server (single or with --index)
  client.py         # KVClient
  replicated_store.py
  cluster_server.py
  cluster_runner.py # primary + secondaries + elect_primary
  indexes.py        # FullTextIndex, IndexedStore (TF-IDF)
  indexed_store.py  # KVStoreWithIndex
  masterless_runner.py
tests/
  test_kv_common.py
  test_acid.py
  test_debug_flaky.py
  test_replication.py
  test_indexes.py
  test_masterless.py
benchmarks/
  bench_throughput.py
  bench_durability.py
```

## GitHub and Telegram

1. Create a new repository on GitHub (e.g. `kvstore-nosql`).
2. Push this project:
   ```bash
   git remote add origin https://github.com/YOUR_USERNAME/kvstore-nosql.git
   git branch -M main
   git push -u origin main
   ```
3. Send the repository link over your Telegram chat.

## License

MIT.
