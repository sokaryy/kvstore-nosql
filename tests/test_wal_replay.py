"""Direct test of WAL replay after bulk_set with debug_flaky (no server)."""

import tempfile
import shutil
from kvstore.store import KVStore


def test_bulk_set_wal_replay_after_skip_apply():
    """BulkSet with debug_flaky=1.0: WAL is written, apply skipped; new store replays and has keys."""
    data_dir = tempfile.mkdtemp(prefix="wal_replay_")
    try:
        store = KVStore(data_dir=data_dir)
        items = [(f"k{i}", f"v{i}") for i in range(5)]
        # Force skip apply: patch would be cleaner; here we call _wal_append_sync then skip apply manually
        payload = [[k, v] for k, v in items]
        import json, base64
        raw = json.dumps(payload)
        wal_line = "BULK_SET\t" + base64.b64encode(raw.encode()).decode()
        store._wal_append_sync(wal_line)
        # Don't apply (simulate debug_flaky=1.0)
        store.close()
        # Reopen and replay
        store2 = KVStore(data_dir=data_dir)
        for k, v in items:
            assert store2.get(k) == v, f"key {k}"
        store2.close()
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
