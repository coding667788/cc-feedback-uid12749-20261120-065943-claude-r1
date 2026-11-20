"""Shared helpers for the test-suite."""
from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from solution.core import execute_step  # noqa: E402
from solution.store import Store  # noqa: E402


def remove_db(path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass


def run_steps(db_path: str, steps: list, reset: bool = False) -> list:
    """Execute steps in-process against db_path and return result records."""
    if reset:
        remove_db(db_path)
    store = Store(db_path)
    try:
        return [execute_step(store, step) for step in steps]
    finally:
        store.close()


def setup_steps(tenant="t1"):
    """A baseline world used by several tests.

    i1 [read,write] c1 [0,100)
    i2 [read]       c2 [0,100)
    i3 [read,write] c3 [0,200)
    svc requires {read: 1, write: 1}; c1, c2, c3 bound.
    """
    return [
        {"op": "create_tenant", "request_id": "s01", "tenant": tenant, "now": 0},
        {"op": "create_identity", "request_id": "s02", "tenant": tenant, "now": 0,
         "identity": "i1", "actions": ["read", "write"]},
        {"op": "create_identity", "request_id": "s03", "tenant": tenant, "now": 0,
         "identity": "i2", "actions": ["read"]},
        {"op": "create_identity", "request_id": "s04", "tenant": tenant, "now": 0,
         "identity": "i3", "actions": ["read", "write"]},
        {"op": "create_credential", "request_id": "s05", "tenant": tenant, "now": 0,
         "credential": "c1", "identity": "i1", "valid_from": 0, "valid_until": 100},
        {"op": "create_credential", "request_id": "s06", "tenant": tenant, "now": 0,
         "credential": "c2", "identity": "i2", "valid_from": 0, "valid_until": 100},
        {"op": "create_credential", "request_id": "s07", "tenant": tenant, "now": 0,
         "credential": "c3", "identity": "i3", "valid_from": 0, "valid_until": 200},
        {"op": "create_service", "request_id": "s08", "tenant": tenant, "now": 0,
         "service": "svc", "requirements": {"read": 1, "write": 1}},
        {"op": "bind", "request_id": "s09", "tenant": tenant, "now": 0,
         "service": "svc", "credential": "c1"},
        {"op": "bind", "request_id": "s10", "tenant": tenant, "now": 0,
         "service": "svc", "credential": "c2"},
        {"op": "bind", "request_id": "s11", "tenant": tenant, "now": 0,
         "service": "svc", "credential": "c3"},
    ]
