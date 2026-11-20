"""Command line entry point: python -m solution --input cases.json --output result.json"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .core import InputError, SyncTimeout, execute_step
from .store import Store


def run(spec: dict) -> dict:
    db_path = spec.get("db_path", ":memory:")
    if not isinstance(db_path, str) or not db_path:
        raise InputError("MALFORMED_REQUEST", "'db_path' must be a non-empty string")
    if spec.get("reset") and db_path != ":memory:":
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass
    steps = spec.get("steps")
    if not isinstance(steps, list):
        raise InputError("MALFORMED_REQUEST", "'steps' must be a list")
    results = []
    with Store(db_path) as store:
        for index, step in enumerate(steps):
            record = {"index": index, "op": step.get("op") if isinstance(step, dict) else None}
            if isinstance(step, dict) and "request_id" in step:
                record["request_id"] = step["request_id"]
            try:
                if not isinstance(step, dict):
                    raise InputError("MALFORMED_REQUEST", "each step must be an object")
                record.update(execute_step(store, step))
            except InputError as exc:
                record.update({"status": "ERROR", "error": exc.code, "message": str(exc)})
            except SyncTimeout as exc:
                record.update({"status": "ERROR", "error": "SYNC_TIMEOUT", "message": str(exc)})
            results.append(record)
    return {"results": results}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="solution",
        description="Offline service-identity retirement coordinator")
    parser.add_argument("--input", required=True, help="path to cases.json")
    parser.add_argument("--output", required=True, help="path to write result.json")
    args = parser.parse_args(argv)

    with open(args.input, "r", encoding="utf-8") as fh:
        spec = json.load(fh)
    # A relative db_path is resolved against the input file's directory so a
    # case file is self-contained regardless of the working directory.
    db_path = spec.get("db_path")
    if isinstance(db_path, str) and db_path and db_path != ":memory:" \
            and not os.path.isabs(db_path):
        spec["db_path"] = os.path.join(os.path.dirname(os.path.abspath(args.input)), db_path)
    try:
        report = run(spec)
    except InputError as exc:
        report = {"results": [], "fatal": {"status": "ERROR", "error": exc.code,
                                           "message": str(exc)}}
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
