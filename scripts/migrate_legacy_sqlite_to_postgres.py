#!/usr/bin/env python3
"""One-time OFFLINE import of legacy SeeAlpha SQLite state into PostgreSQL.

Requires Python 3 (stdlib only), psql, and an already initialized PostgreSQL
seealpha schema. Stop all Engine writers before running. SQLite files are backed
up via the SQLite backup API, including committed WAL state. This script never
deletes or overwrites those backups.
"""
import argparse
import math
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

# Foreign-key parents precede their children.
PARENTS = ("order_intents", "order_requests")
SEQUENCES = ("account_evidence_events", "order_request_views", "events")


def psql(url, sql=None, sql_file=None):
    args = ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-A", "-t", "-d", url]
    if sql_file is not None:
        args += ["--single-transaction", "-f", str(sql_file)]
    else:
        args += ["-c", sql]
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError("psql failed: " + result.stderr.strip()[-1500:])
    return result.stdout.strip()


def quoted(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise ValueError("SQLite integer cannot fit PostgreSQL BIGINT")
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Nonfinite SQLite float")
        return repr(value)
    if isinstance(value, str):
        if "\x00" in value:
            raise ValueError("PostgreSQL TEXT cannot store NUL")
        return "'" + value.replace("'", "''") + "'"
    raise TypeError("Unexpected SQLite value type " + type(value).__name__)


def snapshot(path, out_dir):
    dest = out_dir / path.name
    if dest.exists():
        raise FileExistsError("Refusing to overwrite snapshot " + str(dest))
    with sqlite3.connect("file:" + str(path.resolve()) + "?mode=ro", uri=True) as src:
        with sqlite3.connect(dest) as dst:
            src.backup(dst)
            result = dst.execute("PRAGMA integrity_check").fetchone()[0]
            if result != "ok":
                raise ValueError(f"{path} failed integrity_check: {result}")
    dest.chmod(0o600)
    return dest


def target_columns(url):
    sql = ("SELECT table_name || E'\\t' || column_name "
           "FROM information_schema.columns "
           "WHERE table_schema='seealpha' ORDER BY table_name,ordinal_position")
    columns = {}
    for line in psql(url, sql=sql).splitlines():
        if not line:
            continue
        name, col = line.split("\t", 1)
        columns.setdefault(name, []).append(col)
    if "order_intents" not in columns or "worker_settings" not in columns:
        raise RuntimeError("SeeAlpha PostgreSQL schema is missing")
    return columns


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True,
                        help="Old ENGINE_STATE directory containing book.sqlite and control.sqlite")
    parser.add_argument("--url", default=os.environ.get("SEEALPHA_DATABASE_URL"),
                        help="PostgreSQL URL (defaults to SEEALPHA_DATABASE_URL)")
    parser.add_argument("--backup-dir", type=Path, required=True,
                        help="New, empty, protected audit-backup directory")
    args = parser.parse_args()
    if not args.url:
        parser.error("--url or SEEALPHA_DATABASE_URL is required")
    if not shutil.which("psql"):
        parser.error("psql client is required")
    sources = [args.state / "book.sqlite", args.state / "control.sqlite"]
    present = [p for p in sources if p.is_file()]
    if not present:
        parser.error("No legacy SQLite state files found")
    if args.backup_dir.exists():
        parser.error("--backup-dir must not exist (never overwrite an audit backup)")
    args.backup_dir.mkdir(mode=0o700, parents=True)
    columns = target_columns(args.url)
    backup_files = [snapshot(p, args.backup_dir) for p in present]
    counts = {}
    with tempfile.TemporaryDirectory(prefix="seealpha-pg-cutover-") as temp:
        sql_file = Path(temp) / "import.sql"
        sql_file.touch(mode=0o600)
        with sql_file.open("w", encoding="utf-8") as output:
            output.write("SET LOCAL standard_conforming_strings = on;\n"
                         "SET LOCAL search_path TO seealpha, public;\n")
            for backup in backup_files:
                with sqlite3.connect(backup) as db:
                    names = {row[0] for row in db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'")}
                    usable = [name for name in names if name in columns]
                    usable.sort(key=lambda name: (0 if name in PARENTS else 1, name))
                    for table in usable:
                        selected = [row[1] for row in db.execute(
                            f'PRAGMA table_info("{table}")') if row[1] in columns[table]]
                        if not selected:
                            continue
                        n = db.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                        if n == 0:
                            continue
                        old = int(psql(args.url, sql=f'SELECT COUNT(*) FROM seealpha."{table}"'))
                        if old and not (table == "worker_settings" and old == 1):
                            raise RuntimeError(f"PostgreSQL {table} already contains {old} rows; aborting")
                        if table == "worker_settings":
                            output.write('DELETE FROM seealpha.worker_settings;\n')
                        cols = ",".join('"' + col + '"' for col in selected)
                        select = "SELECT " + cols + f' FROM "{table}"'
                        for row in db.execute(select):
                            vals = ",".join(quoted(v) for v in row)
                            output.write(f'INSERT INTO seealpha."{table}" ({cols}) VALUES ({vals});\n')
                        counts[table] = n
            for table in SEQUENCES:
                if table in counts:
                    output.write(
                        f"SELECT setval(pg_get_serial_sequence('seealpha.{table}','id'), "
                        f"(SELECT MAX(id) FROM seealpha.{table}), true);\n")
        psql(args.url, sql_file=sql_file)

    for table, expected in counts.items():
        actual = int(psql(args.url, sql=f'SELECT COUNT(*) FROM seealpha."{table}"'))
        if actual != expected:
            raise RuntimeError(f"Verification mismatch: {table}: {actual} != {expected}")
    print("Imported and count-verified:", counts)
    print("Audit snapshots retained in:", args.backup_dir)
    print("Strategy issuance index is reconstructed from validated artifacts at Engine startup.")
    print("Keep the old SQLite files until exchange/account evidence is independently reconciled.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("Migration FAILED:", exc, file=sys.stderr)
        sys.exit(1)
