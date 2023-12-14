#! /usr/bin/python3
# Tool to backup and restore Moonraker's LMDB database
#
# Copyright (C) 2022 Eric Callahan <arksine.code@gmail.com>
#
# This file may be distributed under the terms of the GNU GPLv3 license
import argparse
import pathlib
import base64
import tempfile
import re
import time
import struct
import json
import inspect
import sqlite3
from typing import (
    Any,
    Dict,
    Optional,
    TextIO,
    Tuple,
    Generator,
    List,
    Callable,
    Type,
    Union
)
import lmdb

DBRecord = Optional[Union[int, float, bool, str, List[Any], Dict[str, Any]]]
SQL_DB_FILENAME = "moonraker-sql.db"
NAMESPACE_TABLE = "namespace_store"
MAX_NAMESPACES = 100
MAX_DB_SIZE = 200 * 2**20
HEADER_KEY = b"MOONRAKER_DATABASE_START"

LINE_MATCH = re.compile(
    r"^\+(\d+),(\d+):([A-Za-z0-9+/]+={0,2})->([A-Za-z0-9+/]+={0,2})$"
)

class DBToolError(Exception):
    pass


RECORD_ENCODE_FUNCS: Dict[Type, Callable[..., bytes]] = {
    int: lambda x: b"q" + struct.pack("q", x),
    float: lambda x: b"d" + struct.pack("d", x),
    bool: lambda x: b"?" + struct.pack("?", x),
    str: lambda x: b"s" + x.encode(),
    list: lambda x: json.dumps(x).encode(),
    dict: lambda x: json.dumps(x).encode(),
    type(None): lambda x: b"\x00",
}

RECORD_DECODE_FUNCS: Dict[int, Callable[..., DBRecord]] = {
    ord("q"): lambda x: struct.unpack("q", x[1:])[0],
    ord("d"): lambda x: struct.unpack("d", x[1:])[0],
    ord("?"): lambda x: struct.unpack("?", x[1:])[0],
    ord("s"): lambda x: bytes(x[1:]).decode(),
    ord("["): lambda x: json.loads(bytes(x)),
    ord("{"): lambda x: json.loads(bytes(x)),
    0: lambda _: None
}

def encode_record(value: DBRecord) -> bytes:
    try:
        enc_func = RECORD_ENCODE_FUNCS[type(value)]
        return enc_func(value)
    except Exception:
        raise DBToolError(
            f"Error encoding val: {value}, type: {type(value)}"
        )

def decode_record(bvalue: bytes) -> DBRecord:
    fmt = bvalue[0]
    try:
        decode_func = RECORD_DECODE_FUNCS[fmt]
        return decode_func(bvalue)
    except Exception:
        val = bytes(bvalue).decode()
        raise DBToolError(
            f"Error decoding value {val}, format: {chr(fmt)}"
        )

# Use a modified CDBMake Format
# +keylen,datalen:namespace|key->data
# Key length includes the namespace, key and separator (a colon)

def open_db(db_path: str) -> lmdb.Environment:
    return lmdb.open(db_path, map_size=MAX_DB_SIZE,
                     max_dbs=MAX_NAMESPACES)

def _do_dump(namespace: bytes,
             db: object,
             backup: TextIO,
             txn: lmdb.Transaction
             ) -> None:
    expected_key_count: int = txn.stat(db)["entries"]
    # write the namespace header
    ns_key = base64.b64encode(b"namespace_" + namespace).decode()
    ns_str = f"entries={expected_key_count}"
    ns_val = base64.b64encode(ns_str.encode()).decode()
    out = f"+{len(ns_key)},{len(ns_val)}:{ns_key}->{ns_val}\n"
    backup.write(out)
    with txn.cursor(db=db) as cursor:
        count = 0
        remaining = cursor.first()
        while remaining:
            key, value = cursor.item()
            keystr = base64.b64encode(key).decode()
            valstr = base64.b64encode(value).decode()
            out = f"+{len(keystr)},{len(valstr)}:{keystr}->{valstr}\n"
            backup.write(out)
            count += 1
            remaining = cursor.next()
    if expected_key_count != count:
        print("Warning: Key count mismatch for namespace "
              f"'{namespace.decode()}': expected {expected_key_count}"
              f", wrote {count}")

def _write_header(ns_count: int, backup: TextIO):
    val_str = f"namespace_count={ns_count}"
    hkey = base64.b64encode(HEADER_KEY).decode()
    hval = base64.b64encode(val_str.encode()).decode()
    out = f"+{len(hkey)},{len(hval)}:{hkey}->{hval}\n"
    backup.write(out)

def backup(args: Dict[str, Any]):
    source_db = pathlib.Path(args["source"]).expanduser().resolve()
    if not source_db.is_dir():
        print(f"Source path not a folder: '{source_db}'")
        exit(1)
    if source_db.joinpath(SQL_DB_FILENAME).is_file() and not args["force"]:
        sql_path = source_db.joinpath(SQL_DB_FILENAME)
        print("Sqlite Database Detected, Performing SQL backup...")
        bkp_dest = pathlib.Path(args["output"]).expanduser().resolve()
        if bkp_dest.is_file():
            if bkp_dest.samefile(source_db):
                raise DBToolError("Backup Destination is same as source")
            print("Backup File Exists and will be overwritten")
            bkp_dest.unlink()
        src_conn = sqlite3.connect(str(sql_path))
        bkp_conn = sqlite3.connect(str(bkp_dest))
        with src_conn:
            src_conn.backup(bkp_conn, pages=1)
        bkp_conn.close()
        src_conn.close()
        return
    if not source_db.joinpath("data.mdb").exists():
        print(f"No database file found in source path: '{source_db}'")
        exit(1)
    bkp_dest = pathlib.Path(args["output"]).expanduser().resolve()
    print(f"Backing up database at '{source_db}' to '{bkp_dest}'...")
    if bkp_dest.exists():
        print(f"Warning: file at '{bkp_dest}' exists, will be overwritten")
    env = open_db(str(source_db))
    expected_ns_cnt: int = env.stat()["entries"]
    with bkp_dest.open("wt") as f:
        _write_header(expected_ns_cnt, f)
        with env.begin(buffers=True) as txn:
            count = 0
            with txn.cursor() as cursor:
                remaining = cursor.first()
                while remaining:
                    namespace = bytes(cursor.key())
                    db = env.open_db(namespace, txn=txn, create=False)
                    _do_dump(namespace, db, f, txn)
                    count += 1
                    remaining = cursor.next()
    env.close()
    if expected_ns_cnt != count:
        print("Warning: namespace count mismatch: "
              f"expected: {expected_ns_cnt}, wrote: {count}")
    print("Backup complete!")

def _process_header(key: bytes, value: bytes) -> int:
    if key != HEADER_KEY:
        raise DBToolError(
            "Database Backup does not contain a valid header key, "
            f" got {key.decode()}")
    val_parts = value.split(b"=", 1)
    if val_parts[0] != b"namespace_count":
        raise DBToolError(
            "Database Backup has an invalid header value, got "
            f"{value.decode()}")
    return int(val_parts[1])

def _process_namespace(key: bytes, value: bytes) -> Tuple[bytes, int]:
    key_parts = key.split(b"_", 1)
    if key_parts[0] != b"namespace":
        raise DBToolError(
            f"Invalid Namespace Key '{key.decode()}', ID not prefixed")
    namespace = key_parts[1]
    val_parts = value.split(b"=", 1)
    if val_parts[0] != b"entries":
        raise DBToolError(
            f"Invalid Namespace value '{value.decode()}', entry "
            "count not present")
    entries = int(val_parts[1])
    return namespace, entries

def _process_line(line: str) -> Tuple[bytes, bytes]:
    match = LINE_MATCH.match(line)
    if match is None:
        # TODO: use own exception
        raise DBToolError(
            f"Invalid DB Entry match: {line}")
    parts = match.groups()
    if len(parts) != 4:
        raise DBToolError(
            f"Invalid DB Entry, does not contain all data: {line}")
    key_len, val_len, key, val = parts
    if len(key) != int(key_len):
        raise DBToolError(
            f"Invalid DB Entry, key length mismatch. "
            f"Got {len(key)}, expected {key_len}, line: {line}")
    if len(val) != int(val_len):
        raise DBToolError(
            f"Invalid DB Entry, value length mismatch. "
            f"Got {len(val)}, expected {val_len}, line: {line}")
    decoded_key = base64.b64decode(key.encode())
    decoded_val = base64.b64decode(val.encode())
    return decoded_key, decoded_val

def restore(args: Dict[str, Any]):
    dest_path = pathlib.Path(args["destination"]).expanduser().resolve()
    input_db = pathlib.Path(args["input"]).expanduser().resolve()
    if not input_db.is_file():
        print(f"No backup found at path: {input_db}")
        exit(1)
    if not dest_path.exists():
        print(f"Destination path '{dest_path}' does not exist, directory"
              "will be created")
    print(f"Restoring backup from '{input_db}' to '{dest_path}'...")
    bkp_dir: Optional[pathlib.Path] = None
    if dest_path.joinpath("data.mdb").exists():
        bkp_dir = dest_path.parent.joinpath("backup")
        if not bkp_dir.exists():
            bkp_dir = pathlib.Path(tempfile.gettempdir())
        str_time = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        bkp_dir = bkp_dir.joinpath(f"{str_time}/database")
        if not bkp_dir.is_dir():
            bkp_dir.mkdir(parents=True)
        print(f"Warning: database file at found in '{dest_path}', "
              "all data will be overwritten.  Copying existing DB "
              f"to '{bkp_dir}'")
    env = open_db(str(dest_path))
    if bkp_dir is not None:
        env.copy(str(bkp_dir))
    expected_ns_count = -1
    namespace_count = 0
    keys_left = 0
    namespace = b""
    current_db = object()
    with env.begin(write=True) as txn:
        # clear all existing entries
        dbs = []
        with txn.cursor() as cursor:
            remaining = cursor.first()
            while remaining:
                ns = cursor.key()
                dbs.append(env.open_db(ns, txn=txn, create=False))
                remaining = cursor.next()
        for db in dbs:
            txn.drop(db)
        with input_db.open("rt") as f:
            while True:
                line = f.readline()
                if not line:
                    break
                key, val = _process_line(line)
                if expected_ns_count < 0:
                    expected_ns_count = _process_header(key, val)
                    continue
                if not keys_left:
                    namespace, keys_left = _process_namespace(key, val)
                    current_db = env.open_db(namespace, txn=txn)
                    namespace_count += 1
                    continue
                txn.put(key, val, db=current_db)
                keys_left -= 1
    if expected_ns_count != namespace_count:
        print("Warning: Namespace count mismatch, expected: "
              f"{expected_ns_count}, processed {namespace_count}")
    print("Restore Complete")

def _generate_lmdb_entries(
    db_folder: pathlib.Path
) -> Generator[Tuple[str, str, bytes], Any, None]:
    if not db_folder.joinpath("data.mdb").is_file():
        return
    try:
        lmdb_env = open_db(str(db_folder))
    except Exception:
        print(
            "Failed to open lmdb database, aborting conversion"
        )
        return
    lmdb_namespaces: List[Tuple[str, object]] = []
    with lmdb_env.begin(buffers=True) as txn:
        # lookup existing namespaces
        with txn.cursor() as cursor:
            remaining = cursor.first()
            while remaining:
                key = bytes(cursor.key())
                if not key:
                    continue
                db = lmdb_env.open_db(key, txn)
                lmdb_namespaces.append((key.decode(), db))
                remaining = cursor.next()
        # Copy all records
        for (ns, db) in lmdb_namespaces:
            print(f"Converting LMDB namespace '{ns}'")
            with txn.cursor(db=db) as cursor:
                remaining = cursor.first()
                while remaining:
                    key_buf = cursor.key()
                    value = b""
                    try:
                        decoded_key = bytes(key_buf).decode()
                        value = bytes(cursor.value())
                    except Exception:
                        print("Database Key/Value Decode Error")
                        decoded_key = ''
                    remaining = cursor.next()
                    if not decoded_key or not value:
                        hk = bytes(key_buf).hex()
                        print(
                            f"Invalid key or value '{hk}' found in "
                            f"lmdb namespace '{ns}'"
                        )
                        continue
                    if ns == "moonraker":
                        if decoded_key == "database":
                            # Convert "database" field in the "moonraker" namespace
                            # to its own namespace if possible
                            db_info = decode_record(value)
                            if isinstance(db_info, dict):
                                for db_key, db_val in db_info.items():
                                    yield ("database", db_key, encode_record(db_val))
                                continue
                        elif decoded_key == "database_version":
                            yield ("database", decoded_key, value)
                            continue
                    yield (ns, decoded_key, value)
    lmdb_env.close()

def convert(args: Dict[str, Any]) -> None:
    db_folder = pathlib.Path(args["dbpath"])
    if not db_folder.joinpath("data.mdb").is_file():
        print(f"No LMDB database found at {db_folder}, exiting.")
        return
    sql_path = db_folder.joinpath(SQL_DB_FILENAME)
    if sql_path.is_file():
        print(f"SQL Database File Exists at {sql_path}")
        print("Existing entries will be unmodified.")
        val = input("Continue? (y/N): ")
        if val.strip().upper() != "Y":
            print("Aborting Conversion...")
            return
        sql_path.unlink()
    conn = sqlite3.connect(
        str(sql_path), timeout=1., detect_types=sqlite3.PARSE_DECLTYPES
    )
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT * FROM sqlite_schema")
    tables: List[str] = [row[0] for row in cur.fetchall()]
    with conn:
        if NAMESPACE_TABLE not in tables:
            conn.execute(
                inspect.cleandoc(
                    f"""
                    CREATE TABLE {NAMESPACE_TABLE} (
                        namespace TEXT NOT NULL,
                        key TEXT NOT NULL,
                        value record NOT NULL,
                        PRIMARY KEY (namespace, key)
                    )
                    """
                )
            )
            conn.execute(
                f"INSERT INTO {NAMESPACE_TABLE} VALUES(?,?,?)",
                ("database", "table_versions", encode_record({NAMESPACE_TABLE: 1}))
            )
        conn.executemany(
            f"INSERT OR IGNORE INTO {NAMESPACE_TABLE} VALUES(?,?,?)",
            _generate_lmdb_entries(db_folder)
        )
    conn.close()
    print("Conversion Complete")


if __name__ == "__main__":
    # Parse start arguments
    parser = argparse.ArgumentParser(
        description="dbtool - tool to backup/restore/convert Moonraker's database")
    subparsers = parser.add_subparsers(
        title="commands", description="valid commands", required=True,
        metavar="<command>")
    bkp_parser = subparsers.add_parser("backup", help="Backup a database")
    rst_parser = subparsers.add_parser("restore", help="Restore a LMDB database")
    cvt_parser = subparsers.add_parser(
        "convert", help="Convert LMDB database to Sqlite"
    )
    bkp_parser.add_argument(
        "source", metavar="<source path>",
        help="path to the folder containing the database to backup")
    bkp_parser.add_argument(
        "output", metavar="<output file>",
        help="path to the output backup file",
        default="~/moonraker_db.bkp")
    bkp_parser.add_argument(
        "-f", "--force-lmdb", dest="force",
        help="Force LMDB backup when Sqlite is detected")
    bkp_parser.set_defaults(func=backup)
    rst_parser.add_argument(
        "destination", metavar="<destination>",
        help="path to the folder where the lmdb database will be restored")
    rst_parser.add_argument(
        "input", metavar="<input file>",
        help="path of the lmdb backup file to restore from")
    rst_parser.set_defaults(func=restore)
    cvt_parser.add_argument(
        "dbfolder", metavar="<database path>",
        help="Path to LMDB database folder"
    )
    args = parser.parse_args()
    args.func(vars(args))
