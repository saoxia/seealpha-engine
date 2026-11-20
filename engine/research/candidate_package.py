"""Pure, non-executing parser for a restricted AI research ZIP.

User Python is syntax-checked with ast and ast.literal_eval only; it is never
imported, compiled, eval'ed or executed. The Engine uses CandidateSignal.
"""
from __future__ import annotations

import ast
import base64
import binascii
import hashlib
import io
import json
import re
import stat
import zipfile

from engine.research.candidate_template import validate_candidate

_MAX_ZIP = 64 * 1024
_MAX_SOURCE = 32 * 1024
_MAX_MANIFEST = 8 * 1024
_NAMES = {"seealpha-package.json", "strategy.py"}
_ID = re.compile(r"^[a-z][a-z0-9_]{1,40}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$")


def parse_candidate_archive(data: bytes) -> dict:
    if not isinstance(data, bytes) or not 1 <= len(data) <= _MAX_ZIP:
        raise ValueError("Invalid candidate archive size")
    try:
        with zipfile.ZipFile(io.BytesIO(data), "r") as archive:
            members = archive.infolist()
            if len(members) != 2 or {m.filename for m in members} != _NAMES:
                raise ValueError("Candidate archive must contain two fixed files")
            for info in members:
                if (info.is_dir() or info.flag_bits & 0x1
                        or stat.S_IFMT(info.external_attr >> 16) == stat.S_IFLNK
                        or info.file_size > (_MAX_MANIFEST if info.filename.endswith(".json") else _MAX_SOURCE)):
                    raise ValueError("Invalid candidate archive member")
            manifest_bytes = archive.read("seealpha-package.json")
            source_bytes = archive.read("strategy.py")
    except (zipfile.BadZipFile, RuntimeError, OverflowError, KeyError):
        raise ValueError("Candidate archive is invalid") from None
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
        source = source_bytes.decode("utf-8")
        node = ast.parse(source, filename="strategy.py", mode="exec")
        statements = node.body
        if len(statements) == 2 and (isinstance(statements[0], ast.Expr)
                and isinstance(statements[0].value, ast.Constant)
                and isinstance(statements[0].value.value, str)):
            statements = statements[1:]
        if len(statements) != 1 or not isinstance(statements[0], ast.Assign):
            raise ValueError
        definition = statements[0]
        if (len(definition.targets) != 1 or not isinstance(definition.targets[0], ast.Name)
                or definition.targets[0].id != "CANDIDATE"):
            raise ValueError
        # literal_eval accepts only Python literals, not calls, operators, imports,
        # functions, classes, attributes or comprehensions.
        rule = ast.literal_eval(definition.value)
        if (not isinstance(manifest, dict)
                or set(manifest) != {"format", "name", "title", "version", "entry_point"}
                or type(manifest["format"]) is not int or manifest["format"] != 1
                or not isinstance(manifest["name"], str)
                or not _ID.fullmatch(manifest["name"])
                or not isinstance(manifest["version"], str)
                or not _VERSION.fullmatch(manifest["version"])
                or manifest["entry_point"] != "strategy:CANDIDATE"
                or not isinstance(manifest["title"], str)
                or not 1 <= len(manifest["title"]) <= 100):
            raise ValueError
        validated = validate_candidate(rule)
        return {
            "name": manifest["name"], "title": manifest["title"],
            "version": manifest["version"], "recipe": validated,
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    except (SyntaxError, UnicodeError, json.JSONDecodeError, TypeError, ValueError,
            RecursionError, MemoryError):
        raise ValueError("Candidate must be a literal, supported strategy recipe") from None


def decode_candidate_archive(value: str, *, expected_hash: str) -> tuple[bytes, dict]:
    if (not isinstance(value, str) or len(value) > 90_000
            or not isinstance(expected_hash, str)
            or not re.fullmatch(r"[a-f0-9]{64}", expected_hash)):
        raise ValueError("Invalid candidate archive payload")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error):
        raise ValueError("Invalid candidate archive encoding") from None
    parsed = parse_candidate_archive(raw)
    if parsed["sha256"] != expected_hash:
        raise ValueError("Candidate archive checksum mismatch")
    return raw, parsed
