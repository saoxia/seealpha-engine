import hashlib
import io
from pathlib import Path

import pytest

from engine.pipeline import sync_binance_file
from engine.pipeline.sync_binance_file import (
    SyncError,
    destination_for,
    normalize_source,
    parse_checksum,
)

SOURCE_PATH = "data/spot/monthly/klines/ETHUSDT/15m/ETHUSDT-15m-2026-07.zip"
SOURCE_URL = f"https://data.binance.vision/{SOURCE_PATH}"


def test_normalize_source_accepts_site_path() -> None:
    assert normalize_source(SOURCE_PATH) == SOURCE_URL


def test_normalize_source_accepts_official_https_url() -> None:
    assert normalize_source(SOURCE_URL) == SOURCE_URL


@pytest.mark.parametrize(
    "source",
    [
        "http://data.binance.vision/data/example.zip",
        "https://example.com/data/example.zip",
        "https://data.binance.vision/robots.txt",
        "https://data.binance.vision/data/example/",
        "https://user@data.binance.vision/data/example.zip",
        "https://data.binance.vision/data/../example.zip",
    ],
)
def test_normalize_source_rejects_untrusted_location(source: str) -> None:
    with pytest.raises(SyncError):
        normalize_source(source)


def test_destination_preserves_remote_hierarchy(tmp_path: Path) -> None:
    target = destination_for(SOURCE_URL, tmp_path)

    assert target == tmp_path / "spot/monthly/klines/ETHUSDT/15m/ETHUSDT-15m-2026-07.zip"


def test_parse_checksum_accepts_sha256sum_format() -> None:
    digest = "a" * 64
    content = f"{digest}  ETHUSDT-15m-2026-07.zip\n".encode()

    assert parse_checksum(content, "ETHUSDT-15m-2026-07.zip") == digest


def test_parse_checksum_rejects_wrong_filename() -> None:
    content = f"{'a' * 64}  BTCUSDT-15m-2026-07.zip\n".encode()

    with pytest.raises(SyncError, match="BTCUSDT"):
        parse_checksum(content, "ETHUSDT-15m-2026-07.zip")


def test_sync_file_downloads_verifies_and_publishes_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"binance archive"
    digest = hashlib.sha256(payload).hexdigest()
    checksum_content = f"{digest}  ETHUSDT-15m-2026-07.zip\n".encode()

    monkeypatch.setattr(
        sync_binance_file,
        "fetch_checksum",
        lambda *_args: (digest, checksum_content),
    )

    def fake_download(
        source_url: str,
        part_path: Path,
        timeout: float,
        retries: int,
    ) -> None:
        assert source_url == SOURCE_URL
        assert timeout == 10
        assert retries == 2
        part_path.write_bytes(payload)

    monkeypatch.setattr(sync_binance_file, "download_to_part", fake_download)

    target = sync_binance_file.sync_file(
        SOURCE_PATH,
        tmp_path,
        verify_checksum=True,
        force=False,
        timeout=10,
        retries=2,
    )

    assert target.read_bytes() == payload
    assert Path(f"{target}.CHECKSUM").read_bytes() == checksum_content
    assert not target.with_name(f".{target.name}.part").exists()


def test_download_to_part_resumes_with_http_range(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    part_path = tmp_path / ".archive.zip.part"
    part_path.write_bytes(b"abc")
    captured_range: list[str | None] = []

    class FakeResponse:
        def __init__(self) -> None:
            self.headers = {
                "Content-Length": "3",
                "Content-Range": "bytes 3-5/6",
            }
            self.body = io.BytesIO(b"def")

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def getcode(self) -> int:
            return 206

        def read(self, size: int) -> bytes:
            return self.body.read(size)

    def fake_urlopen(request, timeout: float):
        assert timeout == 10
        captured_range.append(request.get_header("Range"))
        return FakeResponse()

    monkeypatch.setattr(sync_binance_file, "urlopen", fake_urlopen)

    sync_binance_file.download_to_part(SOURCE_URL, part_path, timeout=10, retries=1)

    assert captured_range == ["bytes=3-"]
    assert part_path.read_bytes() == b"abcdef"
