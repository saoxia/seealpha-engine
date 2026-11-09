"""从 data.binance.vision 可靠同步一个公开数据归档。

脚本只负责下载和完整性校验，不解压或转换数据。默认将远端目录结构保存在
项目的 ``data/raw/binance`` 目录下。
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import time
from http.client import IncompleteRead
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen

BASE_URL = "https://data.binance.vision/"
ALLOWED_HOST = "data.binance.vision"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "binance" / "raw"
CHUNK_SIZE = 1024 * 1024
MAX_CHECKSUM_SIZE = 64 * 1024
USER_AGENT = "NautilusTrader-project/binance-data-sync"
CHECKSUM_PATTERN = re.compile(r"^([0-9a-fA-F]{64})(?:\s+[ *]?(.+?))?\s*$")


class SyncError(RuntimeError):
    """表示同步无法安全完成。"""


def positive_int(value: str) -> int:
    """解析大于零的整数参数。"""
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("必须是大于零的整数")
    return parsed


def positive_float(value: str) -> float:
    """解析大于零的浮点数参数。"""
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("必须大于零")
    return parsed


def normalize_source(source: str) -> str:
    """把站内路径或完整 URL 规范化为受信任的 Binance HTTPS URL。"""
    source = source.strip()
    if not source:
        raise SyncError("远端文件路径不能为空")

    if "://" not in source:
        source = urljoin(BASE_URL, source.lstrip("/"))

    parsed = urlsplit(source)
    try:
        port = parsed.port
    except ValueError as exc:
        raise SyncError(f"无效的 URL 端口：{source}") from exc

    if (
        parsed.scheme.lower() != "https"
        or parsed.hostname != ALLOWED_HOST
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
    ):
        raise SyncError(f"仅允许从 {BASE_URL} 下载")
    if parsed.query or parsed.fragment:
        raise SyncError("数据文件 URL 不能包含查询参数或片段")

    decoded_path = unquote(parsed.path)
    path = PurePosixPath(decoded_path)
    if (
        not decoded_path.startswith("/data/")
        or decoded_path.endswith("/")
        or path.name in ("", ".", "..")
    ):
        raise SyncError("远端文件必须位于 data.binance.vision 的 /data/ 目录中")
    if ".." in path.parts:
        raise SyncError("远端文件路径不能包含 '..'")

    return urlunsplit(("https", ALLOWED_HOST, parsed.path, "", ""))


def destination_for(source_url: str, output_dir: Path) -> Path:
    """返回保留远端目录结构的本地目标路径。"""
    remote_parts = PurePosixPath(unquote(urlsplit(source_url).path)).parts
    # remote_parts 的前两项固定为 ('/', 'data')。
    return output_dir.joinpath(*remote_parts[2:])


def parse_checksum(content: bytes, expected_filename: str) -> str:
    """解析 Binance 的 sha256sum 格式校验文件。"""
    try:
        lines = content.decode("utf-8-sig").splitlines()
    except UnicodeDecodeError as exc:
        raise SyncError("CHECKSUM 文件不是有效的 UTF-8 文本") from exc

    non_empty_lines = [line.strip() for line in lines if line.strip()]
    if len(non_empty_lines) != 1:
        raise SyncError("CHECKSUM 文件应当只包含一条非空记录")

    match = CHECKSUM_PATTERN.fullmatch(non_empty_lines[0])
    if match is None:
        raise SyncError("无法解析 CHECKSUM 文件")

    checksum, filename = match.groups()
    if filename is not None and Path(filename).name != expected_filename:
        raise SyncError(
            f"CHECKSUM 指向 {filename!r}，但请求的文件是 {expected_filename!r}",
        )
    return checksum.lower()


def sha256_file(path: Path) -> str:
    """分块计算文件的 SHA-256。"""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def retry_delay(attempt: int) -> float:
    """返回有上限的指数退避秒数。"""
    return min(2 ** (attempt - 1), 8)


def should_retry_http(status: int) -> bool:
    """判断 HTTP 状态是否适合重试。"""
    return status == 429 or 500 <= status < 600


def fetch_checksum(source_url: str, timeout: float, retries: int) -> tuple[str, bytes]:
    """下载并解析与数据文件配套的 CHECKSUM。"""
    checksum_url = f"{source_url}.CHECKSUM"
    request = Request(checksum_url, headers={"User-Agent": USER_AGENT})

    for attempt in range(1, retries + 1):
        try:
            with urlopen(request, timeout=timeout) as response:
                content = response.read(MAX_CHECKSUM_SIZE + 1)
            if len(content) > MAX_CHECKSUM_SIZE:
                raise SyncError("CHECKSUM 文件异常大，已停止同步")
            filename = PurePosixPath(unquote(urlsplit(source_url).path)).name
            return parse_checksum(content, filename), content
        except HTTPError as exc:
            if not should_retry_http(exc.code) or attempt == retries:
                if exc.code == 404:
                    raise SyncError(
                        "未找到配套的 CHECKSUM；如确认要跳过校验，请使用 --skip-checksum",
                    ) from exc
                raise SyncError(f"下载 CHECKSUM 失败：HTTP {exc.code}") from exc
        except URLError as exc:
            if attempt == retries:
                raise SyncError(f"下载 CHECKSUM 失败：{exc.reason}") from exc
        except (IncompleteRead, TimeoutError, OSError) as exc:
            if attempt == retries:
                raise SyncError(f"下载 CHECKSUM 失败：{exc}") from exc

        time.sleep(retry_delay(attempt))

    raise AssertionError("unreachable")


def report_progress(downloaded: int, total: int | None) -> None:
    """输出简单且适合终端查看的下载进度。"""
    downloaded_mib = downloaded / (1024 * 1024)
    if total:
        percentage = min(downloaded / total * 100, 100)
        total_mib = total / (1024 * 1024)
        message = f"\r下载中：{downloaded_mib:.1f}/{total_mib:.1f} MiB ({percentage:.1f}%)"
    else:
        message = f"\r下载中：{downloaded_mib:.1f} MiB"
    print(message, end="", flush=True)


def download_to_part(
    source_url: str,
    part_path: Path,
    timeout: float,
    retries: int,
) -> None:
    """下载到临时文件；已有临时内容时尝试 HTTP Range 续传。"""
    for attempt in range(1, retries + 1):
        resume_at = part_path.stat().st_size if part_path.exists() else 0
        headers = {"User-Agent": USER_AGENT}
        if resume_at:
            headers["Range"] = f"bytes={resume_at}-"
        request = Request(source_url, headers=headers)

        try:
            with urlopen(request, timeout=timeout) as response:
                status = response.getcode()
                resumed = resume_at > 0 and status == 206
                if resumed:
                    content_range = response.headers.get("Content-Range")
                    if not content_range or not content_range.startswith(f"bytes {resume_at}-"):
                        raise SyncError("服务器返回的断点续传范围与本地临时文件不一致")
                mode = "ab" if resumed else "wb"
                downloaded = resume_at if resumed else 0
                length_header = response.headers.get("Content-Length")
                try:
                    response_length = int(length_header) if length_header else None
                except ValueError as exc:
                    raise SyncError("服务器返回了无效的 Content-Length") from exc
                total = downloaded + response_length if response_length is not None else None

                with part_path.open(mode) as file:
                    while chunk := response.read(CHUNK_SIZE):
                        file.write(chunk)
                        downloaded += len(chunk)
                        report_progress(downloaded, total)
                    file.flush()
                    os.fsync(file.fileno())
                if total is not None and downloaded != total:
                    raise IncompleteRead(b"", total - downloaded)
                print()
                return
        except HTTPError as exc:
            if exc.code == 416 and resume_at:
                part_path.unlink(missing_ok=True)
                if attempt < retries:
                    continue
            if not should_retry_http(exc.code) or attempt == retries:
                raise SyncError(f"下载数据文件失败：HTTP {exc.code}") from exc
        except (IncompleteRead, URLError, TimeoutError, OSError) as exc:
            if attempt == retries:
                raise SyncError(f"下载数据文件失败：{exc}") from exc

        print(f"下载中断，{retry_delay(attempt):.0f} 秒后重试……", file=sys.stderr)
        time.sleep(retry_delay(attempt))

    raise AssertionError("unreachable")


def write_atomic(path: Path, content: bytes) -> None:
    """在同一目录中原子写入一个小文件。"""
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("wb") as file:
        file.write(content)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary, path)


def sync_file(
    source: str,
    output_dir: Path,
    *,
    verify_checksum: bool,
    force: bool,
    timeout: float,
    retries: int,
) -> Path:
    """同步一个数据文件，并返回最终的本地路径。"""
    source_url = normalize_source(source)
    target = destination_for(source_url, output_dir.resolve())
    target.parent.mkdir(parents=True, exist_ok=True)

    expected_checksum: str | None = None
    checksum_content: bytes | None = None
    if verify_checksum:
        print("正在读取远端 CHECKSUM……")
        expected_checksum, checksum_content = fetch_checksum(source_url, timeout, retries)

    if target.exists() and not force:
        if expected_checksum is None:
            print(f"文件已存在，跳过：{target}")
            return target
        print("正在校验本地文件……")
        if sha256_file(target) == expected_checksum:
            write_atomic(Path(f"{target}.CHECKSUM"), checksum_content or b"")
            print(f"本地文件已经是最新版本：{target}")
            return target
        print("本地文件校验不匹配，将重新下载。")

    part_path = target.with_name(f".{target.name}.part")
    print(f"同步 {source_url}")
    download_to_part(source_url, part_path, timeout, retries)

    if expected_checksum is not None:
        print("正在校验下载结果……")
        actual_checksum = sha256_file(part_path)
        if actual_checksum != expected_checksum:
            part_path.unlink(missing_ok=True)
            raise SyncError(
                f"SHA-256 校验失败：期望 {expected_checksum}，实际 {actual_checksum}",
            )

    os.replace(part_path, target)
    if checksum_content is not None:
        write_atomic(Path(f"{target}.CHECKSUM"), checksum_content)
    print(f"同步完成：{target}")
    return target


def build_argument_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器。"""
    parser = argparse.ArgumentParser(
        description="将一个 data.binance.vision 数据归档可靠同步到本地",
    )
    parser.add_argument(
        "source",
        help="完整 HTTPS URL，或以 data/ 开头的站内文件路径",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"本地数据根目录（默认：{DEFAULT_OUTPUT_DIR}）",
    )
    parser.add_argument(
        "--skip-checksum",
        action="store_true",
        help="不下载和验证配套 CHECKSUM（不推荐）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="即使本地文件校验通过也重新下载",
    )
    parser.add_argument(
        "--timeout",
        type=positive_float,
        default=30.0,
        help="单次网络读取超时秒数（默认：30）",
    )
    parser.add_argument(
        "--retries",
        type=positive_int,
        default=3,
        help="网络失败时最多尝试次数（默认：3）",
    )
    return parser


def main() -> None:
    """运行命令行同步。"""
    args = build_argument_parser().parse_args()
    try:
        sync_file(
            args.source,
            args.output_dir,
            verify_checksum=not args.skip_checksum,
            force=args.force,
            timeout=args.timeout,
            retries=args.retries,
        )
    except SyncError as exc:
        print(f"同步失败：{exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        print("\n同步已取消；临时文件会在下次运行时用于断点续传。", file=sys.stderr)
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
