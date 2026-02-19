from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from urllib.parse import urlparse

import duckdb


def _copy_httpfs_extension_from_wheel(error_text: str) -> bool:
    try:
        import duckdb_extension_httpfs
    except Exception:
        return False

    package_root = Path(duckdb_extension_httpfs.__file__).parent
    source = package_root / "extensions" / f"v{duckdb.__version__}" / "httpfs.duckdb_extension"
    if not source.exists():
        return False

    match = re.search(r'Extension "([^"]*httpfs\.duckdb_extension)" not found', error_text)
    if match:
        target = Path(match.group(1))
    else:
        home = Path(os.getenv("HOME", str(Path.home())))
        target = home / ".duckdb" / "extensions" / f"v{duckdb.__version__}" / "linux_amd64_gcc4" / "httpfs.duckdb_extension"

    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return True


def _ensure_httpfs_loaded(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute("LOAD httpfs")
        return
    except Exception as load_exc:
        load_err = str(load_exc)

    try:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        return
    except Exception as install_exc:
        combined_error = f"{load_err}\n{install_exc}"

    if _copy_httpfs_extension_from_wheel(combined_error):
        con.execute("LOAD httpfs")
        return

    raise RuntimeError(f"Unable to load DuckDB httpfs extension. {combined_error}")


def configure_duckdb_s3(con: duckdb.DuckDBPyConnection, s3_cfg: dict[str, str | None]) -> None:
    _ensure_httpfs_loaded(con)

    con.execute(f"SET s3_region='{(s3_cfg.get('region') or 'us-east-1').replace(chr(39), chr(39) * 2)}'")
    con.execute(
        f"SET s3_access_key_id='{(s3_cfg.get('access_key_id') or '').replace(chr(39), chr(39) * 2)}'"
    )
    con.execute(
        "SET s3_secret_access_key='"
        f"{(s3_cfg.get('secret_access_key') or '').replace(chr(39), chr(39) * 2)}'"
    )

    session_token = s3_cfg.get("session_token")
    if session_token:
        con.execute(f"SET s3_session_token='{session_token.replace(chr(39), chr(39) * 2)}'")

    endpoint = s3_cfg.get("endpoint_url")
    if endpoint:
        parsed = urlparse(endpoint)
        host_port = parsed.netloc or parsed.path
        scheme = (parsed.scheme or "http").lower()
        con.execute(f"SET s3_endpoint='{host_port.replace(chr(39), chr(39) * 2)}'")
        con.execute(f"SET s3_use_ssl={'true' if scheme == 'https' else 'false'}")

    con.execute(f"SET s3_url_style='{(s3_cfg.get('url_style') or 'path').replace(chr(39), chr(39) * 2)}'")
