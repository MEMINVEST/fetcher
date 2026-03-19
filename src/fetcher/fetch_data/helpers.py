from __future__ import annotations

import subprocess

UV_PATH = "uv"


def set_uv_path(path: str) -> None:
    global UV_PATH
    UV_PATH = path


def ensure_latest_pkg(pkg: str) -> None:
    subprocess.run(
        [UV_PATH, "pip", "install", "-U", pkg],
        check=True,
    )
