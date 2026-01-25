import subprocess


def ensure_latest_pkg(pkg: str):
    subprocess.run(
        ["uv", "pip", "install", "-U", pkg],
        check=True,
    )