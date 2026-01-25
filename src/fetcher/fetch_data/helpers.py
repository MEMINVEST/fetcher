import subprocess


def ensure_latest_pkg(pkg: str):
    subprocess.run(
        ["python", "-m", "pip", "install", "-U", pkg],
        check=True,
    )