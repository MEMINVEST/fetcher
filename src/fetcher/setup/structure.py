from __future__ import annotations

from pathlib import Path


def create_bronze(path: str = ".dev/data/bronze") -> None:
    bronze_path: Path = Path(path)
    if bronze_path.exists() and bronze_path.is_dir():
        print("Directory exists")
    else:
        subdirs: list[str] = [
            "metadata",
            "balance_sheet",
            "income_statement",
            "cashflow_statement",
            "daily_data",
            "intraday",
            "analyst_data",
            "ownership_data",
        ]
        for s in subdirs:
            (bronze_path / s).mkdir(parents=True, exist_ok=True)


def clean_dir(root: str = ".dev/data/bronze") -> None:
    root_path = Path(root)
    if not root_path.exists():
        return
    # Remove files and subfolders under root, but keep root and its immediate subfolders.
    for path in sorted(root_path.rglob("*"), reverse=True):
        if path.is_file() or path.is_symlink():
            path.unlink()
        elif path.is_dir() and path.parent != root_path:
            path.rmdir()


def create_dir(path: str) -> None:
    dir_path: Path = Path(path)
    if dir_path.exists() and dir_path.is_dir():
        print("Directory exists")
    else:
        dir_path.mkdir()


def create_data(root: str = ".dev/data") -> None:
    create_bronze(path=f"{root}/bronze")
    create_dir(path=f"{root}/silver")
    create_dir(path=f"{root}/gold")
    create_dir(path=f"{root}/blacklist")
    create_dir(path=f"{root}/silent_fails")
    create_dir(path=f"{root}/logs")
    create_dir(path=f"{root}/tickers")
