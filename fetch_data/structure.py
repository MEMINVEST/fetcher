from pathlib import Path
import os


def create_bronze(path=".dev/data/bronze"):
    path = Path(path)
    if path.exists() and path.is_dir():
        print("Directory exists")
    else:
        subdirs = [
            "metadata",
            "balance_sheet",
            "income_statement",
            "cashflow_statement",
            "daily_data/base",
            "daily_data/update",
            "intraday",
        ]
        for s in subdirs:
            (path / s).mkdir(parents=True, exist_ok=True)


create_bronze()


def clean_bronze(root=".dev/data/bronze"):
    root_path = Path(root)
    if not root_path.exists():
        return
    for path in root_path.rglob("*"):
        if path.is_file() or path.is_symlink():
            path.unlink()


def create_silver(path=".dev/data/silver"):
    path = Path(path)
    if path.exists() and path.is_dir():
        print("Directory exists")
    else:
        path.mkdir()


def clean_silver(root=".dev/data/silver"):
    clean_bronze(root=root)


create_silver()
clean_silver()


def create_gold(path=".dev/data/gold"):
    path = Path(path)
    create_silver(path=path)


def clean_gold(root=".dev/data/gold"):
    clean_bronze(root=root)


create_gold()
clean_gold()
