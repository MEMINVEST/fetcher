from pathlib import Path


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
            "analyst_data",
            "ownership_data",
        ]
        for s in subdirs:
            (path / s).mkdir(parents=True, exist_ok=True)


def clean_dir(root=".dev/data/bronze"):
    root_path = Path(root)
    if not root_path.exists():
        return
    for path in root_path.rglob("*"):
        if path.is_file() or path.is_symlink():
            path.unlink()


def create_dir(path):
    path = Path(path)
    if path.exists() and path.is_dir():
        print("Directory exists")
    else:
        path.mkdir()


def create_data(root=".dev/data"):
    create_bronze(path=f"{root}/bronze")
    create_dir(path=f"{root}/silver")
    create_dir(path=f"{root}/gold")
    create_dir(path=f"{root}/blacklist")
    create_dir(path=f"{root}/silent_fails")
    create_dir(path=f"{root}/logs")
    create_dir(path=f"{root}/tickers")
