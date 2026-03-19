from __future__ import annotations

import contextlib
import datetime as dt
import logging
import os
from pathlib import Path
import random
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed

import polars as pl
from tqdm import tqdm
from curl_cffi.requests import exceptions as creq_exceptions
from yfinance import exceptions as yf_exceptions

from fetcher.fetch_data import daily


class run_daily:
    def __init__(self, root: str = ".dev/data") -> None:
        blacklist_dir: Path = Path(f"{root}/blacklist")
        blacklisted: set[str] = set()
        self.root: str = root

        for path in blacklist_dir.glob("*.txt"):
            with path.open() as f:
                for line in f:
                    line = line.strip()
                    if line:
                        blacklisted.add(line)

        tickers: pl.DataFrame = (
            pl.read_parquet(f"{root}/tickers")
            .filter(pl.col("timestamp") == pl.col("timestamp").max())
        )

        tickers_all: list[str] = tickers.get_column("symbol").to_list()
        tickers_str: list[str] = [t for t in tickers_all if t not in blacklisted]
        self.tickers: list[str] = tickers_str

        self.ts: str = (
            str(dt.datetime.now()).replace(" ", "_").replace(":", "").replace(".", "")
        )
        self.blacklist_location: str = f"{blacklist_dir}/blacklist_{self.ts}.txt"
        self.silent_location: str = f"{root}/silent_fails/silent_{self.ts}.txt"
        self.log_path: str = f"{root}/logs/run_log_{self.ts}.log"

    def retry(
        self, attempt: int, max_retries: int, base_delay: float, ticker: str
    ) -> None:
        if attempt >= max_retries:
            raise
        # Exponential backoff with jitter to reduce thundering herd
        delay: float = base_delay * (2**attempt) + random.uniform(
            0, 0.2 * base_delay
        )
        print(f"Retry {attempt + 1}/{max_retries} for {ticker} in {delay:.1f}s")
        time.sleep(delay)

    def add_to_blacklist(self, ticker: str, blacklist: str) -> None:
        print(f"blacklisted {ticker}")
        with open(blacklist, "a") as f:
            f.write(f"{ticker}\n")

    def add_to_silent(self, ticker: str, silent: str) -> None:
        with open(silent, "a") as f:
            f.write(f"{ticker}\n")

    def _run_one(self, tick: str, max_retries: int = 3) -> None:
        for attempt in range(max_retries + 1):
            try:
                dd: daily.daily_data = daily.daily_data(
                    ticker=tick, bronze_path=f"{self.root}/bronze"
                )
                dd.store_daily()
                return
            except creq_exceptions.Timeout:
                self.retry(
                    attempt=attempt,
                    max_retries=max_retries,
                    base_delay=5.0,
                    ticker=tick,
                )
            except creq_exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status in (0, 401, 429, None):
                    print(f"Error handling {tick}, Status {status}")
                    self.retry(
                        attempt=attempt,
                        max_retries=max_retries,
                        base_delay=60.0,
                        ticker=tick,
                    )
            except yf_exceptions.YFRateLimitError:
                print("Rate limited.")
                self.retry(
                    attempt=attempt,
                    max_retries=max_retries,
                    base_delay=60.0,
                    ticker=tick,
                )
            except yf_exceptions.YFPricesMissingError:
                self.add_to_blacklist(tick, blacklist=self.blacklist_location)
                return

    def run(self) -> None:

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            filename=self.log_path,
        )

        with (
            open(self.log_path, "a", buffering=1) as log_file,
            contextlib.redirect_stdout(log_file),
            contextlib.redirect_stderr(log_file),
        ):

            open(self.blacklist_location, "w").close()
            open(self.silent_location, "w").close()

            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_ticker: dict[Future[None], str] = {
                    executor.submit(self._run_one, tick): tick for tick in self.tickers
                }
                for fut in tqdm(
                    as_completed(future_to_ticker),
                    total=len(future_to_ticker),
                    desc="Daily Data",
                    file=sys.__stdout__,
                ):
                    try:
                        fut.result()
                    except Exception as exc:
                        tick: str = future_to_ticker[fut]
                        print(f"Worker failed for {tick}: {exc}")

        # cleanup of empty directories
        dirs: list[Path] = [
            Path(f"{self.root}/bronze/daily_data/{d}")
            for d in os.listdir(f"{self.root}/bronze/daily_data")
        ]
        # delete if empty
        for d in dirs:
            if d.is_dir():
                has_file: bool = any(sd.is_file() for sd in d.rglob("*"))
                if not has_file:
                    print(f"delete empty path {d}")
                    for sd in sorted(d.rglob("*"), reverse=True):
                        if sd.is_dir():
                            sd.rmdir()
                    d.rmdir()

