from fetcher.fetch_data import fundamentals
from fetcher.fetch_data.fundamentals import SilentFail
import polars as pl
from tqdm import tqdm
import datetime as dt
from pathlib import Path
import logging
import contextlib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import curl_cffi.requests as creq
import yfinance
import os

class run_fundamentals: 
    def __init__(self, root = ".dev/data"): 
        blacklist_dir = Path(f"{root}/blacklist")
        blacklisted = set()

        for path in blacklist_dir.glob("*.txt"):
            with path.open() as f:
                for line in f:
                    line = line.strip()
                    if line:
                        blacklisted.add(line)
         
        tickers = (
            pl.scan_parquet(f"{root}/tickers")
            .filter(pl.col("timestamp") == pl.col("timestamp").max())
            .collect()
        )

        tickers_all = tickers.get_column("symbol").to_list()
        tickers_str = [t for t in tickers_all if t not in blacklisted]
        self.tickers = tickers_str

        self.ts = str(dt.datetime.now()).replace(" ", "_").replace(":", "").replace(".", "")
        self.blacklist_location = f"{blacklist_dir}/blacklist_{self.ts}.txt"
        self.silent_location = f"{root}/silent_fails/silent_{self.ts}.txt"
        self.log_path = f"{root}/logs/run_log_{self.ts}.log"

    def retry(self, attempt, max_retries, base_delay, ticker):
         if attempt >= max_retries:
                    raise
         # Exponential backoff with jitter to reduce thundering herd
         delay = base_delay * (2 ** attempt) + random.uniform(0, 0.2 * base_delay)
         print(f"Retry {attempt + 1}/{max_retries} for {ticker} in {delay:.1f}s")
         time.sleep(delay)

    def add_to_blacklist(self, ticker, blacklist):
         print(f"blacklisted {ticker}")
         with open(blacklist, "a") as f:
                f.write(f"{ticker}\n")

    def add_to_silent(self, ticker, silent):
         with open(silent, "a") as f:
                f.write(f"{ticker}\n")


    def _run_one(self, tick, max_retries = 3):
       for attempt in range(max_retries + 1):
           try:
               f = fundamentals.fundamentals(ticker=tick)
               f.write()
               return
           except creq.exceptions.Timeout: 
                self.retry(attempt = attempt, max_retries = max_retries, base_delay = 5.0, ticker = tick)
           except creq.exceptions.HTTPError as exc: 
                status = exc.response.status_code if exc.response is not None else None
                if status == 404: 
                     self.add_to_blacklist(tick, self.blacklist_location)
                     return
                if status in (0, 401, 429, None):
                   print(f"Error handling {tick}, Status {status}")
                   self.retry(attempt = attempt, max_retries=max_retries, base_delay = 60.0, ticker = tick)
           except yfinance.exceptions.YFRateLimitError: 
                print(f"Rate limited.")
                self.retry(attempt = attempt, max_retries=max_retries, base_delay = 60.0, ticker = tick)
           except SilentFail: 
                print(f"{tick} silently failed.")
                self.add_to_silent(tick, self.silent_location)
                return
           
    def run(self): 

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
            filename=self.log_path,  
        )

        with open(self.log_path, "a", buffering=1) as log_file, \
            contextlib.redirect_stdout(log_file), \
            contextlib.redirect_stderr(log_file):
             
            open(self.blacklist_location, "w").close()
            open(self.silent_location, "w").close()

            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_ticker = {
                    executor.submit(self._run_one, tick): tick
                    for tick in self.tickers
                }
                for fut in tqdm(
                    as_completed(future_to_ticker),
                    total=len(future_to_ticker),
                    desc="Fundamentals",
                    file=sys.__stdout__,
                ):
                    try:
                        fut.result()
                    except Exception as exc:
                        tick = future_to_ticker[fut]
                        print(f"Worker failed for {tick}: {exc}")
