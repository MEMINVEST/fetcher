from __future__ import annotations

import os
from typing import Any

import pandas as pd
import polars as pl
import yfinance as yf

yf.config.debug.hide_exceptions = False

pd.set_option("future.no_silent_downcasting", True)


class daily_data:
    def __init__(self, ticker: str, bronze_path: str = ".dev/data/bronze") -> None:
        self.bronze_path: str = bronze_path
        self._yf: Any = yf.Ticker(ticker)
        self.ts: pd.Timestamp = pd.Timestamp.now()
        self.ts_path: str = (
            str(self.ts).replace(" ", "_").replace(":", "").replace(".", "")
        )
        self.ticker: str = ticker
        self._make_bronze_folder(ticker=ticker, path=bronze_path)
        base_files: list[str] = os.listdir(f"{bronze_path}/daily_data/{ticker}/base")
        self.init_history: bool = len(base_files) == 0

    @staticmethod
    def _make_bronze_folder(ticker: str, path: str) -> None:
        base_path: str = f"{path}/daily_data/{ticker}"
        if not os.path.isdir(base_path):
            os.mkdir(base_path)
            os.mkdir(f"{base_path}/base")
            os.mkdir(f"{base_path}/update")

    def initialize_history(self) -> pd.DataFrame:
        return self._yf.history(period="50y", interval="1d", auto_adjust=False)

    def download_latest(self) -> pd.DataFrame:
        return self._yf.history(period="5d", interval="1d", auto_adjust=False)

    def get_daily(self) -> pl.DataFrame:
        data: pd.DataFrame

        if self.init_history:
            data = (
                self.initialize_history()
                .replace("Infinity", float("inf"))
                .replace("-Infinity", float("-inf"))
            )
        else:
            data = self.download_latest()

        schema: pl.Schema = pl.Schema(
            {
                "ticker": pl.Utf8,
                "timestamp": pl.Datetime("ns"),
                "date": pl.Date,
                "Open": pl.Float64,
                "High": pl.Float64,
                "Low": pl.Float64,
                "Close": pl.Float64,
                "Volume": pl.Float64,
                "Dividends": pl.Float64,
                "Stock Splits": pl.Float64,
            }
        )

        table: pl.DataFrame = (
            pl.from_pandas(data, include_index=True)
            .rename({"Date": "date"})
            .with_columns(
                pl.lit(self.ticker).alias("ticker"), pl.lit(self.ts).alias("timestamp")
            )
            .cast(schema, strict=False)
            .select(
                "ticker",
                "timestamp",
                "date",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Dividends",
                "Stock Splits",
            )
        )
        return table

    def store_daily(self) -> None:
        data: pl.DataFrame = self.get_daily()
        if self.init_history:
            data.write_parquet(
                f"{self.bronze_path}/daily_data/{self.ticker}/base/base_{self.ts_path}.parquet"
            )
        else:
            data.write_parquet(
                f"{self.bronze_path}/daily_data/{self.ticker}/update/update_{self.ts_path}.parquet"
            )
