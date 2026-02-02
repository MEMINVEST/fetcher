import yfinance as yf
import pandas as pd
import polars as pl
import os
import curl_cffi.requests as creq

yf.config.debug.hide_exceptions = False

pd.set_option("future.no_silent_downcasting", True)


class daily_data:
    def __init__(self, ticker, bronze_path=".dev/data/bronze"):
        self.bronze_path = bronze_path
        self._yf = yf.Ticker(ticker)
        self.ts = pd.Timestamp.now()
        self.ts_path = str(self.ts).replace(" ", "_").replace(":", "").replace(".", "")
        self.ticker = ticker
        base_files = os.listdir(f"{bronze_path}/daily_data/base")
        self.init_history = not any(ticker in bf for bf in base_files)

    def initialize_history(self):
        return self._yf.history(period="50y", interval="1d", auto_adjust=False)

    def download_latest(self):
        return self._yf.history(period="5d", interval="1d", auto_adjust=False)

    def get_daily(self):

        if self.init_history:
            data = (
                self.initialize_history()
                .replace("Infinity", float("inf"))
                .replace("-Infinity", float("-inf"))
            )
        else:
            data = self.download_latest()

        schema = {
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

        table = (
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

    def get_intraday(self):

        schema = {
            "ticker": pl.Utf8,
            "timestamp": pl.Datetime("ns"),
            "datetime": pl.Datetime("ns"),
            "Open": pl.Float64,
            "High": pl.Float64,
            "Low": pl.Float64,
            "Close": pl.Float64,
            "Volume": pl.Float64,
        }

        data = (
            self._yf.history(period="5d", interval="1m")
            .replace("Infinity", float("inf"))
            .replace("-Infinity", float("-inf"))
        )

        table = (
            pl.from_pandas(data, include_index=True)
            .rename({"Datetime": "datetime"})
            .with_columns(
                pl.lit(self.ticker).alias("ticker"), pl.lit(self.ts).alias("timestamp")
            )
            .cast(schema, strict=False)
            .select(
                "ticker",
                "timestamp",
                "datetime",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
            )
        )
        return table

    def store_daily(self):
        if self.init_history:
            data = self.get_daily()
            data.write_parquet(
                f"{self.bronze_path}/daily_data/base/{self.ticker}_base{self.ts_path}.parquet"
            )
        else:
            data = self.get_daily()
            data.write_parquet(
                f"{self.bronze_path}/daily_data/update/{self.ticker}_{self.ts_path}.parquet"
            )

        return None

    def store_intraday(self):
        data = self.get_intraday()
        data.write_parquet(
            f"{self.bronze_path}/intraday/{self.ticker}_{self.ts_path}.parquet"
        )

        return None
