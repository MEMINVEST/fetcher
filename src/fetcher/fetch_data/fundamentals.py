from __future__ import annotations

from datetime import timezone, datetime
from functools import cached_property
import os
from typing import Any, Callable

from curl_cffi.requests import exceptions as creq_exceptions
import pandas as pd
import polars as pl
import yfinance as yf
from yfinance import exceptions as yf_exceptions

yf.config.debug.hide_exceptions = False


class SilentFail(Exception):
    pass


class FakeResp:
    status_code: int = 429


class FakeTicker:
    @property
    def info(self) -> Any:
        raise creq_exceptions.HTTPError(msg="BULLSHIT", response=FakeResp())

    @property
    def balance_sheet(self) -> Any:
        raise creq_exceptions.HTTPError(msg="BULLSHIT", response=FakeResp())

    @property
    def cashflow(self) -> Any:
        raise creq_exceptions.HTTPError(msg="BULLSHIT", response=FakeResp())


class fundamentals:
    def __init__(self, ticker: str, bronze_path: str = ".dev/data/bronze") -> None:
        self.bronze_path: str = bronze_path
        self._yf: Any = yf.Ticker(ticker)
        self.fake: FakeTicker = FakeTicker()
        info: dict[str, Any] | None = self._yf.info
        isin: str | None = self._yf.isin
        if isin is None:
            isin = ""

        self.isin: str = isin

        if info is not None:
            self.info = info
        else:
            self.info = None

        self.ts: pd.Timestamp = pd.Timestamp.now()
        self.ts_path: str = (
            str(self.ts).replace(" ", "_").replace(":", "").replace(".", "")
        )
        self.ticker: str = ticker

    @staticmethod
    def _make_bronze_folder(ticker: str, path: str, folder: str) -> None:
        base_path: str = f"{path}/{folder}/{ticker}"
        if not os.path.isdir(base_path):
            os.mkdir(base_path)

    def _handle_http_exceptions(self, fn: Callable[[], Any]) -> Any:
        try:
            data: Any = fn()
        except yf_exceptions.YFRateLimitError:
            raise
        except creq_exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            print(f"Error Handling. Status: {status}")
            return None

        return data

    def __get_tabular(
        self, obj: pd.DataFrame | None, ts: pd.Timestamp, ticker: str
    ) -> pl.DataFrame:
        schema: pl.Schema = pl.Schema(
            {
                "ticker": pl.String,
                "timestamp": pl.Datetime("ns"),
                "item": pl.String,
                "date": pl.Date,
                "value": pl.Float64,
            }
        )

        if obj is None:
            return pl.DataFrame(schema=schema).select(
                "ticker", "timestamp", "item", "date", "value"
            )

        if obj.empty:
            return pl.DataFrame(schema=schema).select(
                "ticker", "timestamp", "item", "date", "value"
            )

        table: pl.DataFrame = (
            pl.from_pandas(obj, include_index=True)
            .rename({"None": "item"})
            .unpivot(index="item", variable_name="date", value_name="value")
            .with_columns(
                pl.lit(ticker).alias("ticker"),
                pl.lit(ts).alias("timestamp"),
                pl.col("date").str.to_datetime(),
            )
            .cast(schema, strict=False)
            .select("ticker", "timestamp", "item", "date", "value")
        )
        return table

    def __get_meta(
        self, obj: dict[str, Any] | None, ts: pd.Timestamp, ticker: str
    ) -> pl.DataFrame:
        schema: pl.Schema = pl.Schema(
            {
                "ticker": pl.String,
                "timestamp": pl.Datetime("ns"),
                "longName": pl.Utf8,
                "shortName": pl.Utf8,
                "sector": pl.Utf8,
                "industry": pl.Utf8,
                "country": pl.Utf8,
                "currency": pl.Utf8,
                "exchange": pl.Utf8,
                "sharesOutstanding": pl.Int64,
                "isin": pl.Utf8,
                "floatShares": pl.Int64,
                "sharesShort": pl.Int64,
                "fullTimeEmployees": pl.Int64,
                "auditRisk": pl.Int64,
                "boardRisk": pl.Int64,
                "compensationRisk": pl.Int64,
                "shareHolderRightsRisk": pl.Int64,
                "overallRisk": pl.Int64,
                "dateShortInterest": pl.Date,
                "lastFiscalYearEnd": pl.Date,
                "nextFiscalYearEnd": pl.Date,
                "mostRecentQuarter": pl.Date,
            }
        )

        if obj is None:
            return pl.DataFrame(schema=schema)

        metarows: dict[str, Any] = {}
        for f in schema.keys():
            if f == "isin":
                metarows[f] = self.isin
            else:
                metarows[f] = obj.get(f)

        if all(not v for v in metarows.values()):
            return pl.DataFrame(schema=schema)

        metarows["ticker"] = ticker
        metarows["timestamp"] = ts

        for k in (
            "dateShortInterest",
            "lastFiscalYearEnd",
            "nextFiscalYearEnd",
            "mostRecentQuarter",
        ):
            v: Any = metarows.get(k)
            metarows[k] = (
                None
                if v is None
                else datetime.fromtimestamp(int(v), tz=timezone.utc).date()
            )

        return pl.DataFrame(metarows, schema=schema)

    def write(self) -> None:

        meta: pl.DataFrame = self.metadata
        bs: pl.DataFrame | None = self.balance_sheet_combined
        inc: pl.DataFrame | None = self.income_statement_combined
        cf: pl.DataFrame | None = self.cashflow_combined
        ad: pl.DataFrame | None = self.analyst_data
        od: pl.DataFrame | None = self.ownership_data

        if all((t is None or t.height == 0) for t in (bs, inc, cf, ad, od)):
            raise SilentFail(f"{self.ticker} silently failed.")
        else:
            if meta is not None:
                self._make_bronze_folder(
                    ticker=self.ticker, path=self.bronze_path, folder="metadata"
                )
                meta.write_parquet(
                    f"{self.bronze_path}/metadata/{self.ticker}/{self.ts_path}.parquet"
                )
            if bs is not None:
                self._make_bronze_folder(
                    ticker=self.ticker,
                    path=self.bronze_path,
                    folder="balance_sheet",
                )
                bs.write_parquet(
                    f"{self.bronze_path}/balance_sheet/{self.ticker}/{self.ts_path}.parquet"
                )
            if inc is not None:
                self._make_bronze_folder(
                    ticker=self.ticker,
                    path=self.bronze_path,
                    folder="income_statement",
                )
                inc.write_parquet(
                    f"{self.bronze_path}/income_statement/{self.ticker}/{self.ts_path}.parquet"
                )
            if cf is not None:
                self._make_bronze_folder(
                    ticker=self.ticker,
                    path=self.bronze_path,
                    folder="cashflow_statement",
                )
                cf.write_parquet(
                    f"{self.bronze_path}/cashflow_statement/{self.ticker}/{self.ts_path}.parquet"
                )
            if ad is not None:
                self._make_bronze_folder(
                    ticker=self.ticker, path=self.bronze_path, folder="analyst_data"
                )
                ad.write_parquet(
                    f"{self.bronze_path}/analyst_data/{self.ticker}/{self.ts_path}.parquet"
                )
            if od is not None:
                self._make_bronze_folder(
                    ticker=self.ticker, path=self.bronze_path, folder="ownership_data"
                )
                od.write_parquet(
                    f"{self.bronze_path}/ownership_data/{self.ticker}/{self.ts_path}.parquet"
                )

    @cached_property
    def metadata(self) -> pl.DataFrame:
        return self.__get_meta(obj=self.info, ts=self.ts, ticker=self.ticker)

    @cached_property
    def balance_sheet(self) -> pl.DataFrame:

        bs: pd.DataFrame | None = self._handle_http_exceptions(
            lambda: self._yf.balance_sheet
        )

        return self.__get_tabular(obj=bs, ts=self.ts, ticker=self.ticker)

    @cached_property
    def balance_sheet_quarterly(self) -> pl.DataFrame:

        bs: pd.DataFrame | None = self._handle_http_exceptions(
            lambda: self._yf.quarterly_balance_sheet
        )

        return self.__get_tabular(obj=bs, ts=self.ts, ticker=self.ticker)

    @cached_property
    def balance_sheet_combined(self) -> pl.DataFrame | None:
        bsc: pl.DataFrame = self.balance_sheet.with_columns(
            pl.lit("yearly").alias("freq")
        ).vstack(
            self.balance_sheet_quarterly.with_columns(pl.lit("quarterly").alias("freq"))
        )

        if bsc.height == 0:
            return None

        return bsc



    @cached_property
    def income_statement(self) -> pl.DataFrame:

        inc: pd.DataFrame | None = self._handle_http_exceptions(
            lambda: self._yf.income_stmt
        )

        return self.__get_tabular(obj=inc, ts=self.ts, ticker=self.ticker)

    @cached_property
    def income_statement_quarterly(self) -> pl.DataFrame:

        inc: pd.DataFrame | None = self._handle_http_exceptions(
            lambda: self._yf.quarterly_income_stmt,
        )

        return self.__get_tabular(obj=inc, ts=self.ts, ticker=self.ticker)

    @cached_property
    def income_statement_combined(self) -> pl.DataFrame | None:
        incc: pl.DataFrame = self.income_statement.with_columns(
            pl.lit("yearly").alias("freq")
        ).vstack(
            self.income_statement_quarterly.with_columns(
                pl.lit("quarterly").alias("freq")
            )
        )

        if incc.height == 0:
            return None

        return incc

    @cached_property
    def cashflow(self) -> pl.DataFrame:

        cf: pd.DataFrame | None = self._handle_http_exceptions(lambda: self._yf.cashflow)

        return self.__get_tabular(obj=cf, ts=self.ts, ticker=self.ticker)

    @cached_property
    def cashflow_quarterly(self) -> pl.DataFrame:

        cf: pd.DataFrame | None = self._handle_http_exceptions(
            lambda: self._yf.quarterly_cashflow
        )
        return self.__get_tabular(obj=cf, ts=self.ts, ticker=self.ticker)

    @cached_property
    def cashflow_combined(self) -> pl.DataFrame | None:
        cfc: pl.DataFrame = self.cashflow.with_columns(
            pl.lit("yearly").alias("freq")
        ).vstack(
            self.cashflow_quarterly.with_columns(pl.lit("quarterly").alias("freq"))
        )

        if cfc.height == 0:
            return None

        return cfc

    def _analyst_data_df(
        self,
        df: pd.DataFrame,
        src: str,
        schema_out: pl.Schema,
    ) -> pl.DataFrame:
        if "period" in df.columns:
            data: pl.DataFrame = pl.from_pandas(df).unpivot(
                index="period", variable_name="item", value_name="value"
            )
        elif df.index.name == "period":
            df = df.reset_index().rename(columns={"index": "period"})
            data = pl.from_pandas(df).unpivot(
                index="period", variable_name="item", value_name="value"
            )
        else:
            print("Unknown format. Returning empty polars df.")
            return pl.DataFrame(schema=schema_out)

        return data.with_columns(pl.lit(src).alias("source")).cast(schema_out)

    def _analyst_data_dict(
        self,
        dictionary: dict[str, Any],
        src: str,
        schema_out: pl.Schema,
    ) -> pl.DataFrame:
        schema: pl.Schema = pl.Schema(
            {
                "current": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "mean": pl.Float64,
                "median": pl.Float64,
            }
        )

        for s in schema.keys():
            if s not in dictionary.keys():
                dictionary[s] = None

        data = (
            pl.DataFrame(dictionary, schema=schema)
            .with_columns(pl.lit("0d").alias("period"))
            .unpivot(index="period", variable_name="item", value_name="value")
            .with_columns(pl.lit(src).alias("source"))
        )

        return data.cast(schema_out)

    @cached_property
    def analyst_data(self) -> pl.DataFrame | None:
        tables: set[str] = {
            "recommendations",
            "analyst_price_targets",
            "earnings_estimate",
            "revenue_estimate",
            "growth_estimates",
            "eps_revisions",
        }

        schema_out: pl.Schema = pl.Schema(
            {
                "period": pl.String,
                "item": pl.String,
                "value": pl.Float64,
                "source": pl.String,
            }
        )

        fetched: list[pl.DataFrame] = []
        for t in tables:
            attr: Any = self._handle_http_exceptions(lambda: getattr(self._yf, t))
            if attr is None:
                fetched.append(pl.DataFrame(schema=schema_out))
            elif isinstance(attr, pd.DataFrame):
                fetched.append(
                    self._analyst_data_df(attr, src=t, schema_out=schema_out)
                )
            elif isinstance(attr, dict):
                fetched.append(
                    self._analyst_data_dict(attr, src=t, schema_out=schema_out)
                )
            else:
                print(f"Unextpected class {type(attr)}. Returning empty polars df.")
                fetched.append(pl.DataFrame(schema=schema_out))

        data: pl.DataFrame = pl.concat(fetched, how="vertical").with_columns(
            pl.lit(self.ticker).alias("ticker"), pl.lit(self.ts).alias("timestamp")
        )

        # convert to none if completely empty to avoid write
        if data.height == 0:
            return None

        return data

    @cached_property
    def ownership_data(self) -> pl.DataFrame | None:
        schema: pl.Schema = pl.Schema(
            {
                "item": pl.String,
                "measure": pl.String,
                "value": pl.Float64,
            }
        )

        major_holders_pd: pd.DataFrame | None = self._handle_http_exceptions(
            lambda: self._yf.major_holders
        )

        if major_holders_pd is None:
            major_holders: pl.DataFrame = pl.DataFrame(schema=schema)
        elif major_holders_pd.empty:
            major_holders = pl.DataFrame(schema=schema)
        else:
            major_holders = (
                pl.DataFrame(major_holders_pd.reset_index(names="item")).with_columns(pl.lit(None).alias("measure"))
                .rename({"Value": "value"})
                .cast(schema)
            )

        insider_pd: pd.DataFrame | None = self._handle_http_exceptions(
            lambda: self._yf.insider_purchases
        )

        if insider_pd is None:
            insider: pl.DataFrame = pl.DataFrame(schema=schema)
        elif insider_pd.empty:
            insider = pl.DataFrame(schema=schema)
        else:
            insider_raw: pl.DataFrame = pl.DataFrame(insider_pd)
            insider = (
                insider_raw.rename({insider_raw.columns[0]: "item"})
                .unpivot(index="item", variable_name="measure", value_name="value")
                .cast(schema)
            )

        data: pl.DataFrame = pl.concat([major_holders, insider], how="diagonal").with_columns(
            pl.lit(self.ticker).alias("ticker"), pl.lit(self.ts).alias("timestamp")
        )

        if data.height == 0:
            return None

        return data
