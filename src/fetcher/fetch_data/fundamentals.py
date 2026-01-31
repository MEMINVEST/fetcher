from financetoolkit import Toolkit
import polars as pl
import pandas as pd
import os
import yfinance as yf
import yfinance.data as yfdata
import requests
import curl_cffi.requests as creq
import time
from functools import cached_property

yf.config.debug.hide_exceptions = False


class SilentFail(Exception):
    pass


class FakeResp:
    status_code = 429


class FakeTicker:
    @property
    def info(self):
        raise creq.exceptions.HTTPError(msg="BULLSHIT", response=FakeResp())

    @property
    def balance_sheet(self):
        raise creq.exceptions.HTTPError(msg="BULLSHIT", response=FakeResp())

    @property
    def cashflow(self):
        raise creq.exceptions.HTTPError(response=FakeResp())


class fundamentals:
    def __init__(self, ticker, bronze_path=".dev/data/bronze"):
        self.bronze_path = bronze_path
        self._yf = yf.Ticker(ticker)
        self.fake = FakeTicker()
        info = self._yf.info
        isin = self._yf.isin
        if isin is None:
            isin = ""

        self.isin = isin

        if info is not None:
            self.info = info
        else:
            self.info = None

        self.ts = pd.Timestamp.now()
        self.ts_path = str(self.ts).replace(" ", "_").replace(":", "").replace(".", "")
        self.ticker = ticker

    def _add_to_blacklist(self):
        with open(self.blacklist, "a") as f:
            f.write(f"{self.ticker}\n")

    def _handle_http_exceptions(self, fn):
        try:
            data = fn()
        except creq.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            print(f"Error Handling. Status: {status}")
            if status == 404:
                data = None

        return data

    def __get_tabular(self, obj, ts, ticker):
        schema = {
            "ticker": pl.String,
            "timestamp": pl.Datetime("ns"),
            "item": pl.String,
            "date": pl.Date,
            "value": pl.Float64,
        }

        if obj.empty:
            return pl.DataFrame(schema=schema).select(
                "ticker", "timestamp", "item", "date", "value"
            )

        if obj is None:
            return pl.DataFrame(schema=schema).select(
                "ticker", "timestamp", "item", "date", "value"
            )

        table = (
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

    def __get_meta(self, obj, ts, ticker):
        schema = {
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
        }

        if obj is None:
            return pl.DataFrame(schema=schema)

        metarows = {}
        for f in schema.keys():
            if f == "isin":
                metarows[f] = self.isin
            else:
                metarows[f] = obj.get(f)

        if all(not v for v in metarows.values()):
            return pl.DataFrame(schema=schema)

        metarows["ticker"] = ticker
        metarows["timestamp"] = ts

        return pl.DataFrame(metarows, schema=schema)

    def write(self):
        meta = self.metadata
        bs = self.balance_sheet_combined
        inc = self.income_statement_combined
        cf = self.cashflow_combined
        ad = self._handle_http_exceptions(lambda: self.analyst_data) # this needs to be catched earlier
        od = self._handle_http_exceptions(lambda: self.ownership_data) # here too

        if all(t.height == 0 for t in (meta, bs, inc, cf, ad, od)):
            raise SilentFail(f"{self.ticker} silently failed.")
        else:
            if meta is not None:
                meta.write_parquet(
                    f"{self.bronze_path}/metadata/{self.ticker}{self.ts_path}.parquet"
                )
            if bs is not None:
                bs.write_parquet(
                    f"{self.bronze_path}/balance_sheet/{self.ticker}{self.ts_path}.parquet"
                )
            if inc is not None:
                inc.write_parquet(
                    f"{self.bronze_path}/income_statement/{self.ticker}{self.ts_path}.parquet"
                )
            if cf is not None:
                cf.write_parquet(
                    f"{self.bronze_path}/cashflow_statement/{self.ticker}{self.ts_path}.parquet"
                )
            if ad is not None:
                ad.write_parquet(
                    f"{self.bronze_path}/analyst_data/{self.ticker}{self.ts_path}.parquet"
                )
            if od is not None:
                od.write_parquet(
                    f"{self.bronze_path}/ownership_data/{self.ticker}{self.ts_path}.parquet"
                )

    @cached_property
    def metadata(self):
        return self.__get_meta(obj=self.info, ts=self.ts, ticker=self.ticker)

    @cached_property
    def balance_sheet(self):

        bs = self._handle_http_exceptions(lambda: self._yf.balance_sheet)

        return self.__get_tabular(obj=bs, ts=self.ts, ticker=self.ticker)

    @cached_property
    def balance_sheet_quarterly(self):

        bs = self._handle_http_exceptions(lambda: self._yf.quarterly_balance_sheet)

        return self.__get_tabular(obj=bs, ts=self.ts, ticker=self.ticker)

    @cached_property
    def balance_sheet_combined(self):
        return self.balance_sheet.with_columns(pl.lit("yearly").alias("freq")).vstack(
            self.balance_sheet_quarterly.with_columns(pl.lit("quarterly").alias("freq"))
        )

    @cached_property
    def income_statement(self):

        inc = self._handle_http_exceptions(lambda: self._yf.income_stmt)

        return self.__get_tabular(obj=inc, ts=self.ts, ticker=self.ticker)

    @cached_property
    def income_statement_quarterly(self):

        inc = self._handle_http_exceptions(
            lambda: self._yf.quarterly_income_stmt,
        )

        return self.__get_tabular(obj=inc, ts=self.ts, ticker=self.ticker)

    @cached_property
    def income_statement_combined(self):
        return self.income_statement.with_columns(
            pl.lit("yearly").alias("freq")
        ).vstack(
            self.income_statement_quarterly.with_columns(
                pl.lit("quarterly").alias("freq")
            )
        )

    @cached_property
    def cashflow(self):

        cf = self._handle_http_exceptions(lambda: self._yf.cashflow)

        return self.__get_tabular(obj=cf, ts=self.ts, ticker=self.ticker)

    @cached_property
    def cashflow_quarterly(self):

        cf = self._handle_http_exceptions(lambda: self._yf.quarterly_cashflow)
        return self.__get_tabular(obj=cf, ts=self.ts, ticker=self.ticker)

    @cached_property
    def cashflow_combined(self):
        return self.cashflow.with_columns(pl.lit("yearly").alias("freq")).vstack(
            self.cashflow_quarterly.with_columns(pl.lit("quarterly").alias("freq"))
        )

    def _analyst_data_df(self, df, source, schema_out):
        # is_pd_df = isinstance(df, )
        if "period" in df.columns:
            data = pl.from_pandas(df).unpivot(
                index="period", variable_name="item", value_name="value"
            )
        elif df.index.name == "period":
            df = df.reset_index().rename(columns={"index": "period"})
            data = pl.from_pandas(df).unpivot(
                index="period", variable_name="item", value_name="value"
            )
        else:
            print("Unknown format. None.")
            return None

        return data.with_columns(pl.lit(source).alias("source")).cast(schema_out)

    def _analyst_data_dict(self, dictionary, source, schema_out):
        schema = {
            "current": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "mean": pl.Float64,
            "median": pl.Float64,
        }

        for s in schema.keys():
            if s not in dictionary.keys():
                dictionary[s] = None

        data = (
            pl.DataFrame(dictionary, schema=schema)
            .with_columns(pl.lit("0d").alias("period"))
            .unpivot(index="period", variable_name="item", value_name="value")
            .with_columns(pl.lit(source).alias("source"))
        )

        return data.cast(schema_out)

    @cached_property
    def analyst_data(self):
        tables = {
            "recommendations",
            "analyst_price_targets",
            "earnings_estimate",
            "revenue_estimate",
            "growth_estimates",
            "eps_revisions",
        }

        schema_out = {
            "period": pl.String,
            "item": pl.String,
            "value": pl.Float64,
            "source": pl.String,
        }

        fetched = []
        for t in tables:
            attr = getattr(self._yf, t)
            if isinstance(attr, pd.core.frame.DataFrame):
                fetched.append(
                    self._analyst_data_df(attr, source=t, schema_out=schema_out)
                )
            elif isinstance(attr, dict):
                fetched.append(
                    self._analyst_data_dict(attr, source=t, schema_out=schema_out)
                )
            else:
                print(f"Unextpected class {type(attr)}.")

        data = pl.concat(fetched, how="vertical").with_columns(
            pl.lit(self.ticker).alias("ticker"), pl.lit(self.ts).alias("timestamp")
        )

        return data

    @cached_property
    def ownership_data(self):
        schema = {
            "item": pl.String,
            "measure": pl.String,
            "value": pl.Float64,
        }

        major_holders_pd = self._yf.major_holders

        if major_holders_pd is None:
            major_holders = pl.DataFrame(schema=schema)
        elif major_holders_pd.empty:
            major_holders = pl.DataFrame(schema=schema)
        else:
            major_holders = (
                pl.DataFrame(major_holders_pd.reset_index(names="item")).with_columns(pl.lit(None).alias("measure"))
                .rename({"Value": "value"})
                .cast(schema)
            )

        insider_pd = self._yf.insider_purchases

        if insider_pd is None:
            insider = pl.DataFrame(schema=schema)
        elif insider_pd.empty:
            insider = pl.DataFrame(schema=schema)
        else:
            insider_raw = pl.DataFrame(insider_pd)
            insider = (
                insider_raw.rename({insider_raw.columns[0]: "item"})
                .unpivot(index="item", variable_name="measure", value_name="value")
                .cast(schema)
            )

        data = pl.concat([major_holders, insider], how="diagonal").with_columns(
            pl.lit(self.ticker).alias("ticker"), pl.lit(self.ts).alias("timestamp")
        )

        return data

# Worker failed for 0HV.MU: cannot access local variable 'data' where it is not associated with a value
# test = fundamentals(ticker="0HV.MU")
# test.write()
