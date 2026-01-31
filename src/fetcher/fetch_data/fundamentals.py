from financetoolkit import Toolkit
import polars as pl
import pandas as pd
import os
import yfinance as yf
import yfinance.data as yfdata
import requests
import curl_cffi.requests as creq
import time
from datetime import timezone, datetime
from functools import cached_property

yf.config.debug.hide_exceptions = False

class SilentFail(Exception):
    pass

class FakeResp:
    status_code = 429

class FakeTicker:
    @property
    def info(self):
        raise creq.exceptions.HTTPError(msg = "BULLSHIT", response=FakeResp())

    @property
    def balance_sheet(self):
        raise creq.exceptions.HTTPError(msg = "BULLSHIT", response=FakeResp())

    @property
    def cashflow(self):
        raise creq.exceptions.HTTPError(response=FakeResp())
    
class fundamentals:
    def __init__(self, ticker, bronze_path=".dev/data/bronze"):

        self.bronze_path = bronze_path
        self._yf = yf.Ticker(ticker)
        self.fake = FakeTicker()
        info = self._yf.info
    
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
        # for w in self.exp_rl_wait: 
        #     try:
        data = fn()
        return data
        #     except creq.exceptions.Timeout:
        #         print(f"Timeout for {self.ticker}. Retrying in {w}s")
        #         time.sleep(w)
        #         continue
        #     except creq.exceptions.HTTPError as exc:
        #         status = exc.response.status_code if exc.response is not None else None
        #         print(f"Error Handling. Status: {status}")
        #         if status == 404:
        #             print(f"Invalid ticker: {self.ticker}")
        #             return data
        #         if status == 429:
        #             print(f"Rate limited. Waiting {w} seconds before retry.")
        #             time.sleep(w)
        #             continue
        #         if status in (0, None): 
        #             print(f"Status 0, retry after {w} seconds.")
        #             time.sleep(w)
        #             continue
                    
        # raise creq.exceptions.HTTPError("Rate limited after retries or connection lost.")
                        
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

        if obj is None: 
            return pl.DataFrame(schema=schema)
       
        metarows = {}
        for f in schema.keys():
            metarows[f] = obj.get(f)

        if all(not v for v in metarows.values()):
            return pl.DataFrame(schema=schema)

        metarows["ticker"] = ticker
        metarows["timestamp"] = ts

        for k in ("dateShortInterest", "lastFiscalYearEnd", "nextFiscalYearEnd", "mostRecentQuarter"):
            v = metarows.get(k)
            metarows[k] = None if v is None else datetime.fromtimestamp(int(v), tz=timezone.utc).date()

        return pl.DataFrame(metarows, schema=schema)

    def write(self):
        # if self.is404: 
        #     # here we can write to blacklist!
        #     print(f"Invalid ticker: {self.ticker}")
        #     self._add_to_blacklist()
        #     return None

        meta = self.metadata
        bs = self.balance_sheet_combined
        inc = self.income_statement_combined
        cf = self.cashflow_combined

        if all(t.height == 0 for t in (meta, bs, inc, cf)):
            raise SilentFail(f"{self.ticker} silently failed.")
        else: 
            meta.write_parquet(
                f"{self.bronze_path}/metadata/{self.ticker}{self.ts_path}.parquet"
            )
            bs.write_parquet(
                f"{self.bronze_path}/balance_sheet/{self.ticker}{self.ts_path}.parquet"
            )
            inc.write_parquet(
                f"{self.bronze_path}/income_statement/{self.ticker}{self.ts_path}.parquet"
            )
            cf.write_parquet(
                f"{self.bronze_path}/cashflow_statement/{self.ticker}{self.ts_path}.parquet"
            )

    @cached_property
    def metadata(self):
        return self.__get_meta(obj=self.info, ts=self.ts, ticker=self.ticker)
            
    @cached_property
    def balance_sheet(self):

        bs = self._handle_http_exceptions(lambda: self._yf.balance_sheet)

        #bs = self._handle_http_exceptions(lambda: self.fake.balance_sheet) # for debugging 429 responses
                    
        return self.__get_tabular(
            obj=bs, ts=self.ts, ticker=self.ticker
        )

    @cached_property
    def balance_sheet_quarterly(self):

        bs = self._handle_http_exceptions(lambda: self._yf.quarterly_balance_sheet)

        return self.__get_tabular(
            obj=bs, ts=self.ts, ticker=self.ticker
        )

    @cached_property
    def balance_sheet_combined(self):
        return self.balance_sheet.with_columns(pl.lit("yearly").alias("freq")).vstack(
            self.balance_sheet_quarterly.with_columns(pl.lit("quarterly").alias("freq"))
        )

    @cached_property
    def income_statement(self):

        inc = self._handle_http_exceptions(lambda: self._yf.income_stmt)

        return self.__get_tabular(
            obj=inc, ts=self.ts, ticker=self.ticker
        )

    @cached_property
    def income_statement_quarterly(self):

        inc = self._handle_http_exceptions(lambda: self._yf.quarterly_income_stmt,)

        return self.__get_tabular(
            obj=inc, ts=self.ts, ticker=self.ticker
        )

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
        return self.__get_tabular(
            obj=cf, ts=self.ts, ticker=self.ticker
        )

    @cached_property
    def cashflow_combined(self):
        return self.cashflow.with_columns(pl.lit("yearly").alias("freq")).vstack(
            self.cashflow_quarterly.with_columns(pl.lit("quarterly").alias("freq"))
        )
    