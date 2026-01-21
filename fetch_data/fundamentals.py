from financetoolkit import Toolkit
import polars as pl
import pandas as pd
import os
import yfinance as yf
import yfinance.data as yfdata
import requests
import curl_cffi.requests as creq
import time

yf.config.debug.hide_exceptions = False

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
        self.exp_rl_wait = [2 ** e for e in range(5, 11)]
        self.is404 = False
        try: 
            self._yf.info
        except creq.exceptions.HTTPError as exc: 
            status = exc.response.status_code if exc.response is not None else None
            print(f"Error: {status}")
            if status == 404: 
                self.is404 = True
                
        self.ts = pd.Timestamp.now()
        self.ts_path = str(self.ts).replace(" ", "_").replace(":", "").replace(".", "")
        self.ticker = ticker

    def _handle_http_exceptions(self, fn):
        for w in self.exp_rl_wait: 
                try:
                    data = fn()
                    return data
                except creq.exceptions.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else None
                    if status == 404:
                        print(f"Invalid ticker: {self.ticker}")
                        return data
                    if status == 429:
                        print(f"Rate limited. Waiting {w} seconds before retry.")
                        time.sleep(w)
                        continue
                    
        raise creq.exceptions.HTTPError("Rate limited after retries")
                        
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
        }

        metarows = {}
        for f in schema.keys():
            metarows[f] = obj.get(f)

        if all(not v for v in metarows.values()):
            return pl.DataFrame(schema=schema)

        metarows["ticker"] = ticker
        metarows["timestamp"] = ts

        return pl.DataFrame(metarows, schema=schema)

    def write(self):
        if self.is404: 
            # here we can write to blacklist!
            print(f"Invalid ticker: {self.ticker}")
            return None

        self.metadata.write_parquet(
            f"{self.bronze_path}/metadata/{self.ticker}{self.ts_path}.parquet"
        )
        self.balance_sheet_combined.write_parquet(
            f"{self.bronze_path}/balance_sheet/{self.ticker}{self.ts_path}.parquet"
        )
        self.income_statement_combined.write_parquet(
            f"{self.bronze_path}/income_statement/{self.ticker}{self.ts_path}.parquet"
        )
        self.cashflow_combined.write_parquet(
            f"{self.bronze_path}/cashflow_statement/{self.ticker}{self.ts_path}.parquet"
        )

    @property
    def metadata(self):

        for w in self.exp_rl_wait: 
                try: 
                    info = self._yf.info
                    return self.__get_meta(obj=info, ts=self.ts, ticker=self.ticker)
                except creq.exceptions.HTTPError as exc:
                            status = exc.response.status_code if exc.response is not None else None
                            if status == 404:
                                print(f"Invalid ticker: {self.ticker}")
                                return self.__get_meta(obj=dict(), ts=self.ts, ticker=self.ticker)
                            if status == 429:
                                print(f"Rate limited. Waiting {w} seconds before retry.")
                                time.sleep(w)
        
        raise creq.exceptions.HTTPError("Rate limited after retries")
        
    @property
    def balance_sheet(self):

        bs = self._handle_http_exceptions(lambda: self._yf.balance_sheet)

        #bs = self._handle_http_exceptions(lambda: self.fake.balance_sheet) # for debugging 429 responses
                    
        return self.__get_tabular(
            obj=bs, ts=self.ts, ticker=self.ticker
        )

    @property
    def balance_sheet_quarterly(self):

        bs = self._handle_http_exceptions(lambda: self._yf.quarterly_balance_sheet)

        return self.__get_tabular(
            obj=bs, ts=self.ts, ticker=self.ticker
        )

    @property
    def balance_sheet_combined(self):
        return self.balance_sheet.with_columns(pl.lit("yearly").alias("freq")).vstack(
            self.balance_sheet_quarterly.with_columns(pl.lit("quarterly").alias("freq"))
        )

    @property
    def income_statement(self):

        inc = self._handle_http_exceptions(lambda: self._yf.income_stmt)

        return self.__get_tabular(
            obj=inc, ts=self.ts, ticker=self.ticker
        )

    @property
    def income_statement_quarterly(self):

        inc = self._handle_http_exceptions(lambda: self._yf.quarterly_income_stmt,)

        return self.__get_tabular(
            obj=inc, ts=self.ts, ticker=self.ticker
        )

    @property
    def income_statement_combined(self):
        return self.income_statement.with_columns(
            pl.lit("yearly").alias("freq")
        ).vstack(
            self.income_statement_quarterly.with_columns(
                pl.lit("quarterly").alias("freq")
            )
        )

    @property
    def cashflow(self):

        cf = self._handle_http_exceptions(lambda: self._yf.cashflow)

        return self.__get_tabular(obj=cf, ts=self.ts, ticker=self.ticker)

    @property
    def cashflow_quarterly(self):

        cf = self._handle_http_exceptions(lambda: self._yf.quarterly_cashflow)
        return self.__get_tabular(
            obj=cf, ts=self.ts, ticker=self.ticker
        )

    @property
    def cashflow_combined(self):
        return self.cashflow.with_columns(pl.lit("yearly").alias("freq")).vstack(
            self.cashflow_quarterly.with_columns(pl.lit("quarterly").alias("freq"))
        )
    
# test = fundamentals("CTPX.MU")
# test.metadata

class daily_data:
    def __init__(self, ticker):
        self._yf = yf.Ticker(ticker)
        self.ts = pd.Timestamp.now()
        self.ts_path = str(self.ts).replace(" ", "_").replace(":", "").replace(".", "")
        self.ticker = ticker
        base_files = os.listdir("{self.bronze_path}/daily_data/base")
        self.init_history = not any(ticker in bf for bf in base_files)

    def initialize_history(self):
        return self._yf.history(period="50y", interval="1d")

    def download_latest(self):
        return self._yf.history(period="5d", interval="1d")

    def get_daily(self):
        if self.init_history:
            data = self.initialize_history()
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

        data = self._yf.history(period="5d", interval="1m")

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


class ratios:
    def __init__(self, ticker, bronze_path=".dev/data/bronze"):
        self.metadata = ticker
        self.ticker = ticker.select("symbol").item()
        self.bronze_path = bronze_path
        self.company = Toolkit(
            tickers=self.ticker, enforce_source="YahooFinance", quarterly=False
        )
        self.company_q = Toolkit(
            tickers=self.ticker, enforce_source="YahooFinance", quarterly=True
        )
        self.ts = pd.Timestamp.now()
        self.ts_path = str(self.ts).replace(" ", "_").replace(":", "").replace(".", "")

    def _convert_stmt_to_polars(self, pandas_df: pd.core.frame.DataFrame):
        if pandas_df.empty:
            return None

        pandas_reset = pandas_df.reset_index()
        index_cols = list(pandas_reset.columns[: pandas_df.index.nlevels])
        if len(index_cols) == 1:
            pandas_reset = pandas_reset.rename(columns={index_cols[0]: "item"})
        else:
            pandas_reset["item"] = (
                pandas_reset[index_cols].astype(str).agg(" | ".join, axis=1)
            )
            pandas_reset = pandas_reset.drop(columns=index_cols)

        return (
            pl.from_pandas(pandas_reset)
            .unpivot(index="item", value_name="value", variable_name="time_raw")
            .with_columns(
                pl.col("time_raw").str.extract(r"(\d{4})").alias("year").cast(int),
                pl.col("time_raw").str.extract(r"Q(\d{1})").alias("quarter").cast(int),
            )
        )

    def _stack(self, yearly, quarterly):
        if quarterly is None:
            if yearly is None:
                return None
            else:
                return yearly

        if yearly is None:
            if quarterly is None:
                return None
            else:
                return quarterly

        if (yearly is not None) and (quarterly is not None):
            return yearly.vstack(quarterly)

    #     def _balance_sheet(self):
    #         bs_y = self._convert_stmt_to_polars(
    #             pandas_df=self.company.get_balance_sheet_statement(progress_bar=False)
    #         )
    #         bs_q = self._convert_stmt_to_polars(
    #             pandas_df=self.company_q.get_balance_sheet_statement(progress_bar=False)
    #         )

    #         bs = self._stack(yearly = bs_y, quarterly = bs_q)

    #         # check if folder in bronze exists and create if not
    #         bs_path = os.path.join(self.bronze_path, "balance_sheet")
    #         if not os.path.isdir(bs_path):
    #             os.mkdir(bs_path)

    #         if bs is None:
    #             return None

    #         return bs.pivot(
    #             values="value", index=["time_raw", "year", "quarter"], on="item"
    #         ).with_columns(pl.col("quarter").fill_null(4), pl.lit(self.ticker).alias("symbol"))

    #     def _income_stmt(self):
    #         is_y = self._convert_stmt_to_polars(
    #             pandas_df=self.company.get_income_statement(progress_bar=False)
    #         )
    #         is_q = self._convert_stmt_to_polars(
    #             pandas_df=self.company_q.get_income_statement(progress_bar=False)
    #         )
    #         _is = self._stack(yearly = is_y, quarterly = is_q)

    #         if _is is None:
    #             return None

    #         # check if folder in bronze exists and create if not
    #         is_path = os.path.join(self.bronze_path, "income_statement")
    #         if not os.path.isdir(is_path):
    #             os.mkdir(is_path)

    #         return _is.pivot(
    #             values="value", index=["time_raw", "year", "quarter"], on="item"
    #         ).with_columns(pl.col("quarter").fill_null(4), pl.lit(self.ticker).alias("symbol"))

    #     def _cashflow_stmt(self):
    #         cf_y = self._convert_stmt_to_polars(
    #             pandas_df=self.company.get_cash_flow_statement(progress_bar=False)
    #         )
    #         cf_q = self._convert_stmt_to_polars(
    #             pandas_df=self.company_q.get_cash_flow_statement(progress_bar=False)
    #         )
    #         cf = self._stack(yearly = cf_y, quarterly = cf_q)

    #         if cf is None:
    #             return None

    #         # check if folder in bronze exists and create if not
    #         cf_path = os.path.join(self.bronze_path, "cashflow_statement")
    #         if not os.path.isdir(cf_path):
    #             os.mkdir(cf_path)

    #         return cf.pivot(
    #             values="value", index=["time_raw", "year", "quarter"], on="item"
    #         ).with_columns(pl.col("quarter").fill_null(4), pl.lit(self.ticker).alias("symbol"))

    def _ratios(self):
        r_y = self._convert_stmt_to_polars(
            pandas_df=self.company.ratios.collect_all_ratios()
        )
        r_q = self._convert_stmt_to_polars(
            pandas_df=self.company_q.ratios.collect_all_ratios()
        )
        _r = self._stack(yearly=r_y, quarterly=r_q)

        if _r is None:
            return None

        # check if folder in bronze exists and create if not
        _r_path = os.path.join(self.bronze_path, "ratios")
        if not os.path.isdir(_r_path):
            os.mkdir(_r_path)

        return _r.pivot(
            values="value", index=["time_raw", "year", "quarter"], on="item"
        ).with_columns(
            pl.col("quarter").fill_null(4), pl.lit(self.ticker).alias("symbol")
        )

    def write(self):
        filename = f"{self.ticker}_{self.ts_path}.parquet"

        # try:
        #     self._balance_sheet().write_parquet(os.path.join(self.bronze_path, "balance_sheet", filename))
        # except Exception:
        #     print(f"No balance_sheet for {self.ticker}.")

        # try:
        #     self._income_stmt().write_parquet(os.path.join(self.bronze_path, "income_statement", filename))
        # except Exception:
        #     print(f"No income_statement for {self.ticker}.")

        # try:
        #     self._cashflow_stmt().write_parquet(os.path.join(self.bronze_path, "cashflow_statement", filename))
        # except Exception:
        #     print(f"No cashflow_statement for {self.ticker}.")

        try:
            self._ratios().write_parquet(
                os.path.join(self.bronze_path, "ratios", filename)
            )
        except Exception:
            print(f"No ratios for {self.ticker}.")


# tickers = pl.scan_parquet(".dev/data/tickers").filter(pl.col("timestamp") == pl.col("timestamp").max()).collect()
# sap = tickers.filter(pl.col("symbol") == "00W.MU")

# test = fundamentals(ticker = sap)
# test.write()
