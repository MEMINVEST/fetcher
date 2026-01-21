import update_tickers.helpers as help
import datetime as dt

# ensure newest version of financedatabase
help.ensure_latest_pkg(pkg="financedatabase")

import financedatabase as fd
import polars as pl

# define relevant exchanges
relevant_suffixes = ["MU", "DE", "HA"]

# Get equity tickers
equities = (
    pl.from_pandas(fd.Equities().data.reset_index(names="symbol"))
    .with_columns(
        pl.col("symbol").str.extract(r"\.([^.]+)$", 1).alias("exchange_suffix")
    )
    .filter(pl.col("exchange_suff  # expressionix").is_in(relevant_suffixes))
)

# write tickers to parquet
ts = str(dt.datetime.now()).replace(" ", "_").replace(":", "")
equities.select("symbol", "name").with_columns(
    pl.lit(dt.datetime.now()).alias("timestamp")
).write_parquet(f".dev/data/tickers/ticker_{ts}.parquet")
