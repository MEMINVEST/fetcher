from collections.abc import Sequence
from typing import TYPE_CHECKING

import fetcher.fetch_data.helpers as help
import datetime as dt

if TYPE_CHECKING:
    pass

# ensure newest version of financedatabase
help.ensure_latest_pkg(pkg="financedatabase")


def update(
    root: str = ".dev/data", exchanges: Sequence[str] = ("MU", "DE", "HA")
) -> None:
    import financedatabase as fd
    import polars as pl

    relevant_suffixes: Sequence[str] = exchanges
    # Get equity tickers
    equities: pl.DataFrame = (
        pl.from_pandas(fd.Equities().data.reset_index(names="symbol"))
        .with_columns(
            pl.col("symbol").str.extract(r"\.([^.]+)$", 1).alias("exchange_suffix")
        )
        .filter(pl.col("exchange_suffix").is_in(relevant_suffixes))
    )

    # write tickers to parquet
    ts: str = str(dt.datetime.now()).replace(" ", "_").replace(":", "")
    equities.with_columns(pl.lit(dt.datetime.now()).alias("timestamp")).write_parquet(
        f"{root}/tickers/ticker_{ts}.parquet"
    )
