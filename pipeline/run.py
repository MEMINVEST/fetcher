from fetch_data import fundamentals
import polars as pl
from tqdm import tqdm

tickers = (
    pl.scan_parquet(".dev/data/tickers")
    .filter(pl.col("timestamp") == pl.col("timestamp").max())
    .collect()
)

tickers_str = tickers.get_column("symbol").to_list()

for tick in tqdm(tickers_str[0:20], total=len(tickers_str[0:20]), desc="Fundamentals"):
    f = fundamentals.fundamentals(ticker=tick)
    f.write()
