To test the library simply install it: 


```bash
python -m pip git+https://github.com/MEMINVEST/fetcher/
```

Then run set root wherever you want to have the data and run

```python
  # set root to define where the data lives
  root = "data"

  # set up file system 
  import fetcher.setup.structure as setup
  setup.create_data(root)

  # get most recent list of tickers for exchanges you specify
  from fetcher.fetch_data.update_tickers import update
  update(root)

  # download fundamentals
  from fetcher.pipeline.run_fundamentals import run_fundamentals
  run_fundamentals(root).run()

  # download daily data
  from fetcher.pipeline.run_daily import run_daily
  run_daily(root).run()
```
