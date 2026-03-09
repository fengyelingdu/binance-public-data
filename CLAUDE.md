# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Binance public market data downloader. Downloads historical SPOT, USD-M Futures, and COIN-M Futures data (klines, trades, aggTrades, index/mark/premium price klines) from https://data.binance.vision/.

## Commands

```bash
# Install dependencies
pip install -r python/requirements.txt   # only pandas

# Download klines (example)
python python/download-kline.py -s BTCUSDT -i 1h -startDate 2024-01-01 -endDate 2024-01-31 -t spot

# Download trades / aggTrades
python python/download-trade.py -s BTCUSDT -startDate 2024-01-01
python python/download-aggTrade.py -s BTCUSDT -startDate 2024-01-01

# Download futures-specific klines (index/mark/premium)
python python/download-futures-indexPriceKlines.py -s BTCUSDT -i 1h -t um
python python/download-futures-markPriceKlines.py -s BTCUSDT -i 1h -t um
python python/download-futures-premiumIndexKlines.py -s BTCUSDT -i 1h -t um

# Common flags across all scripts:
#   -s SYMBOL [SYMBOL ...]   trading pairs
#   -t {spot,um,cm}          market type (default: spot; futures scripts only support um/cm)
#   -i INTERVAL              kline interval (1s,1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1mo)
#   -startDate / -endDate    date range (YYYY-MM-DD)
#   -folder FOLDER           custom output directory
#   -c                       download CHECKSUM files for verification
#   -skip-monthly / -skip-daily   skip monthly or daily data
```

Set `STORE_DIRECTORY` env var to override default download location (`python/data/`).

## Architecture

- **`python/enums.py`** — Constants: year ranges, intervals, trading types, base URL
- **`python/utility.py`** — Shared core: CLI arg parsing (`get_parser`), path construction (`get_path`), symbol fetching (`get_all_symbols`), file download with progress (`download_file`)
- **`python/download-*.py`** — Each script handles one data type; all share the same pattern: parse args → resolve symbols → iterate date ranges → call `download_file`
- **`shell/`** — Bash equivalents for simple/concurrent downloads

All download scripts follow an identical structure: monthly downloads first, then daily downloads, with date-range filtering. The download URL pattern is `{BASE_URL}data/{trading_type}/{frequency}/{data_type}/{symbol}/{interval_if_applicable}/{filename}.zip`.

Binance API endpoints used for symbol discovery:
- SPOT: `/api/v3/exchangeInfo`
- USD-M: `/fapi/v1/exchangeInfo`
- COIN-M: `/dapi/v1/exchangeInfo`

## Key Details

- Only runtime dependency is `pandas` (used for date range generation via `pd.date_range`)
- SPOT data uses microsecond timestamps since 2025-01-01
- No test suite exists in this project
