#!/usr/bin/env python

"""
比对合成的 K线数据与 Binance 原始下载数据是否一致。

用法:
  python python/compare-kline.py -a AGGREGATED.csv -b ORIGINAL.csv  # 通用格式
  python python/compare-kline.py -a D:/data/binance/spot/1s/BTCUSDT/aggregated/BTCUSDT-1m-2026-03-06.csv -b D:/data/binance/spot/1m/BTCUSDT/BTCUSDT-1m-2026-03-06.csv  # 比对1m
  python python/compare-kline.py -a D:/data/binance/spot/1s/BTCUSDT/aggregated/BTCUSDT-5m-2026-03-06.csv -b D:/data/binance/spot/5m/BTCUSDT/BTCUSDT-5m-2026-03-06.csv  # 比对5m
"""

import sys
import csv
from decimal import Decimal
from argparse import ArgumentParser

COLUMN_NAMES = [
    "open_time", "open", "high", "low", "close",
    "volume", "close_time", "quote_volume",
    "trades", "taker_buy_volume", "taker_buy_quote_volume", "ignore"
]

# 需要精确比较的数值列 (索引)
PRICE_COLS = [1, 2, 3, 4]      # open, high, low, close
SUM_COLS = [5, 7, 9, 10]       # volume, quote_volume, taker_buy_volume, taker_buy_quote_volume
INT_COLS = [0, 6, 8]           # open_time, close_time, trades


def read_csv(filepath):
    rows = []
    with open(filepath, "r", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            rows.append(r)
    return rows


def compare(file_a, file_b):
    rows_a = read_csv(file_a)
    rows_b = read_csv(file_b)

    print(f"  文件 A (合成): {file_a}  ({len(rows_a)} 行)")
    print(f"  文件 B (原始): {file_b}  ({len(rows_b)} 行)")

    if len(rows_a) != len(rows_b):
        print(f"\n  [不一致] 行数不同: A={len(rows_a)}, B={len(rows_b)}")

    min_len = min(len(rows_a), len(rows_b))
    diff_count = 0

    for i in range(min_len):
        ra, rb = rows_a[i], rows_b[i]
        row_diffs = []

        for col in INT_COLS:
            va, vb = int(ra[col]), int(rb[col])
            if va != vb:
                row_diffs.append((COLUMN_NAMES[col], ra[col], rb[col]))

        for col in PRICE_COLS + SUM_COLS:
            va, vb = Decimal(ra[col]), Decimal(rb[col])
            if va != vb:
                row_diffs.append((COLUMN_NAMES[col], ra[col], rb[col]))

        if row_diffs:
            diff_count += 1
            if diff_count <= 20:
                open_time = ra[0]
                print(f"\n  第 {i+1} 行 (open_time={open_time}) 有 {len(row_diffs)} 处差异:")
                for col_name, va, vb in row_diffs:
                    print(f"    {col_name:30s}  合成={va:>25s}  原始={vb:>25s}")

    if diff_count == 0:
        print(f"\n  [完全一致] 所有 {min_len} 行数据完全相同!")
    else:
        if diff_count > 20:
            print(f"\n  ... 仅显示前 20 处差异")
        print(f"\n  [不一致] 共 {diff_count}/{min_len} 行存在差异")

    return diff_count == 0 and len(rows_a) == len(rows_b)


def main():
    parser = ArgumentParser(description="比对合成 K线与原始 K线数据")
    parser.add_argument("-a", dest="file_a", required=True, help="合成的 CSV 文件")
    parser.add_argument("-b", dest="file_b", required=True, help="原始下载的 CSV 文件")
    args = parser.parse_args()

    print(f"\n比对开始:")
    match = compare(args.file_a, args.file_b)
    sys.exit(0 if match else 1)


if __name__ == "__main__":
    main()
