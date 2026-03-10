#!/usr/bin/env python

"""
将 1s K线数据合成为 1m 和 5m K线。

用法:
  python python/aggregate-kline.py -f D:/data/binance/spot/1s/BTCUSDT/BTCUSDT-1s-2026-03-06.csv
  python python/aggregate-kline.py -f INPUT.csv -o D:/output

输出默认保存到输入文件同级的 aggregated/ 子目录下。
"""

import os
import sys
import csv
from decimal import Decimal
from argparse import ArgumentParser

COLUMNS = [
    "open_time", "open", "high", "low", "close",
    "volume", "close_time", "quote_volume",
    "trades", "taker_buy_volume", "taker_buy_quote_volume", "ignore"
]

# 微秒
INTERVAL_US = {
    "1m": 60_000_000,
    "5m": 300_000_000,
}


def normalize_to_us(ts):
    """将时间戳统一转为微秒。13位=毫秒，16位=微秒。"""
    if len(ts) <= 13:
        return int(ts) * 1000
    return int(ts)


def read_1s_data(filepath):
    """读取 1s CSV，自动检测时间戳精度（13位毫秒 or 16位微秒）。"""
    rows = []
    with open(filepath, "r", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            rows.append(r)
    # 根据第一行 open_time 判断精度
    if rows:
        ts_len = len(rows[0][0])
        is_ms = ts_len <= 13
    else:
        is_ms = False
    return rows, is_ms


def aggregate(rows_1s, interval_us, is_ms):
    """将 1s 行按 interval_us 分组聚合。"""
    buckets = {}
    for r in rows_1s:
        open_time = normalize_to_us(r[0])
        bucket_start = (open_time // interval_us) * interval_us
        if bucket_start not in buckets:
            buckets[bucket_start] = []
        buckets[bucket_start].append(r)

    result = []
    for bucket_start in sorted(buckets.keys()):
        group = buckets[bucket_start]
        open_price = group[0][1]
        high_price = max(group, key=lambda x: Decimal(x[2]))[2]
        low_price = min(group, key=lambda x: Decimal(x[3]))[3]
        close_price = group[-1][4]
        volume = sum(Decimal(x[5]) for x in group)
        close_time = bucket_start + interval_us - 1
        quote_volume = sum(Decimal(x[7]) for x in group)
        trades = sum(int(x[8]) for x in group)
        taker_buy_vol = sum(Decimal(x[9]) for x in group)
        taker_buy_quote = sum(Decimal(x[10]) for x in group)

        # 输出时间戳保持原始精度
        if is_ms:
            out_open = str(bucket_start // 1000)
            out_close = str(close_time // 1000)
        else:
            out_open = str(bucket_start)
            out_close = str(close_time)

        result.append([
            out_open,
            open_price,
            high_price,
            low_price,
            close_price,
            f"{volume:.8f}",
            out_close,
            f"{quote_volume:.8f}",
            str(trades),
            f"{taker_buy_vol:.8f}",
            f"{taker_buy_quote:.8f}",
            "0",
        ])
    return result


def write_csv(rows, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"  已写入: {filepath}  ({len(rows)} 行)")


def main():
    parser = ArgumentParser(description="将 1s K线合成为 1m/5m K线")
    parser.add_argument("-f", dest="input_file", required=True, help="1s CSV 文件路径")
    parser.add_argument("-o", dest="output_dir", default=None,
                        help="输出目录 (默认: 输入文件同级目录)")
    args = parser.parse_args()

    input_path = args.input_file
    if not os.path.exists(input_path):
        print(f"文件不存在: {input_path}")
        sys.exit(1)

    # 从文件名解析信息: BTCUSDT-1s-2026-03-06.csv
    basename = os.path.basename(input_path)  # BTCUSDT-1s-2026-03-06.csv
    parts = basename.replace(".csv", "").split("-", 2)  # ['BTCUSDT', '1s', '2026-03-06']
    symbol = parts[0]
    date_str = parts[2]

    output_dir = args.output_dir or os.path.join(os.path.dirname(input_path), "aggregated")

    print(f"读取 1s 数据: {input_path}")
    rows_1s, is_ms = read_1s_data(input_path)
    print(f"  共 {len(rows_1s)} 行, 时间戳精度: {'毫秒(13位)' if is_ms else '微秒(16位)'}")

    for interval_name, interval_us in INTERVAL_US.items():
        print(f"\n合成 {interval_name} K线...")
        agg = aggregate(rows_1s, interval_us, is_ms)
        out_file = os.path.join(output_dir, f"{symbol}-{interval_name}-{date_str}.csv")
        write_csv(agg, out_file)


if __name__ == "__main__":
    main()
