#!/usr/bin/env python

"""
将 1s K线数据按偏置窗口批量聚合为 1m 和 5m K线。

偏置含义: 数据窗口提前 N 秒开始、提前 N 秒结束，但 open_time 保持原始对齐。
例如 offset=2 时，标记为 00:01:00 的 1m K线，实际用 [00:00:58, 00:01:57] 的 1s 数据聚合。

用法:
  python python/aggregate-kline-offset.py                                          # 默认: BTCUSDT ETHUSDT, offset=2
  python python/aggregate-kline-offset.py -s BTCUSDT -offset 2 -startDate 2026-03-06 -endDate 2026-03-06
  python python/aggregate-kline-offset.py -s BTCUSDT ETHUSDT -offset 5 -startDate 2024-01-01
"""

import os
import sys
import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
from argparse import ArgumentParser, RawTextHelpFormatter

import pandas as pd

DEFAULT_DATA_DIR = r"D:\data\binance\spot\1s"

INTERVAL_US = {
    "1m": 60_000_000,
    "5m": 300_000_000,
}

ONE_DAY_US = 86400_000_000


def normalize_to_us(ts):
    """将时间戳统一转为微秒。13位=毫秒，16位=微秒。"""
    if len(ts) <= 13:
        return int(ts) * 1000
    return int(ts)


def detect_is_ms(rows):
    """根据第一行判断时间戳精度。"""
    if rows:
        return len(rows[0][0]) <= 13
    return False


def read_csv_rows(filepath):
    """读取 CSV 返回行列表，文件不存在返回空列表。"""
    if not os.path.exists(filepath):
        return []
    rows = []
    with open(filepath, "r", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            rows.append(r)
    return rows


def get_csv_path(data_dir, symbol, d):
    """构造 1s CSV 文件路径。"""
    return os.path.join(data_dir, symbol, f"{symbol}-1s-{d}.csv")


def aggregate_offset(rows_1s, interval_us, offset_us, day_start_us, is_ms):
    """
    按偏置窗口分组聚合。

    偏置分组逻辑:
      shifted = open_time + offset_us
      bucket_label = floor(shifted / interval_us) * interval_us
      数据窗口 = [bucket_label - offset_us, bucket_label - offset_us + interval_us - 1]
      输出 open_time = bucket_label (整分钟对齐)

    只保留 bucket_label 落在 [day_start_us, day_start_us + ONE_DAY_US) 的 K 线。
    """
    buckets = {}
    for r in rows_1s:
        open_time = normalize_to_us(r[0])
        shifted = open_time + offset_us
        bucket_label = (shifted // interval_us) * interval_us
        if bucket_label not in buckets:
            buckets[bucket_label] = []
        buckets[bucket_label].append(r)

    day_end_us = day_start_us + ONE_DAY_US

    result = []
    for bucket_label in sorted(buckets.keys()):
        # 只保留当天的 K 线
        if bucket_label < day_start_us or bucket_label >= day_end_us:
            continue

        group = buckets[bucket_label]
        open_price = group[0][1]
        high_price = max(group, key=lambda x: Decimal(x[2]))[2]
        low_price = min(group, key=lambda x: Decimal(x[3]))[3]
        close_price = group[-1][4]
        volume = sum(Decimal(x[5]) for x in group)
        close_time = bucket_label + interval_us - 1
        quote_volume = sum(Decimal(x[7]) for x in group)
        trades = sum(int(x[8]) for x in group)
        taker_buy_vol = sum(Decimal(x[9]) for x in group)
        taker_buy_quote = sum(Decimal(x[10]) for x in group)

        if is_ms:
            out_open = str(bucket_label // 1000)
            out_close = str(close_time // 1000)
        else:
            out_open = str(bucket_label)
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


def date_to_day_start_us(d):
    """将 date 对象转为当天 00:00:00 的微秒时间戳。"""
    dt = datetime(d.year, d.month, d.day)
    return int(dt.timestamp()) * 1_000_000


def parse_args():
    parser = ArgumentParser(
        description="将 1s K线按偏置窗口批量聚合为 1m/5m K线",
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument(
        "-s", dest="symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
        help="交易对 (默认: BTCUSDT ETHUSDT)",
    )
    parser.add_argument(
        "-d", dest="data_dir", default=DEFAULT_DATA_DIR,
        help=f"1s 数据根目录 (默认: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "-offset", dest="offset", type=int, default=2,
        help="偏置秒数 (默认: 2)",
    )
    parser.add_argument(
        "-startDate", dest="start_date", default="2020-01-01",
        help="起始日期 YYYY-MM-DD (默认: 2020-01-01)",
    )
    parser.add_argument(
        "-endDate", dest="end_date", default=datetime.today().strftime("%Y-%m-%d"),
        help="结束日期 YYYY-MM-DD (默认: 今天)",
    )
    parser.add_argument(
        "-o", dest="output_dir", default=None,
        help="输出目录 (默认: {data_dir}/{SYMBOL}/aggregated)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    offset_us = args.offset * 1_000_000

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    dates = pd.date_range(start=start, end=end).date.tolist()

    for symbol in args.symbols:
        symbol = symbol.upper()
        output_dir = args.output_dir or os.path.join(args.data_dir, symbol, "aggregated", f"offset{args.offset}s")
        os.makedirs(output_dir, exist_ok=True)

        total = len(dates)
        print(f"\n{'='*60}")
        print(f"  {symbol}  |  offset={args.offset}s  |  {total} 天")
        print(f"  数据目录: {os.path.join(args.data_dir, symbol)}")
        print(f"  输出目录: {output_dir}")
        print(f"{'='*60}")

        processed = 0
        skipped = 0

        prev_day_rows = None
        prev_day_date = None

        for i, d in enumerate(dates, 1):
            d_str = d.strftime("%Y-%m-%d") if hasattr(d, 'strftime') else str(d)
            today_path = get_csv_path(args.data_dir, symbol, d_str)

            # 读取当天数据
            today_rows = read_csv_rows(today_path)
            if not today_rows:
                skipped += 1
                prev_day_rows = None
                prev_day_date = None
                sys.stdout.write(f"\r  [{i}/{total}] 已处理 {processed} | 跳过 {skipped}")
                sys.stdout.flush()
                continue

            is_ms = detect_is_ms(today_rows)
            day_start_us = date_to_day_start_us(d)

            # 读取前一天最后 offset 秒的数据（用于跨天边界）
            prev_date = d - timedelta(days=1)
            prev_str = prev_date.strftime("%Y-%m-%d") if hasattr(prev_date, 'strftime') else str(prev_date)

            if prev_day_date == prev_str and prev_day_rows is not None:
                tail_rows = prev_day_rows
            else:
                prev_path = get_csv_path(args.data_dir, symbol, prev_str)
                tail_rows = read_csv_rows(prev_path)

            # 只取前一天最后 offset 秒的数据
            if tail_rows and args.offset > 0:
                prev_day_end_us = day_start_us  # 前一天结束 = 当天开始
                cutoff_us = prev_day_end_us - offset_us
                tail_rows = [r for r in tail_rows if normalize_to_us(r[0]) >= cutoff_us]
            else:
                tail_rows = []

            # 合并: 前一天尾部 + 当天全部
            combined = tail_rows + today_rows

            # 对每个时间周期聚合
            for interval_name, interval_us in INTERVAL_US.items():
                agg = aggregate_offset(combined, interval_us, offset_us, day_start_us, is_ms)
                out_file = os.path.join(
                    output_dir,
                    f"{symbol}-{interval_name}-offset{args.offset}s-{d_str}.csv"
                )
                write_csv(agg, out_file)

            # 缓存当天数据供下一天使用
            prev_day_rows = today_rows
            prev_day_date = d_str

            processed += 1
            sys.stdout.write(f"\r  [{i}/{total}] 已处理 {processed} | 跳过 {skipped}")
            sys.stdout.flush()

        print(f"\n  完成! 处理 {processed} 天, 跳过 {skipped} 天")

    print("\n全部完成!")


if __name__ == "__main__":
    main()
