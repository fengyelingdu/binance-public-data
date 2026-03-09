#!/usr/bin/env python

"""
一站式下载 Binance 1s K线数据并自动解压为 CSV。

默认下载 BTCUSDT 和 ETHUSDT 的全部 daily 1s kline 数据，
解压后保存到 D:/data/binance/spot/1s/{SYMBOL}/ 目录下。

用法:
  python download-kline-csv.py
  python download-kline-csv.py -s BTCUSDT
  python download-kline-csv.py -s BTCUSDT ETHUSDT -startDate 2024-01-01 -endDate 2024-12-31
  python download-kline-csv.py -s BTCUSDT -o D:/my/custom/path
"""

import os
import sys
import shutil
import time
import zipfile
import urllib.request
from datetime import datetime, date
from argparse import ArgumentParser, RawTextHelpFormatter

import pandas as pd

BASE_URL = "https://data.binance.vision/"
PERIOD_START_DATE = "2020-01-01"
DEFAULT_OUTPUT_DIR = r"D:\data\binance\spot\1s"


def parse_args():
    parser = ArgumentParser(
        description="下载 Binance spot daily 1s K线数据并自动解压为 CSV",
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument(
        "-s", dest="symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
        help="交易对，多个用空格分隔 (默认: BTCUSDT ETHUSDT)",
    )
    parser.add_argument(
        "-startDate", dest="start_date", default=PERIOD_START_DATE,
        help="起始日期 YYYY-MM-DD (默认: 2020-01-01)",
    )
    parser.add_argument(
        "-endDate", dest="end_date", default=datetime.today().strftime("%Y-%m-%d"),
        help="结束日期 YYYY-MM-DD (默认: 今天)",
    )
    parser.add_argument(
        "-o", dest="output_dir", default=DEFAULT_OUTPUT_DIR,
        help=f"输出根目录 (默认: {DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args()


def download_and_extract(url, csv_dest_path, tmp_dir, max_retries=3):
    """下载 zip 并解压 CSV 到目标路径，失败返回 (False, 原因)，成功返回 (True, None)。"""
    tmp_zip = os.path.join(tmp_dir, "tmp.zip")
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            urllib.request.urlretrieve(url, tmp_zip)
            last_error = None
            break
        except urllib.error.HTTPError as e:
            if os.path.exists(tmp_zip):
                os.remove(tmp_zip)
            return False, f"HTTP {e.code} {e.reason}"
        except (urllib.error.ContentTooShortError, urllib.error.URLError, ConnectionError, TimeoutError) as e:
            last_error = str(e)
            if os.path.exists(tmp_zip):
                os.remove(tmp_zip)
            if attempt < max_retries:
                time.sleep(2 * attempt)
                continue
            return False, f"重试{max_retries}次后仍失败: {last_error}"

    try:
        with zipfile.ZipFile(tmp_zip, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                return False, "zip 内无 CSV 文件"
            zf.extract(csv_names[0], tmp_dir)
            extracted = os.path.join(tmp_dir, csv_names[0])
            shutil.move(extracted, csv_dest_path)
    except zipfile.BadZipFile:
        return False, "zip 文件损坏"
    finally:
        if os.path.exists(tmp_zip):
            os.remove(tmp_zip)

    return True, None


def main():
    args = parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    dates = pd.date_range(start=start, end=end).strftime("%Y-%m-%d").tolist()

    for symbol in args.symbols:
        symbol = symbol.upper()
        dest_dir = os.path.join(args.output_dir, symbol)
        os.makedirs(dest_dir, exist_ok=True)

        total = len(dates)
        print(f"\n{'='*60}")
        print(f"  {symbol}  |  {total} 天  |  {args.start_date} ~ {args.end_date}")
        print(f"  保存到: {dest_dir}")
        print(f"{'='*60}")

        downloaded = 0
        skipped = 0
        failed = 0

        tmp_dir = os.path.join(args.output_dir, symbol, ".tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        try:
            for i, d in enumerate(dates, 1):
                file_name = f"{symbol}-1s-{d}.csv"
                csv_path = os.path.join(dest_dir, file_name)

                if os.path.exists(csv_path):
                    skipped += 1
                    sys.stdout.write(f"\r  [{i}/{total}] 已跳过 {skipped} | 下载 {downloaded} | 失败 {failed}")
                    sys.stdout.flush()
                    continue

                url = f"{BASE_URL}data/spot/daily/klines/{symbol}/1s/{symbol}-1s-{d}.zip"
                if download_and_extract(url, csv_path, tmp_dir):
                    downloaded += 1
                else:
                    failed += 1

                sys.stdout.write(f"\r  [{i}/{total}] 已跳过 {skipped} | 下载 {downloaded} | 失败 {failed}")
                sys.stdout.flush()

        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        print(f"\n  完成! 下载 {downloaded}, 跳过 {skipped}, 失败 {failed}")

    print("\n全部完成!")


if __name__ == "__main__":
    main()
