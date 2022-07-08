import logging
from datetime import datetime
from glob import glob
from io import StringIO
from pathlib import Path
import argparse

import pandas as pd
import numpy as np
import pytz
import requests
from line_notify import LineNotify


def main(line_access_token, data_folder="./data"):
    create_data_folder(data_folder=data_folder)
    log_date = datetime.now(tz=pytz.timezone("Asia/Taipei")).strftime("%Y%m%d")

    logging.basicConfig(
        level=logging.DEBUG,
        filename=f"{data_folder}/logs/log_{log_date}.txt",
        format="%(asctime)s [%(levelname)s]: %(message)s",
        encoding="utf-8",
    )
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.debug("Started.")

    for market in ["twse", "tpex"]:

        fetched_time = datetime.now(tz=pytz.timezone("Asia/Taipei")).strftime(
            "%Y%m%d%H%M%S"
        )
        new_csv = f"{data_folder}/{market}/{market}_listing_{fetched_time}.csv"

        listing_df = get_listing_df(market)
        old_listing_csv = get_listing_csv(market, data_folder=data_folder)

        save_csv = False

        if old_listing_csv is None:
            save_csv = True

        elif old_listing_csv is not None:
            new_df, updated_df = compare_listing_df(listing_df, old_listing_csv)

            if (new_df is not None) or (updated_df is not None):
                save_csv = True

                report = f"{market.upper()}\n"
                report += create_report(new_df, updated_df)
                send_line_notify(line_access_token, report)
                logging.debug(report)

        if save_csv:
            listing_df.to_csv(new_csv)

    logging.debug("Ended.")

    return


def create_data_folder(data_folder="./data"):
    sub_folders = ["twse", "tpex", "logs"]
    data_folder_exists = Path(data_folder).exists()

    if not data_folder_exists:
        Path(data_folder).mkdir()

        for sub_folder in sub_folders:
            sub_folder = f"{data_folder}/{sub_folder}"
            Path(sub_folder).mkdir()

    return


def get_listing_df(market):
    urls = {
        "twse": "https://www.twse.com.tw/company/applylistingCsvAndHtml?type=open_data",
        "tpex": "https://www.tpex.org.tw/web/regular_emerging/apply_schedule/applicant"
        "/applicant_companies_download_UTF-8.php?l=zh-tw&y=ALL",
    }

    url = urls[market.lower()]

    listing_data = requests.get(url).content.decode("utf-8")
    listing_df = pd.read_csv(StringIO(listing_data), dtype="str").pipe(
        clean_listing_df, market=market
    )

    return listing_df


def clean_listing_df(listing_df, market):

    markets = {
        "twse": {
            "drop_col": "索引",
            "company_code": "公司代號",
        },
        "tpex": {
            "drop_col": None,
            "company_code": "股票代號",
        },
    }

    market = market.lower()
    drop_col = markets[market]["drop_col"]
    company_code = markets[market]["company_code"]

    if drop_col is not None:
        listing_df = listing_df.drop(columns=drop_col)

    listing_df["uid"] = listing_df[company_code] + "-" + listing_df["申請日期"]
    listing_df = listing_df.set_index("uid")

    return listing_df


def get_listing_csv(market, data_folder="./data"):
    sub_folder = f"{data_folder}/{market}"
    listing_csvs = glob(f"{sub_folder}/*.csv")

    listing_csv = None
    if len(listing_csvs) > 0:
        datetime_list = []
        for csv in listing_csvs:
            csv_datatime = Path(csv).stem.split("_")[-1]
            csv_datatime = int(csv_datatime)
            datetime_list.append(csv_datatime)
        latest_datetime = max(datetime_list)

        listing_csv = f"{data_folder}/{market}/{market}_listing_{latest_datetime}.csv"

    return listing_csv


def compare_listing_df(listing_df, old_listing_csv):

    old_df = pd.read_csv(old_listing_csv, index_col="uid", dtype="str")
    is_same = listing_df.equals(old_df)

    new_df, updated_df = None, None
    if not is_same:
        same_uid = listing_df.index.isin(old_df.index)
        new_df = listing_df[~same_uid]
        if new_df.empty:
            new_df = None

        updated_df = listing_df[same_uid]
        if updated_df.empty:
            updated_df = None

        else:
            updated_df = (
                listing_df[same_uid]
                .compare(old_df, align_axis=0)
                .melt(ignore_index=False)
                .reset_index()
                .query("level_1 == 'self'")
                .drop(columns="level_1")
            )

            updated_df["value"] += " (更新)"
            updated_array = updated_df.to_numpy()

            updated_uid = listing_df.index.isin(updated_df["uid"])
            updated_df = listing_df[updated_uid]

            for item in updated_array:
                index = item[0]
                col = item[1]
                value = item[2]
                updated_df.loc[index, col] = value

    return new_df, updated_df


def create_report(new_df, updated_df):

    df_dict = {"新申請": new_df, "最近更新": updated_df}

    report = ""
    for df_type, df in df_dict.items():
        if df is None:
            continue
        else:
            report += f"{df_type}\n"
            report += "\n"

        for record in df.to_dict(orient="records"):
            for key, value in record.items():
                if value is np.nan:
                    continue
                else:
                    report += f"{key}: {value}\n"
            report += "\n"

    return report


def send_line_notify(line_access_token, message):
    notify = LineNotify(line_access_token)
    notify.send(message)

    return


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("line_access_token")
    args = parser.parse_args()

    main(args.line_access_token)
