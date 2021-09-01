import pandas as pd
import numpy as np
import json
import click
from pathlib import Path
from loguru import logger



def load_data(path):
    """
    load_data loads data from file
    """
    with open(path, 'r') as f:
        return json.loads(f.read())


def process(data, dtypes):
    """
    process convert data to tabular data
    """

    monthly_records = []
    now_records = []

    for l in data:
        last_update = l["LASTUPDATE"]
        l_from = l["FROM"]
        l_to = l["TO"]
        for k, v in l["DATAHISTORY"].items():
            for k_idx, k_val in enumerate(v):
                row = {}
                row["updated_at"] = last_update
                row["year"] = int(f"20{k}")
                row["month"] = k_idx + 1
                row["day"] = np.nan
                row["from"] = l_from
                row["to"] = l_to
                row["value"] = k_val
                monthly_records.append(row)
        l_DATANOW = l["DATANOW"]
        l_DATE = pd.to_datetime(l["LASTUPDATE"])
        l_row = {
            "updated_at": last_update,
            "year": l_DATE.year,
            "month": l_DATE.month,
            "day": int(l_DATE.day),
            "from": l_from,
            "to": l_to,
            "value": l_DATANOW
        }
        now_records.append(l_row)


    df_monthly = pd.DataFrame(monthly_records)
    df_now = pd.DataFrame(now_records)

    if dtypes:
        for t in dtypes:
            df_monthly[t] = df_monthly[t].astype(dtypes[t])

    return {
        "monthly": df_monthly,
        "now": df_now
    }


def not_in_new_downloads(df_existing, df_new, identifiers=None):
    """
    not_in_new_downloads remove records that are in current new downloads from existing data
    """
    if identifiers is None:
        identifiers = ["year", "month", "day", "from", "to"]

    logger.debug(f"Output file already has {len(df_existing)} records")
    df_existing = df_existing.loc[
        ~df_existing.apply(
            lambda x: tuple([x[col] for col in identifiers]), axis=1
        ).isin(
            df_new.apply(lambda x: tuple([x[col] for col in identifiers]), axis=1).values
        )
    ]
    logger.debug(f"Output file contains {len(df_existing)} records not in current downloads")

    return df_existing


@click.command()
@click.option('--input_file', '-i', type=click.Path(exists=True), default='data/timocom_barometer.json', help='input file')
@click.option('--output_file', '-o', default='data/timocom_barometer.csv', help='output file')
@click.option('--output_file_now', '-on', default='data/timocom_barometer_now.csv', help='output file for all current data')
def main(input_file, output_file, output_file_now):

    dtypes = {"year": int, "month": int, "day": float, "from": str, "to": str, "value": float}

    data = load_data(input_file)

    processed = process(data, dtypes=dtypes)

    df_monthly = processed["monthly"]

    if Path(output_file).exists():
        df_monthly_existing = pd.read_csv(output_file, dtype=dtypes)
        df_monthly_existing = not_in_new_downloads(df_monthly_existing, df_monthly)
    else:
        df_monthly_existing = pd.DataFrame()

    # Merge new data with existing data

    df_monthly = pd.concat([df_monthly_existing, df_monthly])
    df_monthly.to_csv(output_file, index=False)

    ######
    # Cache current values
    df_now = processed["now"]

    if Path(output_file_now).exists():
        df_now_existing = pd.read_csv(output_file_now, dtype=dtypes)
        df_now_existing = not_in_new_downloads(df_now_existing, df_now)
    else:
        df_now_existing = pd.DataFrame()

    df_now = pd.concat([df_now_existing, df_now])
    df_now.to_csv(output_file_now, index=False)


if __name__ == "__main__":
    main()