from datetime import datetime
import sys
import pandas as pd
import numpy as np
import json


def do_import(filename):
    df = _get_data_frame(filename)
    df = _clean_data(df)
    data = _get_dataset(df)
    metadata = {
        'updated': datetime.now().date().isoformat(),
    }
    formatted = {
        'metadata': metadata,
        'chart_data': data,
    }
    _write_file(formatted)


def _get_data_frame(filename):
    return pd.read_csv(filename, encoding="UTF-8")


def _clean_data(df):
    return df.replace(np.nan, '', regex=True)


def _get_dataset(df):
    dates = _load_dates(df)
    data = _load_data(df)
    return {
        'labels': dates,
        'datasets': data,
    }


def _write_file(data):
    with open('front-end/data.json', 'w') as f:
        f.write(json.dumps(data, indent=2))

def _load_dates(df):
    dates = df.iloc[:, 0][4:]
    dates = pd.to_datetime(dates, dayfirst=True)
    # dates = dates.apply(lambda x: x.to_
    last_date = None
    for i, date in enumerate(dates):
        if last_date:
            assert last_date <= date, 'date {} found out of order! {} < {}'.format(i, date, last_date)
        last_date = date

    return [str(d.date()) for d in dates.tolist()]


def _load_data(df):
    return [_parse_chunk(df, 4 * i + 1) for i in range(13)]


def _parse_chunk(df, start_index):
    cols = 4
    data = df.iloc[:, start_index:start_index + cols]
    dam_name = data.iloc[1][0]
    storage_data = data.iloc[:, 1][4:]
    storage_data = storage_data.str.replace('\s+', '')  # strip spaces
    storage_data = pd.to_numeric(storage_data, errors='coerce')
    storage_data = storage_data.replace(np.nan, None, regex=True)  # replace NaN with previous value
    return {
        'label': dam_name,
        'data': storage_data.tolist()
    }

if __name__ == '__main__':
    filename = 'data/Dam levels update 2012-2017.csv'
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    do_import(filename)
