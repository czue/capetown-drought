import sys
import pandas as pd
import json


def do_import(filename):
    df = _get_data_frame(filename)
    data = _get_dataset(df)
    _write_file(data)


def _get_data_frame(filename):
    return pd.read_csv(filename, encoding = "ISO-8859-1")


def _get_dataset(df):
    dates = _load_dates(df)
    data = _load_data(df)
    return {
        'labels': dates,
        'datasets': data,
    }


def _write_file(data):
    with open('output/data-out.json', 'w') as f:
        f.write(json.dumps(data, indent=2))


def _load_dates(df):
    dates = df.iloc[:, 0][4:]
    dates = pd.to_datetime(dates)
    # dates = dates.apply(lambda x: x.to_
    return [str(d) for d in dates.tolist()]


def _load_data(df):
    return [_parse_chunk(df, 4 * i + 1) for i in range(13)]


def _parse_chunk(df, start_index):
    cols = 4
    data = df.iloc[:, start_index:start_index + cols]
    dam_name = data.iloc[1][0]
    storage_data = data.iloc[:, 1][4:]
    storage_data = storage_data.str.replace('\s+', '')  # strip spaces
    storage_data = pd.to_numeric(storage_data, errors='coerce')
    return {
        'label': dam_name,
        'data': storage_data.tolist()
    }

if __name__ == '__main__':
    filename = 'data/Dam levels update 2012-2017.csv'
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    do_import(filename)
