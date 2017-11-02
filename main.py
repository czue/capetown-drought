from datetime import datetime, timedelta
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
    _write_file(formatted, 'all-data.json')
    data['datasets'] = _filter_minor_dams(data['datasets'])
    _write_file(formatted, 'main-data.json')


def _get_data_frame(filename):
    return pd.read_csv(filename, encoding="ISO-8859-1")


def _clean_data(df):
    return df.replace(np.nan, '', regex=True)


def _get_dataset(df):
    dates = _load_dates(df)
    data = _load_data(df)
    return {
        'labels': dates,
        'datasets': data,
    }


def _write_file(data, filename):
    with open('front-end/{}'.format(filename), 'w') as f:
        f.write(json.dumps(data, indent=2))


def _filter_minor_dams(dam_data):
    MINOR_DAMS = set(dam.lower() for dam in [
        "Hely-Hutchinson",
        "Woodhead",
        "Victoria",
        "Alexandra",
        "De Villiers",
        "KleinPlaats",
        "Lewis Gay",
    ])
    return list(filter(lambda data: data['label'].lower() not in MINOR_DAMS, dam_data))


def _load_dates(df):
    KNOWN_BROKEN_DATES = {
        datetime(2017, 8, 8): datetime(2017, 5, 8),
        datetime(2018, 5, 19): datetime(2017, 5, 19),
        datetime(2019, 5, 20): datetime(2017, 5, 20),
        datetime(2020, 5, 21): datetime(2017, 5, 21),
        datetime(2021, 5, 22): datetime(2017, 5, 22),
    }
    dates = df.iloc[:, 0][4:]
    dates = pd.to_datetime(dates, dayfirst=True)
    # dates = dates.apply(lambda x: x.to_
    last_date = None
    for i, date in enumerate(dates):
        if last_date and date - last_date != timedelta(days=1):
            if date in KNOWN_BROKEN_DATES:
                date = KNOWN_BROKEN_DATES[date]
            else:
                assert False, 'date {} found anomalous data! {} is not the day after {}'.format(i, date, last_date)

        last_date = date

    return [str(d.date()) for d in dates.tolist()]


def _load_data(df):
    return [_parse_chunk(df, 4 * i + 1) for i in range(13)]


def _parse_chunk(df, start_index):
    cols = 4
    data = df.iloc[:, start_index:start_index + cols]
    dam_name = data.iloc[1][0]
    dam_name = dam_name.replace('\ufffd', 'Ë')  # hack: fix Voëlvlei
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
