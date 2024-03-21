import pandas as pd
import yfinance as yf

from datetime import timedelta, datetime
from typing import Iterator, Dict
import pytz
import warnings

warnings.filterwarnings("ignore")


def _timeformatter(original_timestamp: str) -> str:
    # Convert string to datetime object
    dt = datetime.strptime(original_timestamp, "%Y-%m-%d %H:%M:%S%z")

    # Convert to UTC time zone
    utc_dt = dt.astimezone(pytz.utc)

    # Format as ISO 8601 string
    iso_8601_utc = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return iso_8601_utc


def data_generator() -> Iterator[Dict]:
    data_1min_msft = yf.download(
        "MSFT", start=pd.to_datetime("today") - timedelta(5), interval="1m"
    )
    
    data_1min_msft = data_1min_msft.reset_index()
    data_1min_msft.loc[:, "Quotes"] = "MSFT"
    data_1min_msft["Datetime"] = (
        data_1min_msft["Datetime"].astype(str).apply(_timeformatter)
    )
    data_1min_msft.drop("Adj Close", axis=1, inplace=True)

    num_of_data = data_1min_msft.shape[0]
    for row in range(0, num_of_data, 1):
        yield data_1min_msft.iloc[row].to_dict()


if __name__ == "__main__":
    data_gen = data_generator()
    for data in data_gen:
        print(data)
        break
