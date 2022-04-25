import datetime
import logging
import time
from datetime import timedelta
from pprint import pprint

import holidays
from numpy import maximum
import pytz
from secmaster.common.tools import DatabaseConnector, progressbar_print
from secmaster.db.models import Bar, Symbol
from secmaster.tda_client.tda_client import get_tda_client
from sqlalchemy import func, select

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_last_candle(s, symbol, field=None):
    """
    For a given symbol will return data from last candle stored in the SECMASTER database
    if a field parameter is none, returns the Bar object.

    :param s: database session obj
    :param symbol: symbol str
    :param field: The name of the data filed. Must exist on the Bar object
    :return: Bar obj or datafield str, datetime, etc
    """
    # Get maximum date
    try:
        stmt = select(func.max(Bar.date)).where(Bar.symbol_id == symbol)
        last_date = s.execute(stmt).first()[0]
        # Quick exit if you want only the date
        if field == "date":
            return last_date
    except TypeError:
        return None

    # Process if you want something else
    try:
        stmt = select(Bar).where(Bar.symbol_id == symbol and Bar.date == last_date)
        last_bar = s.execute(stmt).first()[0]
        if field is None:
            return last_bar
        else:
            x = last_bar.as_dict()
            return x[field]
    except TypeError:
        return None


def previous_working_day(a_date):
    """
    https://stackoverflow.com/a/62601512/3512107
    :param a_date: datetime date
    :return: The previous US working date for a given date.
    """
    x = a_date.weekday()
    us_holidays = holidays.USA(years=a_date.year)

    us_holidays.pop(datetime.date(2021, 12, 31))
    # The idea is that on Mondays yo have to go back 3 days, on Sundays 2, and 1 in any other day.
    # The statement (lastBusDay.weekday() + 6) % 7 just re-bases the Monday from 0 to 6."
    offset = max(1, (a_date.weekday() + 6) % 7 - 3)
    most_recent = a_date - datetime.timedelta(offset)

    if most_recent not in us_holidays:
        return most_recent
    else:
        return previous_working_day(most_recent)


def sanitize_response(resp):
    """
    Delete candles with Nan values
    None for empty responses
    :param resp:
    :return: corrected resp
    """

    resp=resp.json()

    if resp["empty"] == True:
        return None

    ans = []
    for d in resp["candles"]:
        if (
            d["open"] == "NaN"
            or d["high"] == "NaN"
            or d["low"] == "NaN"
            or d["close"] == "NaN"
            or d["volume"] == "NaN"
        ):
            continue
        else:
            ans.append(d)

    resp["candles"] = ans
    return resp


def get_tda_prices(client, symbol, date_from=None, date_to=None):
    """
    For a given symbol get market prices


    :param session: SECMASTER session
    :param client: tda client
    :param symbol: stock symbol
    :param tda_query_delay: time in seconds as float, you dont want to upset TDA server.
    :return: _description_
    
    """

    # Some logging messages in tda/auth.py best muted

    if date_from is None:
        # Get all bars available
        r = client.get_price_history(
            symbol,
            period_type=client.PriceHistory.PeriodType.YEAR,
            period=client.PriceHistory.Period.TWENTY_YEARS,
            frequency_type=client.PriceHistory.FrequencyType.DAILY,
            frequency=client.PriceHistory.Frequency.DAILY,
            need_extended_hours_data=False,
        )
    else:
        # or get bars within a date range
        r = client.get_price_history(
            symbol,
            period_type=client.PriceHistory.PeriodType.YEAR,
            start_datetime=date_from,
            end_datetime=date_to,
            frequency_type=client.PriceHistory.FrequencyType.DAILY,
            frequency=client.PriceHistory.Frequency.DAILY,
            need_extended_hours_data=False,
        )

    assert r.status_code == 200, r.raise_for_status()
    ans = sanitize_response(r)
    return ans


def update_bars(s, symbol, tda_bars):
    """

    :param session:
    :param symbol:
    :param tda_bars:
    :return:
    """
    # convert epoch from tda response to datetime field:date
    tda_bars = [
        dict(
            item,
            **{
                "date": datetime.datetime.fromtimestamp(
                    item["datetime"] / 1000
                ).astimezone(pytz.UTC)
            },
        )
        for item in tda_bars
    ]

    # delete field datetime for each dictionary in list
    tda_bars = [
        {key: val for key, val in sub.items() if key != "datetime"} for sub in tda_bars
    ]

    # add some field to each dictionary, to match the bar object model
    update_values = {
        "symbol_id": symbol,
        "provider": "TDA",
        "interval": "EOD",
        "last_updated": datetime.datetime.utcnow(),
    }
    tda_bars = [dict(item, **update_values) for item in tda_bars]

    # Convert the list of dictionaries to a list of bar objects
    bars = [Bar(**one_bar) for one_bar in tda_bars]

    # save the data
    s.add_all(bars)
    s.commit()

    return True


def remove_unwanted_symbols(all_symbols, unwanted):
    """
    Remove some elements from a list. 
    In some cases, some symbols have data problems or whatever other reason
    you dont want to update market data

    :param all_symbols: list of strings with symbols
    :param unwanted: list of strings with symbols
    :return: list of strings with symbols
    """

    for each_symbol in unwanted:
        if each_symbol in all_symbols:
            all_symbols.remove(each_symbol)
    return all_symbols


def get_symbols_to_update(s, unwanted):
    """
    Query the SECMASTED for all symbols, then remove some unwanted

    :param s: SECMASTER session
    :param unwanted: _list of strings with symbols
    :return: list of strings with symbols
    """
    # get all symbols in the database
    symbols = s.execute(select(Symbol.id)).all()
    symbols = [x[0] for x in symbols]
    # remove some symbols with problems
    symbols = remove_unwanted_symbols(symbols, unwanted)
    return symbols


def get_dates_for_update(symbol):
    """
    Compute the dates for a bar update
    from last update + 1 until last working day in the U.S.

    :param symbol: symbol str
    :return: date_from, date_to
    """
    
    last_candle_date = get_last_candle(session, each_symbol, "date")

    if last_candle_date is None:
        return None, None
    else:
        date_from = last_candle_date + timedelta(days=1)
        date_to = previous_working_day(datetime.datetime.utcnow()).replace(
        hour=5, minute=0, second=0, microsecond=0)

        
        return date_from, date_to



if __name__ == "__main__":
    start_time = datetime.datetime.now()
    logger.info("Starting to update market data")
    unwanted = ["CEI", "DCTH", "RSLS", "TOPS", "UVXY", "GMGI"]
    session = DatabaseConnector().session()
    tda_client = get_tda_client()

    symbols = get_symbols_to_update(session, unwanted=unwanted)
    # symbols = ["TSLA", "AAPL"]

    logger.info(f"Ready to update {len(symbols)} symbols")
    counter = 0
    for each_symbol in symbols:

        date_from, date_to = get_dates_for_update(each_symbol)
        
        # Only can update if dates are in the past
        if date_from is not None:
            if date_from >= date_to:
                counter += 1
                progressbar_print(counter, len(symbols))
                continue


        response = get_tda_prices(tda_client, each_symbol, date_from, date_to)

        if response is None:
            continue
        else:
            update_bars(session, each_symbol, response["candles"])

        counter += 1
        progressbar_print(counter, len(symbols))

        # Do not upset TDA server
        if counter % 10 == 0:
            time.sleep(1)
        elif counter % 100 == 0:
            time.sleep(3)
        else:
            time.sleep(0.2)



    session.close()
    elapsed_time = datetime.datetime.now() - start_time
    logger.info(f"Done updating data for {len(symbols)} symbols in {elapsed_time}.")

