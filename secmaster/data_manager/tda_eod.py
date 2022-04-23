import datetime
import logging
import time
from datetime import timedelta
from pprint import pprint

import holidays
import pytz
from sqlalchemy import func


from db.models import Bar, Symbol
from common.tools import DatabaseConnector

from tda_client.tda_client import get_tda_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def last_bar_datetime(s, symbol):
    """

    :param s:
    :param symbol:
    :return:
    """
    last_bar = s.query(func.max(Bar.date)).where(
        Bar.symbol_id == symbol).first()[0]
    if last_bar is None:
        return None
    else:
        return last_bar.replace(tzinfo=pytz.UTC)


def previous_working_day(check_day_):
    """
    https://stackoverflow.com/a/62601512/3512107
    :param check_day_:
    :return:
    """
    x = check_day_.weekday()
    us_holidays = holidays.USA(years=check_day_.year)

    us_holidays.pop(datetime.date(2021, 12, 31))
    # The idea is that on Mondays yo have to go back 3 days, on Sundays 2, and 1 in any other day.
    # The statement (lastBusDay.weekday() + 6) % 7 just re-bases the Monday from 0 to 6."
    offset = max(1, (check_day_.weekday() + 6) % 7 - 3)
    most_recent = check_day_ - datetime.timedelta(offset)

    if most_recent not in us_holidays:
        return most_recent
    else:
        return previous_working_day(most_recent)


def sanitize_response(resp):
    """
    Delete candles with Nan values
    :param resp:
    :return: corrected resp
    """
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


def get_tda_prices(session, client, symbol):
    """


    :param session: 
    :type session: _type_
    :param client: _description_
    :type client: _type_
    :param symbol: _description_
    :type symbol: _type_
    :return: _description_
    :rtype: _type_
    """

    # Get the right number of candles
    last_bar = last_bar_datetime(session, symbol)
    if last_bar is None:
        r = client.get_price_history(
            symbol,
            period_type=client.PriceHistory.PeriodType.YEAR,
            period=client.PriceHistory.Period.TWENTY_YEARS,
            frequency_type=client.PriceHistory.FrequencyType.DAILY,
            frequency=client.PriceHistory.Frequency.DAILY,
            need_extended_hours_data=False,
        )
        time.sleep(2)
    else:
        # datetime in database are all UTC
        # requesting candles from TDA server with US/Eastern time +05:00
        start_datetime = last_bar + timedelta(days=1)
        today = datetime.datetime.now(
            tz=pytz.timezone("US/Eastern")).astimezone(pytz.UTC)
        today = today.replace(hour=6, minute=0, second=0, microsecond=0)
        last_business_day = previous_working_day(today)

        if start_datetime < last_business_day:
            r = client.get_price_history(
                symbol,
                period_type=client.PriceHistory.PeriodType.YEAR,
                start_datetime=start_datetime,
                end_datetime=last_business_day,
                frequency_type=client.PriceHistory.FrequencyType.DAILY,
                frequency=client.PriceHistory.Frequency.DAILY,
                need_extended_hours_data=False,
            )
            time.sleep(2)
        else:
            logger.info(f"{symbol}: up to date {last_business_day}.")
            return None

    assert r.status_code == 200, r.raise_for_status()
    ans = sanitize_response(r.json())
    return ans


def update_bars(session, symbol, tda_bars):
    """

    :param session:
    :param symbol:
    :param tda_bars:
    :return:
    """
    logger.info(f"{symbol}: updating {len(tda_bars)} bars.")

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

    # add some filed to each dictionary, to match the bar object model
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
    session.add_all(bars)
    session.commit()

    return True


if __name__ == "__main__":
    db_session = DatabaseConnector().session()
    tda_client = get_tda_client()

    symbols = [x[0] for x in db_session.query(Symbol.id).all()]

    symbols.remove("CEI")
    symbols.remove("DCTH")
    symbols.remove("RSLS")
    symbols.remove("TOPS")
    symbols.remove("UVXY")

    symbols = ["AMZN", "NFLX"]

    for each_symbol in symbols:
        response = get_tda_prices(db_session, tda_client, each_symbol)
        if response is None:
            continue
        elif response["empty"]:
            logger.info(f"{each_symbol}: No data in response.")
        else:
            # pprint(response['candles'])
            update_bars(db_session, each_symbol, response["candles"])

    db_session.close()
