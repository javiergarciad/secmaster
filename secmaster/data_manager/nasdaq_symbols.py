import logging
import time
from pathlib import Path
import pandas as pd
import yfinance as yf

from secmaster.common.tools import ftp_server, progressbar_print, get_project_root
from secmaster.common.config import Config

from secmaster.common.tools import DatabaseConnector
from secmaster.db.models import Symbol, Provider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The files to import
FILES = [
    {
        "filename": "nasdaqlisted.txt",
        "cols": [0, 1, 3],
        "provider": "NASDAQ",
        "exclude": ["$", "."],
    },
    {
        "filename": "otherlisted.txt",
        "cols": [0, 1, 6],
        "provider": "NASDAQ",
        "exclude": ["$", "."],
    },
]


def download_nasdaq_files(
    ftp_host, ftp_dir, ftp_user, ftp_pass, filenames_list, destination_dir
):
    logging.info("Starting to download {} NASDAQ files".format(
        len(filenames_list)))

    ftp = ftp_server(ftp_host, ftp_dir, ftp_user, ftp_pass)

    for each_file in filenames_list:
        destination_filepath = Path(destination_dir, each_file)
        logger.info("Downloading file: {}".format(each_file))

        with open(destination_filepath, "wb") as my_file:

            def callback(data):
                my_file.write(data)

            ftp.retrbinary("RETR " + each_file, callback)

            logger.info('Done with file: "{}"'.format(each_file))

    ftp.quit()
    logger.info(
        'Done downloading {} files from "{}/{}" as "{}"'.format(
            len(filenames_list), ftp_host, ftp_dir, ftp_user
        )
    )
    return True


def exclude_symbol(string, exclude_list):
    """

    :string:
    :exclude_list:

    """
    for i in string:
        if i in exclude_list:
            return True

    return False


def validate_provider(s, filename, provider_id):
    """

    :param s:
    :param filename:
    :param provider_id:
    :return:
    """
    ans = s.query(Provider.id).where(Provider.id == provider_id).first()[0]

    if ans is None:
        logger.error(
            'Provider for file "{}" not created in database. Exiting process.'.format(
                filename
            )
        )
        raise SystemExit
    else:
        return ans


def get_symbol_info(symbol):
    """

    """

    ticket = yf.Ticker(symbol)
    i = ticket.info

    try:
        _validate = i["shortName"]
        _validate = i["symbol"]
        return i
    except KeyError:
        return None


def update_database_symbols(s, filepath, cols, exclude_characters, provider_id):
    """

    :param cols:
    :param s:
    :param filepath:
    :param exclude_characters:
    :param provider_id:
    :return:
    """
    filename = Path(filepath).parts[-1]
    logger.info('Starting database update for file "{}".'.format(filename))

    # get symbols already in the database
    symbols_in_db = [x[0] for x in s.query(Symbol.id).all()]

    # some counters
    exclude = 0
    new_symbols = 0
    already_in_db = 0
    total_counter = 0
    # a dictionary with results for each file processed
    ans = {"filename": filename}

    data = pd.read_csv(filepath_or_buffer=filepath, sep="|", usecols=cols)

    # Iterating on a DF not the most efficient, but it's not a lot of marketdata.
    symbols_to_add = []

    for row in data.itertuples(index=False):
        # print some nice progress bar
        total_counter += 1
        progressbar_print(
            total_counter, len(data), prefix="Progress:", suffix="Complete", length=50
        )

        if row[2] == "N":
            name = row[1][0:99]
            symbol = row[0]

            if symbol not in symbols_in_db:
                if exclude_symbol(symbol, exclude_characters):
                    exclude += 1
                else:
                    # create new symbol
                    symbols_to_add.append(
                        Symbol(
                            id=symbol,
                            name=name,
                            provider=provider_id,
                        )
                    )
                    new_symbols += 1
            else:
                already_in_db += 1

            # Save to database every n
            if total_counter % 1000 == 0:
                s.add_all(symbols_to_add)
                s.commit()
                symbols_to_add = []

    s.add_all(symbols_to_add)
    s.commit()
    ans["new_symbols"] = new_symbols
    ans["already_in_db"] = already_in_db
    ans["exclude"] = exclude

    logger.info(f"Database updated for {filename}.")
    logger.info(
        f"New symbols:{new_symbols}, Exclude:{exclude}, Already in Database:{already_in_db}"
    )

    return ans


if __name__ == "__main__":
    """
    Get a list of all stock symbols from NASDAQ and update the database
    https://quant.stackexchange.com/questions/1640/where-to-download-list-of-all-common-stocks-traded-on-nyse-nasdaq-and-amex
    https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs
    """
    DOWNLOAD = True

    # database session
    db_session = DatabaseConnector().session()

    # where downloaded files are to be stored
    destination = Path(get_project_root(), "assets", "nasdaq")
    destination.mkdir(parents=True, exist_ok=True)

    # download the files
    if DOWNLOAD:
        downloaded = download_nasdaq_files(
            ftp_host=Config.NASDAQ_FTP_SERVER,
            ftp_dir=Config.NASDAQ_FTP_DIR,
            ftp_user=Config.NASDAQ_FTP_USER,
            ftp_pass=Config.NASDAQ_FTP_PASS,
            filenames_list=[x["filename"] for x in FILES],
            destination_dir=destination,
        )
    else:
        downloaded = True

    # and update the db with the nasdaq files info
    if downloaded:
        for f in FILES:
            result = update_database_symbols(
                s=db_session,
                filepath=Path(destination, f["filename"]),
                cols=f["cols"],
                exclude_characters=f["exclude"],
                provider_id=validate_provider(
                    s=db_session, filename=f["filename"], provider_id=f["provider"]
                ),
            )
    logger.info("New symbols created, but have not industry data")
    db_session.close()
