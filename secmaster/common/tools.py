import ftplib
import logging
from pathlib import Path
import socket

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from secmaster.common.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_project_root():
    """Returns project root folder.

    """
    return Path(__file__).parent.parent.parent


def ftp_server(host, dirname, user, password):
    """

    :return:
    """
    try:
        ftp = ftplib.FTP(host=host, user=user, passwd=password)
        logger.info('connected to: "{}" as "{}"'.format(host, user))
        ftp.cwd(dirname)
        logger.info('change directory to: "{}"'.format(dirname))
        return ftp
    except (socket.error, socket.gaierror) as e:
        logger.error("cannot reach {} - {}".format(host, e))
        raise SystemExit


class DatabaseConnector:
    """

    :returns SQLAlchemy MYSQL engine or session
    """

    def __init__(self):
        db_config = Config()

        self.db_user = db_config.DB_USER
        self.db_password = db_config.DB_PASSWORD
        self.db_host = db_config.DB_HOST
        self.db_port = db_config.DB_PORT
        self.db_name = db_config.DB_NAME

    #  --------------------------------------------
    def engine(self):
        """
        :return:SQLAlchemy engine
        """

        db_url = "mysql+pymysql://{}:{}@{}:{}/{}".format(
            self.db_user, self.db_password, self.db_host, self.db_port, self.db_name
        )
        # MYSQL server must be running
        try:
            # TODO: review how to reuse connections to database
            # engine = create_engine(db_url)
            engine = create_engine(db_url, poolclass=NullPool)
            return engine
        except SQLAlchemyError as e:
            logger.error("Database server not responding: {}".format(e))
            raise SystemExit

    def session(self):
        """
        :return: sqlalchemy session
        """
        try:
            engine = self.engine()
            session_factory = sessionmaker(bind=engine)
            # session = scoped_session(session_factory)
            session = session_factory()
            return session
        except SQLAlchemyError as e:
            logger.error("Can not create database session: {}".format(e))
            raise SystemExit

    def connection(self):
        """
        :return: mysql connection object
        """
        try:
            engine = self.engine()
            return engine.connect()
        except SQLAlchemyError as e:
            logger.error("Can not create database connection: {}".format(e))
            raise SystemExit


def split_list(lst, chunk):
    """
    Break the list into a list of list of size n
    :param lst:
    :param chunk:
    :return:
    """
    ans = [
        lst[i * chunk : (i + 1) * chunk] for i in range((len(lst) + chunk - 1) // chunk)
    ]
    return ans


def progressbar_print(
    iteration,
    total,
    prefix="",
    suffix="",
    decimals=1,
    length=100,
    fill="█",
    print_end=" ",
):
    """
    Thanks to: https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
    Call in a loop to create terminal progress bar

    :param print_end: end character (e.g. "\r", "\r\n") (Str)
    :param iteration: total iterations (Int)
    :param total:
    :param prefix: prefix string (Str)
    :param suffix: suffix string (Str)
    :param decimals: positive number of decimals in percent complete (Int)
    :param length: character length of bar (Int)
    :param fill: bar fill character (Str)
    :return:
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + "-" * (length - filled_length)
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end=print_end)
    # Print New Line on Complete
    if iteration == total:
        print()


if __name__ == "__main__":
    print(get_project_root())
