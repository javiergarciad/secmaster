import os
from pathlib import Path, PurePath

from dotenv import load_dotenv

basedir = Path(__file__).parent.parent.parent
env_path = Path(basedir, ".env")
load_dotenv(env_path)


class Config(object):
    
    # SECMASTER
    DB_NAME = os.environ.get("DB_NAME")
    DB_HOST = os.environ.get("DB_HOST")
    DB_PORT = os.environ.get("DB_PORT")
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")

    # NASDAQ FTP
    NASDAQ_FTP_SERVER = os.environ.get("NASDAQ_FTP_SERVER")
    NASDAQ_FTP_USER = os.environ.get("NASDAQ_FTP_USER")
    NASDAQ_FTP_PASS = os.environ.get("NASDAQ_FTP_PASS")
    NASDAQ_FTP_DIR = os.environ.get("NASDAQ_FTP_DIR")

    # TDA
    TDA_API_KEY = os.environ.get("TDA_API_KEY")
    TDA_CALLBACK_URL = os.environ.get("TDA_CALLBACK_URL")


if __name__ == "__main__":

    db_config = Config()
    print(db_config.DB_NAME)
    print(db_config.DB_HOST)
    print(db_config.DB_PASSWORD)
    print(db_config.DB_USER)
