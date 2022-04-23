import json
from pathlib import Path
from pprint import pprint

from tda import auth

from common.config import Config
from common.tools import get_project_root

API_KEY = Config.TDA_API_KEY
REDIRECT_URI = Config.TDA_CALLBACK_URL
TOKEN_PATH = Path(get_project_root(), "secmaster", "tda_client", "token.json")


def get_tda_client():
    try:
        c = auth.client_from_token_file(TOKEN_PATH, API_KEY)
    except FileNotFoundError:
        from selenium import webdriver

        with webdriver.Firefox() as driver:
            c = auth.client_from_login_flow(driver, API_KEY, REDIRECT_URI, TOKEN_PATH)

    return c

