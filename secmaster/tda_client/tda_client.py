import json
from pathlib import Path
from pprint import pprint

from httpx import main
from secmaster.common.config import Config
from secmaster.common.tools import get_project_root
from tda import auth

API_KEY = Config.TDA_API_KEY
REDIRECT_URI = Config.TDA_CALLBACK_URL
# IMPORTANT: must be in .gitignore
TOKEN_PATH = Path(get_project_root(), "assets", "token.json")


def get_tda_client():
    try:
        c = auth.client_from_token_file(TOKEN_PATH, API_KEY)
    except FileNotFoundError:
        from selenium import webdriver
        geckopath = Path(get_project_root(), "assets", "geckodriver")
        with webdriver.Firefox(executable_path=geckopath) as driver:
            c = auth.client_from_login_flow(driver, API_KEY, REDIRECT_URI, TOKEN_PATH)

    return c


