import json
from pathlib import Path
from pprint import pprint

from httpx import main
from secmaster.common.config import Config
from secmaster.common.tools import get_project_root
from tda import auth

API_KEY = Config.TDA_API_KEY
REDIRECT_URI = Config.TDA_CALLBACK_URL

# IMPORTANT: path must be in .gitignore, its your account here !!!!
TOKEN_PATH = Path(get_project_root(), "assets", "token.json")


def get_tda_client():
    try:
        c = auth.client_from_token_file(TOKEN_PATH, API_KEY)
    except FileNotFoundError:
        from selenium import webdriver
        geckopath = Path(get_project_root(), "assets", "geckodriver")
        with webdriver.Firefox(executable_path=geckopath) as driver:
            # The TDA-API package is kind of communicative, a lot of logging msgs.
            # I decided to switch most of them from INFO to DEBUG on the enviroment I am
            # working. A matter of personal taste. 
            # Unfortunatly the package has not a verbose setting, so I cant doing programaticly
            # If you wish to make the same in your enviroment, go to:
            # ../tda/auth.py and modify 

            c = auth.client_from_login_flow(driver, API_KEY, REDIRECT_URI, TOKEN_PATH)

    return c


if __name__ == "__main__":
    c = get_tda_client()
    print(c)