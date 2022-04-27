import logging

from sqlalchemy.exc import SQLAlchemyError

from common.tools import DatabaseConnector
from db.models import Provider, Interval

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


logger.info("Starting Securities Master database initialization.")
s = DatabaseConnector(db_name="SECMASTER").session()
try:
    to_add = [
        Provider(id="TDA"),
        Interval(id="EOD"),
    ]
    s.add_all(to_add)
    s.commit()
    s.close()
    logger.info("Database correctly initialized.")
except SQLAlchemyError as e:
    s.close()
    error = str(e.__dict__["orig"])
    logger.error(error)
    logger.error("Exiting process, review error.")
    raise SystemExit
