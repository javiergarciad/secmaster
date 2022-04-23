import logging
from datetime import datetime

from sqlalchemy import select, and_, update, exc

from secmaster.common.tools import DatabaseConnector, progressbar_print
from secmaster.data_manager.nasdaq_symbols import get_symbol_info
from secmaster.db.models import Symbol

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def update_symbols_info(s):
    """
    Update info from yahoo for symbols without sector, industry information.
    :param s: database session to secmaster
    :return:
    """
    logger.info("update symbol info initialized.")

    stmt = select(Symbol.id).where(
        and_(Symbol.industry == None, Symbol.quote_type == None)
    )
    query = s.execute(stmt).all()
    symbols_to_update = [x[0] for x in query]

    logger.info(f"Ready to update {len(symbols_to_update)} symbol's info.")
    counter = 0
    for each_symbol in symbols_to_update:
        # Get the data from yahoo
        info = get_symbol_info(each_symbol)
        if info is not None:
            sector = (info.get("sector", None),)
            industry = (info.get("industry", None),)
            quote_type = (info.get("quoteType", None),)

            update_symbol_stmt = (
                update(Symbol)
                .where(Symbol.id == each_symbol)
                .values(
                    industry=industry,
                    sector=sector,
                    quote_type=quote_type,
                    last_updated=datetime.utcnow(),
                )
            )

            try:
                s.execute(update_symbol_stmt)
                s.commit()
            except exc.SQLAlchemyError:
                logger.warning(f"Error updating symbol: {each_symbol}, moving on.")
                continue

        progressbar_print(counter, len(symbols_to_update))
        counter += 1

    logger.info(f"Done updating {counter} symbol's info.")
    return True


if __name__ == "__main__":
    # database session
    db_session = DatabaseConnector().session()

    update = update_symbols_info(db_session)
    db_session.close()
    
