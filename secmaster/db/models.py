import datetime
from email.policy import default
from xmlrpc.client import Boolean

from sqlalchemy import (
    Column,
    Integer,
    Boolean,
    String,
    DateTime,
    Numeric,
    ForeignKey,
    BigInteger,
)
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.orm import declarative_base, relationship

# declarative base class
Base = declarative_base()


class Interval(Base):
    __tablename__ = "intervals"
    id = Column(String(55), primary_key=True)
    candle = relationship("Bar")

    def __repr__(self):
        return str({c.name: getattr(self, c.name) for c in self.__table__.columns})


class Provider(Base):
    __tablename__ = "providers"
    id = Column(String(255), primary_key=True)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)
    symbols = relationship("Symbol")
    earnings = relationship("EarningDate")
    candles = relationship("Bar")

    def __repr__(self):
        return str({c.name: getattr(self, c.name) for c in self.__table__.columns})


class Symbol(Base):
    __tablename__ = "symbols"
    id = Column(String(120), primary_key=True)
    name = Column(String(255))
    sector = Column(String(255))
    industry = Column(String(255))
    quote_type = Column(String(255))
    provider = Column(String(55), ForeignKey("providers.id"))
    to_update = Column(Boolean, default=True)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)
    earnings = relationship("EarningDate")
    candles = relationship("Bar")

    def __repr__(self):
        return str({c.name: getattr(self, c.name) for c in self.__table__.columns})

    @hybrid_property
    def close(self):
        return self.candles[-1].close

    @hybrid_method
    def performance(self, days):
        p1 = self.candles[-1].close
        p0 = self.candles[-(days + 1)].close
        return round(p1 / p0 - 1, 6)

    @performance.expression
    def performance(cls, days):
        p1 = cls.candles[-1].close
        p0 = cls.candles[-(days + 1)].close
        return round(p1 / p0 - 1, 6)


class EarningDate(Base):
    __tablename__ = "earning_dates"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol_id = Column(String(55), ForeignKey("symbols.id"))
    earning_date = Column(DateTime)
    provider = Column(String(55), ForeignKey("providers.id"))
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return str({c.name: getattr(self, c.name) for c in self.__table__.columns})


class Bar(Base):
    __tablename__ = "bars"
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol_id = Column(String(55), ForeignKey("symbols.id"))
    date = Column(DateTime)
    open = Column(Numeric(asdecimal=False, precision=12, scale=4))
    high = Column(Numeric(asdecimal=False, precision=12, scale=4))
    low = Column(Numeric(asdecimal=False, precision=12, scale=4))
    close = Column(Numeric(asdecimal=False, precision=12, scale=4))
    volume = Column(BigInteger)
    interval = Column(String(55), ForeignKey("intervals.id"))
    provider = Column(String(55), ForeignKey("providers.id"))
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return str(
            {
                "symbol": self.symbol_id,
                "date": self.date,
                "open": self.open,
                "high": self.high,
                "low": self.low,
                "close": self.close,
                "volume": self.volume,
                "provider": self.provider,
                "interval": self.interval,
                "last_updated": self.last_updated,
            }
        )

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
