from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Company(Base):
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True)
    ticker = Column(String, unique=True, nullable=False)
    name = Column(String)
    parsing_run_id = Column(Integer, ForeignKey('parsing_runs.id'))
    created_at = Column(DateTime, default=datetime.now)

class YearlyDividend(Base):
    __tablename__ = 'yearly_dividends'

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    year = Column(String)
    total_amount = Column(String)
    created_at = Column(DateTime, default=datetime.now)

class DividendPayment(Base):
    __tablename__ = 'dividend_payments'

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    year = Column(String)
    amount = Column(String)
    cutoff_date = Column(String)
    payment_date = Column(String)
    created_at = Column(DateTime, default=datetime.now)

class ParsingRun(Base):
    __tablename__ = 'parsing_runs'

    id = Column(Integer, primary_key=True)
    start_time = Column(DateTime, default=datetime.now)
    end_time = Column(DateTime)
    tickers_found = Column(Integer, default=0)
    tickers_processed = Column(Integer, default=0)
    status = Column(String, default='running') 