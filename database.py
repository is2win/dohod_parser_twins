from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from config import DB_PATH

Base = declarative_base()

class ParsingRun(Base):
    __tablename__ = 'parsing_runs'
    
    id = Column(Integer, primary_key=True)
    start_time = Column(DateTime, default=datetime.now)
    end_time = Column(DateTime, nullable=True)
    status = Column(String, default='running')
    tickers_processed = Column(Integer, default=0)
    tickers_found = Column(Integer, default=0)

class Company(Base):
    __tablename__ = 'companies'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String)
    name = Column(String)
    sector = Column(String)
    parsing_run_id = Column(Integer, ForeignKey('parsing_runs.id'))
    parsed_at = Column(DateTime, default=datetime.now)
    
    # Создаем составной уникальный ключ из ticker и parsing_run_id
    __table_args__ = (
        # Этот индекс гарантирует уникальность пары (ticker, parsing_run_id)
        # т.е. один и тот же тикер может быть добавлен в разных запусках парсера
        # но в рамках одного запуска тикер уникален
        UniqueConstraint('ticker', 'parsing_run_id', name='unique_ticker_per_run'),
    )

class YearlyDividend(Base):
    __tablename__ = 'yearly_dividends'
    
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'))
    year = Column(String)
    total_amount = Column(String)
    created_at = Column(DateTime, default=datetime.now)

class DividendPayment(Base):
    __tablename__ = 'dividend_payments'
    
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'))
    year = Column(String)
    amount = Column(String)
    cutoff_date = Column(String)
    payment_date = Column(String)
    created_at = Column(DateTime, default=datetime.now)

# Создаем подключение к базе данных
engine = create_engine(f'sqlite:///{DB_PATH}')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine) 