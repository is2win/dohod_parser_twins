import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
from typing import List, Dict, Optional, Set
from config import BASE_URL, DIVIDEND_URL, REQUEST_DELAY
from database import Session, ParsingRun, Company, YearlyDividend, DividendPayment
import re

class DividendParser:
    def __init__(self, max_tickers: Optional[int] = None):
        self.session = Session()
        self.max_tickers = max_tickers
        self.parsing_run = self._create_parsing_run()
        self.processed_tickers: Set[str] = set()
        
    def _create_parsing_run(self) -> ParsingRun:
        """Создает новую запись о запуске парсинга"""
        run = ParsingRun()
        self.session.add(run)
        self.session.commit()
        return run
    
    def _get_tickers_list(self) -> List[Dict[str, str]]:
        """Получает список всех тикеров с главной страницы"""
        response = requests.get(DIVIDEND_URL)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Находим таблицу с тикерами по ID
        table = soup.find('table', {'id': 'table-dividend'})
        if not table:
            raise Exception("Не удалось найти таблицу с тикерами")
            
        tickers = []
        seen_tickers = set()  # Множество для отслеживания уникальных тикеров
        rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')[1:]
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                # Находим ссылку и извлекаем тикер из неё
                link = cols[0].find('a')
                if link and 'href' in link.attrs:
                    href = link['href']
                    ticker = href.split('/')[-1]  # Получаем тикер из URL
                    
                    # Пропускаем дубликаты
                    if ticker in seen_tickers:
                        continue
                        
                    seen_tickers.add(ticker)
                    # Первая колонка (Акция) содержит название компании, получаем текст из ссылки
                    name = link.text.strip()
                    # Вторая колонка содержит сектор
                    sector = cols[1].text.strip() if len(cols) > 1 else ""
                    tickers.append({'ticker': ticker, 'name': name, 'sector': sector})
        
        if self.max_tickers:
            # Если указан лимит, применяем его
            tickers = tickers[:self.max_tickers]
            
        self.parsing_run.tickers_found = len(tickers)
        self.session.commit()
        return tickers
    
    def _parse_company_page(self, ticker: str, name: str, sector: str) -> None:
        """Парсит страницу компании"""
        # Пропускаем уже обработанные тикеры
        if ticker in self.processed_tickers:
            print(f"Тикер {ticker} уже был обработан, пропускаем")
            return
            
        url = f"{BASE_URL}/ik/analytics/dividend/{ticker}"
        print(f"Парсинг страницы: {url}")
        
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Создаем запись о компании
            company = Company(
                ticker=ticker,
                name=name,
                sector=sector,  # Добавляем сектор
                parsing_run_id=self.parsing_run.id  # Сохраняем ID запуска парсера
            )
            self.session.add(company)
            self.session.commit()
            
            # Находим все таблицы на странице
            tables = soup.find_all('table', {'class': 'content-table'})
            print(f"Найдено таблиц content-table на странице: {len(tables)}")
            
            if len(tables) >= 1:
                print("Парсинг годовых дивидендов")
                self._parse_yearly_dividends(company, tables[0])
                
            if len(tables) >= 2:
                print("Парсинг всех выплат")
                self._parse_all_dividends(company, tables[1])
                
            self.parsing_run.tickers_processed += 1
            self.processed_tickers.add(ticker)  # Добавляем тикер в множество обработанных
            self.session.commit()
            
        except Exception as e:
            print(f"Ошибка при парсинге компании {ticker}: {str(e)}")
            self.session.rollback()
        
    def _parse_yearly_dividends(self, company: Company, table: BeautifulSoup) -> None:
        """Парсит таблицу с годовыми дивидендами"""
        rows = table.find_all('tr')[1:]  # Пропускаем заголовок
        print(f"Найдено строк в таблице годовых дивидендов: {len(rows)}")
        
        for i, row in enumerate(rows):
            cols = row.find_all('td')
            if len(cols) >= 2:  # Год и сумма
                try:
                    print(f"\nОбработка строки {i+1}:")
                    data = [col.text.strip() for col in cols]
                    print(f"Содержимое колонок: {data}")
                    
                    # Пропускаем прогнозы
                    if 'прогноз' in data[0].lower() or 'след' in data[0].lower():
                        continue
                        
                    # Извлекаем год и сумму как текст
                    year_text = data[0]
                    amount_text = data[1]
                    
                    # Создаем запись о годовых дивидендах
                    dividend = YearlyDividend(
                        company_id=company.id,
                        year=year_text,
                        total_amount=amount_text
                    )
                    self.session.add(dividend)
                    print("Запись добавлена в базу данных")
                    
                except Exception as e:
                    print(f"Ошибка при парсинге годовых дивидендов: {e}")
                    print(f"Данные строки: {[col.text.strip() for col in cols]}")
            
        self.session.commit()
                
    def _parse_all_dividends(self, company: Company, table: BeautifulSoup) -> None:
        """Парсит таблицу со всеми выплатами"""
        rows = table.find_all('tr')[1:]  # Пропускаем заголовок
        print(f"Найдено строк в таблице всех выплат: {len(rows)}")
        
        for i, row in enumerate(rows, 1):
            print(f"\nОбработка строки {i}:")
            cols = row.find_all('td')
            
            try:
                data = [col.text.strip() for col in cols]
                print(f"Содержимое колонок: {data}")
                
                # Пропускаем прогнозы
                if any('прогноз' in col.lower() for col in data):
                    continue
                
                # Сохраняем данные как текст
                cutoff_date = data[0] if len(data) > 0 else ""
                payment_date = data[1] if len(data) > 1 else ""
                year = data[2] if len(data) > 2 else ""
                amount = data[3] if len(data) > 3 else ""
                
                # Создаем запись о выплате
                payment = DividendPayment(
                    company_id=company.id,
                    year=year,
                    amount=amount,
                    cutoff_date=cutoff_date,
                    payment_date=payment_date
                )
                self.session.add(payment)
                print("Запись добавлена в базу данных")
                
            except Exception as e:
                print(f"Ошибка при парсинге всех выплат: {e}")
                print(f"Данные строки: {data if 'data' in locals() else 'нет данных'}")
                continue
        
        self.session.commit()
    
    def run(self) -> None:
        """Запускает процесс парсинга"""
        try:
            tickers = self._get_tickers_list()
            print(f"Найдено тикеров: {len(tickers)}")
            
            for ticker_data in tickers:
                print(f"\nПарсинг {ticker_data['ticker']} - {ticker_data['name']} - Сектор: {ticker_data['sector']}")
                self._parse_company_page(ticker_data['ticker'], ticker_data['name'], ticker_data['sector'])
                time.sleep(REQUEST_DELAY)
                
            self.parsing_run.status = 'completed'
            self.parsing_run.end_time = datetime.now()
            self.session.commit()
            
        except Exception as e:
            print(f"Критическая ошибка: {str(e)}")
            self.parsing_run.status = 'failed'
            self.parsing_run.end_time = datetime.now()
            self.session.commit()
        finally:
            self.session.close()

if __name__ == "__main__":
    # Запускаем парсер без ограничений на количество тикеров
    parser = DividendParser()
    parser.run() 