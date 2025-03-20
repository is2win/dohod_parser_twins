import os
import sqlite3
import pandas as pd
from datetime import datetime
from config import DB_PATH

def ensure_diff_dir_exists():
    """Создает директорию для отчетов, если она не существует"""
    diff_dir = 'diff'
    if not os.path.exists(diff_dir):
        os.makedirs(diff_dir)
    return diff_dir

def get_last_two_run_ids(conn):
    """Получает ID последних двух запусков парсера"""
    query = "SELECT id, start_time, end_time FROM parsing_runs ORDER BY start_time DESC LIMIT 2"
    runs = pd.read_sql_query(query, conn)
    
    if len(runs) < 2:
        return None, None, None
    
    last_run_id = runs.iloc[0]['id']
    prev_run_id = runs.iloc[1]['id']
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    return last_run_id, prev_run_id, timestamp

def compare_companies(conn, last_run_id, prev_run_id, diff_dir, timestamp):
    """Сравнивает компании между двумя запусками"""
    # Получаем компании из последнего запуска
    last_companies_query = f"""
    SELECT ticker, name, sector, parsed_at
    FROM companies
    WHERE parsing_run_id = {last_run_id}
    """
    
    # Получаем компании из предыдущего запуска
    prev_companies_query = f"""
    SELECT ticker, name, sector, parsed_at
    FROM companies
    WHERE parsing_run_id = {prev_run_id}
    """
    
    last_companies = pd.read_sql_query(last_companies_query, conn)
    prev_companies = pd.read_sql_query(prev_companies_query, conn)
    
    # Компании, которые есть только в последнем запуске (новые)
    new_companies = last_companies[~last_companies['ticker'].isin(prev_companies['ticker'])]
    
    # Компании, которые были в предыдущем запуске, но отсутствуют в последнем (удаленные)
    removed_companies = prev_companies[~prev_companies['ticker'].isin(last_companies['ticker'])]
    
    # Компании, которые изменились
    merged = pd.merge(
        last_companies, prev_companies, 
        on='ticker', suffixes=('_last', '_prev')
    )
    changed_companies = merged[(merged['name_last'] != merged['name_prev']) | 
                               (merged['sector_last'] != merged['sector_prev'])]
    
    # Формируем отчет
    with open(f"{diff_dir}/companies_diff_{timestamp}.txt", 'w', encoding='utf-8') as f:
        f.write(f"Отчет по изменениям в компаниях между запусками {prev_run_id} и {last_run_id}\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("НОВЫЕ КОМПАНИИ:\n")
        f.write("-" * 80 + "\n")
        if len(new_companies) == 0:
            f.write("Нет новых компаний\n")
        else:
            for _, row in new_companies.iterrows():
                f.write(f"Тикер: {row['ticker']}, Название: {row['name']}, Сектор: {row['sector']}\n")
        f.write("\n")
        
        f.write("УДАЛЕННЫЕ КОМПАНИИ:\n")
        f.write("-" * 80 + "\n")
        if len(removed_companies) == 0:
            f.write("Нет удаленных компаний\n")
        else:
            for _, row in removed_companies.iterrows():
                f.write(f"Тикер: {row['ticker']}, Название: {row['name']}, Сектор: {row['sector']}\n")
        f.write("\n")
        
        f.write("ИЗМЕНЕННЫЕ КОМПАНИИ:\n")
        f.write("-" * 80 + "\n")
        if len(changed_companies) == 0:
            f.write("Нет измененных компаний\n")
        else:
            for _, row in changed_companies.iterrows():
                f.write(f"Тикер: {row['ticker']}\n")
                
                if row['name_last'] != row['name_prev']:
                    f.write(f"Старое название: {row['name_prev']}\n")
                    f.write(f"Новое название: {row['name_last']}\n")
                
                if row['sector_last'] != row['sector_prev']:
                    f.write(f"Старый сектор: {row['sector_prev']}\n")
                    f.write(f"Новый сектор: {row['sector_last']}\n")
                    
                f.write("-" * 40 + "\n")
        
    return new_companies, removed_companies, changed_companies

def compare_yearly_dividends(conn, last_run_id, prev_run_id, diff_dir, timestamp):
    """Сравнивает годовые дивиденды между двумя запусками"""
    # Получаем годовые дивиденды из последнего запуска
    last_dividends_query = f"""
    SELECT c.ticker, yd.year, yd.total_amount
    FROM yearly_dividends yd
    JOIN companies c ON yd.company_id = c.id
    WHERE c.parsing_run_id = {last_run_id}
    """
    
    # Получаем годовые дивиденды из предыдущего запуска
    prev_dividends_query = f"""
    SELECT c.ticker, yd.year, yd.total_amount
    FROM yearly_dividends yd
    JOIN companies c ON yd.company_id = c.id
    WHERE c.parsing_run_id = {prev_run_id}
    """
    
    last_dividends = pd.read_sql_query(last_dividends_query, conn)
    prev_dividends = pd.read_sql_query(prev_dividends_query, conn)
    
    # Создаем уникальные ключи для сравнения
    last_dividends['dividend_key'] = last_dividends['ticker'] + '_' + last_dividends['year'].astype(str)
    prev_dividends['dividend_key'] = prev_dividends['ticker'] + '_' + prev_dividends['year'].astype(str)
    
    # Годовые дивиденды, которые есть только в последнем запуске (новые)
    new_dividends = last_dividends[~last_dividends['dividend_key'].isin(prev_dividends['dividend_key'])]
    
    # Годовые дивиденды, которые были в предыдущем запуске, но отсутствуют в последнем (удаленные)
    removed_dividends = prev_dividends[~prev_dividends['dividend_key'].isin(last_dividends['dividend_key'])]
    
    # Создаем словари для быстрого поиска сумм годовых дивидендов
    last_dict = {row['dividend_key']: row for _, row in last_dividends.iterrows()}
    prev_dict = {row['dividend_key']: row for _, row in prev_dividends.iterrows()}
    
    # Находим общие ключи
    common_keys = set(last_dict.keys()) & set(prev_dict.keys())
    
    # Находим годовые дивиденды с изменениями в суммах
    changed_rows = []
    for key in common_keys:
        last_row = last_dict[key]
        prev_row = prev_dict[key]
        
        if last_row['total_amount'] != prev_row['total_amount']:
            changed_rows.append({
                'ticker': last_row['ticker'],
                'year': last_row['year'],
                'total_amount_last': last_row['total_amount'],
                'total_amount_prev': prev_row['total_amount']
            })
    
    changed_dividends = pd.DataFrame(changed_rows)
    
    # Формируем отчет
    with open(f"{diff_dir}/yearly_dividends_diff_{timestamp}.txt", 'w', encoding='utf-8') as f:
        f.write(f"Отчет по изменениям в годовых дивидендах между запусками {prev_run_id} и {last_run_id}\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("НОВЫЕ ГОДОВЫЕ ДИВИДЕНДЫ:\n")
        f.write("-" * 80 + "\n")
        if len(new_dividends) == 0:
            f.write("Нет новых данных о годовых дивидендах\n")
        else:
            for _, row in new_dividends.iterrows():
                f.write(f"Тикер: {row['ticker']}, Год: {row['year']}\n")
                f.write(f"Сумма: {row['total_amount']}\n")
                f.write("-" * 40 + "\n")
        f.write("\n")
        
        f.write("УДАЛЕННЫЕ ГОДОВЫЕ ДИВИДЕНДЫ:\n")
        f.write("-" * 80 + "\n")
        if len(removed_dividends) == 0:
            f.write("Нет удаленных данных о годовых дивидендах\n")
        else:
            for _, row in removed_dividends.iterrows():
                f.write(f"Тикер: {row['ticker']}, Год: {row['year']}\n")
                f.write(f"Сумма: {row['total_amount']}\n")
                f.write("-" * 40 + "\n")
        f.write("\n")
        
        f.write("ИЗМЕНЕННЫЕ ГОДОВЫЕ ДИВИДЕНДЫ:\n")
        f.write("-" * 80 + "\n")
        if len(changed_dividends) == 0:
            f.write("Нет измененных данных о годовых дивидендах\n")
        else:
            for _, row in changed_dividends.iterrows():
                f.write(f"Тикер: {row['ticker']}, Год: {row['year']}\n")
                f.write(f"Старая сумма: {row['total_amount_prev']}\n")
                f.write(f"Новая сумма: {row['total_amount_last']}\n")
                f.write("-" * 40 + "\n")
    
    return new_dividends, removed_dividends, changed_dividends

def compare_dividend_payments(conn, last_run_id, prev_run_id, diff_dir, timestamp):
    """Сравнивает выплаты дивидендов между двумя запусками"""
    # Получаем выплаты из последнего запуска
    last_payments_query = f"""
    SELECT c.ticker, dp.year, dp.amount, dp.cutoff_date, dp.payment_date
    FROM dividend_payments dp
    JOIN companies c ON dp.company_id = c.id
    WHERE c.parsing_run_id = {last_run_id}
    """
    
    # Получаем выплаты из предыдущего запуска
    prev_payments_query = f"""
    SELECT c.ticker, dp.year, dp.amount, dp.cutoff_date, dp.payment_date
    FROM dividend_payments dp
    JOIN companies c ON dp.company_id = c.id
    WHERE c.parsing_run_id = {prev_run_id}
    """
    
    last_payments = pd.read_sql_query(last_payments_query, conn)
    prev_payments = pd.read_sql_query(prev_payments_query, conn)
    
    # Создаем уникальные ключи для сравнения (без суммы выплаты)
    last_payments['payment_key'] = last_payments['ticker'] + '_' + last_payments['year'].astype(str) + '_' + last_payments['cutoff_date'] + '_' + last_payments['payment_date']
    prev_payments['payment_key'] = prev_payments['ticker'] + '_' + prev_payments['year'].astype(str) + '_' + prev_payments['cutoff_date'] + '_' + prev_payments['payment_date']
    
    # Выплаты, которые есть только в последнем запуске (новые)
    new_payments = last_payments[~last_payments['payment_key'].isin(prev_payments['payment_key'])]
    
    # Выплаты, которые были в предыдущем запуске, но отсутствуют в последнем (удаленные)
    removed_payments = prev_payments[~prev_payments['payment_key'].isin(last_payments['payment_key'])]
    
    # Создаем словари для быстрого поиска сумм выплат
    last_dict = {row['payment_key']: row for _, row in last_payments.iterrows()}
    prev_dict = {row['payment_key']: row for _, row in prev_payments.iterrows()}
    
    # Находим общие ключи
    common_keys = set(last_dict.keys()) & set(prev_dict.keys())
    
    # Находим выплаты с изменениями в суммах
    changed_rows = []
    for key in common_keys:
        last_row = last_dict[key]
        prev_row = prev_dict[key]
        
        if last_row['amount'] != prev_row['amount']:
            changed_rows.append({
                'ticker': last_row['ticker'],
                'year': last_row['year'],
                'cutoff_date': last_row['cutoff_date'],
                'payment_date': last_row['payment_date'],
                'amount_last': last_row['amount'],
                'amount_prev': prev_row['amount']
            })
    
    changed_payments = pd.DataFrame(changed_rows)
    
    # Формируем отчет
    with open(f"{diff_dir}/dividend_payments_diff_{timestamp}.txt", 'w', encoding='utf-8') as f:
        f.write(f"Отчет по изменениям в выплатах дивидендов между запусками {prev_run_id} и {last_run_id}\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("НОВЫЕ ВЫПЛАТЫ:\n")
        f.write("-" * 80 + "\n")
        if len(new_payments) == 0:
            f.write("Нет новых выплат\n")
        else:
            for _, row in new_payments.iterrows():
                f.write(f"Тикер: {row['ticker']}, Год: {row['year']}\n")
                f.write(f"Размер: {row['amount']}\n")
                f.write(f"Дата отсечки: {row['cutoff_date']}\n")
                f.write(f"Дата выплаты: {row['payment_date']}\n")
                f.write("-" * 40 + "\n")
        f.write("\n")
        
        f.write("УДАЛЕННЫЕ ВЫПЛАТЫ:\n")
        f.write("-" * 80 + "\n")
        if len(removed_payments) == 0:
            f.write("Нет удаленных выплат\n")
        else:
            for _, row in removed_payments.iterrows():
                f.write(f"Тикер: {row['ticker']}, Год: {row['year']}\n")
                f.write(f"Размер: {row['amount']}\n")
                f.write(f"Дата отсечки: {row['cutoff_date']}\n")
                f.write(f"Дата выплаты: {row['payment_date']}\n")
                f.write("-" * 40 + "\n")
        f.write("\n")
        
        f.write("ИЗМЕНЕННЫЕ ВЫПЛАТЫ:\n")
        f.write("-" * 80 + "\n")
        if len(changed_payments) == 0:
            f.write("Нет измененных выплат\n")
        else:
            for _, row in changed_payments.iterrows():
                f.write(f"Тикер: {row['ticker']}, Год: {row['year']}\n")
                f.write(f"Дата отсечки: {row['cutoff_date']}, Дата выплаты: {row['payment_date']}\n")
                f.write(f"Старый размер: {row['amount_prev']}\n")
                f.write(f"Новый размер: {row['amount_last']}\n")
                f.write("-" * 40 + "\n")
    
    return new_payments, removed_payments, changed_payments

def create_summary_report(last_run_id, prev_run_id, diff_dir, timestamp, 
                         new_companies, removed_companies, changed_companies,
                         dividend_differences, 
                         new_payments, removed_payments, changed_payments):
    """Создает сводный отчет по всем изменениям"""
    file_prefix = f"iter_{prev_run_id}_{last_run_id}_"
    
    # Пути к файлам
    companies_file = f"{diff_dir}/{file_prefix}companies_diff_{timestamp}.txt"
    yearly_dividends_file = f"{diff_dir}/{file_prefix}yearly_dividends_diff_{timestamp}.txt"
    dividend_payments_file = f"{diff_dir}/{file_prefix}dividend_payments_diff_{timestamp}.txt"
    summary_file = f"{diff_dir}/{file_prefix}summary_diff_{timestamp}.txt"
    
    # Переименовываем уже созданные файлы
    os.rename(f"{diff_dir}/companies_diff_{timestamp}.txt", companies_file)
    os.rename(f"{diff_dir}/yearly_dividends_diff_{timestamp}.txt", yearly_dividends_file)
    os.rename(f"{diff_dir}/dividend_payments_diff_{timestamp}.txt", dividend_payments_file)
    
    # Распаковываем информацию о дивидендах
    if isinstance(dividend_differences, tuple) and len(dividend_differences) == 3:
        new_dividends, removed_dividends, changed_dividends = dividend_differences
    else:
        # Для обратной совместимости
        new_dividends = pd.DataFrame()
        removed_dividends = pd.DataFrame()
        changed_dividends = dividend_differences
    
    # Создаем сводный отчет
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"СВОДНЫЙ ОТЧЕТ ПО ИЗМЕНЕНИЯМ МЕЖДУ ЗАПУСКАМИ {prev_run_id} И {last_run_id}\n")
        f.write("=" * 80 + "\n\n")
        
        # Изменения в компаниях
        f.write("КОМПАНИИ:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Новые компании: {len(new_companies)}\n")
        f.write(f"Удаленные компании: {len(removed_companies)}\n")
        f.write(f"Измененные компании: {len(changed_companies)}\n")
        f.write("\n")
        
        # Изменения в годовых дивидендах
        f.write("ГОДОВЫЕ ДИВИДЕНДЫ:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Новые годовые дивиденды: {len(new_dividends)}\n")
        f.write(f"Удаленные годовые дивиденды: {len(removed_dividends)}\n")
        f.write(f"Измененные годовые дивиденды: {len(changed_dividends)}\n")
        f.write("\n")
        
        # Изменения в выплатах
        f.write("ВЫПЛАТЫ ДИВИДЕНДОВ:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Новые выплаты: {len(new_payments)}\n")
        f.write(f"Удаленные выплаты: {len(removed_payments)}\n")
        f.write(f"Измененные выплаты: {len(changed_payments)}\n")
        f.write("\n")
        
        # Общее количество изменений
        total_changes = (len(new_companies) + len(removed_companies) + len(changed_companies) + 
                         len(new_dividends) + len(removed_dividends) + len(changed_dividends) + 
                         len(new_payments) + len(removed_payments) + len(changed_payments))
        
        f.write("ИТОГО:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Общее количество изменений: {total_changes}\n")
        
    return {
        'summary_file': summary_file,
        'companies_file': companies_file,
        'yearly_dividends_file': yearly_dividends_file,
        'dividend_payments_file': dividend_payments_file,
        'has_differences': total_changes > 0
    }

def main():
    """Основная функция для запуска сравнения"""
    # Создаем директорию для отчетов
    diff_dir = ensure_diff_dir_exists()
    
    # Подключаемся к базе данных
    conn = sqlite3.connect(DB_PATH)
    
    # Получаем ID последних двух запусков
    last_run_id, prev_run_id, timestamp = get_last_two_run_ids(conn)
    
    if not last_run_id or not prev_run_id:
        print("Недостаточно запусков для сравнения (нужно минимум 2)")
        return {
            'has_differences': False,
            'files': {}
        }
    
    print(f"Сравниваем запуски {prev_run_id} и {last_run_id}")
    
    # Сравниваем компании
    print("Сравниваем компании...")
    new_companies, removed_companies, changed_companies = compare_companies(
        conn, last_run_id, prev_run_id, diff_dir, timestamp
    )
    
    # Сравниваем годовые дивиденды
    print("Сравниваем годовые дивиденды...")
    dividend_differences = compare_yearly_dividends(
        conn, last_run_id, prev_run_id, diff_dir, timestamp
    )
    
    # Сравниваем выплаты дивидендов
    print("Сравниваем выплаты дивидендов...")
    new_payments, removed_payments, changed_payments = compare_dividend_payments(
        conn, last_run_id, prev_run_id, diff_dir, timestamp
    )
    
    # Создаем сводный отчет
    print("Создаем сводный отчет...")
    result = create_summary_report(
        last_run_id, prev_run_id, diff_dir, timestamp,
        new_companies, removed_companies, changed_companies,
        dividend_differences,
        new_payments, removed_payments, changed_payments
    )
    
    # Закрываем соединение с базой данных
    conn.close()
    
    print(f"Анализ завершен. Отчеты сохранены в директории '{diff_dir}'")
    
    return {
        'has_differences': result['has_differences'],
        'files': {
            'summary': result['summary_file'],
            'companies': result['companies_file'],
            'yearly_dividends': result['yearly_dividends_file'],
            'dividend_payments': result['dividend_payments_file']
        }
    }

if __name__ == "__main__":
    result = main()
    
    # Выводим результат для использования в скриптах
    if result['has_differences']:
        print("Обнаружены расхождения между запусками.")
    else:
        print("Расхождений между запусками не обнаружено.") 