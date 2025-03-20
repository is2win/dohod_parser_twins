#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path

from parser import DividendParser
import analyze_diff

def parse_arguments():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description='Инструмент для сбора и анализа данных о дивидендах с dohod.ru',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '-p', '--parse-only', 
        action='store_true',
        help='Запустить только парсер без анализа различий'
    )
    
    parser.add_argument(
        '-a', '--analyze-only', 
        action='store_true',
        help='Запустить только анализ различий без парсера'
    )
    
    parser.add_argument(
        '-t', '--max-tickers', 
        type=int,
        default=None,
        help='Максимальное количество тикеров для обработки'
    )
    
    parser.add_argument(
        '-v', '--verbose', 
        action='store_true',
        help='Включить подробный вывод'
    )
    
    return parser.parse_args()

def setup_environment():
    """Настраивает окружение для работы скриптов"""
    # Создаем директории, если их нет
    os.makedirs('data', exist_ok=True)
    os.makedirs('diff', exist_ok=True)
    
    # Проверяем, есть ли файл базы данных
    db_path = Path('data/dividends.db')
    if not db_path.exists():
        print("ВНИМАНИЕ: База данных не существует и будет создана автоматически")

def run_parser(max_tickers=None):
    """Запускает парсер дивидендов"""
    print("-" * 80)
    print(f"Запуск парсера дивидендов: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    
    try:
        # Создаем и запускаем парсер
        parser = DividendParser(max_tickers=max_tickers)
        parser.run()
        print("Парсер успешно завершил работу")
        return True
    except Exception as e:
        print(f"ОШИБКА: Парсер завершился с ошибкой: {str(e)}")
        return False

def run_analyzer():
    """Запускает анализ расхождений между запусками"""
    print("\n" + "-" * 80)
    print(f"Запуск анализа расхождений: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    
    try:
        result = analyze_diff.main()
        
        if result.get('has_differences', False):
            print("\nОбнаружены расхождения между запусками!")
            if 'files' in result and result['files']:
                print("\nФайлы с отчетами:")
                for name, file_path in result['files'].items():
                    print(f"- {name}: {file_path}")
        else:
            print("\nРасхождений между запусками не обнаружено или недостаточно запусков для сравнения (нужно минимум 2)")
            
        return True
    except Exception as e:
        print(f"ОШИБКА: Анализ расхождений завершился с ошибкой: {str(e)}")
        return False

def main():
    """Основная функция"""
    start_time = time.time()
    
    # Парсим аргументы командной строки
    args = parse_arguments()
    
    print("=" * 80)
    print("ЗАПУСК ОБРАБОТКИ ДИВИДЕНДНЫХ ДАННЫХ")
    print("=" * 80)
    
    if args.verbose:
        print(f"Аргументы: {args}")
    
    # Проверяем и настраиваем окружение
    setup_environment()
    
    parser_success = True
    analyzer_success = True
    
    # Запускаем парсер, если не указан флаг analyze-only
    if not args.analyze_only:
        parser_success = run_parser(max_tickers=args.max_tickers)
    else:
        print("Парсер пропущен (указан флаг --analyze-only)")
    
    # Запускаем анализ, если не указан флаг parse-only и парсер отработал успешно
    if not args.parse_only and parser_success:
        analyzer_success = run_analyzer()
    elif args.parse_only:
        print("\nАнализ расхождений пропущен (указан флаг --parse-only)")
    elif not parser_success:
        print("\nАнализ расхождений НЕ запущен из-за ошибки в парсере")
    
    # Общая информация о выполнении
    elapsed_time = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"Общее время выполнения: {elapsed_time:.2f} секунд ({elapsed_time/60:.2f} минут)")
    
    if args.parse_only:
        status = "Успешно" if parser_success else "С ошибками"
    elif args.analyze_only:
        status = "Успешно" if analyzer_success else "С ошибками"
    else:
        status = "Успешно" if parser_success and analyzer_success else "С ошибками"
    
    print(f"Статус выполнения: {status}")
    print("=" * 80)
    
    return 0 if (args.parse_only and parser_success) or \
               (args.analyze_only and analyzer_success) or \
               (parser_success and analyzer_success) else 1

if __name__ == "__main__":
    sys.exit(main()) 