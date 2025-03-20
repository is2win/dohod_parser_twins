from pathlib import Path

# Базовые URL
BASE_URL = "https://www.dohod.ru"
DIVIDEND_URL = f"{BASE_URL}/ik/analytics/dividend"

# Настройки базы данных
DB_PATH = Path("data/dividends.db")

# Настройки парсера
MAX_TICKERS_PER_RUN = None  # Без ограничений на количество тикеров
REQUEST_DELAY = 3  # Задержка между запросами в секундах для продакшена

# Создаем директорию для данных если её нет
DB_PATH.parent.mkdir(parents=True, exist_ok=True) 