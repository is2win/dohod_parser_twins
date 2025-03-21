# Запросы к базе данных дивидендов

Этот документ содержит описание структуры базы данных и примеры SQL запросов для работы с данными о дивидендах, собранными парсером с сайта dohod.ru.

## Структура базы данных

База данных использует SQLite и состоит из следующих таблиц:

### 1. parsing_runs

Таблица содержит информацию о запусках парсера.

| Поле | Тип | Описание |
|------|-----|----------|
| id | INTEGER | Первичный ключ |
| start_time | DATETIME | Время начала запуска парсера |
| end_time | DATETIME | Время завершения запуска парсера |
| tickers_found | INTEGER | Количество найденных тикеров |
| tickers_processed | INTEGER | Количество обработанных тикеров |
| status | TEXT | Статус запуска ('running', 'completed', 'failed') |

### 2. companies

Таблица содержит информацию о компаниях.

| Поле | Тип | Описание |
|------|-----|----------|
| id | INTEGER | Первичный ключ |
| ticker | TEXT | Тикер компании |
| name | TEXT | Название компании |
| parsing_run_id | INTEGER | Внешний ключ к таблице parsing_runs |
| created_at | DATETIME | Время создания записи |

### 3. yearly_dividends

Таблица содержит информацию о годовых дивидендах компаний.

| Поле | Тип | Описание |
|------|-----|----------|
| id | INTEGER | Первичный ключ |
| company_id | INTEGER | Внешний ключ к таблице companies |
| year | TEXT | Год выплаты дивидендов |
| total_amount | TEXT | Общая сумма дивидендов за год |
| created_at | DATETIME | Время создания записи |

### 4. dividend_payments

Таблица содержит информацию о конкретных выплатах дивидендов.

| Поле | Тип | Описание |
|------|-----|----------|
| id | INTEGER | Первичный ключ |
| company_id | INTEGER | Внешний ключ к таблице companies |
| year | TEXT | Год выплаты |
| amount | TEXT | Сумма выплаты |
| cutoff_date | TEXT | Дата закрытия реестра |
| payment_date | TEXT | Дата выплаты |
| created_at | DATETIME | Время создания записи |

## Примеры SQL запросов

### Базовые запросы

#### Получение списка всех компаний

```sql
SELECT id, ticker, name
FROM companies
ORDER BY ticker;
```

#### Получение уникальных компаний (без дубликатов по тикеру)

```sql
SELECT DISTINCT ticker, name
FROM companies
ORDER BY ticker;
```

#### Получение информации о запусках парсера

```sql
SELECT id, 
       start_time, 
       end_time, 
       ROUND((julianday(end_time) - julianday(start_time)) * 24 * 60, 2) AS duration_minutes,
       tickers_found, 
       tickers_processed,
       status
FROM parsing_runs
ORDER BY start_time DESC;
```

### Запросы к данным о дивидендах

#### Получение годовых дивидендов для конкретной компании

```sql
SELECT c.ticker, c.name, yd.year, yd.total_amount
FROM yearly_dividends yd
JOIN companies c ON yd.company_id = c.id
WHERE c.ticker = 'SBER'
ORDER BY yd.year DESC;
```

#### Получение всех выплат для конкретной компании

```sql
SELECT c.ticker, c.name, dp.year, dp.amount, dp.cutoff_date, dp.payment_date
FROM dividend_payments dp
JOIN companies c ON dp.company_id = c.id
WHERE c.ticker = 'SBER'
ORDER BY dp.year DESC, dp.cutoff_date DESC;
```

#### Топ-10 компаний с наибольшими дивидендами за последний год

```sql
SELECT c.ticker, c.name, yd.year, yd.total_amount
FROM yearly_dividends yd
JOIN companies c ON yd.company_id = c.id
WHERE yd.year = '2023'
ORDER BY CAST(REPLACE(REPLACE(yd.total_amount, ',', '.'), ' ', '') AS REAL) DESC
LIMIT 10;
```

### Аналитические запросы

#### Средний размер дивидендов по годам

```sql
SELECT yd.year, 
       COUNT(DISTINCT c.ticker) AS companies_count,
       AVG(CAST(REPLACE(REPLACE(yd.total_amount, ',', '.'), ' ', '') AS REAL)) AS avg_dividend
FROM yearly_dividends yd
JOIN companies c ON yd.company_id = c.id
GROUP BY yd.year
ORDER BY yd.year DESC;
```

#### Количество выплат дивидендов по месяцам

```sql
SELECT 
    CASE 
        WHEN payment_date LIKE '__.01.____' THEN 'Январь'
        WHEN payment_date LIKE '__.02.____' THEN 'Февраль'
        WHEN payment_date LIKE '__.03.____' THEN 'Март'
        WHEN payment_date LIKE '__.04.____' THEN 'Апрель'
        WHEN payment_date LIKE '__.05.____' THEN 'Май'
        WHEN payment_date LIKE '__.06.____' THEN 'Июнь'
        WHEN payment_date LIKE '__.07.____' THEN 'Июль'
        WHEN payment_date LIKE '__.08.____' THEN 'Август'
        WHEN payment_date LIKE '__.09.____' THEN 'Сентябрь'
        WHEN payment_date LIKE '__.10.____' THEN 'Октябрь'
        WHEN payment_date LIKE '__.11.____' THEN 'Ноябрь'
        WHEN payment_date LIKE '__.12.____' THEN 'Декабрь'
        ELSE 'Неизвестно'
    END AS month,
    COUNT(*) AS payments_count
FROM dividend_payments
WHERE payment_date LIKE '__.__.____'
GROUP BY month
ORDER BY MIN(SUBSTR(payment_date, 4, 2));
```

#### Компании, увеличившие дивиденды за последний год

```sql
SELECT c.ticker, c.name, 
       prev.year AS prev_year, prev.total_amount AS prev_amount,
       curr.year AS curr_year, curr.total_amount AS curr_amount,
       ROUND(
         (CAST(REPLACE(REPLACE(curr.total_amount, ',', '.'), ' ', '') AS REAL) - 
          CAST(REPLACE(REPLACE(prev.total_amount, ',', '.'), ' ', '') AS REAL)) /
          CAST(REPLACE(REPLACE(prev.total_amount, ',', '.'), ' ', '') AS REAL) * 100, 2) AS growth_percent
FROM yearly_dividends curr
JOIN yearly_dividends prev ON curr.company_id = prev.company_id AND curr.year = '2023' AND prev.year = '2022'
JOIN companies c ON curr.company_id = c.id
WHERE CAST(REPLACE(REPLACE(curr.total_amount, ',', '.'), ' ', '') AS REAL) > 
      CAST(REPLACE(REPLACE(prev.total_amount, ',', '.'), ' ', '') AS REAL)
ORDER BY growth_percent DESC;
```

### Запросы для сравнения между запусками

#### Компании, добавленные в последнем запуске

```sql
SELECT pr.id AS run_id, pr.start_time, c.ticker, c.name
FROM companies c
JOIN parsing_runs pr ON c.parsing_run_id = pr.id
WHERE pr.id = (SELECT MAX(id) FROM parsing_runs)
  AND c.ticker NOT IN (
    SELECT c2.ticker 
    FROM companies c2 
    WHERE c2.parsing_run_id < pr.id
  )
ORDER BY c.ticker;
```

#### Компании с изменившимися годовыми дивидендами в последнем запуске

```sql
SELECT 
    c_last.ticker,
    c_last.name,
    yd_last.year,
    yd_last.total_amount AS current_amount,
    yd_prev.total_amount AS previous_amount
FROM parsing_runs pr_last
JOIN parsing_runs pr_prev ON pr_last.id > pr_prev.id
JOIN companies c_last ON c_last.parsing_run_id = pr_last.id
JOIN companies c_prev ON c_prev.parsing_run_id = pr_prev.id AND c_prev.ticker = c_last.ticker
JOIN yearly_dividends yd_last ON yd_last.company_id = c_last.id
JOIN yearly_dividends yd_prev ON yd_prev.company_id = c_prev.id AND yd_prev.year = yd_last.year
WHERE pr_last.id = (SELECT MAX(id) FROM parsing_runs)
  AND pr_prev.id = (SELECT MAX(id) FROM parsing_runs WHERE id < pr_last.id)
  AND yd_last.total_amount != yd_prev.total_amount
ORDER BY c_last.ticker, yd_last.year;
```

#### Изменения в конкретных выплатах между запусками

```sql
SELECT 
    c_last.ticker,
    dp_last.year,
    dp_last.cutoff_date,
    dp_last.amount AS current_amount,
    dp_prev.amount AS previous_amount,
    dp_last.payment_date AS current_payment_date,
    dp_prev.payment_date AS previous_payment_date
FROM parsing_runs pr_last
JOIN parsing_runs pr_prev ON pr_last.id > pr_prev.id
JOIN companies c_last ON c_last.parsing_run_id = pr_last.id
JOIN companies c_prev ON c_prev.parsing_run_id = pr_prev.id AND c_prev.ticker = c_last.ticker
JOIN dividend_payments dp_last ON dp_last.company_id = c_last.id
JOIN dividend_payments dp_prev ON dp_prev.company_id = c_prev.id 
    AND dp_prev.year = dp_last.year 
    AND dp_prev.cutoff_date = dp_last.cutoff_date
WHERE pr_last.id = (SELECT MAX(id) FROM parsing_runs)
  AND pr_prev.id = (SELECT MAX(id) FROM parsing_runs WHERE id < pr_last.id)
  AND (dp_last.amount != dp_prev.amount OR dp_last.payment_date != dp_prev.payment_date)
ORDER BY c_last.ticker, dp_last.year;
```

### Статистические запросы

#### Статистика годовых дивидендов по компаниям

```sql
SELECT 
    c.ticker, 
    c.name,
    COUNT(yd.id) AS years_with_dividends,
    MIN(yd.year) AS first_year,
    MAX(yd.year) AS last_year,
    MAX(CAST(REPLACE(REPLACE(yd.total_amount, ',', '.'), ' ', '') AS REAL)) AS max_dividend,
    AVG(CAST(REPLACE(REPLACE(yd.total_amount, ',', '.'), ' ', '') AS REAL)) AS avg_dividend
FROM companies c
JOIN yearly_dividends yd ON yd.company_id = c.id
GROUP BY c.ticker, c.name
ORDER BY avg_dividend DESC;
```

#### Статистика по количеству выплат в году

```sql
SELECT 
    dp.year,
    COUNT(DISTINCT c.ticker) AS companies_count,
    COUNT(dp.id) AS payments_count,
    ROUND(COUNT(dp.id) * 1.0 / COUNT(DISTINCT c.ticker), 2) AS avg_payments_per_company
FROM dividend_payments dp
JOIN companies c ON dp.company_id = c.id
GROUP BY dp.year
ORDER BY dp.year DESC;
```

#### Статистика по запускам парсера

```sql
SELECT 
    id,
    start_time,
    end_time,
    ROUND((julianday(end_time) - julianday(start_time)) * 24 * 60, 2) AS duration_minutes,
    tickers_found,
    tickers_processed,
    ROUND(tickers_processed * 100.0 / tickers_found, 2) AS processing_success_rate,
    status
FROM parsing_runs
ORDER BY start_time DESC;
```

### Сложные аналитические запросы

#### Компании с наиболее стабильными выплатами

```sql
SELECT 
    c.ticker, 
    c.name, 
    COUNT(DISTINCT dp.year) AS years_with_payments,
    COUNT(dp.id) AS total_payments,
    ROUND(COUNT(dp.id) * 1.0 / COUNT(DISTINCT dp.year), 2) AS payments_per_year,
    GROUP_CONCAT(DISTINCT dp.year ORDER BY dp.year DESC) AS years
FROM companies c
JOIN dividend_payments dp ON dp.company_id = c.id
GROUP BY c.ticker, c.name
HAVING COUNT(DISTINCT dp.year) >= 5
ORDER BY payments_per_year DESC, years_with_payments DESC
LIMIT 20;
```

#### Рейтинг компаний по росту дивидендов за последние 5 лет

```sql
WITH yearly_data AS (
    SELECT 
        c.ticker,
        c.name,
        yd.year,
        CAST(REPLACE(REPLACE(yd.total_amount, ',', '.'), ' ', '') AS REAL) AS amount,
        ROW_NUMBER() OVER (PARTITION BY c.ticker ORDER BY yd.year) AS row_num
    FROM companies c
    JOIN yearly_dividends yd ON yd.company_id = c.id
    WHERE yd.year IN ('2019', '2020', '2021', '2022', '2023')
),
first_last AS (
    SELECT
        ticker,
        name,
        MAX(CASE WHEN year = '2019' THEN amount ELSE NULL END) AS amount_2019,
        MAX(CASE WHEN year = '2023' THEN amount ELSE NULL END) AS amount_2023
    FROM yearly_data
    GROUP BY ticker, name
    HAVING amount_2019 IS NOT NULL AND amount_2023 IS NOT NULL
)
SELECT
    ticker,
    name,
    amount_2019,
    amount_2023,
    amount_2023 - amount_2019 AS absolute_growth,
    ROUND((amount_2023 - amount_2019) / amount_2019 * 100, 2) AS percent_growth
FROM first_last
WHERE amount_2019 > 0
ORDER BY percent_growth DESC
LIMIT 20;
```

#### Корреляция между размером годовых дивидендов и количеством выплат

```sql
WITH dividend_stats AS (
    SELECT 
        c.ticker,
        c.name,
        COUNT(DISTINCT dp.id) AS payments_count,
        AVG(CAST(REPLACE(REPLACE(yd.total_amount, ',', '.'), ' ', '') AS REAL)) AS avg_yearly_dividend
    FROM companies c
    JOIN yearly_dividends yd ON yd.company_id = c.id
    JOIN dividend_payments dp ON dp.company_id = c.id AND dp.year = yd.year
    GROUP BY c.ticker, c.name
)
SELECT 
    COUNT(*) AS companies_count,
    AVG(payments_count) AS avg_payments_per_company,
    AVG(avg_yearly_dividend) AS avg_dividend,
    ROUND(
        (AVG(payments_count * avg_yearly_dividend) - AVG(payments_count) * AVG(avg_yearly_dividend)) /
        (SQRT((AVG(payments_count * payments_count) - AVG(payments_count) * AVG(payments_count)) * 
             (AVG(avg_yearly_dividend * avg_yearly_dividend) - AVG(avg_yearly_dividend) * AVG(avg_yearly_dividend)))
    ), 3) AS correlation_coefficient
FROM dividend_stats;
```

## Советы по работе с базой данных

1. При работе с суммами дивидендов учитывайте, что они хранятся как текст, и вам может потребоваться преобразование в числовой формат с помощью `CAST` и `REPLACE`.

2. Для ускорения запросов с частыми фильтрами рекомендуется создать индексы:

```sql
-- Индекс для поиска компаний по тикеру
CREATE INDEX idx_companies_ticker ON companies (ticker);

-- Индекс для связи годовых дивидендов с компаниями
CREATE INDEX idx_yearly_dividends_company_id ON yearly_dividends (company_id);

-- Индекс для связи выплат с компаниями
CREATE INDEX idx_dividend_payments_company_id ON dividend_payments (company_id);

-- Комбинированный индекс для поиска выплат по году и компании
CREATE INDEX idx_dividend_payments_company_year ON dividend_payments (company_id, year);
```

3. Для сохранения результатов запроса в файл CSV можно использовать SQLite команду:

```sql
.mode csv
.headers on
.output result.csv
SELECT c.ticker, c.name, yd.year, yd.total_amount FROM yearly_dividends yd JOIN companies c ON yd.company_id = c.id;
.output stdout
``` 