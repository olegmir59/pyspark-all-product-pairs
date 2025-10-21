# PySpark: Продукты и Категории

Проект для работы с продуктами и категориями в PySpark. Реализует получение всех пар "Продукт - Категория" и продуктов без категорий.

## Описание задачи

В PySpark приложении датафреймами заданы продукты, категории и их связи:
- Каждому продукту может соответствовать несколько категорий или ни одной
- Каждой категории может соответствовать несколько продуктов или ни одного

Метод `get_product_category_pairs` возвращает датафрейм со всеми парами «Имя продукта – Имя категории» и именами всех продуктов, у которых нет категорий (с NULL в поле category_name).

## Структура проекта

```
.
├── product_categories.py      # Основной модуль с методом
├── test_product_categories.py # Тесты
├── example_usage.py           # Пример использования
├── requirements.txt           # Зависимости
└── README.md                  # Документация
```

## Установка

1. Убедитесь, что у вас установлен Python 3.7+
2. Установите зависимости:

```bash
pip install -r requirements.txt
```

## Использование

### Основной метод

```python
from pyspark.sql import SparkSession
from product_categories import get_product_category_pairs

spark = SparkSession.builder.appName("Example").getOrCreate()

# Создайте датафреймы products_df, categories_df, product_categories_df
# ...

result = get_product_category_pairs(
    products_df,
    categories_df,
    product_categories_df
)

result.show()
```

### Входные данные

Метод принимает три датафрейма:

1. **products_df** - продукты:
   - `product_id` (IntegerType): ID продукта
   - `product_name` (StringType): Название продукта

2. **categories_df** - категории:
   - `category_id` (IntegerType): ID категории
   - `category_name` (StringType): Название категории

3. **product_categories_df** - связи продуктов и категорий:
   - `product_id` (IntegerType): ID продукта
   - `category_id` (IntegerType): ID категории

### Выходные данные

Датафрейм с колонками:
- `product_name` (StringType): Название продукта
- `category_name` (StringType): Название категории (NULL для продуктов без категорий)

## Запуск примера

### Windows (PowerShell)

Используйте готовый скрипт:

```powershell
.\run_example.ps1
```

Или запустите вручную:

```powershell
$env:PYSPARK_PYTHON = "python.exe"
$env:PYSPARK_DRIVER_PYTHON = "python.exe"
python example_usage.py
```

### Linux/Mac

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
python example_usage.py
```

Пример покажет работу метода на тестовых данных с различными сценариями:
- Продукты с одной категорией
- Продукты с несколькими категориями
- Продукты без категорий

## Запуск тестов

### Windows (PowerShell)

Используйте готовый скрипт:

```powershell
.\run_tests.ps1
```

Или запустите вручную:

```powershell
$env:PYSPARK_PYTHON = "python.exe"
$env:PYSPARK_DRIVER_PYTHON = "python.exe"
pytest test_product_categories.py -v
```

### Linux/Mac

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
pytest test_product_categories.py -v
```

### Тестовые сценарии

Реализованы следующие тесты:

1. **test_products_with_categories** - продукты с категориями возвращаются корректно
2. **test_products_without_categories** - продукты без категорий имеют NULL в category_name
3. **test_product_with_multiple_categories** - продукт с несколькими категориями создает несколько записей
4. **test_empty_product_categories** - все продукты без категорий (пустая таблица связей)
5. **test_complex_scenario** - комплексный сценарий с разными комбинациями

## Технические детали

Реализация использует:
- **LEFT JOIN** между products и product_categories для сохранения продуктов без категорий
- **LEFT JOIN** с categories для получения названий категорий
- Выбор только необходимых колонок с алиасами

## Требования

- Python 3.7+
- PySpark 3.0.0+ (версия 3.5.x рекомендуется для Java 8-14)
- pytest 7.0.0+ (для тестов)
- Java 8+ (для PySpark 3.x)

## Решение проблем

### Ошибка: PYTHON_VERSION_MISMATCH

Если вы видите ошибку о несоответствии версий Python, установите переменные окружения:

**Windows:**
```powershell
$env:PYSPARK_PYTHON = "полный\путь\к\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "полный\путь\к\python.exe"
```

**Linux/Mac:**
```bash
export PYSPARK_PYTHON=/path/to/python3
export PYSPARK_DRIVER_PYTHON=/path/to/python3
```

### Ошибка: Java version mismatch

- PySpark 3.x требует Java 8-14
- PySpark 4.x требует Java 17+

Убедитесь, что установлена правильная версия Java и `JAVA_HOME` настроен корректно.

## Лицензия

MIT

