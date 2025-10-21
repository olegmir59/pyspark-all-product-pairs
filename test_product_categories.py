"""
Тесты для модуля product_categories.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from product_categories import get_product_category_pairs


@pytest.fixture(scope="session")
def spark():
    """Создание SparkSession для тестов."""
    spark = SparkSession.builder \
        .appName("ProductCategoriesTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_products_with_categories(spark):
    """Тест: продукты с категориями возвращаются корректно."""
    # Подготовка тестовых данных
    products_data = [
        (1, "Продукт A"),
        (2, "Продукт B")
    ]
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False)
    ])
    products_df = spark.createDataFrame(products_data, products_schema)
    
    categories_data = [
        (1, "Категория 1"),
        (2, "Категория 2")
    ]
    categories_schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), False)
    ])
    categories_df = spark.createDataFrame(categories_data, categories_schema)
    
    product_categories_data = [
        (1, 1),
        (2, 2)
    ]
    product_categories_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("category_id", IntegerType(), False)
    ])
    product_categories_df = spark.createDataFrame(
        product_categories_data,
        product_categories_schema
    )
    
    # Выполнение
    result = get_product_category_pairs(
        products_df,
        categories_df,
        product_categories_df
    )
    
    # Проверка
    result_list = result.collect()
    assert len(result_list) == 2
    
    result_dict = {row.product_name: row.category_name for row in result_list}
    assert result_dict["Продукт A"] == "Категория 1"
    assert result_dict["Продукт B"] == "Категория 2"


def test_products_without_categories(spark):
    """Тест: продукты без категорий возвращаются с NULL в category_name."""
    # Подготовка тестовых данных
    products_data = [
        (1, "Продукт A"),
        (2, "Продукт B без категории")
    ]
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False)
    ])
    products_df = spark.createDataFrame(products_data, products_schema)
    
    categories_data = [
        (1, "Категория 1")
    ]
    categories_schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), False)
    ])
    categories_df = spark.createDataFrame(categories_data, categories_schema)
    
    product_categories_data = [
        (1, 1)  # Только первый продукт имеет категорию
    ]
    product_categories_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("category_id", IntegerType(), False)
    ])
    product_categories_df = spark.createDataFrame(
        product_categories_data,
        product_categories_schema
    )
    
    # Выполнение
    result = get_product_category_pairs(
        products_df,
        categories_df,
        product_categories_df
    )
    
    # Проверка
    result_list = result.collect()
    assert len(result_list) == 2
    
    # Проверяем, что продукт с категорией есть
    product_a = [row for row in result_list if row.product_name == "Продукт A"][0]
    assert product_a.category_name == "Категория 1"
    
    # Проверяем, что продукт без категории есть и category_name = NULL
    product_b = [row for row in result_list if row.product_name == "Продукт B без категории"][0]
    assert product_b.category_name is None


def test_product_with_multiple_categories(spark):
    """Тест: продукт с несколькими категориями возвращает несколько записей."""
    # Подготовка тестовых данных
    products_data = [
        (1, "Продукт Мультикатегория")
    ]
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False)
    ])
    products_df = spark.createDataFrame(products_data, products_schema)
    
    categories_data = [
        (1, "Категория 1"),
        (2, "Категория 2"),
        (3, "Категория 3")
    ]
    categories_schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), False)
    ])
    categories_df = spark.createDataFrame(categories_data, categories_schema)
    
    product_categories_data = [
        (1, 1),
        (1, 2),
        (1, 3)
    ]
    product_categories_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("category_id", IntegerType(), False)
    ])
    product_categories_df = spark.createDataFrame(
        product_categories_data,
        product_categories_schema
    )
    
    # Выполнение
    result = get_product_category_pairs(
        products_df,
        categories_df,
        product_categories_df
    )
    
    # Проверка
    result_list = result.collect()
    assert len(result_list) == 3
    
    categories_found = {row.category_name for row in result_list}
    assert categories_found == {"Категория 1", "Категория 2", "Категория 3"}


def test_empty_product_categories(spark):
    """Тест: все продукты без категорий (пустая таблица связей)."""
    # Подготовка тестовых данных
    products_data = [
        (1, "Продукт A"),
        (2, "Продукт B")
    ]
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False)
    ])
    products_df = spark.createDataFrame(products_data, products_schema)
    
    categories_data = [
        (1, "Категория 1")
    ]
    categories_schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), False)
    ])
    categories_df = spark.createDataFrame(categories_data, categories_schema)
    
    # Пустая таблица связей
    product_categories_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("category_id", IntegerType(), False)
    ])
    product_categories_df = spark.createDataFrame([], product_categories_schema)
    
    # Выполнение
    result = get_product_category_pairs(
        products_df,
        categories_df,
        product_categories_df
    )
    
    # Проверка
    result_list = result.collect()
    assert len(result_list) == 2
    
    # Все продукты должны иметь NULL в category_name
    for row in result_list:
        assert row.category_name is None


def test_complex_scenario(spark):
    """Тест: комплексный сценарий с разными комбинациями."""
    # Подготовка тестовых данных
    products_data = [
        (1, "Продукт A"),
        (2, "Продукт B"),
        (3, "Продукт C"),
        (4, "Продукт D")
    ]
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False)
    ])
    products_df = spark.createDataFrame(products_data, products_schema)
    
    categories_data = [
        (1, "Электроника"),
        (2, "Одежда"),
        (3, "Книги")
    ]
    categories_schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), False)
    ])
    categories_df = spark.createDataFrame(categories_data, categories_schema)
    
    product_categories_data = [
        (1, 1),  # Продукт A в Электронике
        (2, 1),  # Продукт B в Электронике
        (2, 2),  # Продукт B также в Одежде
        (3, 3)   # Продукт C в Книгах
        # Продукт D не имеет категорий
    ]
    product_categories_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("category_id", IntegerType(), False)
    ])
    product_categories_df = spark.createDataFrame(
        product_categories_data,
        product_categories_schema
    )
    
    # Выполнение
    result = get_product_category_pairs(
        products_df,
        categories_df,
        product_categories_df
    )
    
    # Проверка
    result_list = result.collect()
    # Ожидаем: A-1, B-1, B-2, C-1, D-NULL = 5 записей
    assert len(result_list) == 5
    
    # Группируем результаты по продуктам
    from collections import defaultdict
    product_to_categories = defaultdict(list)
    for row in result_list:
        product_to_categories[row.product_name].append(row.category_name)
    
    # Проверяем каждый продукт
    assert "Электроника" in product_to_categories["Продукт A"]
    assert len(product_to_categories["Продукт A"]) == 1
    
    assert "Электроника" in product_to_categories["Продукт B"]
    assert "Одежда" in product_to_categories["Продукт B"]
    assert len(product_to_categories["Продукт B"]) == 2
    
    assert "Книги" in product_to_categories["Продукт C"]
    assert len(product_to_categories["Продукт C"]) == 1
    
    assert product_to_categories["Продукт D"] == [None]
    assert len(product_to_categories["Продукт D"]) == 1

