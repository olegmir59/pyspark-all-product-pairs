"""
Пример использования модуля product_categories.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from product_categories import get_product_category_pairs


def main():
    # Создание SparkSession
    spark = SparkSession.builder \
        .appName("ProductCategoriesExample") \
        .master("local[*]") \
        .getOrCreate()
    
    # Создание тестовых данных
    print("=" * 80)
    print("ПРИМЕР ИСПОЛЬЗОВАНИЯ: Продукты и Категории")
    print("=" * 80)
    
    # Продукты
    products_data = [
        (1, "Ноутбук Dell XPS"),
        (2, "Смартфон iPhone"),
        (3, "Книга 'Python для начинающих'"),
        (4, "Джинсы Levi's"),
        (5, "Наушники Sony"),
        (6, "Продукт без категории")
    ]
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False)
    ])
    products_df = spark.createDataFrame(products_data, products_schema)
    
    print("\nПродукты:")
    products_df.show(truncate=False)
    
    # Категории
    categories_data = [
        (1, "Электроника"),
        (2, "Одежда"),
        (3, "Книги"),
        (4, "Аксессуары")
    ]
    categories_schema = StructType([
        StructField("category_id", IntegerType(), False),
        StructField("category_name", StringType(), False)
    ])
    categories_df = spark.createDataFrame(categories_data, categories_schema)
    
    print("\nКатегории:")
    categories_df.show(truncate=False)
    
    # Связи продуктов и категорий
    product_categories_data = [
        (1, 1),  # Ноутбук - Электроника
        (2, 1),  # iPhone - Электроника
        (2, 4),  # iPhone - Аксессуары (может быть в нескольких категориях)
        (3, 3),  # Книга - Книги
        (4, 2),  # Джинсы - Одежда
        (5, 1),  # Наушники - Электроника
        (5, 4)   # Наушники - Аксессуары
        # Продукт 6 не имеет категорий
    ]
    product_categories_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("category_id", IntegerType(), False)
    ])
    product_categories_df = spark.createDataFrame(
        product_categories_data,
        product_categories_schema
    )
    
    print("\nСвязи продуктов и категорий:")
    product_categories_df.show(truncate=False)
    
    # Получение результата
    result = get_product_category_pairs(
        products_df,
        categories_df,
        product_categories_df
    )
    
    print("\n" + "=" * 80)
    print("РЕЗУЛЬТАТ: Все пары 'Продукт - Категория' + Продукты без категорий")
    print("=" * 80)
    result.show(truncate=False)
    
    # Дополнительная аналитика
    print("\nПродукты без категорий (category_name = NULL):")
    result.filter(result.category_name.isNull()).show(truncate=False)
    
    print("\nКоличество категорий для каждого продукта:")
    from pyspark.sql.functions import count, when, col
    result.groupBy("product_name") \
        .agg(
            count(when(col("category_name").isNotNull(), True)).alias("категорий")
        ) \
        .orderBy("product_name") \
        .show(truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    main()

