"""
PySpark модуль для работы с продуктами и категориями.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def get_product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_categories_df: DataFrame
) -> DataFrame:
    """
    Возвращает датафрейм со всеми парами "Имя продукта - Имя категории"
    и всеми продуктами без категорий.
    
    Args:
        products_df: DataFrame с колонками [product_id, product_name]
        categories_df: DataFrame с колонками [category_id, category_name]
        product_categories_df: DataFrame с колонками [product_id, category_id]
    
    Returns:
        DataFrame с колонками [product_name, category_name]
        Для продуктов без категорий category_name будет NULL
    """
    # Делаем LEFT JOIN продуктов со связями (чтобы сохранить продукты без категорий)
    result = products_df.join(
        product_categories_df,
        products_df.product_id == product_categories_df.product_id,
        "left"
    )
    
    # Затем LEFT JOIN с категориями для получения имен категорий
    result = result.join(
        categories_df,
        product_categories_df.category_id == categories_df.category_id,
        "left"
    )
    
    # Выбираем только нужные колонки
    result = result.select(
        products_df.product_name.alias("product_name"),
        categories_df.category_name.alias("category_name")
    )
    
    return result

