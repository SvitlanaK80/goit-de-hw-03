from pyspark.sql import SparkSession


# Створюємо SparkSession 
spark = SparkSession.builder \
    .appName("DE_HW_03_Data_Analysis") \
    .master("local[*]") \
    .getOrCreate()


# Шляхи до файлів
users_path = r"D:\spark_test\users.csv"
purchases_path = r"D:\spark_test\purchases.csv"
products_path = r"D:\spark_test\products.csv"


# 1. Читаємо дані
users_df = spark.read.csv(users_path, header=True, inferSchema=True)
purchases_df = spark.read.csv(purchases_path, header=True, inferSchema=True)
products_df = spark.read.csv(products_path, header=True, inferSchema=True)


# 1-2. Перевірка
print("=== USERS ===")
users_df.show(5)

print("=== PURCHASES ===")
purchases_df.show(5)

print("=== PRODUCTS ===")
products_df.show(5)

# 2. Очищення даних (видаляємо NULL значення)
users_df_clean = users_df.dropna()
purchases_df_clean = purchases_df.dropna()
products_df_clean = products_df.dropna()


# 2-1. Перевіримо кількість рядків ДО і ПІСЛЯ

print("=== USERS ===")
print("До очищення:", users_df.count())
print("Після очищення:", users_df_clean.count())

print("=== PURCHASES ===")
print("До очищення:", purchases_df.count())
print("Після очищення:", purchases_df_clean.count())

print("=== PRODUCTS ===")
print("До очищення:", products_df.count())
print("Після очищення:", products_df_clean.count())

from pyspark.sql.functions import col, sum


# 3-1. Об'єднуємо purchases і products по product_id
purchases_products_df = purchases_df_clean.join(
    products_df_clean,
    on="product_id",
    how="inner"
)


# 3-2. Додаємо колонку "total_amount" = quantity * price
purchases_products_df = purchases_products_df.withColumn(
    "total_amount",
    col("quantity") * col("price")
)


# 3-3. Групуємо по категорії і рахуємо суму
category_sales = purchases_products_df.groupBy("category") \
    .agg(sum("total_amount").alias("total_sales"))


# 3-4. Виводимо результат
print("=== TOTAL SALES BY CATEGORY ===")
category_sales.show()

from pyspark.sql.functions import col, sum

# 4-1. Об'єднуємо purchases + users
purchases_users_df = purchases_df_clean.join(
    users_df_clean,
    on="user_id",
    how="inner"
)


# 4-2. Фільтруємо користувачів віком 18–25
young_users_df = purchases_users_df.filter(
    (col("age") >= 18) & (col("age") <= 25)
)


# 4-3. Додаємо products
full_df = young_users_df.join(
    products_df_clean,
    on="product_id",
    how="inner"
)


# 4-4. Рахуємо total_amount
full_df = full_df.withColumn(
    "total_amount",
    col("quantity") * col("price")
)


# 4-5. Групуємо по категорії
young_category_sales = full_df.groupBy("category") \
    .agg(sum("total_amount").alias("total_sales"))


# 4-6. Виводимо результат
print("=== SALES (AGE 18-25) BY CATEGORY ===")
young_category_sales.show()

from pyspark.sql.functions import col, round


# 5-1. Загальна сума витрат
total_sum = young_category_sales.agg({"total_sales": "sum"}).collect()[0][0]


# 5-2. Додаємо колонку з відсотком (з округленням)
category_share = young_category_sales.withColumn(
    "percentage",
    round((col("total_sales") / total_sum) * 100, 2)
)


# 5-3. Виводимо результат
print("=== CATEGORY SHARE (AGE 18-25) ===")
category_share.show()

# 6-1. Сортуємо по відсотку (спадання)
top_categories = category_share.orderBy(
    col("percentage").desc()
)


# 6-2. Беремо ТОП-3
top_3_categories = top_categories.limit(3)


# 6-3. Виводимо результат
print("=== TOP 3 CATEGORIES (AGE 18-25) ===")
top_3_categories.show()

# 5. Закриваємо Spark
spark.stop()