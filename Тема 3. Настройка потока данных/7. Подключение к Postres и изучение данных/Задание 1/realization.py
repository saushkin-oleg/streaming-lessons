from pyspark.sql import SparkSession

# Создаем Spark сессию с необходимым JDBC драйвером
spark = (
    SparkSession.builder
    .appName("PostgreSQL Connection")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.4.2")
    .getOrCreate()
)

# Параметры подключения к PostgreSQL
jdbc_url = "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de"
connection_properties = {
    "user": "student",
    "password": "de-student",
    "driver": "org.postgresql.Driver"
}

# Читаем данные из таблицы marketing_companies
df = (spark.read
      .format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", "public.marketing_companies")
      .option("user", connection_properties["user"])
      .option("password", connection_properties["password"])
      .option("driver", connection_properties["driver"])
      .load())

# Подсчитываем количество строк
row_count = df.count()
print(f"Количество строк в таблице marketing_companies: {row_count}")