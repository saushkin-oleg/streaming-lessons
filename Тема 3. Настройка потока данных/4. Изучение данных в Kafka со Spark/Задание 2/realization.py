from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# необходимая библиотека с идентификатором в maven
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
}

def spark_init() -> SparkSession:
    spark = (SparkSession.builder
        .appName("Read and deserialize from persist_topic")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate())
    return spark

def load_df(spark: SparkSession) -> DataFrame:
    df = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
        .options(**kafka_security_options)
        .option("subscribe", "persist_topic")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load())
    return df

def transform(df: DataFrame) -> DataFrame:
    # Определяем схему для JSON данных
    value_schema = StructType([
        StructField("subscription_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True)
    ])
    
    # Десериализуем данные:
    # 1. Преобразуем key из binary в string
    # 2. Преобразуем value из binary в string (сохраняем как отдельную колонку)
    # 3. Парсим JSON из value
    # 4. Добавляем все метаданные
    result_df = (df
        .select(
            f.col("key").cast("string").alias("key"),
            f.col("value").cast("string").alias("value"),  # сохраняем строковое представление value
            f.from_json(f.col("value").cast("string"), value_schema).alias("parsed_value"),
            f.col("topic"),
            f.col("partition"),
            f.col("offset"),
            f.col("timestamp"),
            f.col("timestampType")
        )
        .select(
            f.col("key"),
            f.col("value"),  # обязательно включаем в финальный датафрейм
            f.col("parsed_value.subscription_id"),
            f.col("parsed_value.name"),
            f.col("parsed_value.description"),
            f.col("parsed_value.price"),
            f.col("parsed_value.currency"),
            f.col("topic"),
            f.col("partition"),
            f.col("offset"),
            f.col("timestamp"),
            f.col("timestampType")
        )
    )
    
    return result_df

spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)

df.printSchema()
df.show(truncate=False)