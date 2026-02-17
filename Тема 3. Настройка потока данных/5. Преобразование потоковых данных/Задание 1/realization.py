from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

# необходимая библиотека с идентификатором в maven
kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
}

# Укажите ваш топик
TOPIC_NAME = 'student.topic.cohort1.ocmarus'  # замените на ваш реальный топик

def spark_init() -> SparkSession:
    spark = (SparkSession.builder
        .appName("Structured Streaming from Kafka")
        .config("spark.jars.packages", kafka_lib_id)
        .getOrCreate())
    return spark

def load_df(spark: SparkSession) -> DataFrame:
    df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
        .options(**kafka_security_options)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load())
    return df

def transform(df: DataFrame) -> DataFrame:
    # Определяем схему для JSON данных
    value_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])
    
    # Трансформируем данные
    result_df = (df
        .select(f.from_json(f.col("value").cast("string"), value_schema).alias("data"))
        .select(
            f.col("data.client_id"),
            f.col("data.timestamp"),
            f.col("data.lat"),
            f.col("data.lon")
        )
    )
    
    return result_df

spark = spark_init()

source_df = load_df(spark)
output_df = transform(source_df)

query = (output_df
         .writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .trigger(once=True)
         .start())

try:
    query.awaitTermination()
finally:
    query.stop()