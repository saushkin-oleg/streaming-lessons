from datetime import datetime
from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

# Укажите ваши топики - ЗАМЕНИТЕ НА СВОИ
TOPIC_NAME_91 = 'student.topic.cohort13.ocmarus.out'  # выходной топик
TOPIC_NAME_IN = 'student.topic.cohort13.ocmarus'      # входной топик


def spark_init(test_name) -> SparkSession:
    spark = (SparkSession.builder
        .appName(test_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.2")
        .getOrCreate())
    return spark


postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}


def read_marketing(spark: SparkSession) -> DataFrame:
    jdbc_url = "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de"
    
    marketing_df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "public.marketing_companies")
        .option("user", postgresql_settings["user"])
        .option("password", postgresql_settings["password"])
        .option("driver", "org.postgresql.Driver")
        .load())
    
    return marketing_df


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";'
}


def read_client_stream(spark: SparkSession) -> DataFrame:
    raw_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
        .options(**kafka_security_options)
        .option("subscribe", TOPIC_NAME_IN)
        .option("startingOffsets", "earliest")
        .load())
    
    client_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])
    
    client_df = (raw_df
        .select(
            f.from_json(f.col("value").cast("string"), client_schema).alias("data"),
            f.col("offset")
        )
        .select(
            f.col("data.client_id"),
            f.col("data.timestamp"),
            f.col("data.lat"),
            f.col("data.lon"),
            f.col("offset")
        )
        .withWatermark("timestamp", "10 minutes")
        .dropDuplicates(["client_id", "timestamp"])
    )
    
    return client_df


def join(user_df, marketing_df) -> DataFrame:
    # Выполняем CROSS JOIN
    joined_df = user_df.crossJoin(marketing_df)
    
    # Вычисляем a по формуле гаверсинуса
    joined_df = joined_df.withColumn(
        "a",
        f.pow(f.sin(f.radians(marketing_df.point_lat - user_df.lat) / 2), 2) +
        f.cos(f.radians(user_df.lat)) * f.cos(f.radians(marketing_df.point_lat)) *
        f.pow(f.sin(f.radians(marketing_df.point_lon - user_df.lon) / 2), 2)
    )
    
    # Вычисляем расстояние
    joined_df = joined_df.withColumn(
        "distance",
        f.atan2(f.sqrt(f.col("a")), f.sqrt(-f.col("a") + 1)) * 12742000
    )
    
    # Приводим к IntegerType
    joined_df = joined_df.withColumn(
        "distance",
        f.col("distance").cast(IntegerType())
    )
    
    # Создаем все необходимые колонки для выходного сообщения
    joined_df = joined_df.withColumn("adv_campaign_id", marketing_df.id)
    joined_df = joined_df.withColumn("adv_campaign_name", marketing_df.name)
    joined_df = joined_df.withColumn("adv_campaign_description", marketing_df.description)
    joined_df = joined_df.withColumn("adv_campaign_start_time", marketing_df.start_time)
    joined_df = joined_df.withColumn("adv_campaign_end_time", marketing_df.end_time)
    joined_df = joined_df.withColumn("adv_campaign_point_lat", marketing_df.point_lat)
    joined_df = joined_df.withColumn("adv_campaign_point_lon", marketing_df.point_lon)
    
    # client_id с substring (используем строку 'client_id', не колонку)
    joined_df = joined_df.withColumn(
        "client_id",
        f.substring("client_id", 0, 6)
    )
    
    # created_at
    joined_df = joined_df.withColumn(
        "created_at",
        f.lit(datetime.now())
    )
    
    # Фильтруем по расстоянию (до 1 км) - ВАЖНО для теста
    filtered_df = joined_df.filter(f.col("distance") <= 1000)
    
    # Подготавливаем для Kafka: оборачиваем все поля в JSON для value
    result_df = filtered_df.select(
        f.to_json(
            f.struct(
                f.col("client_id"),
                f.col("distance"),
                f.col("adv_campaign_id"),
                f.col("adv_campaign_name"),
                f.col("adv_campaign_description"),
                f.col("adv_campaign_start_time"),
                f.col("adv_campaign_end_time"),
                f.col("adv_campaign_point_lat"),
                f.col("adv_campaign_point_lon"),
                f.col("created_at")
            )
        ).alias("value")
    )
    
    return result_df


def run_query(df):
    return (df
            .writeStream
            .outputMode("append")
            .format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .options(**kafka_security_options)
            .option("topic", TOPIC_NAME_91)
            .option("checkpointLocation", "test_query")
            .trigger(processingTime="15 seconds")
            .start())


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    output = join(client_stream, marketing_df)
    query = run_query(output)

    while query.isActive:
        print(f"query information: runId={query.runId}, "
              f"status is {query.status}, "
              f"recent progress={query.recentProgress}")
        sleep(30)

    query.awaitTermination()