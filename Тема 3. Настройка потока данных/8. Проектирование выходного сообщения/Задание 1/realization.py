from datetime import datetime
 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
 
 
KAFKA_LIB_ID = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
TOPIC_NAME = 'student.topic.cohort15.facerooll'
 
 
def spark_init(test_name) -> SparkSession:
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
 
    spark = (
        SparkSession.builder.appName(test_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
    )
 
    return spark
 
 
postgresql_settings = {
    'user': 'student',
    'password': 'de-student',
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'dbtable': 'public.marketing_companies',
    'driver': 'org.postgresql.Driver'
}
 
 
def read_marketing(spark: SparkSession) -> DataFrame:
    return spark.read \
                .format('jdbc') \
                .option('url', postgresql_settings['url']) \
                .option('driver', postgresql_settings['driver']) \
                .option('dbtable', postgresql_settings['dbtable']) \
                .option('user', postgresql_settings['user']) \
                .option('password', postgresql_settings['password']) \
                .load()
 
 
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}
 
 
schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True)
])
 
 
def read_client_stream(spark: SparkSession) -> DataFrame:
    return spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091") \
                .option("subscribe", "student.topic.cohort15.facerooll") \
                .options(**kafka_security_options) \
                .load() \
                .selectExpr("CAST(value AS STRING)", "offset") \
                .withColumn("data", f.from_json("value", schema)) \
                .select("data.*", "offset")
 
 
def join(user_df, marketing_df) -> DataFrame:
    result = user_df.crossJoin(marketing_df)
    return result.select(
        user_df.client_id,
        marketing_df.id.alias("adv_campaign_id"),
        marketing_df.name.alias("adv_campaign_name"),
        marketing_df.description.alias("adv_campaign_description"),
        marketing_df.start_time.alias("adv_campaign_start_time"),
        marketing_df.end_time.alias("adv_campaign_end_time"),
        marketing_df.point_lat.alias("adv_campaign_point_lat"),
        marketing_df.point_lon.alias("adv_campaign_point_lon"),
        user_df.timestamp.alias("created_at"),
        user_df.offset
    )
 
 
if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)
 
    query = (result
            .writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .start())
    query.awaitTermination()