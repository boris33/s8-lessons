from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType
 
# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
kafka_lib_id = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
 
# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}
 
# Замените YOUR_TOPIC_NAME на имя вашего топика
TOPIC_NAME = 'student.topic.cohort15.facerooll'
 
 
def spark_init() -> SparkSession:
    spark = SparkSession.builder \
        .appName("KafkaTopicProcessing") \
        .config("spark.jars.packages", kafka_lib_id) \
        .getOrCreate()
    return spark
 
 
def load_df(spark: SparkSession) -> DataFrame:
    return spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091") \
                .option("subscribe", TOPIC_NAME) \
                .options(**kafka_security_options) \
                .load()
 
 
def transform(df: DataFrame) -> DataFrame:
    # Схема данных
    schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])
 
    df = (df.dropDuplicates(['key', 'timestamp'])
          .withWatermark('timestamp', '5 minute'))
 
    return (df 
            .withColumn('value', f.col('value').cast(StringType())) 
            .withColumn('event', f.from_json(f.col('value'), schema)) 
            .selectExpr('event.*') .withColumn('timestamp', f.from_unixtime(f.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType())) 
            .dropDuplicates(["client_id", "timestamp"]) .withWatermark('timestamp', '10 minutes') )
 
 
spark = spark_init()
 
source_df = load_df(spark)
output_df = transform(source_df)
output_df.printSchema()
 
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