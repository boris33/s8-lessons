from pyspark.sql import SparkSession

spark = (
        SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0") \
        .getOrCreate()
        )

postgresql_settings = {
    'user': 'student',
    'password': 'de-student',
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'dbtable': 'public.marketing_companies',
    'driver': 'org.postgresql.Driver'
}

df = (spark.read
        .format('jdbc') \
        .option('url', postgresql_settings['url']) \
        .option('driver', postgresql_settings['driver']) \
        .option('dbtable', postgresql_settings['dbtable']) \
        .option('user', postgresql_settings['user']) \
        .option('password', postgresql_settings['password']) \
        .load())
df.count()
