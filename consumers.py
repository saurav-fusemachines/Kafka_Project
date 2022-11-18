import json
import requests
from json import dumps, loads
# dumps converts python object to json string
# loads converts json string to python object
from kafka import KafkaConsumer


from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField,ArrayType,LongType,StringType,DoubleType
from pyspark.sql.functions import split, explode,col

spark = SparkSession.builder.appName('products_consumer')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")\
    .getOrCreate()


consumer= KafkaConsumer('topic2', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                              enable_auto_commit=True, value_deserializer=lambda x: loads(x.decode('utf-8')))


# Reading from Kafka
df= spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic1") \
    .option("startingOffsets", "earliest") \
    .load()

# # Converting the value column from binary to string inroder to read the data
df = df.selectExpr("CAST(value AS STRING)")


# schema_product = "status STRING, code STRING, total STRING, id int, name STRING, description STRING, ean STRING, upc STRING, image STRING, net_price STRING, taxes STRING, price STRING, categories STRING, tags STRING"

product_schemas = StructType([StructField('code', LongType(), True), StructField('data', ArrayType(StructType([StructField('categories', ArrayType(LongType(), True), True), 
StructField('description', StringType(), True), StructField('ean', StringType(), True), StructField('id', LongType(), True), 
StructField('image', StringType(), True), StructField('images', ArrayType(StructType([StructField('description', StringType(), True), 
StructField('title', StringType(), True), StructField('url', StringType(), True)]), True), True), StructField('name', StringType(), True), 
StructField('net_price', DoubleType(), True), StructField('price', StringType(), True), StructField('tags', ArrayType(StringType(), True), True), 
StructField('taxes', LongType(), True), StructField('upc', StringType(), True)]), True), True), StructField('status', StringType(), True), 
StructField('total', LongType(), True)])

# explode to columns from json
df_product = df.select(F.from_json(F.col("value"), product_schemas).alias(
    "data")).select("data.*")

df1=df_product.select("*", explode("data").alias("exploded_data"))

df2 = df1.select("*",F.col("exploded_data.*")).drop("code","status","total","data","exploded_data")


# Writing the stream to the console
query = df2 \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path","stream-json")    \
    .option("checkpointLocation","checkpoint")    \
    .option("header","true")    \
    .start()

query.awaitTermination()

# streamed_df = spark.read.json('/home/saurav/Documents/Fusemachines/Kafka_Project/stream-json/part-00000-ae2467cf-e927-4438-be51-1f5e78e7e7d9-c000.json')
# streamed_df.select("*").show()

# streamed_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
#                                    dbtable='PRODUCTS', user='fusemachines', password='hello123').mode('overwrite').save()

