import requests
import json
from json import dumps, loads
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('kafkaConsumer').config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1").getOrCreate()

response_api = requests.get("https://fakerapi.it/api/v1/products?_quantity=86")

data = response_api.text
parse_json = json.loads(data)

print(parse_json)
with open('response_data.json', 'w') as f:
    json.dump(parse_json, f, indent=4)

producer = KafkaProducer(bootstrap_servers=[
                         'localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

# producer.send('kafkapython1', json.dumps(response_api.json()).encode('utf-8'))

producer.send('topic2',value=response_api.json())
producer.flush()

product_rdd = spark.sparkContext.parallelize([json.dumps(response_api.json())])
schema_df = spark.read.json(product_rdd)
schema_product = schema_df.schema

with open('schema_products.json', 'w') as f:
    json.dump(schema_product.jsonValue(), f, indent=4)

with open('schema_products.json', 'r') as f:
    schema1 = F.StructType.fromJson(json.load(f)) 

print(schema1)