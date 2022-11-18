import requests
import json
from json import dumps, loads
from kafka import KafkaConsumer

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('kafkaConsumer').config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1").getOrCreate()

response_api = requests.get("https://fakerapi.it/api/v1/products?_quantity=86")

# data = response_api.text
# parse_json = json.loads(data)
# print(parse_json)

consumer = KafkaConsumer('topic2',bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                              enable_auto_commit=True, value_deserializer=lambda x: loads(x.decode('utf-8')))


for msg in consumer:
    print(msg)
    with open('data.json', 'w') as f:
        json.dump(msg.value, f)