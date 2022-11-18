import requests
import json
from json import dumps, loads
from kafka import KafkaConsumer, KafkaProducer

response_api = requests.get("https://fakerapi.it/api/v1/products?_quantity=86")

data = response_api.text
parse_json = json.loads(data)

print(parse_json)

producer = KafkaProducer(bootstrap_servers=[
                         'localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

# producer.send('kafkapython1', json.dumps(response_api.json()).encode('utf-8'))


producer.send('topic',value=response_api.json())
producer.flush()
with open("example.json","w") as writefile:
    writefile.write(response_api.text)



