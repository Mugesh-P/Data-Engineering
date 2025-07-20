from kafka import KafkaConsumer
from time import sleep
import json
from json import loads, dumps
from s3fs import S3FileSystem

consumer = KafkaConsumer(
  'Consumer-name',
  bootstrap_servers = [':8080'], #system IP
  value_deserializer = lambda x: loads(x.decode('utf-8'))
)

for c in consumer:
  print(c.value)

s3 = S3FileSystem()

for count, i in enumerate(consumer):
  with s3.open("paste-s3-link", 'w') as file:
    json.dump(i.value, file)
