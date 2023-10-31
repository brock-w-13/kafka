import time
import random
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer

TOPIC_NAME = "Testing"

producer = KafkaProducer(
    bootstrap_servers=f"kafka-3b87ff22-broclee73-bbc3.a.aivencloud.com:18969",
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
    value_serializer=lambda x: 
    	dumps(x).encode('utf-8')
)

for e in range(10):
    id_num = random.randint(1000,9999)
    price = random.randint(10,20)
    date_time = str(datetime.now())
    data = {'ID' : id_num, 'stock' : 'Aiven', 'price' : price, 'date_time' : date_time}
    producer.send(TOPIC_NAME,  value=data)
    time.sleep(1)

producer.close()
