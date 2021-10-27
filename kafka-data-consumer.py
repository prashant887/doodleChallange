from kafka import KafkaProducer,KafkaConsumer
import json
import time
from datetime import datetime

brokers='localhost:9092'
topic='events'
sleep_time=300
offset='latest'
consumer = KafkaConsumer(bootstrap_servers=brokers, auto_offset_reset=offset,consumer_timeout_ms=1000)
consumer.subscribe([topic])

print("uid,ts,timstmp")

while(True):
    for message in consumer:
        #print(message)
        d=json.loads(message.value)
        print(d.keys())
        uid=d.get('uid','NA')
        ts=int(d.get('ts',0))
        timstmp=datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        print("{},{},{}".format(uid,ts,timstmp))
        #print("\n \n")
        #time.sleep(30)