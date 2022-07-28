from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                    value_serializer=lambda v: json.dumps(v).encode('ascii'),
                    key_serializer=lambda v: json.dumps(v).encode('ascii')
                    )
topic_name='test-topic-partitioned'
print("Sending messages to partitions....")
#send an event(message)
producer.send(topic_name,
              key={"id":0},
              value={"name":"Frank", "Item":"Shoe"},
              partition=0
             )
producer.send(topic_name,
              key={"id":1},
              value={"name":"John", "Item":"Shirt"},
              partition=1
             )
time.sleep(2)
producer.flush()


producer.send(topic_name,
              key={"id":0},
              value={"name":"Mark", "Item":"Pens"},
              
             )
producer.send(topic_name,
              key={"id":1},
              value={"name":"Jim", "Item":"Printer"},
              
             )
time.sleep(2)
producer.flush()