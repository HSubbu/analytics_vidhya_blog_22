from kafka import KafkaProducer
import json
import time 

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                    value_serializer=lambda v: json.dumps(v).encode('ascii'),
                    key_serializer=lambda v: json.dumps(v).encode('ascii')
                    )
topic_name='test-topic'
#send an event(message)
print(f"Sending event to {topic_name}")
producer.send(
 topic_name,
 key={"id":1},
 value={"name":"John", "Item":"Iphone"}
)
time.sleep(3)
producer.flush()
#next messages
print(f"Sending event to {topic_name}")
producer.send(
 topic_name,
 key={"id":2},
 value={"name":"Mary", "Item":"Laptop"}
)
time.sleep(3)
producer.flush()
print(f"Sending event to {topic_name}")
producer.send(
 topic_name,
 key={"id":3},
 value={"name":"Jim", "Item":"Fitness Band"}
)
time.sleep(3)
producer.flush()
print(f"Sending event to {topic_name}")
producer.send(
 topic_name,
 key={"id":4},
 value={"name":"Lisa", "Item":"watch"}
)
time.sleep(3)
producer.flush()