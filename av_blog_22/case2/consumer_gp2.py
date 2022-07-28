from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], 
                         auto_offset_reset='latest',
                         group_id='my_test_gp_2')
topic_name='test-topic'
print("Available Kafka topcs are ..",consumer.topics())

consumer.subscribe(topics=[topic_name])
consumer.subscription()                         
for message in consumer:    
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))