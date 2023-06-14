from confluent_kafka import Consumer

consumer_config = {
            'bootstrap.servers':'localhost:9092',
            'group.id':'TempHum-consumer',
            'auto.offset.reset':'latest'
            }

c=Consumer(consumer_config)
print('Kafka Consumer has been initiated...')
# print('Available topics to consume: ', c.list_topics().topics)

def assignment_callback(consumer, topic_partitions):
    for tp in topic_partitions:
        print(tp.topic)
        print(tp.partition)
        print(tp.offset)



c.subscribe(['TempHum'], on_assign=assignment_callback)

while True:
    event = c.poll(timeout=1.0)
    if event is None:
        continue
    if event.error():
        print(f'Received {event.error()} from the topic')
    else:
        key = event.key()
        val = event.value()
        print(f'Received {val} with a key of {key}')
