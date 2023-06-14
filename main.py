from confluent_kafka import Producer
import json
import time
import logging
from parq_to_pandas import *

kafka_topic_name= 'TempHum'
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


p=Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')

def delivery_callback(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)

def main():
    pd=Parq_to_pandas()
    for i in range(len(pd)):
        print("The loop starts")
        start_time = time.time()
        data={
           'id': int(pd.iat[i,0]),
           'temperature': int(pd.iat[i,1]),
           'humidity': int(pd.iat[i,2]),
           'date_time': pd.iat[i,3],
           }
        print("--- Loop ends: %s seconds ---" % (time.time() - start_time))
        m=json.dumps(data)
        p.poll(0.1)
        p.produce(topic=kafka_topic_name, value=m.encode('utf-8') ,on_delivery=delivery_callback)
        #p.produce('TempHum', m.encode('utf-8'),callback=delivery_callback)
        p.flush()
        print("--- Flush ends ends: %s seconds ---" % (time.time() - start_time))

start_time = time.time()
main()
print("--- %s seconds ---" % (time.time() - start_time))