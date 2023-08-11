from confluent_kafka import Consumer,KafkaError
import json


Bootstrap_server = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
API_key = '3EHVO4FB3WJ6ISKR'
API_secret = 'lnkLzfWTsiYxtdvNLaJCtatn4RlOIWSehkrRyLt6bhKkFQts6evtSGmV39oPRfHr'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'

Kafka_topic = 'Topic_P1'

conf = {
    'sasl.mechanism': SSL_MACHENISM,
    'bootstrap.servers':Bootstrap_server,
    'security.protocol': SECURITY_PROTOCOL,
    'sasl.username': API_key,
    'sasl.password': API_secret,            
    'group.id':'group1',   
    'auto.offset.reset':'earliest'
}

consumer = Consumer(conf)

consumer.subscribe(['Topic_P1'])

while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    
    if msg.error():
        print('Reached end of partition, continuing...')
        continue
        
    value = msg.value().decode('utf-8')
    
    print(f'message received:{value}')

consumer.close()
        