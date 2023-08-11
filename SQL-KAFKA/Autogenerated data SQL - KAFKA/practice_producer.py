import mysql.connector as conn
import random
import string
from uuid import uuid4
import time
from datetime import datetime
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import pandas as pd
from confluent_kafka import Producer
import json

mydb = conn.Connect(host = "localhost", port="3306", user = "root", passwd = "root")
cursor = mydb.cursor()
cursor.execute("use sqltable")


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
    'sasl.password': API_secret               
}

producer = Producer(conf)

columns=['name', 'age', 'email', 'created_at']

def delivery_report(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {}'.format(msg.value(), msg.topic()))
 

# class Emp:   
#     def __init__(self,record:dict):
#         for k,v in record.items():
#             setattr(self,k,v)
        
#         self.record=record
   
#     @staticmethod
#     def dict_to_car(data:dict,ctx):
#         return Car(record=data)

#     def __str__(self):
#         return f"{self.record}"

serializer = StringSerializer('utf_8')
df = pd.read_sql('select * from studentinfo', con=mydb)
# emps:list[Emp]=[]
for index,row in df.iterrows():
    row['created_at'] = row['created_at'].strftime('%Y-%m-%d %H:%M:%S')
    message = row.to_dict()
    json_message = json.dumps(message)
    producer.produce(topic = 'Topic_P1',value = json_message, on_delivery = delivery_report)
    #Producer.poll(0)
  
producer.flush()  