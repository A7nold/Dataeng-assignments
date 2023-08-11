import merged_dataframes
from confluent_kafka import Consumer,KafkaError
import json
import pandas as pd
import boto3

#connect to confluent kafka server
Bootstrap_server = 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092'
API_key = 'XOZNRBMGQYEJNLMX'
API_secret = '99ETgxUsjRP/XYXrMJ9jE3GxOqrsYErw4hRfE3i/VZzcylc6xPivFODrwfi5ovUQ'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'

consumer_config = {
    'sasl.mechanism': SSL_MACHENISM,
    'bootstrap.servers':Bootstrap_server,
    'security.protocol': SECURITY_PROTOCOL,
    'sasl.username': API_key,
    'sasl.password': API_secret,
    'group.id':'group_top_20_stocks',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

topics = ['Top_10_open','Top_10_close','Top_10_volume']

#subscribe to the topic in kafka
consumer.subscribe(topics)

list_messages_topics = {topic: [] for topic in topics}

#to print and save messages
try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition for topic '{msg.topic()}'")
            else:
                print(f"Consumer error: {msg.error()}")
            continue

        # Get the topic and value of the message
        topic = msg.topic()
        value = msg.value().decode('utf-8')
        
        #convert the message value which is in json format to python dictionary and store it in list_messages_topic for each topic
        list_messages_topics[topic].append(json.loads(value))

        # Print the message on the output terminal
        print(f"Topic: {topic}, Message: {value}")

        consumer.commit()
        
except KeyboardInterrupt:
    print("Consumer process interrupted by the user.")
finally:
    # Close the consumer when done
    consumer.close()

#connect to S3 Bucket in aws
access_key = 'AKIAVBV2DE56S6KCTHPS'
Secret_access_key = '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'
region = 'ap-south-1'

bucket_name = 'top-9-stock-p2'
#create a boto3 client to interact with aws
s3_client = boto3.client('s3',aws_access_key_id = access_key,aws_secret_access_key = Secret_access_key,region_name = region) 

#messages in  three different topic are stored in three dataframes and then sent to 3 locations in S3
for topic, messages in list_messages_topics.items():    
    df = pd.DataFrame(messages)
    df_csv = df.to_csv(index=False)
    object_key = f'rawdata/ns/{topic}.csv' 
    s3_client.put_object(Bucket = bucket_name,Key = object_key,Body = df_csv)
    print(f"Data uploaded to S3 bucket: s3://{bucket_name}/{object_key}")



            
            
            
            
#rough:
    # for topic, messages in list_messages_topics.items:    
    
#     #consumer_df = pd.DataFrame(list_messages)
#     df = pd.DataFrame(messages)
#     df_csv = df.to_csv(index=False)

#     open_records_csv = consumer_df.to_csv(index= False)

#print(consumer_df)

# merged_dataframes.initiate_database()

# s3_df = merged_dataframes.merged_dfs('open')

#print(consumer_df)
# df_csv = consumer_df.to_csv(index=False)
#open_records_csv = consumer_df.to_csv(index= False)
#consumer_df = pd.DataFrame(list_messages)
#open_records_csv = s3_df.to_csv(index= False)