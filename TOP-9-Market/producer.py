import merged_dataframes
from confluent_kafka import Producer
import json
import pandas as pd

Bootstrap_server = 'pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092'
API_key = 'XOZNRBMGQYEJNLMX'
API_secret = '99ETgxUsjRP/XYXrMJ9jE3GxOqrsYErw4hRfE3i/VZzcylc6xPivFODrwfi5ovUQ'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'

producer_config = {
    'sasl.mechanism': SSL_MACHENISM,
    'bootstrap.servers':Bootstrap_server,
    'security.protocol': SECURITY_PROTOCOL,
    'sasl.username': API_key,
    'sasl.password': API_secret
}

merged_dataframes.initiate_database()

producer = Producer(producer_config)

def produce_records(df,topic:str):
    for _, row in df.iterrows():
        # Convert the key and value to JSON format
        # key = json.dumps(row['key'])
        # value = json.dumps(row['value'])
        the_message = row.to_dict()    
        message = json.dumps(the_message)
        # Send record to Kafka topic
        producer.produce(topic,value=message)

    # Flush the producer to ensure all messages are sent
    producer.flush()
    
def last_offset_from_checkfile():
    last_offsets = {}
    try:
        with open('checkpoint.txt','r') as f:
            for line in f:
                topic,offset = line.strip().split(',')
                last_offsets[topic] = int(offset)
                #last_offset = int(f.readline())
    except FileNotFoundError:
        pass
    return last_offsets


def update_last_offset_to_checkfile(topic, offset):
    last_offsets[topic] = offset
    with open('checkpoint.txt','w') as f:
        for topic,offset in last_offsets.items():
            f.write(f"{topic},{offset}\n")

open_df = merged_dataframes.merged_dfs('open')
closed_df = merged_dataframes.merged_dfs('close')
volume_df = merged_dataframes.merged_dfs('volume')

dataframes = [open_df,closed_df,volume_df]

topics = ['Top_10_open','Top_10_close','Top_10_volume']
            
last_offsets = last_offset_from_checkfile()

# produce_records(closed_df,'Top_10_close')

for df,topic in zip(dataframes,topics):
    new_records = df[df.index > last_offsets.get(topic,-1)]
    #convert date to string format as it cannot be produced in kafka 
    new_records['date'] = new_records['date'].dt.strftime("%m-%d-%Y")
    if not new_records.empty:
        produce_records(new_records,topic)
        
        last_offsets[topic] = df.index.max()
        
        update_last_offset_to_checkfile(topic,last_offsets[topic])


        
    
    
    
# rough work:
# with open ('checkpoints.txt','r') as f:
# f.write(str(last_offset))
        