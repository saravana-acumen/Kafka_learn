from kafka import KafkaConsumer
from datetime import datetime
import json 
import time
from google.cloud import bigquery

# consumer = KafkaConsumer('quickstart-events', auto_offset_reset='earliest'
#                          group_id='my-group', enable_auto_commit=False, bootstrap_servers=['localhost:9092'],
#            value_deserializer=lambda m: json.loads(m.decode('utf-8')))

def insert_into_table(rows):
    client = bigquery.Client()
    table_id = "bcs-csw-int-stage-np.adf_poc.product_view"
    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def consumer(topic,group):
    consumer = None
    try:
        consumer = KafkaConsumer(topic, group_id=group, auto_offset_reset='earliest',
                            bootstrap_servers=['localhost:9092'],api_version=(0,10),consumer_timeout_ms=10000,
                            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                                )
        
    except Exception as ex:
        print(str(ex))

    return consumer

rows_to_insert = [
    {"event_type": "ViewProduct", "user": "user1", "product": "product1", "timestamp": 1696039246.874144}    
]    
# insert_into_table(rows_to_insert)
rows_to_insert = []
while True:
    consumer = consumer("product-view","productview-group4")
    if consumer:
        for message in consumer:
            # print(message.topic.decode)
            # print(message.partition)
            print(message.offset)
            # print(message.key)
            print(message.value)
            rows_to_insert.append(json.loads(message.value))

            if len(rows_to_insert) == 10:
                insert_into_table(rows_to_insert)
                rows_to_insert=[]

        if len(rows_to_insert) > 0:
            insert_into_table(rows_to_insert)
            rows_to_insert=[]
    consumer.close()
    time.sleep(10)     
    
    
