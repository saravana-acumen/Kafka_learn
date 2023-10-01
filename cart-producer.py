from kafka import KafkaProducer
from datetime import datetime
import json
import time
import random

# define the on success and on error callback functions
def on_success(record):    
    print(record.topic)
    print(record.partition)
    print(record.offset)

def on_error(excp):
    raise Exception(excp)

class CartItems:
    def __init__(self,user,product,quantity,cartno):
        self.event_type = 'InCart'
        self.user = user
        self.cartid = cartno
        self.product = product
        self.quantity = quantity
        self.timestamp = datetime.now().timestamp() 

def subscribe(producer,data):
    key = bytes(datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), 'utf-8')
    producer.send('InCart', value=data, key=key).add_callback(on_success).add_errback(on_error)
    producer.flush()

    
users = ['user1','user2','user3','user4','user5','user6','user7','user8','user9','user10']
products = ['product1','product2','product3','product4','product5','product6','product7','product8','product9','product10']

# create a producer. broker is running on localhost
producer = KafkaProducer(retries=5, bootstrap_servers=['localhost:9092']
                         ,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i,user in enumerate(users):
    for j,product in enumerate(products):
        vp = CartItems(user,product,j+1,random.randrange(1,10))
        message = json.dumps(vp.__dict__)
        subscribe(producer,message)
        if i == j:
            break;
        time.sleep(30)    
# send the message to fintechexplained-topic
# # arr = bytes("hello world", 'utf-8')
# person = '{"name": "Bob", "languages": ["English", "French"]}'
# arr = json.loads(person)
# key = bytes(datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), 'utf-8')
# producer.send('quickstart-events', value=arr, key=key).add_callback(on_success).add_errback(on_error)
# # block until all async messages are sent
# producer.flush()