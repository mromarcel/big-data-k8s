import logging as log
import json
import os
import time
import argparse
import requests
import numpy    as np
from faker      import Faker
from sqlalchemy import create_engine
from time import sleep
from datetime   import datetime
from dotenv     import load_dotenv
from kafka import KafkaProducer

'''
    id	1924
    uid	"8e8c5d38-087d-4079-84f8-cf522b220280"
    plan	"Student"
    status	"Pending"
    payment_method	"Google Pay"
    subscription_term	"Biennal"
    payment_term	"Full subscription"
'''

# api-endpoint
URL_subscription = 'https://random-data-api.com/api/subscription/random_subscription'

fake = Faker(["pt_BR"])
PARAMS = {'size': 1}

log.basicConfig(format="%(asctime)s (%(levelname)s) %(message)s", datefmt="[%Y-%m-%d %H:%M:%S]", level=log.INFO)

# init application

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generate fake data...')

    parser.add_argument("topic", type=str, default='topic', help="topic name")
        
    parser.add_argument("bootstrap_server", type=str, default="localhost",
                        help="Kafka broker endpoint")
    
    parser.add_argument('-n', type=int, default=1,
                        help='sample size')

    parser.add_argument('--interval', type=int, default=3 ,
                        help='interval of generating fake data in seconds')
                        
    parser.add_argument("--level", type=int, default=0, help='debug level 0,1,2')

    args = parser.parse_args()
    
    log.debug("Parameter: "
    +"\n        TOPIC: "+args.topic
    +"\n        SERVER: "+args.bootstrap_server
    +"\n        Count: "+str(args.n)
    +"\n        Intervalo: "+str(args.interval)
)

    #-----------------------------------------------------------------    
if args.level == 1:
    log.info("Starting producer ...")

producer = KafkaProducer(bootstrap_servers=[args.bootstrap_server],value_serializer=lambda x:json.dumps(x).encode("utf-8"))
count = 0

while count < args.n :
    data = json.loads("{}")
    #Subscription
    response_subscription = requests.get(url = URL_subscription, params = PARAMS)
    json_obj_subscription = response_subscription.json()
    data = json_obj_subscription[0]
    
    #Brazilian User
    data["user_name"] = fake.name()
    data["birth_date"] = str(fake.date_of_birth()) 
    data["gender"] = np.random.choice(["M", "F"], p=[0.5, 0.5]) 
    
    #Brasilian Address
    data["state_UF"] = fake.estado_sigla()
    data["address"]= fake.address().replace('\r', ' ').replace('\n', ' ')
    
    #Add datetime
    data["createdAt"]= str(fake.date_time_between(start_date="-1y", end_date="now"))
    data["updatedAt"] = round(time.time() * 1000)

    
    count += 1

    if args.level == 2:
        log.info("\n"+("---" * 20))
        print("[item: " + str(count) + "]\n")
        for key,value in data.items():
            print((" " * 3) + "'" + str(key) + "'" + " : " + "'" +str(value) + "'" )
            
    print("\n"+("-" * 50))
 
    
    try:
        producer.send(args.topic, value=data)
    except (Exception) as error:
        log.error(error)
       
    sleep(args.interval if args.interval is not None else 0)
    
