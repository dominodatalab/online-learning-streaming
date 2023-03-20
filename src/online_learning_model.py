# Import dependencies
import random
from confluent_kafka import TopicPartition,Producer,Consumer
import os


import logging
logging.basicConfig(level=logging.INFO)


feature_topic=os.environ['FEATURE_TOPIC_NAME']
inference_group_id = os.environ['INFERENCE_GROUP_ID']
prediction_topic_prefix = os.environ['PREDICTION_TOPIC_PREFIX']
prediction_topic_suffix = os.environ['PREDICTION_TOPIC_SUFFIX']
PREDICTION_TOPIC=f'{prediction_topic_prefix}{prediction_topic_suffix}'
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('kafka_bootstrap_servers')
KAFKA_USER_NAME = os.environ.get('kafka_username')
KAFKA_PASSWORD = os.environ.get('kafka_password')



import time
import os
import json
from confluent_kafka import TopicPartition,Producer,Consumer
import certifi
import threading
from river import metrics
from river import tree
from river import ensemble
from river import evaluate
from river import compose
from river import naive_bayes
from time import time

from river import anomaly
from river import compose
from river import datasets
from river import metrics
from river import preprocessing



latest_version=-1
models = {}



def return_range(strg, loc, toks):
    if len(toks)==1:
        return int(toks[0])
    else:
        return range(int(toks[0]), int(toks[1])+1)



model = tree.HoeffdingAdaptiveTreeClassifier(grace_period=100,  delta=1e-5, leaf_prediction='nb', nb_threshold=10,seed=0)    
cnt = 0
def consume_features(group_id:str):  
    global cnt
    global latest_version
    global model
    global KAFKA_USER_NAME
    global KAFKA_PASSWORD
    global KAFKA_BOOTSTRAP_SERVERS
    print('Initialized ')
    #Only one model instance recieves the message (Each has the SAME consumer group)
    features_consumer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'sasl.username': KAFKA_USER_NAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'ssl.ca.location': certifi.where(),
                     'group.id': group_id,
                     'enable.auto.commit': False,
                     'auto.offset.reset': 'earliest'}
    features_consumer = Consumer(features_consumer_conf)    
    print(f'\nNow subscribing to features topic {feature_topic}')
    features_consumer.subscribe([feature_topic])

    client_id='client-1'
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'sasl.username': KAFKA_USER_NAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'ssl.ca.location': certifi.where(),
                     'client.id': prediction_topic_suffix}    
    predictions_producer = Producer(producer_conf)
    
    msg = None
    error_cnt = 0
    while(True):           
        msg = features_consumer.poll(timeout=1.0)                
        if msg is None: continue
        if msg.error():
            error_cnt = error_cnt + 1
            if msg.error().code() == KafkaError._PARTITION_EOF:
                    
                    if(error_cnt%1000==0):
                        print('error')
                        print(msg)
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
        else:        
            message = json.loads(msg.value().decode("utf-8"))
            if(cnt%10000==0):
                print(message)
            cnt = cnt + 1
                
            st = message['st']
            data = message['f']
            y = (message['y']=='true')              
            try:                    
                score = model.predict_one(data)
                model = model.learn_one(data,y)                    
                end = time()
                message['score']=score
                message['duration']=(end-st)
                new_message = {}
                new_message['y']=(message['y']=='true')          
                new_message['score']=score
                new_message['duration']=(end-st)
                new_message['mem_usage']=model._raw_memory_usage
                v= json.dumps(new_message).encode('utf-8')
                try:
                    predictions_producer.produce(PREDICTION_TOPIC, value=v, key=str(cnt))
                except:
                    print(f'Queue full, flushing {cnt}')
                    producer.flush()
            except:
                print('ignored')
                ignored = ignored + 1
    print('CLOSING')
    consumer.close()    


def predict(x):
    global cnt
    global model
    model_score = model.predict_one(x)
    print(model_score)
    return dict(score=str(model_score),features=x,count=cnt)

def init():   
    global inference_group_id
    cf = threading.Thread(target=consume_features, args=(inference_group_id,))
    cf.start()
    consume_features(inference_group_id)
    print('Feature Consumption Thread Started')
    #consume_features(inference_group_id)

print('Sleeping for 10 seconds')
time.sleep(10)

init()
print('started')