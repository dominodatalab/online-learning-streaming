import sys
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


from river import anomaly
from river import compose
from river import datasets
from river import metrics
from river import preprocessing
import uuid
import pickle
import codecs

latest_version=-1
models = {}


KAFKA_BOOTSTRAP_SERVERS = os.environ.get('kafka_bootstrap_servers')
KAFKA_USERNAME = os.environ.get('kafka_username')
KAFKA_PASSWORD = os.environ.get('kafka_password')

MODEL_UPDATE_TOPIC = os.environ.get('MODEL_UPDATE_TOPIC','model_updates')

def get_latest_model(group_id): 
    global latest_version    
    global models
    attempt=1    
    model_update_consumer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'sasl.username': KAFKA_USERNAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'ssl.ca.location': certifi.where(),
                     'group.id': str(uuid.uuid1()),
                     'enable.auto.commit': False,
                     'auto.offset.reset': 'earliest'}
    model_update_consumer = Consumer(model_update_consumer_conf)
    model_update_consumer.subscribe([MODEL_UPDATE_TOPIC])    
    cnt = 0 
    elapsed_time = 0
    sleep_time = 1
    while(True):
        
        if(cnt>4): break
        messages = model_update_consumer.consume(num_messages=1,timeout=0.1)    
        for msg in messages:
            cnt = cnt + 1
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                    (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    sys.stderr.write(f'Error code{msg.error().code()} \n')
            else:                
                model_json = json.loads(msg.value().decode("utf-8"))
                picked_str=model_json['m']
                model_instance = pickle.loads(codecs.decode(model_json['m'].encode(), "base64"))
                model_version = int(model_json['v'])
                print(f'Retrived - {model_version}')
                models[model_version]=model_instance
                if(model_version>latest_version):
                    latest_version = model_version
        
        time.sleep(sleep_time)
        elapsed_time = elapsed_time + sleep_time
        if(elapsed_time%20==0):
            print(f'Running for {elapsed_time} s')
            
    print('Returning')


def predict(x,version=-1):    
    global latest_version
    global  models
    if version<0:
        version = latest_version
    if(version>0):        
        score = models[version].predict_one(x)
        return dict(score=str(score),model_version=str(version))
    else:
        return dict(score=-1,model_version=latest_version,error='No model initialized')
    
def init(model,version):
    global models
    global latest_version    
    latest_version = version
    models[latest_version]=model
    
def init_get_latest_model():   
    cf = threading.Thread(target=get_latest_model, args=('test_grp',))
    cf.start()
    print('Done initializing')
    
    
