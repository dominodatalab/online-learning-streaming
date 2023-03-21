# Import dependencies
import random
from confluent_kafka import TopicPartition,Producer,Consumer
import os
import traceback

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


'''
model_artifact = tree.HoeffdingAdaptiveTreeClassifier(grace_period=100,  delta=1e-5, leaf_prediction='nb', nb_threshold=10,seed=0) 
'''
model_artifact = ensemble.AdaptiveRandomForestClassifier(leaf_prediction="mc")

cnt = 0
import sys
def consume_features(group_id:str):  
    ignored = 0
    global cnt
    global latest_version
    global model_artifact
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
                     'enable.auto.commit': True,
                     'auto.commit.interval.ms':10000,         
                     'auto.offset.reset': 'latest'}
    features_consumer = Consumer(features_consumer_conf)    
    print(f'\nNow subscribing to features topic {feature_topic}')
    features_consumer.subscribe([feature_topic])

    
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'sasl.username': KAFKA_USER_NAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'batch.num.messages': 2048,                
                     'linger.ms': 100,
                     'ssl.ca.location': certifi.where(),
                     'client.id': group_id}    
    predictions_producer = Producer(producer_conf)
    
    msg = None
    error_cnt = 0
    end_learn_ts = 0
    st_learn_ts = 0
    prediction_lag = 0
    st = 0
    while(True):           
        msg = features_consumer.poll(timeout=0.1)    
        
        if msg is None: continue
        if msg.error():
            error_cnt = error_cnt + 1
            if msg.error().code() == KafkaError._PARTITION_EOF:
                    
                    if(error_cnt%1000==0):
                        #features_consumer.commit()
                        print('error')
                        print(msg)
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
        else:       
            
            try:         
                message = json.loads(msg.value().decode("utf-8"))            
                cnt = cnt + 1
                st = message['st']
                f = message['f']
                y = (message['y']=='true')              
                if(cnt==1):
                    prediction_lag = time.time()-st
                end = time.time()
                if(cnt%1024==0 and cnt>0):
                    print(str(end-st-prediction_lag))
                    print(f'Sanity check {cnt}')
                    predictions_producer.flush()
                st_learn_ts = time.time()
                st_learn_ts = time.time()
                score = model_artifact.predict_one(f)
                model_artifact = model_artifact.learn_one(f,y)      
                end_learn_ts = time.time()
                end = time.time()
                new_message = {}
                new_message['y']=(message['y']=='true')          
                new_message['score']=score
                new_message['duration']=(end-st-prediction_lag)
                new_message['grp_id']=group_id
                
                new_message['learn_ds']=(end_learn_ts-st_learn_ts)
                new_message['mem_usage']=model_artifact._raw_memory_usage
                
                try:
                    predictions_producer.produce(PREDICTION_TOPIC, value=json.dumps(new_message).encode('utf-8'), key=str(cnt))
                except:
                    print(f'Queue full, flushing {cnt}')
                    predictions_producer.flush()
                    features_consumer.commit()
                
            except Exception as  e:      
                print(json.loads(msg.value().decode("utf-8")))
                print(e, file=sys.stdout)
                ignored = ignored + 1
                print(f'ignored ={ignored} total = {cnt}')
                
    print('CLOSING')
    features_consumer.commit()
    features_consumer.close()    

global started=False
def predict(x):
    global cnt
    global model_artifact
    global started
    if(not started):
        init()
        started = True
    model_score = model_artifact.predict_one(x)
    print(model_score)
    return dict(score=str(model_score),features=x,count=str(cnt), model=str(model_score))

def init():   
    global inference_group_id
    cf = threading.Thread(target=consume_features, args=(inference_group_id,))
    cf.start()
    #consume_features(inference_group_id)
import time
print('Sleeping for 10 seconds')
time.sleep(1)

init()
print('started')