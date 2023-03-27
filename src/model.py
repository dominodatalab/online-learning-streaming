# Import dependencies
import random
from confluent_kafka import TopicPartition,Producer,Consumer
import os
import traceback

import logging
logging.basicConfig(level=logging.INFO)


FEATURE_TOPIC=os.environ['FEATURE_TOPIC']
GROUP_ID = os.environ['INFERENCE_GROUP_ID']
PREDICTION_TOPIC = os.environ['PREDICTION_TOPIC']
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('kafka_bootstrap_servers')
KAFKA_USERNAME = os.environ.get('kafka_username')
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
                     'sasl.username': KAFKA_USERNAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'ssl.ca.location': certifi.where(),
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.commit.interval.ms':1000,         
                     'auto.offset.reset': 'latest'}
    features_consumer = Consumer(features_consumer_conf)  
    
    print(f'\nNow subscribing to features topic:{FEATURE_TOPIC}')
    features_consumer = Consumer(features_consumer_conf)    
    features_consumer.subscribe([FEATURE_TOPIC])

    
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'sasl.username': KAFKA_USERNAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'batch.num.messages': 2048,                
                     'linger.ms': 100,
                     'ssl.ca.location': certifi.where(),
                     'client.id': GROUP_ID}    
    predictions_producer = Producer(producer_conf)
    cnt = 0
    ignored=0
    msg = None
    error_cnt = 0
    end_learn_ts = 0
    st_learn_ts = 0

    st_processing_time = 0
    
    learning_durations=[]
    prediction_durations=[]
    processing_durations = []
    score_and_truth = []
    mem_usage = []
    end_to_end_processing_durations = []
    messaging_latencies = []
    while(True):           
       
        messages = features_consumer.consume(num_messages=1000,timeout=0.1)    
        if len(messages)==0: continue
        
        for msg in messages:
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
                try:         
                    msg_arrival_time = time.time()
                    message = json.loads(msg.value().decode("utf-8"))            
                    cnt = cnt + 1
                    
                    f = message['f']
                    y = (message['y']=='true')              
                    msg_produce_ts = message['st']
                    new_message={}
                    new_message['id']=message['id']
                    new_message['y']=y
                    
                    #messaging_latencies.append(msg_arrival_time-msg_produce_ts)
                    if(cnt==1):
                        st_processing_time = time.time()
                    
                    new_message['msg_l']=(msg_arrival_time-msg_produce_ts)
                                
                    st_learn_ts = time.time()
                    model_artifact = model_artifact.learn_one(f,y)      
                    end_learn_ts = time.time()
                    new_message['l_dur'] = (end_learn_ts-st_learn_ts)
                        
                    st_prediction_time = time.time()            
                    score = model_artifact.predict_one(f)
                    new_message['score'] = score
                    score_and_truth.append({'y':y,'score':score})
                    end_prediction_time = time.time()
                    new_message['t_dur'] = (end_prediction_time-st_prediction_time)


                    msg_departure_time = time.time()
                    new_message['p_dur'] = (msg_departure_time-msg_arrival_time)
                    end_to_end_processing_durations.append(msg_departure_time-msg_produce_ts)
                    new_message['e_e_dur']= (msg_departure_time-msg_produce_ts)
                    predictions_producer.produce(PREDICTION_TOPIC,value=json.dumps(new_message).encode('utf-8'), key=str(cnt))  
                    if(cnt%1000==0):
                        print(f'Processed {cnt}')
                        new_message = (model_artifact._raw_memory_usage)     
                        predictions_producer.flush()
                    
                
                except Exception as  e:      
                    print(e)
                    ignored = ignored + 1
                    if(ignored%100==0):
                        print(e, file=sys.stdout)
                        print(f'ignored ={ignored} total = {cnt}')
    predictions_producer.flush()
    features_consumer.commit()
    features_consumer.close() 
    
    
    
   


def predict(x):
    print('Now predicting')
    global model_artifact
    model_score = model_artifact.predict_one(x)
    print(model_score)
    return dict(score=str(model_score),features=x,count=str(cnt), model=str(model_score))

def init():   
    global GROUP_ID
    print('Now initializing')
    cf = threading.Thread(target=consume_features, args=(GROUP_ID,))
    cf.start()
    print('Done initializing')
    

#print('Sleeping for 1 seconds')
#time.sleep(1)

init()
print('Started Model')