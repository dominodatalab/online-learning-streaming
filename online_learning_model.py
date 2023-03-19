# Import dependencies
import random
from confluent_kafka import TopicPartition,Producer,Consumer





model_topic_name_prefix=os.environ['MODEL_TOPIC_NAME_PREFIX']
inference_group_id = os.environ['INFERENCE_GROUP_ID']
prediction_topic_suffix = os.environ['PREDICTION_TOPIC_SUFFIX']

FEATURES_TOPIC=f'{model_topic_name_prefix}-features'
PREDICTION_TOPIC=f'{model_topic_name_prefix}-predictions-{prediction_topic_suffix}'


KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_USER_NAME = os.environ.get('KAFKA_USER_NAME')
KAFKA_PASSWORD = os.environ.get('KAFKA_PASSWORD')
#KAFKA_FEATURES_TOPIC_PARTITION_RANGE = os.environ.get('KAFKA_FEATURES_TOPIC_PARTITION_RANGE')


import time
import os
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

group_id = f'grp-{uuid}'

latest_version=-1
models = {}



def return_range(strg, loc, toks):
    if len(toks)==1:
        return int(toks[0])
    else:
        return range(int(toks[0]), int(toks[1])+1)




    


model = tree.HoeffdingAdaptiveTreeClassifier(grace_period=100,  delta=1e-5, leaf_prediction='nb', nb_threshold=10,seed=0)    

def consume_features(group_id:str):  
    global latest_version
    global model
    
    print('Initialized')
    #Only one model instance recieves the message (Each has the SAME consumer group)
    features_consumer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'sasl.username': KAFKA_USER_NAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'ssl.ca.location': certifi.where(),
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'latest'}
    features_consumer = Consumer(features_consumer_conf)    
    
    features_consumer.subscribe([FEATURES_TOPIC])

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
    cnt = 0
    while(True):
        msg = consumer.poll(timeout=0.1)
        if msg is None: continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
            else:            
                cnt = cnt + 1
                message = loads(msg.value().decode("utf-8"))
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
                        producer.produce(PREDICTION_TOPIC, value=v, key=str(cnt))
                    except:
                        print(f'Queue full, flushing {cnt}')
                        producer.flush()
                    #auc = auc.update(y, score)
                    #f1 = f1.update(y, score)
                    #recall = recall.update(y, score)
                    #durs.append(end-st)
                    #i = i + 1                
                    #if i%1000==0: #Sample model memory every 1000 records
                        #mem.append(model._raw_memory_usage)
                except:
                    ignored = ignored + 1
    consumer.close()    
    #mean = statistics.mean(durs)
    #median = statistics.median(durs)
    #max1 = max(durs)
    #min2 = min(durs)
    #total_records = len(durs)
    #memory_usage = statistics.mean(mem)
            



def predict(x):
    global model
    model_score = model.predict_one(x)
    return dict(score=model_score,features=data)

def init():
    cf = threading.Thread(target=consume_features, args=(inference_group_id,))
    cf.start()
    print('Feature Consumption Thread Started')

init()