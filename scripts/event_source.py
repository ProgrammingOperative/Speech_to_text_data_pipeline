from kafka import KafkaProducer
from datetime import datetime
from json import dumps
from time import sleep
from kafka import KafkaConsumer
from json import loads
from kafka.admin import KafkaAdminClient, NewTopic
import pandas as pd
import numpy as np


class EventSource:
    
    def __init__():
        pass
    
    def extract_sentence(df,col_name, topic, prod, num_sentence = -1): 
    counter = 0
    for i in range(len(df)):
        articles = df.loc[i,col_name].split("።")
        index = 0
        size = len(articles)
        while((counter != num_sentence) and (index < size)):
            sentence = articles[index]
            if(len(sentence) < 2):
                counter = counter+1
                index = index+1
                continue
            prod.send(topic, sentence)
            counter = counter+1
            index = index+1
        
        if(counter == num_sentence):
            break
        
        
    def json_serializer(x):
        val = dumps(x).encode('utf-8')

        return val

    def producer_init(server, serializer=None):
        producer = KafkaProducer(bootstrap_servers=server,
                                 value_serializer=lambda x: serializer(x))
        return producer