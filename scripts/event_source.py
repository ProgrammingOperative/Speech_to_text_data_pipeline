from kafka import KafkaProducer
from kafka import KafkaConsumer
from datetime import datetime
from json import dumps
from json import loads   
from time import sleep
from kafka import KafkaConsumer
from json import loads
from kafka.admin import KafkaAdminClient, NewTopic
import pandas as pd
import numpy as np


class EventSource:
    
    def __init__(self):
        pass
    
    def extract_sentence(self, df,col_name, topic, prod, num_sentence = -1, os=[0,0]):
        """
        Args:
            df:
            col_name:
            topic:
            prod:
            num_sentence:
            
        Returns: None
        """
        counter = 0
        offset = os[:]
        i = 0
        while(i < len(df)):
            i = i + offset[0]
            articles = df.loc[i,col_name].split("።")
            index = offset[1]
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
            offset[1] = 0
            offset[0] = 0
            i = i+1
            if(counter == num_sentence):
                break
        

    
    
    def producer_init(self, server):
        """
        Args:
            server:
            serializer:
            
        Returns:
            producer:
        """
        producer = KafkaProducer(bootstrap_servers=server,
                                 value_serializer=lambda x: dumps(x).encode('utf-8'))
        return producer
         
        
    def consumer_init(self, topic, server, offset, commit, group, deser):
        """
        Args:
            topic:
            server:
            offset:
            commit:
            group:
            deser:
        
        Returns:
            consumer:
        """
        consumer = KafkaConsumer(topic,
                                 bootstrap_servers=server,
                                 auto_offset_reset=offset,
                                 enable_auto_commit=commit,
                                 group_id=group,
                                 value_deserializer=lambda x: deser(x))

        return consumer


    def create_topic(self, server, cl_id, topic_name, partitions, replica):
        """
        Args:
            server:
            cl_id:
            topic_name:
            partitions:
            replica:
            
        Returns:
            message.
        """
        admin_client = KafkaAdminClient(
            bootstrap_servers=server, 
            client_id=cl_id
        )

        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=partitions, 
                                    replication_factor=replica))

        message = admin_client.create_topics(new_topics=topic_list, validate_only=False)
        
        return message


    def load_text(self, topic_name, server):
        consumer = KafkaConsumer(topic_name,
                                bootstrap_servers=server,
                                auto_offset_reset='latest',
                                enable_auto_commit=False,
                                value_deserializer=lambda x: loads(x.decode('utf-8')))

        
        for message in consumer:
            return message.value