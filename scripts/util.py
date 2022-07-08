import pandas as pd
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


class utils:

    def __init__(self):
        pass


    def extract_sentence(df, col_name, id_len, char_limit):
        """
        Args:
            df:
            col_name:
            id_len:
            char_limit:
            
        Returns: None
        """
        id = 1
        st_df = pd.DataFrame(columns = ['Id', 'Text'])
        texts = []
        ids = []
            
        for i in range(len(df)):
            articles = df.loc[i,col_name].split("።")
            for text in articles:
                if(len(text) < char_limit):
                    texts.append(text)
                    ids.append("0"*(id_len-(len(str(id))))+str(id))
                    id = id+1

        st_df['Id'] = ids
        st_df['Text'] = texts

        return st_df


    def df_loader(self, folder, name):
        """
        loads dataframes
        Args: 
            folder: parent folder
            name: file name with extension
        Return: 
            df: dataframe object
        """
        path = f"/mnt/10ac-batch-5/week9/reiten/{folder}/{name}"
        df = pd.read_csv(path)

        return df


    def df_saver(self, df, folder, name):
        """
        saves dataframes
        Args:
            df: dataframe tobe saved.
            folder: parent folder
            name: name of file with extension
        Returns: None
        """
        path = f"/mnt/10ac-batch-5/week9/reiten/{folder}/{name}"
        df.to_csv(path)

    
    def producer_init(self, server = ['localhost:9092']):
        """
        Args:
            server: url and port of server.
            
        Returns:
            producer:
        """
        producer = KafkaProducer(bootstrap_servers=server,
                                 value_serializer=lambda x: dumps(x).encode('utf-8'))
        return producer

    
    def consumer_init(self, topic, server, offset = "latest", commit = False):
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
                                 value_deserializer=lambda x: loads(x.decode('utf-8')))

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
