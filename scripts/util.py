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


class Utils:

    def __init__(self):
        pass
    
    # create data loader.
    def check_id(self, id, pro_df):
        try:
            for i in range(len(pro_df)):
                if(id == pro_df.loc[i,"Id"]):
                    return False
        except:
            pass
        
        return True

    def produce_text(self, df, pro_df, producer, topic, num=1000000, freq = 0.5):
        
        counter = 0
        while (counter < len(df) and (counter < num)):
            id = df.loc[counter,"Id"]
            if (self.check_id(id, pro_df)):
                text = df.loc[counter,"Text"]
                data = [id, text]
                producer.send(topic, data) 
                #print(data)
            counter = counter + 1
            sleep(freq)

    def extract_sentences(self, df, id_len = 6, char_limit = 150):
        """
        Args:
            df: original dataframe
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
            articles = df.loc[i,"article"].split("።")
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

    
    def consumer_init(self, topic, server = ['localhost:9092'], offset = "latest", commit = False):
        """
        Args:
            topic: kafka topic
            server: port and url of the server where the kafka is found
            offset: the type of offset during reset (latest, or earliest)
            commit: boolean to track processed items
            group: name of group
        
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
