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
import wave
import codecs
from tqdm import tqdm
import array
import json
import audioop
import soundfile as sf
import librosa  # for audio processing
import librosa.display
import logging
import soundfile as sf
import pandas as pd
import numpy as np
# from regex import D
import sys
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# importing scripts
sys.path.insert(1, '..')
sys.path.append("..")
sys.path.append(".")


class Utils:

    def __init__(self, filehandler):
        """
        initilize logger
        """
        file_handler = logging.FileHandler(filehandler)
        formatter = logging.Formatter(
            "time: %(asctime)s, function: %(funcName)s, module: %(name)s, message: %(message)s \n")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    
    # create data loader.
    def check_id(self, id, pro_df):
        try:
            for i in range(len(pro_df)):
                if(id == pro_df.loc[i,"Id"]):
                    return False
        except:
            pass

        return True

    def produce_text(self, df, producer, topic, pro_df = None,  num=1000000, freq = 2):
        print("producer")
        counter = 0
        while (counter < len(df) and (counter < num)):
            id = df.loc[counter,"Id"]
            if pro_df is not None:
                if (self.check_id(id, pro_df)):
                    text = df.loc[counter,"Text"]
                    data = [str(id), text]
                    producer.send(topic, data) 
                    print(data)
            else:
                text = df.loc[counter,"Text"]
                data = [str(id), text]
                producer.send(topic, data)
                print(data)

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
                    ids.append("audio"+("0"*(id_len-(len(str(id)))))+str(id))
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
                                 consumer_timeout_ms = 20000,
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

    def load_audio(self, path):
        """
        path: path to audio file to be loadded
        return: array of audio samples and sampling rate.
        """
        samples, sample_rate = librosa.load(path, sr=None, mono=False)
        
        return [samples, sample_rate]

    def save_audio(self, audio, path):
        """
        audio: a list that contains audio sample and rate
        path: location and name of new file
        """
        
        if(len(audio[0])==2):
            with sf.SoundFile(path, 'w', audio[1], 2, 'PCM_24') as f:
                f.write(np.transpose(audio[0]))
        else:
            sf.write(path, audio[0], audio[1])


    def add_channel_count(self, df, col):
        """
        It identifies number of channels in the audio files
        and adds a new column with the identified number
        """
        n_list = []
        for i in range(df.shape[0]):
            try:
                #data = wave.open(df.loc[i, col], mode='rb')
                sample, sampler = self.load_audio(df.loc[i,col])
            except:
                n_list.append(400)  # 400 means the data is missing
                continue
            print(sample.shape)
            channel = sample.shape
            n_list.append(channel)
        df["Number_of_channels"] = n_list

        logger.info("new column successfully added: channels count")

        return df

    
    def add_duration(self, df, col):
        d_list = []
        for i in range(df.shape[0]):
            try:
                data = wave.open(df.loc[i, col], mode='rb')
                print(data.getnchannels)
            except:
                d_list.append(400)  # 400 means the data is missing
                continue
            frames = data.getnframes()
            rate = data.getframerate()
            duration = frames / float(rate)
            d_list.append(duration)
        df["Duration"] = d_list

        logger.info("new column successfully added: Duration")

        return df


    def change_rate(self, audio, sr):
        """
        audio: a list that contains audio sample and rate
        sr: new sampling rate to be applied
        return: resampled audio array with sampling rate
        """
        resampled = librosa.resample(audio[0], orig_sr=audio[1], target_sr=sr)
        
        logger.info("rate changed")
        
        return [resampled, sr]