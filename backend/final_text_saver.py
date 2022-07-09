from logging import exception
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath("./scripts/"))
from util import Utils
util = Utils("./logs/text_saver.log")

def main():
    try:
        consumer = util.consumer_init("processed", offset="earliest")
        ids = []
        texts = []
        urls = []
        try:
            df = util.df_loader("interim", "kafka_set.csv")
        except:
            df = pd.DataFrame(columns = ['Id', 'Text', 'Url'])
        for message in consumer:
            
            ids.append(message.value["id"])
            texts.append(message.value["text"])
            urls.append(message.value["url"])
            print(message.value["id"])

        df2 = pd.DataFrame(columns = ['Id', 'Text', 'Url'])
        df2["Id"] = ids
        df2["Text"] = texts
        df2["Url"] = urls
        frames = [df, df2]
        result = pd.concat(frames)
        util.df_saver(result, "interim", "kafka_set.csv")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()