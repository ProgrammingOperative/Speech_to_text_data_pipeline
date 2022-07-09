import sys
import os
import pandas as pd

sys.path.append(os.path.abspath("."))
from util import Utils
util = Utils("../logs/text_saver.log")

def main():
    try:
        consumer = util.consumer_init("processed", offset="earliest")

        for message in consumer:
            try:
                df = util.df_loader("interim", "kafka_set.csv")
            except:
                df = st_df = pd.DataFrame(columns = ['Id', 'Text'])
        
            data.append(message)
    except:
        pass

if "__name__" == "__main__":
    main()