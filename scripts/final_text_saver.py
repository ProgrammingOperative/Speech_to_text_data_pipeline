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
                df = pd.DataFrame(columns = ['Id', 'Text'])
            row = pd.Series(message)
            df.append(row, ignore_index=True)
            util.df_saver(df, "interim", "kafka_set.csv")
    except:
        pass

if "__name__" == "__main__":
    main()