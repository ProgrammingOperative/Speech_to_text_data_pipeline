import sys
import os

sys.path.append(os.path.abspath("./scripts/"))
from util import Utils
util = Utils("../logs/text_producer.logs")

def main():
    # load cleaned sentences
    clean_set = util.df_loader("interim", "clean_set.csv")
    print(clean_set.shape)
    # kafka producer initialization
    producer = util.producer_init()
    
    try:
        pro_df = util.df_loader("processed", "final_set.csv")
        util.produce_text(clean_set, producer, "unprocessed", pro_df)
    except:
        util.produce_text(clean_set, producer, "unprocessed")

if __name__ == "__main__":
    main()
