import sys
import os

sys.path.append(os.path.abspath("."))
from util import Utils
util = Utils("../logs/text_cleaner.log")

def main():
    # load Original Dataset
    original_df = util.df_loader("unprocessed", "original_set.csv")
    df = util.extract_sentences(original_df)
    util.df_saver(df, "interim", "clean_set.csv")


if "__name__" == "__main__":
    main()