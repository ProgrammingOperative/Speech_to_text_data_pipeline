import sys
import os

sys.path.append(os.path.abspath("."))
from util import Utils
util = Utils()
def main():
    # load Original Dataset
    original_df = util.df_loader("unprocessed", "original.csv")
    df = util.extract_sentences(original_df)
    util.df_saver(df, "transition", "clean_set.csv")


if "__name_" == "__main__":
    main()