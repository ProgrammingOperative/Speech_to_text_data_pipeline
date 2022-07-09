import sys
import os

sys.path.append(os.path.abspath("./scripts/"))
from util import Utils
util = Utils("./logs/final_audio.log")

URL = "/mnt/10ac-batch-5/week9/reiten/processed/audio/"
def main():
    try:
        meta = util.df_loader("interim", "kafka_set.csv")
        meta = util.add_duration(meta, "Url")
        meta = util.add_channel_count(meta, "Url")
        util.df_saver(meta, "processed", "final_set.csv")
        for i in range(len(meta)):
            try:
                sam, rat = util.load_audio(meta.loc[i, "Url"])
                audio = [sam, rat]
            except:
                continue
            path = f"{URL}{meta.loc[i, 'Id']}.wav"
            util.save_audio(audio, path)
    except:
        pass

if __name__ == "__main__":
    main()