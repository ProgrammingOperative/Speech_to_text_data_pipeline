import pandas as pd


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
        path = f"/mnt/10ac-batch-5/week9/reiten/{folder}/{name}"
        df.to_csv(path)
