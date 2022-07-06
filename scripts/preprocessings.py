import pandas as pd
import re
import os 

class DataProcessor():
    
     def __init__(self, filename):
        self.filename=filename