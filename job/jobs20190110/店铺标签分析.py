import pandas as pd
import numpy as np
orignal=pd.read_table(r'D:\job_xiaoe\job_20190110\prodata.txt',header=None,delimiter='\t',low_memory=False,na_values='null',encoding='utf-8')
orignal.columns=['appid','productid','label1','label2','label3']
appdata=orignal[orignal['label3'].notnull()][['appid','label1','label2','label3']]
print(appdata)