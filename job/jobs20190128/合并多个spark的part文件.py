import pandas as pd
import numpy as np
import re
import os
filepath=[]
for i in range(1,4):
    filepath.append(r'D:\job_xiaoe\job_20190110\Count_Label'+str(i)+"\\")
for i in  filepath:
    for parent,dirname,filenames in os.walk(i):
        pattern=re.compile(r'^part.*')
        f=open(parent+"label1_all.txt","a+",encoding="utf-8")
        for i in filenames:
            result=re.findall(pattern,i)
            if len(result)>0:
                data=open(parent+result[0],"r",encoding="utf-8")
                f.write(data.read())
                data.close()
            else:
                pass
        f.close()

