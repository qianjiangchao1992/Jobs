import pandas as pd
import numpy as np
f=open(r"D:\job_xiaoe\job_20190214\output\part-00000",encoding="utf-8")
data=f.readlines()
data=[i.strip("\n") for i in data]
data=[i.strip(")").strip("(").split(",") for i in data]
dataresult=pd.DataFrame(data,columns=["type","counts"])
dataresult["type"]=dataresult["type"].astype(str)
dataresult["counts"]=dataresult["counts"].astype(int)
print(dataresult.info())
dataresult.to_excel(r"D:\job_xiaoe\job_20190214\output\part-00000.xlsx")
