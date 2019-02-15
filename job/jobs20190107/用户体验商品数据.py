import DataBaseConn as DBC
import pandas as pd
import numpy as np
bussconn=DBC.DataBaseConnect('db_ex_business').BussinessDataBase()
busscurson=bussconn.cursor()
data=pd.read_excel(r'D:\job_xiaoe\job_20190107\用户体验图文.xlsx')
data.columns=['id']
datatuple=tuple(data.id.values.tolist())
# print(datatuple)
sql='select id,title from t_image_text where id in{}'.format(datatuple)
busscurson.execute(sql)
result=busscurson.fetchall()
resultdf=pd.DataFrame(np.array(result),columns=['resource_id','comment'])
resultdf.to_excel(r'D:\job_xiaoe\job_20190107\用户体验图文内容.xlsx')
busscurson.close()
bussconn.close()