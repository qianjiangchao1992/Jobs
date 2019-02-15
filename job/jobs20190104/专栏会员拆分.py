import DataBaseConn as DBC
import pandas as pd
import numpy as np
original=pd.read_excel(r'D:\job_xiaoe\job_20190104\三四线城市课程专栏会员.xlsx')
original.columns=['product_id']
originaldatatuple=tuple(original['product_id'].values.tolist())
def getProductDivideData():
    result=pd.DataFrame()
    bussconn=DBC.DataBaseConnect('db_ex_business').BussinessDataBase()
    busscurson=bussconn.cursor()
    sql_sp= 'select id from t_pay_products where id in {} and is_member=0;'.format(originaldatatuple)
    sql_vip='select id from t_pay_products where id in {} and is_member=1 and member_type=1;'.format(originaldatatuple)
    sql_dsp='select id from t_pay_products where id in {} and is_member=1 and member_type=2;'.format(originaldatatuple)
    ##分出来专栏
    busscurson.execute(sql_sp)
    data_sp=busscurson.fetchall()
    data_spdf=pd.DataFrame(np.array(data_sp),columns=['id'])
    data_spdf['type']='专栏'
    result=result.append(data_spdf)
    print('专栏已成功划分')
    ##分出来会员
    busscurson.execute(sql_vip)
    data_vip = busscurson.fetchall()
    data_vipdf = pd.DataFrame(np.array(data_vip), columns=['id'])
    data_vipdf['type'] = '会员'
    result = result.append(data_vipdf)
    print('会员已成功划分')
    ##分出来大专栏
    busscurson.execute(sql_dsp)
    data_dsp = busscurson.fetchall()
    data_dspdf = pd.DataFrame(np.array(data_dsp), columns=['id'])
    data_dspdf['type'] = '大专栏'
    result = result.append(data_dspdf)
    print('大专栏已成功划分')
    ##存入结果
    result.to_excel(r'D:\job_xiaoe\job_20190104\三四线课程专栏会员拆分结果.xlsx')
    busscurson.close()
    bussconn.close()
getProductDivideData()
