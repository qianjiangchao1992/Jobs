import DataBaseConn as DBC
import pandas as pd
import numpy as np
datedicts={'2016Q1':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2016-01-01 00:00:00' and created_at<='2016-03-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2016Q2':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2016-04-01 00:00:00' and created_at<='2016-06-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2016Q3':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2016-07-01 00:00:00' and created_at<='2016-09-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2016Q4':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2016-10-01 00:00:00' and created_at<='2016-12-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id",
'2017Q1':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2017-01-01 00:00:00' and created_at<='2017-03-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2017Q2':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2017-04-01 00:00:00' and created_at<='2017-06-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2017Q3':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2017-07-01 00:00:00' and created_at<='2017-09-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2017Q4':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2017-10-01 00:00:00' and created_at<='2017-12-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2018Q1':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2018-01-01 00:00:00' and created_at<='2018-03-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2018Q2':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2018-04-01 00:00:00' and created_at<='2018-06-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2018Q3':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2018-07-01 00:00:00' and created_at<='2018-09-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;",
'2018Q4':r"select app_id,sum(price) as Prices,count(order_id)as Orders FROM t_orders where created_at>='2018-10-01 00:00:00' and created_at<='2018-11-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10 group by app_id;"}
def GetData():
    data=pd.read_table(r'C:\Users\Administrator\Desktop\Appid_Kind.txt',encoding='utf-8',delimiter='\t',low_memory=False)
    data.columns=['app_id','type']
    result=data[data['type']!=0]
    result=result.reset_index(drop=True)
    return result
def GetUserPriceInfo(datelist):
    userconn=DBC.DataBaseConnect('db_ex_business').BussinessDataBase()
    usercurson=userconn.cursor()
    s1=datelist[0]
    s2=datelist[1]
    usercurson.execute(s2)
    print('done!')
    data=usercurson.fetchall()
    usercurson.close()
    userconn.close()
    if len(data)>0:
        result = pd.DataFrame(np.array(data), columns=['app_id', 'prices', 'orders'])
        result.insert(0, 'dt', s1)
        return result
    else:
        return pd.DataFrame([[s1,0,0,0]],columns=['dt','app_id','prices','orders'])
def main():
    result=GetData()
    data=[]
    for i in datedicts.items():
        try:
            datelist=list(i)
            data_test=GetUserPriceInfo(datelist)
            data_test['app_id'] = data_test['app_id'].astype('object')
            data_temp=data_test.merge(result,on='app_id')
            data_temp['prices'] = data_temp['prices'].astype('int64')
            data_temp['orders'] = data_temp['orders'].astype('int64')
            data_temp['type']=data_temp['type'].astype('str')
            for k in ('101','102','103','104','105','106','107','108','109','110','111'):
                if len(data_temp[data_temp['type']==k])>0:
                    Sumprice=data_temp[data_temp['type']==k]['prices'].sum()
                    CountOrders=data_temp[data_temp['type']==k]['orders'].sum()
                    data.append([datelist[0],k,Sumprice,CountOrders])
                else:
                    print('null')
                    data.append([datelist[0],0,0,0])
            print('done!!!!')
        except:
            print('erro')
        # print(data_temp)
    result_fy=pd.DataFrame(data,columns=['dt','Sumprice','CountOrders','Usercount'])
    result_fy.to_excel(r'C:\Users\Administrator\Desktop\hangye_price.xlsx')
if __name__ == '__main__':
    main()

