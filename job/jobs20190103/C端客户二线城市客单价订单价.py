#用户消费分析（分析）
import DataBaseConn as DBC
import numpy as np
import pandas as pd
datedicts={'2016Q1':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2016-01-01 00:00:00' and created_at<='2016-03-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2016Q2':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2016-04-01 00:00:00' and created_at<='2016-06-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2016Q3':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2016-07-01 00:00:00' and created_at<='2016-09-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2016Q4':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2016-10-01 00:00:00' and created_at<='2016-12-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2017Q1':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2017-01-01 00:00:00' and created_at<='2017-03-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2017Q2':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2017-04-01 00:00:00' and created_at<='2017-06-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2017Q3':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2017-07-01 00:00:00' and created_at<='2017-09-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2017Q4':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2017-10-01 00:00:00' and created_at<='2017-12-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2018Q1':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2018-01-01 00:00:00' and created_at<='2018-03-31 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2018Q2':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2018-04-01 00:00:00' and created_at<='2018-06-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2018Q3':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2018-07-01 00:00:00' and created_at<='2018-09-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;",
'2018Q4':r"select user_id,price as Price,order_id as Orders FROM t_orders where created_at>='2018-10-01 00:00:00' and created_at<='2018-11-30 23:59:59' and order_state=1 and price>0 and wx_app_type!=10;"}
def GetCity01():
    testconn = DBC.DataBaseConnect('db_ex_chain').TestDataBase()
    testcurson = testconn.cursor()
    sql="SELECT user_id,city_type FROM t_user_city_distr where city_type=1;"
    testcurson.execute(sql)
    data=testcurson.fetchall()
    result=pd.DataFrame(np.array(data),columns=['user_id','city_type'])
    return result
def GetCity02():
    testconn = DBC.DataBaseConnect('db_ex_chain').TestDataBase()
    testcurson = testconn.cursor()
    sql="SELECT user_id,city_type FROM t_user_city_distr where city_type=2 ;"
    testcurson.execute(sql)
    data=testcurson.fetchall()
    result=pd.DataFrame(np.array(data),columns=['user_id','city_type'])
    return result
def GetCity03():
    testconn = DBC.DataBaseConnect('db_ex_chain').TestDataBase()
    testcurson = testconn.cursor()
    sql="SELECT user_id,city_type FROM t_user_city_distr where city_type=3 ;"
    testcurson.execute(sql)
    data=testcurson.fetchall()
    result=pd.DataFrame(np.array(data),columns=['user_id','city_type'])
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
        result = pd.DataFrame(np.array(data), columns=['user_id', 'prices', 'orders'])
        result.insert(0, 'dt', s1)
        return result
    else:
        return pd.DataFrame([[s1,0,0,0]],columns=['dt','user_id','prices','orders'])
def main():
    # data_01=GetCity01()
    # print(len(data_01))
    data_02=GetCity02()
    print(len(data_02))
    # data_03=GetCity03()
    # print(len(data_03))
    data=[]
    for i in datedicts.items():
        try:
            datelist=list(i)
            data_test=GetUserPriceInfo(datelist)
            data_test['user_id'] = data_test['user_id'].astype('object')
            data_temp=data_test.merge(data_02,on='user_id')
            data_temp['prices'] = data_temp['prices'].astype('int64')
            if len(data_temp[data_temp['city_type']=='2'])>0:
                Sumprice=data_temp[data_temp['city_type']=='2']['prices'].sum()
                CountOrders=data_temp[data_temp['city_type']=='2']['city_type'].count()
                Usercount=pd.DataFrame(data_temp[data_temp['city_type'] == '2']['user_id']).drop_duplicates()['user_id'].count()
                print(data.append([datelist[0],Sumprice,CountOrders,Usercount]))
                data.append([datelist[0],Sumprice,CountOrders,Usercount])
            else:
                print('null')
                data.append([datelist[0],0,0,0])
        except:
            print('erro')
        # print(data_temp)
    result_fy=pd.DataFrame(data,columns=['dt','Sumprice','CountOrders','Usercount'])
    result_fy.to_excel(r'C:\Users\Administrator\Desktop\city_price_02.xlsx')
if __name__ == '__main__':
    main()

