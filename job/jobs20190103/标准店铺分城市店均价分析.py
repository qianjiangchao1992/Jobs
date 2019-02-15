import numpy as np
import pandas as pd
import DataBaseConn as DBC

datedicts={
'2017Q1':r"select app_id,predicted_city,sum_income from t_user_analysis where date='2017-03-31' and version_type in (3,4) and predicted_city is not null;",
'2017Q2':r"select app_id,predicted_city,sum_income from t_user_analysis where date='2017-06-30' and version_type in (3,4) and predicted_city is not null;",
'2017Q3':r"select app_id,predicted_city,sum_income from t_user_analysis where date='2017-09-30' and version_type in (3,4) and predicted_city is not null;",
'2017Q4':r"select app_id,predicted_city,sum_income from t_user_analysis where date='2017-12-31' and version_type in (3,4) and predicted_city is not null;",
'2018Q1':r"select app_id,predicted_city,sum_income from t_user_analysis where date='2018-03-31' and version_type in (3,4) and predicted_city is not null;",
'2018Q2':r"select app_id,predicted_city,sum_income from t_user_analysis where date='2018-06-30' and version_type in (3,4) and predicted_city is not null;",
'2018Q3':r"select app_id,predicted_city,sum_income from t_user_analysis where date='2018-09-30' and version_type in (3,4) and predicted_city is not null;",
'2018Q4':r"select app_id,predicted_city,sum_income from t_user_analysis where date='2018-11-30' and version_type in (3,4) and predicted_city is not null;"}
def GetCityType():
    data=pd.read_excel(r'C:\Users\Administrator\Desktop\BsideCity.xlsx')
    return data
def GetAppidCity(datelist):
    conn = DBC.DataBaseConnect('db_ex_chain').O_ServeseWxbDataBase()
    curson = conn.cursor()
    s1 = datelist[0]
    s2 = datelist[1]
    curson.execute(s2)
    data=curson.fetchall()
    curson.close()
    conn.close()
    if len(data)>0:
        result = pd.DataFrame(np.array(data), columns=['app_id', 'predicted_city','sum_income'])
        result.insert(0, 'dt', s1)
        return result
    else:
        return pd.DataFrame([[s1,0,0,0]],columns=['dt','app_id', 'predicted_city','sum_income'])
def main():
    datacity=GetCityType()
    data=[]
    for i in datedicts.items():
        try:
            datelist=list(i)
            data_appid=GetAppidCity(datelist)
            data_appid['predicted_city']=data_appid['predicted_city'].astype('object')
            data_temp=data_appid.merge(datacity,on='predicted_city')
            data_temp['ciyt_type']=data_temp['ciyt_type'].astype('str')
            data_temp['sum_income']=data_temp['sum_income'].astype('int64')
            for j in ('1','2','3'):
                if len(data_temp[data_temp['ciyt_type']==j])>0:
                    numcounts=data_temp[data_temp['ciyt_type']==j]['app_id'].count()
                    sumprices=data_temp[data_temp['ciyt_type']==j]['sum_income'].sum()
                    data.append([datelist[0],j,numcounts,sumprices,round(sumprices/numcounts/100,2)])
                else:
                    data.append([datelist[0],0,0,0,0])
                    print('null')
        except Exception as e:
            print(e)
    result=pd.DataFrame(data,columns=['dt','city_type','counts','sumprices','averageprice'])
    result.to_excel(r'C:\Users\Administrator\Desktop\BsideBZ_Price.xlsx')
if __name__ == '__main__':
    main()