import pandas as pd
import datetime
import HiveTestConn as HTC
todays=datetime.datetime.strftime(datetime.datetime.today(),"%Y%m%d")
data=pd.read_excel(r'D:\job_xiaoe\job_'+todays+r'\bigcustomerinfo.xlsx')
data.columns=["functionmodule","customername","label","keyinfo","advisortime"]
data["functionmodule"]=data["functionmodule"].fillna(method="pad")
data["advisortime"]=data["advisortime"].astype("str")
data=data.fillna("")
result=[]
for i in data["advisortime"].values.tolist():
    j=i.split(".")
    if len(j[1])>1 and len(j[2])>1:
        result.append(str(j[0]) + "-" + str(j[1]) + "-" + str(j[2]))
    elif len(j[1])>1 and len(j[2])<=1:
        result.append(str(j[0]) + "-" + str(j[1]) + "-" +"0"+str(j[2]))
    elif len(j[1])<=1 and len(j[2])>1:
        result.append(str(j[0]) + "-" +"0"+ str(j[1]) + "-" + str(j[2]))
    else:
        result.append(str(j[0]) + "-" + "0" + str(j[1]) + "-" +"0"+ str(j[2]))
data["advisortime"]=result
data_fy=data[["advisortime","functionmodule","customername","label","keyinfo"]]
data_conf=list(set(data["functionmodule"].values.tolist()))
data_conf_values=[]
for i in data_conf:
    data_conf_values.append([i])
data_fv_values=data_fy.values.tolist()
hiveconn=HTC.HiveTest().HiveTestDataBase()
hivecurson=hiveconn.cursor()
#清除表t_bigcustomer_advisor_info信息
deletesql="truncate table t_bigcustomer_advisor_info;"
insertsql="insert into t_bigcustomer_advisor_info(dt,function_module,customer_name,label,keyword_info) values (%s,%s,%s,%s,%s);"
hivecurson.execute(deletesql)
hivecurson.executemany(insertsql,data_fv_values)
#清除表t_bigcustomer_advisor_conf信息
deleteconfsql="truncate table t_bigcustomer_advisor_conf;"
insertconfsql="insert into t_bigcustomer_advisor_conf(functionmodule) values (%s);"
hivecurson.execute(deleteconfsql)
hivecurson.executemany(insertconfsql,data_conf_values)
hiveconn.commit()
hivecurson.close()
hiveconn.close()
print("完成更新")
