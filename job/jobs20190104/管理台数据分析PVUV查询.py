import pandas as pd
import numpy as np
import DataBaseConn as DBC
appid=pd.read_excel(r'D:\job_xiaoe\job_20190104\12月份标准版店铺最新版.xlsx')
appidtuple=(tuple(appid.app_id.values.tolist()))
def getDataPvUv():
    result=pd.DataFrame()
    logsconn=DBC.DataBaseConnect('db_ex_logs').LogsDataBase()
    logscurson=logsconn.cursor()
    sql_manage='select count(*) as PV,count(distinct app_id) as uvappid,count(distinct phone_name) from t_admin_log_2018_12 where app_id in {};'.format(appidtuple)
    sql_dataanalysis='select count(*) as PV,count(distinct app_id) as uvappid,count(distinct phone_name) from t_admin_log_2018_12 where app_id in {} and target_url in("https://admin.xiaoe-tech.com/data_analysis/get_charge_analysis","https://admin.xiaoe-tech.com/data_analysis/get_overview_data","https://admin.xiaoe-tech.com/data_analysis/get_resource_detail","https://admin.xiaoe-tech.com/data_analysis/get_resource_num","https://admin.xiaoe-tech.com/data_analysis/get_traffic_data","https://admin.xiaoe-tech.com/data_analysis/get_user_overview");'.format(appidtuple)
    sql_usergroup='select count(*) as PV,count(distinct app_id) as uvappid,count(distinct phone_name) from t_admin_log_2018_12 where app_id in {} and target_url regexp "https://admin.xiaoe-tech.com/customer_gruop";'.format(appidtuple)
    ##提取管理台标准版PV/UV
    logscurson.execute(sql_manage)
    managedata=logscurson.fetchall()
    managedf=pd.DataFrame(np.array(managedata),columns=['pv','uvappid','uvphone'])
    result=result.append(managedf)
    print('管理台PV/UV已成功写入！')
    ##提取数据分析标准版PV/UV
    logscurson.execute(sql_dataanalysis)
    analysisdata = logscurson.fetchall()
    analysisdf = pd.DataFrame(np.array(analysisdata), columns=['pv', 'uvappid', 'uvphone'])
    result = result.append(analysisdf)
    print('数据分析PV/UV已成功写入！')
    ##提取用户分组标准版PV/UV
    logscurson.execute(sql_usergroup)
    usergroupdata = logscurson.fetchall()
    usergroupdf = pd.DataFrame(np.array(usergroupdata), columns=['pv', 'uvappid', 'uvphone'])
    result = result.append(usergroupdf)
    print('用户分组PV/UV已成功写入！')
    result.to_excel(r'D:\job_xiaoe\job_20190104\管理台数据分析库用户分组PVUV统计.xlsx')
    logscurson.close()
    logsconn.close()
getDataPvUv()
