# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import numpy as np
import pymysql
from sshtunnel import SSHTunnelForwarder
class LogsKpi:
    def __init__(self):
        self.today=datetime.datetime.today()
        self.days=datetime.datetime.strftime(self.today,'%Y-%m-%d')
    def HiveTestDataBase(self):
        try:
            server = SSHTunnelForwarder(
                ('118.25.130.12', 22),  # B机器的配置
                ssh_password='Xiaoe@20180908',
                ssh_username='root',
                remote_bind_address=('10.0.0.10', 3306)  # A机器的配置
            )
            server.start()
            conn = pymysql.connect(host='127.0.0.1',  # 此处必须是是127.0.0.1
                                        user='hive_test',
                                        passwd='%3u*^LK9SWf7',
                                        db='db_hive_test', charset='utf8mb4',
                                        port=server.local_bind_port)
            print("跳板机链接成功，数据库连接成功！")
            return conn
        except Exception as e:
            print("产生异常:",e)
    def getOriginalData(self):
        Testconn = self.HiveTestDataBase()
        Testconsur=Testconn.cursor()
        try:
            ##提取IM表源数据
            sql_im = 'select DATE_FORMAT(visitor_time,"%Y-%m-%d") as dt,visitor_name,receiver_account,phone_num,case when dis_entrance regexp"售前" then"售前" when dis_entrance regexp"售后" then"售后" else substring(receiver,1,2) end as service_type,advisory_category,advisory_two_category,advisory_mark ' \
                     'from t_im_record_d where substring(receiver,1,2) in ("售前","售后") and DATE_FORMAT(update_at,"%Y-%m-%d")="{}" and advisory_category<>"";'.format(self.days)
            Testconsur.execute(sql_im)
            data_im = Testconsur.fetchall()
            im_data_df = pd.DataFrame(np.array(data_im))
            im_data_df.columns = ['dt', 'visitor_name', 'receiver_account', 'phone_num', 'service_type',
                                  'advisory_category', 'advisory_two_category', 'advisory_mark']
            im_data_df['types'] = '在线'
            im_data_df = im_data_df[['dt', 'types', 'visitor_name', 'receiver_account', 'phone_num', 'service_type',
                                     'advisory_category', 'advisory_two_category', 'advisory_mark']]
            ##提取tel表源数据
            sql_tel = 'select DATE_FORMAT(start_service_time,"%Y-%m-%d")as dt,type,visitor_name,receiver_account,case when type="呼入" then calling_num when type="呼出"then called_num else null end as phone_num,case when shunt_group regexp"售前"then "售前" when shunt_group regexp"售后"then "售后" else substring(receiver,1,2) end as service_type,advisory_category,advisory_two_category,advisory_mark ' \
                      'from t_telephone_record_d where substring(receiver,1,2) in ("售前","售后") and DATE_FORMAT(update_at,"%Y-%m-%d")="{}" and advisory_category<>"" ;'.format(self.days)
            Testconsur.execute(sql_tel)
            data_tel = Testconsur.fetchall()
            tel_data_df = pd.DataFrame(np.array(data_tel))
            tel_data_df.columns = ['dt', 'types', 'visitor_name', 'receiver_account', 'phone_num', 'service_type',
                                   'advisory_category',
                                   'advisory_two_category', 'advisory_mark']
            tel_data_df = tel_data_df[
                ['dt', 'types', 'visitor_name', 'receiver_account', 'phone_num', 'service_type',
                 'advisory_category', 'advisory_two_category', 'advisory_mark']]
            result_merge = tel_data_df.append(im_data_df)
            print("数据提取成功")
            result=[]
            resultdatas=result_merge.values.tolist()
            for i in resultdatas:
                labelsplit = i[6].split(";")
                for j in labelsplit:
                    k=[]
                    k.extend(i[:6])
                    k.append(j)
                    k.extend(i[7:])
                    result.append(k)
            resultfinallydata=pd.DataFrame(result)
            resultfinallydata.columns=['dt', 'types', 'visitor_name', 'receiver_account', 'phone_num', 'service_type', 'advisory_category', 'advisory_two_category', 'advisory_mark']
            result_merge_valus = resultfinallydata.values.tolist()
            print("数据处理完毕！")
            ###插入到t_service_data_analysis
            sql_insert = "insert into t_service_data_analysis(dt,types,visitor_name,receiver_account,phone_num,service_type,advisory_category,advisory_two_category,advisory_mark) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            Testconsur.executemany(sql_insert, result_merge_valus)
            Testconn.commit()
            Testconn.close()
            Testconsur.close()
            print("数据插入成功！")
            return 0
        except Exception as e:
            print("产生异常:",e)
            return 1
def insertServiceDataAnalysis():
    Logs = LogsKpi()
    n=Logs.getOriginalData()
    s=3
    if(n*s>0):
        s -= 1
        n = Logs.getOriginalData()
    else:
        pass
def main():
    Logs = LogsKpi()
    Logs.getOriginalData()
    print("代码运行完毕")
main()
