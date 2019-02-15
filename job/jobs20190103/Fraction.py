import pymysql
import pandas
import numpy as np
import datetime
from sshtunnel import SSHTunnelForwarder
loggingdate = datetime.datetime.strftime(datetime.datetime.today(), "%Y%m%d")
f = open(r'/home/qjc/qianjiangchao/Service/log/Label{}.txt'.format(loggingdate), 'a+')
class AppidLabel:
    def __init__(self):
        self.OServeseWxbdatabase = {
            'host': '57b1c46320e1f.sh.cdb.myqcloud.com',
            'port': 4677,
            'user': 'outer_select',
            'password': 'g5#wsB@gIytC05I6',
            'charset': 'utf8'
        }
    def O_ServeseWxbDataBase(self,dbname):
        conn = pymysql.connect(host=self.OServeseWxbdatabase['host'],
                               port=self.OServeseWxbdatabase['port'],
                               user=self.OServeseWxbdatabase['user'],
                               password=self.OServeseWxbdatabase['password'],
                               database=dbname,
                               charset=self.OServeseWxbdatabase['charset'])
        return conn
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
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(),"%Y-%m-%d %H:%M:%S"))+"\n"+"跳板机连接成功！"+"\n")
            return conn
        except Exception as e:
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(),"%Y-%m-%d %H:%M:%S"))+"\n"+str(e)+"\n")
    def getAppidLabel(self):
        try:
            Oserconn=self.O_ServeseWxbDataBase("db_ex_chain")
            Osercurson=Oserconn.cursor()
            sql='select distinct app_id,case when kind=101 then "个人提升" when kind=102 then "女性时尚" when kind=103 then "生活文艺" when kind=104 then "读书文化" when kind=105 then "情感心理" when kind=106 then "母婴亲子" when kind=107 then "教育培训" when kind=108 then "财经楼市" when kind=109 then "职场创业" when kind=110 then "军事时政" else "医疗健康" end as types,phone from t_joined_extra where kind in (101,102,103,104,105,106,107,108,109,110,111);'
            Osercurson.execute(sql)
            data=Osercurson.fetchall()
            return data
        except Exception as e:
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S")) + "\n" + str(e) + "\n")
            return None
    def insertAppidLabel(self):
        try:
            data=self.getAppidLabel()
            ##清空t_service_appid_label表
            sql1="truncate table t_service_appid_label;"
            sql2="insert into t_service_appid_label(appid,labels,phone) values(%s,%s,%s);"
            hiveconn=self.HiveTestDataBase()
            hiveconsur=hiveconn.cursor()
            hiveconsur.execute(sql1)
            hiveconsur.executemany(sql2,data)
            hiveconn.commit()
            hiveconsur.close()
            hiveconn.close()
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S")) + "\n" + "数据更新成功!" + "\n")
            print("over!")
            return 0
        except Exception as e:
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S")) + "\n" + str(e) + "\n")
            return 1
def main():
    Appid=AppidLabel()
    count=3
    single=Appid.insertAppidLabel()
    while(single*count>0):
        single = Appid.insertAppidLabel()
        count=count-1
if __name__ == '__main__':
    main()







