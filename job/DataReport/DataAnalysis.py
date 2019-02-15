# -*- coding: utf-8 -*-
import pymysql
import datetime
class LogsKpi:
    def __init__(self):
        self.today=datetime.datetime.today()
        self.yesterday=datetime.datetime.strftime(self.today+datetime.timedelta(days=-1),'%Y-%m-%d')
        self.yesterday_y_m=self.yesterday[:4]+'_'+self.yesterday[5:7]
        self.Logsdatabase = {
            'host': '******',
            'port': *****,
            'user': '******',
            'password': '******',
            'charset': 'utf8'
        }
        self.Testdatabase = {
            'host': '******',
            'port': ***,
            'user': '****',
            'password': '***',
            'charset': 'utf8'
        }
        self.Bussinessdatabase = {
            'host': '******',
            'port': ****,
            'user': '****',
            'password': '****',
            'charset': 'utf8'
        }
        self.DataAnalisisCSidedatabase = {
            'host': '*****',
            'port': *****,
            'user': '*****',
            'password': *******
            'charset': 'utf8'
        }
    def LogsDataBase(self,dname):
        # 日志
        # self.dbname=dbname
        conn = pymysql.connect(host=self.Logsdatabase['host'],
                               port=self.Logsdatabase['port'],
                               user=self.Logsdatabase['user'],
                               password=self.Logsdatabase['password'],
                               database=dname,
                               charset=self.Logsdatabase['charset'])
        return conn
    def TestDataBase(self,dname):
        # 测试库连接
        conn = pymysql.connect(host=self.Testdatabase['host'],
                               port=self.Testdatabase['port'],
                               user=self.Testdatabase['user'],
                               password=self.Testdatabase['password'],
                               database=dname,
                               charset=self.Testdatabase['charset'])
        return conn
    def BussinessDataBase(self,dname):
        # 业务库(常用）
        # self.dbname=dbname
        conn = pymysql.connect(host=self.Bussinessdatabase['host'],
                               port=self.Bussinessdatabase['port'],
                               user=self.Bussinessdatabase['user'],
                               password=self.Bussinessdatabase['password'],
                               database=dname,
                               charset=self.Bussinessdatabase['charset'])
        return conn
    def DataAnalisisCSideDataBase(self,dname):
        conn = pymysql.connect(host=self.DataAnalisisCSidedatabase['host'],
                               port=self.DataAnalisisCSidedatabase['port'],
                               user=self.DataAnalisisCSidedatabase['user'],
                               password=self.DataAnalisisCSidedatabase['password'],
                               database=dname,
                               charset=self.DataAnalisisCSidedatabase['charset'])
        return conn
    def BsideLogs_6_url(self, daybefore, dayafter):
        f = open(r'/home/qjc/qianjiangchao/DataAnalysis/logging.txt', 'a+')
        try:
            Logsconn = self.LogsDataBase('db_ex_logs')
            Logsconsur=Logsconn.cursor()
            Testconn=self.TestDataBase('db_ex_chain')
            Testconsur = Testconn.cursor()
            ###B端6个页面PV,UV
            sql1 = 'select date_format (created_at,"%Y-%m-%d") as dt,target_url,count(*)as PV,count(distinct app_id) as AppIdUV,count(distinct phone_name)as PhoneUV from t_admin_log_{0} where target_url in("https://admin.xiaoe-tech.com/data_analysis/get_charge_analysis","https://admin.xiaoe-tech.com/data_analysis/get_overview_data","https://admin.xiaoe-tech.com/data_analysis/get_resource_detail","https://admin.xiaoe-tech.com/data_analysis/get_resource_num","https://admin.xiaoe-tech.com/data_analysis/get_traffic_data","https://admin.xiaoe-tech.com/data_analysis/get_user_overview") and created_at>=DATE_FORMAT(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {2} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d"),target_url order by date_format (created_at,"%Y-%m-%d"),target_url;' \
                .format(self.yesterday_y_m, daybefore, dayafter)
            ###B端6个页面PV,UV汇总
            sql2 = 'select date_format (created_at,"%Y-%m-%d") as dt,count(*)as PV,count(distinct app_id) as AppIdUV,count(distinct phone_name)as PhoneUV from t_admin_log_{0} where target_url in("https://admin.xiaoe-tech.com/data_analysis/get_charge_analysis","https://admin.xiaoe-tech.com/data_analysis/get_overview_data","https://admin.xiaoe-tech.com/data_analysis/get_resource_detail","https://admin.xiaoe-tech.com/data_analysis/get_resource_num","https://admin.xiaoe-tech.com/data_analysis/get_traffic_data","https://admin.xiaoe-tech.com/data_analysis/get_user_overview") and created_at>=DATE_FORMAT(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {2} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d");' \
                .format(self.yesterday_y_m, daybefore, dayafter)
            ###用户分群3个页面PV,UV
            sql3 = 'select date_format (created_at,"%Y-%m-%d") as dt,target_url,count(*)as PV,count(distinct app_id) as AppIdUV,count(distinct phone_name)as PhoneUV from t_admin_log_{0} where target_url regexp "https://admin.xiaoe-tech.com/customer_gruop" and created_at>=DATE_FORMAT(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {2} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d"),target_url order by date_format (created_at,"%Y-%m-%d"),target_url;' \
                .format(self.yesterday_y_m, daybefore, dayafter)
            ###用户分群3个页面PV,UV汇总
            sql4 = 'select date_format (created_at,"%Y-%m-%d") as dt,count(*)as PV,count(distinct app_id) as AppIdUV,count(distinct phone_name)as PhoneUV from t_admin_log_{0} where target_url regexp "https://admin.xiaoe-tech.com/customer_gruop" and created_at>=DATE_FORMAT(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {2} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d");' \
                .format(self.yesterday_y_m, daybefore, dayafter)
            result = []
            for i in (sql1, sql2, sql3, sql4):
                Logsconsur.execute(i)
                result.append(Logsconsur.fetchall())
            Logsconsur.close()
            Logsconn.close()
            try:
                result_allurl = []
                for j in result[0]:
                    print(j)
                    if 'get_charge_analysis' in j[1]:
                        result_allurl.append((j[0], '交易分析', 1, j[1], j[2], j[3], j[4]))
                    elif 'get_overview_data' in j[1]:
                        result_allurl.append((j[0], '数据概况', 1, j[1], j[2], j[3], j[4]))
                    elif 'get_resource_detail' in j[1]:
                        result_allurl.append((j[0], '商品分析详情页', 1, j[1], j[2], j[3], j[4]))
                    elif 'get_resource_num' in j[1]:
                        result_allurl.append((j[0], '商品分析', 1, j[1], j[2], j[3], j[4]))
                    elif 'get_traffic_data' in j[1]:
                        result_allurl.append((j[0], '流量分析', 1, j[1], j[2], j[3], j[4]))
                    elif 'get_user_overview' in j[1]:
                        result_allurl.append((j[0], '用户分析', 1, j[1], j[2], j[3], j[4]))
                    else:
                        pass
                for j in result[1]:
                    result_allurl.append((j[0], '汇总', 1, '汇总', j[1], j[2], j[3]))
                print('B端6个页面数据读取完成！')
            except Exception as e:
                print(e)
            sql_insert = "insert into t_bside_pvuv(dated,urlname,urltype,url,pv,appid_uv,phone_uv) values(%s,%s,%s,%s,%s,%s,%s) "
            print(result_allurl)
            Testconsur.executemany(sql_insert, result_allurl)
            Testconn.commit()
            Testconn.close()
            Testconsur.close()
            print("成功批量插入数据库！！！")
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(),"%Y-%m-%d %H:%M:%S"))+'\n'+"B端6个页面已成功插入"+'\n')
            f.close()
            return 0
        except Exception as e:
            print('产生异常：')
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S")) + '\n' + str(e)+'\n')
            f.close()
            print(e)
            return 1
    def UserGroupUrl(self,daybefore,dayafter):
        f = open(r'/home/qjc/qianjiangchao/DataAnalysis/logging.txt', 'a+')
        try:
            Logsconn = self.LogsDataBase('db_ex_logs')
            Logsconsur = Logsconn.cursor()
            Testconn = self.TestDataBase('db_ex_chain')
            Testconsur = Testconn.cursor()
            DataCsideconn=self.DataAnalisisCSideDataBase('db_ex_crowd')
            DataCsideconsur=DataCsideconn.cursor()
            Bussinessconn=self.BussinessDataBase('db_ex_business')
            Bussinessconsur=Bussinessconn.cursor()
            result = []
            sql1= 'select date_format (created_at,"%Y-%m-%d") as dt,count(*)as PV,count(distinct app_id) as AppIdUV,count(distinct phone_name)as PhoneUV from t_admin_log_{0} where target_url regexp "https://admin.xiaoe-tech.com/customer_gruop" and created_at>=DATE_FORMAT(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {2} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d");' \
                .format(self.yesterday_y_m, daybefore, dayafter)
            sql2='select count(id)as GroupNumbers from t_crowd where template_id=0 and app_id not in ("app38itOR341547", "apprnDA0ZDw4581", "appweq0zKjm4147", "apppcHqlTPT3482") and created_at>=DATE_FORMAT(date_sub(current_date(),interval {0} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d");'.format(daybefore, dayafter)
            sql3='select count(*)as GroupMessages from t_messages where type=2 and created_at>=DATE_FORMAT(date_sub(current_date(),interval {0} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d");'.format(daybefore, dayafter)
            sql4='select count(*) from t_coupon where spread_type = 1 and id IN ( SELECT DISTINCT cou_id FROM t_coupon_plan WHERE type IN ( 2, 3 ) ) and created_at>=DATE_FORMAT(date_sub(current_date(),interval {0} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d");'.format(daybefore, dayafter)
            sql5='select count(*) as groupspullnumbers from t_admin_log_{0} where target_url = "https://admin.xiaoe-tech.com/download_manage/download_csv" and params regexp \'"source":"13"\' and created_at>=DATE_FORMAT(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {2} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d");' \
                .format(self.yesterday_y_m, daybefore, dayafter)
            #sql6='select count(distinct cou_id)as GroupCoupons from t_coupon_plan where crowd_id=0 and created_at>=DATE_FORMAT(date_sub(current_date(),interval {0} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d");'.format(daybefore, dayafter)
            #sql4='select count(distinct cou_id)as GroupCoupons from t_coupon_plan where crowd_id>0 and created_at>=DATE_FORMAT(date_sub(current_date(),interval {0} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") group by date_format (created_at,"%Y-%m-%d");'.format(daybefore, dayafter)
            Logsconsur.execute(sql1)
            datasql1 = Logsconsur.fetchall()
            DataCsideconsur.execute(sql2)
            datasql2=DataCsideconsur.fetchall()
            Bussinessconsur.execute(sql3)
            datasql3=Bussinessconsur.fetchall()
            Bussinessconsur.execute(sql4)
            datasql4=Bussinessconsur.fetchall()
            Logsconsur.execute(sql5)
            datasql5 =Logsconsur.fetchall()
            result.append(datasql1[0][0])
            result.append(datasql1[0][1])
            result.append(datasql1[0][2])
            result.append(datasql1[0][3])
            try:
                result.append(datasql2[0][0])
            except:
                result.append(0)
            try:
                result.append(datasql3[0][0])
            except:
                result.append(0)
            try:
                result.append(datasql4[0][0])
            except:
                result.append(0)
            try:
                result.append(datasql5[0][0])
            except:
                result.append(0)
            print(result)
            DataCsideconsur.close()
            DataCsideconn.close()
            Bussinessconsur.close()
            Bussinessconn.close()
            Logsconsur.close()
            Logsconn.close()
            sql_insert="insert into t_usergroup_analysis(dated,pv,uv_appid,uv_phone,groupnumbers,groupmessages,groupcoupons,grouppullnumbers) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            Testconsur.executemany(sql_insert,[(result[0],result[1],result[2],result[3],result[4],result[5],result[6],result[7])])
            Testconn.commit()
            Testconn.close()
            Testconsur.close()
            print('用户分群数据成功插入')
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S")) + '\n' + '用户分群数据成功插入'+'\n')
            f.close()
            return 0
        except Exception as e:
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S")) + '\n' + str(e)+'\n')
            f.close()
            print('产生异常：')
            print(e)
            return 1
    def PageAnalysis(self,daybefore,dayafter):
        f = open(r'/home/qjc/qianjiangchao/DataAnalysis/logging.txt', 'a+')
        try:
            Logsconn = self.LogsDataBase('db_ex_logs')
            Logsconsur = Logsconn.cursor()
            Testconn = self.TestDataBase('db_ex_chain')
            Testconsur = Testconn.cursor()
            Bussinessconn = self.BussinessDataBase('db_ex_business')
            Bussinessconsur = Bussinessconn.cursor()
            result=[]
            sql1='select date_format (created_at,"%Y-%m-%d") as dt,count(*)as PV,count(distinct app_id) as AppIdUV,count(distinct phone_name)as PhoneUV from t_admin_log_{0} where target_url in("https://admin.xiaoe-tech.com/open_detail","https://admin.xiaoe-tech.com/new_channel","https://admin.xiaoe-tech.com/channel_admin") and created_at>=DATE_FORMAT(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {2} day),"%Y-%m-%d 00:00:00");' \
                    .format(self.yesterday_y_m, daybefore, dayafter)
            sql2='select count(id)as ChannelNumbers from t_channels where created_at>=DATE_FORMAT(date_sub(current_date(),interval {0} day),"%Y-%m-%d 00:00:00") and created_at< date_format(date_sub(current_date(),interval {1} day),"%Y-%m-%d 00:00:00");' \
                    .format(daybefore, dayafter)
            Logsconsur.execute(sql1)
            datasql1=Logsconsur.fetchall()
            Bussinessconsur.execute(sql2)
            datasql2=Bussinessconsur.fetchall()
            try:
                result.append(datasql1[0][0])
            except:
                result.append(0)
            try:
                result.append(datasql1[0][1])
            except:
                result.append(0)
            try:
                result.append(datasql1[0][2])
            except:
                result.append(0)
            try:
                result.append(datasql1[0][3])
            except:
                result.append(0)
            try:
                result.append(datasql2[0][0])
            except:
                result.append(0)
            sql_insert="insert into t_userpage_analysis(dated,pv,uv_appid,uv_phone,channelnumbers) values(%s,%s,%s,%s,%s)"
            Testconsur.executemany(sql_insert,[(result[0],result[1],result[2],result[3],result[4])])
            Testconn.commit()
            Testconn.close()
            Testconsur.close()
            print("页面统计数据插入成功！")
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S")) + '\n' + "页面统计数据插入成功"+'\n')
            f.close()
            return 0
        except Exception as e:
            f.write(str(datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S")) + '\n' + str(e)+'\n')
            f.close()
            print('产生异常：')
            print(e)
            return 1
def insertBsidepvuv(nums):
    Logs = LogsKpi()
    n=Logs.BsideLogs_6_url(nums,nums-1)
    while(n):
        try:
            n=Logs.BsideLogs_6_url(nums,nums-1)
        except Exception as e:
            n=Logs.BsideLogs_6_url(nums,nums-1)
def insertUserGroup(nums):
    Logs = LogsKpi()
    n=Logs.UserGroupUrl(nums, nums - 1)
    while (n):
        try:
            n=Logs.UserGroupUrl(nums, nums-1)
        except Exception as e:
            n = Logs.UserGroupUrl(nums, nums-1)
            print('error', e)
def insertPageAnalysis(nums):
    Logs = LogsKpi()
    n=Logs.PageAnalysis(nums, nums-1)
    while (n):
        try:
            n = Logs.PageAnalysis(nums, nums - 1)
        except Exception as e:
            n = Logs.PageAnalysis(nums, nums - 1)
            print('error', e)
def main():
    nums=1
    insertBsidepvuv(nums)
    insertUserGroup(nums)
    insertPageAnalysis(nums)
    print('successful')
main()