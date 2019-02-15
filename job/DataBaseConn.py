import pymysql
from sshtunnel import SSHTunnelForwarder
class DataBaseConnect():
    def __init__(self,dname):
        self.dname=dname
        self.Testdatabase={
           
            }
        self.OServeseWxbdatabase = {
            
        }
        self.Logsdatabase = {
            
        }
        self.Subprimebussinessdatabase = {
           
        }

        self.Bussinessdatabase = {
           
        }
        self.Massagesenddatabase = {
            
        }
        self.DataAnalisisCSidedatabase = {
            
        }
        self.DataAnalysisdatabase = {
           
        }
        self.DataAlive={
            
        }
    def AliveDataBase(self):
        # 直播库连接
        conn = pymysql.connect(host=self.DataAlive['host'],
                               port=self.DataAlive['port'],
                               user=self.DataAlive['user'],
                               password=self.DataAlive['password'],
                               database=self.dname,
                               charset=self.DataAlive['charset'])
        return conn
    def TestDataBase(self):
        # 测试库连接
        conn = pymysql.connect(host=self.Testdatabase['host'],
                               port=self.Testdatabase['port'],
                               user=self.Testdatabase['user'],
                               password=self.Testdatabase['password'],
                               database=self.dname,
                               charset=self.Testdatabase['charset'])
        return conn
    def O_ServeseWxbDataBase(self):
        # O端wxb库（常用）
        # self.dbname=dbname
        conn = pymysql.connect(host=self.OServeseWxbdatabase['host'],
                               port=self.OServeseWxbdatabase['port'],
                               user=self.OServeseWxbdatabase['user'],
                               password=self.OServeseWxbdatabase['password'],
                               database=self.dname,
                               charset=self.OServeseWxbdatabase['charset'])
        return conn
    def LogsDataBase(self):
        # 日志
        # self.dbname=dbname
        conn = pymysql.connect(host=self.Logsdatabase['host'],
                               port=self.Logsdatabase['port'],
                               user=self.Logsdatabase['user'],
                               password=self.Logsdatabase['password'],
                               database=self.dname,
                               charset=self.Logsdatabase['charset'])
        return conn
    def SubprimeBussinessDataBase(self):
        # 次级业务
        # self.dbname=dbname
        conn = pymysql.connect(host=self.Subprimebussinessdatabase['host'],
                               port=self.Subprimebussinessdatabase['port'],
                               user=self.Subprimebussinessdatabase['user'],
                               password=self.Subprimebussinessdatabase['password'],
                               database=self.dname,
                               charset=self.Subprimebussinessdatabase['charset'])
        return conn
    def BussinessDataBase(self):
        # 业务库(常用）
        # self.dbname=dbname
        conn = pymysql.connect(host=self.Bussinessdatabase['host'],
                               port=self.Bussinessdatabase['port'],
                               user=self.Bussinessdatabase['user'],
                               password=self.Bussinessdatabase['password'],
                               database=self.dname,
                               charset=self.Bussinessdatabase['charset'])
        return conn
    def MessageSendDataBase(self):
        # 消息推送数据库
        # self.dbname=dbname
        conn = pymysql.connect(host=self.Massagesenddatabase['host'],
                               port=self.Massagesenddatabase['port'],
                               user=self.Massagesenddatabase['user'],
                               password=self.Massagesenddatabase['password'],
                               database=self.dname,
                               charset=self.Massagesenddatabase['charset'])
        return conn
    def DataAnalisisDataBase(self):
        # 数据分析库
        # self.dbname=dbname
        conn = pymysql.connect(host=self.DataAnalysisdatabase['host'],
                               port=self.DataAnalysisdatabase['port'],
                               user=self.DataAnalysisdatabase['user'],
                               password=self.DataAnalysisdatabase['password'],
                               database=self.dname,
                               charset=self.DataAnalysisdatabase['charset'])
        return conn
    def DataAnalisisCSideDataBase(self):
        conn = pymysql.connect(host=self.DataAnalisisCSidedatabase['host'],
                               port=self.DataAnalisisCSidedatabase['port'],
                               user=self.DataAnalisisCSidedatabase['user'],
                               password=self.DataAnalisisCSidedatabase['password'],
                               database=self.dname,
                               charset=self.DataAnalisisCSidedatabase['charset'])
        return conn
