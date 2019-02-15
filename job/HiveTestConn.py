import pymysql
from sshtunnel import SSHTunnelForwarder
class HiveTest():
    def __init__(self):
        pass
    def HiveTestDataBase(self):
        try:
            server = SSHTunnelForwarder(
                ('*******', 22),  # B机器的配置
                ssh_password='******',
                ssh_username='*****',
                remote_bind_address=('******', 3306)  # A机器的配置
            )
            server.start()
            conn = pymysql.connect(host='127.0.0.1',  # 此处必须是是127.0.0.1
                                   user='*****',
                                   passwd='*****',
                                   db='******', charset='utf8mb4',
                                   port=server.local_bind_port)
            # print("连接成功！")
            return conn
        except Exception as e:
            print("链接失败:", e)