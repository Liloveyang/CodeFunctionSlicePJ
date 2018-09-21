#coding=utf-8
'''
建立数据库信息，收集stackoverflow上面关于poi项目的所有帖子的：标题（title),帖子内容（body),回复内容(answer），最高评分赞的回复内容

建立：poi项目所有的方法的实体名字（entity）,方法的解释(explain)
'''

import pymysql


class MysqlConnectFactory(object):
    INFO_1 = {'MYSQL_IPADDRESS':"10.141.221.87",'MYSQL_USER':"root",'MYSQL_PASSWORD':"root",'MYSQL_DATABASENAME':"domainkg"}
    INFO_2 = {'MYSQL_IPADDRESS': "10.141.221.85", 'MYSQL_USER': "root", 'MYSQL_PASSWORD': "root",'MYSQL_DATABASENAME': "poi_soinfo_zfy"}
    INFO_3 = {'MYSQL_IPADDRESS': "10.131.252.160", 'MYSQL_USER': "root", 'MYSQL_PASSWORD': "root",'MYSQL_DATABASENAME': "stackoverflow"}

    @classmethod
    def get_connect(cls,connect_info):
        connection = None
        try:
            # 本质建立和mysql-server 建立socket通信
            connection = pymysql.connect(connect_info['MYSQL_IPADDRESS'],connect_info['MYSQL_USER'],connect_info['MYSQL_PASSWORD'],connect_info['MYSQL_DATABASENAME'],port=3306)
        except Exception as e:
            print(e)
        return connection

    @classmethod
    def close_connect(cls,connection):
        if connection:
            connection.close()



if __name__ =="__main__":
    con = MysqlConnectFactory.get_connect(MysqlConnectFactory.INFO_1)
    statement = con.cursor()
    print(statement)
    MysqlConnectFactory.close_connect(con)
