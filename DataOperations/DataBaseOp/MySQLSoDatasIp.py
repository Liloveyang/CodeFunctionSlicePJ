# coding=utf-8
import SoDatasI
from MysqlConnectFactory import MysqlConnectFactory
import threading

'''
 从mysql 数据库中获取stackOverflow的数据
'''

#生成器包装类这个不能用，因为send会丢失前面send的数据
# def _pre_deal(func):
#     def core(*args,**kwargs):
#         gen = func(*args,**kwargs)
#         gen.send(None)
#         return gen
#     return core


class MySQLSoDatasIp(SoDatasI.SoDatasI, object):

    # 每次查询5000条
    SIZE = 5000

    def __init__(self,threadLocal,connect_info):
        # 利用多线程保护起来1.不用局部传参 2.避免资源浪费
        self.threadLocal = threadLocal
        self.threadLocal.connect_so = MysqlConnectFactory.get_connect(connect_info)
        self.statement_so = self.threadLocal.connect_so.cursor()

    # override
    def fetch_soposts(self, flag):
        yield from self.__deal_soposts_info(self.statement_so)

    '''
    处理方法全面，以及方法的描述信息 利用mysql的分页和python生成器实现每次5000条的处理，避免内存存不下
    '''
    def __deal_soposts_info(self, statement):
        sql_1 = "select count(id) from apache_poi_so_question"
        sql_2 = "select question_id,title,body_markdown,view_count from apache_poi_so_question order by id asc limit %s,%s"
        try:
            statement.execute(sql_1)
            total = statement.fetchone()[0]
            # 能够整除 就少循环一次
            times = total // MySQLSoDatasIp.SIZE
            if total % MySQLSoDatasIp.SIZE != 0:
                times += 1
            for i in range(times):
                statement.execute(sql_2, (i * MySQLSoDatasIp.SIZE, MySQLSoDatasIp.SIZE))
                data = statement.fetchall()
                yield data
        except Exception as e:
            raise e

        return True


    def fetch_soposts_answer(self, flag):
        yield from self.__deal_soposts_answer_info(self.statement_so)

    '''
    处理方法全面，以及方法的描述信息 利用mysql的分页和python生成器实现每次5000条的处理，避免内存存不下
    '''
    def __deal_soposts_answer_info(self, statement):
        sql_1 = "select count(id) from apache_poi_so_answer"
        sql_2 = "select id,body_markdown,up_vote_count,question_id from apache_poi_so_answer order by id asc limit %s,%s"
        try:
            statement.execute(sql_1)
            total = statement.fetchone()[0]
            # 能够整除 就少循环一次
            times = total // MySQLSoDatasIp.SIZE
            if total % MySQLSoDatasIp.SIZE != 0:
                times += 1
            for i in range(times):
                statement.execute(sql_2, (i * MySQLSoDatasIp.SIZE, MySQLSoDatasIp.SIZE))
                data = statement.fetchall()
                yield data
        except Exception as e:
            raise e

        return True


    def fetch_pj(self, pj):
        yield from self.__deal_method_entityInfo(self.statement_so)

    '''
     处理方法全面，以及方法的描述信息 利用mysql的分页和python生成器实现每次5000条的处理，避免内存存不下
    '''
    def __deal_method_entityInfo(self,statement):
          sql_1 = "select count(id) from java_all_api_entity"
          sql_2 = "select id,qualified_name,short_description from java_all_api_entity order by id asc limit %s,%s"
          try:
             statement.execute(sql_1)
             total = statement.fetchone()[0]
             #能够整除 就少循环一次
             times = total // MySQLSoDatasIp.SIZE
             if total % MySQLSoDatasIp.SIZE !=0:
                  times+=1
             for i in range(times):
                 statement.execute(sql_2,(i*MySQLSoDatasIp.SIZE,MySQLSoDatasIp.SIZE))
                 data = statement.fetchall()
                 yield data
          except Exception as e:
              raise e
          return True


    def close_connect(self):
        if self.statement_so:
            self.statement_so.close()
        if self.threadLocal.connect_so:
            self.threadLocal.connect_so.close()



if __name__=='__main__':
    #import threading
    threadLocal = threading.local()
    so = MySQLSoDatasIp(threadLocal,MysqlConnectFactory.INFO_1)
    gen = so.fetch_soposts("fadsf")
    for i in gen:
        print(i[5])