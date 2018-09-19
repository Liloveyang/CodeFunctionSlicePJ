# coding=utf-8
from MysqlConnectFactory import MysqlConnectFactory
import threading
'''
 从mysql 数据库中获取stackOverflow的数据
'''




class MySQLStoreSoDatas(object):

    def __init__(self, threadLocal,connect_info):
        # 利用多线程保护起来1.不用局部传参 2.避免资源浪费
        self.threadLocal = threadLocal
        self.threadLocal.connect_st = MysqlConnectFactory.get_connect(connect_info)
        self.statement_st = self.threadLocal.connect_st.cursor()

    '''
        批量存储so上的帖子answer信息数据
        datas 5000条数据  每一个是（id,body_markdown,up_vote_count,question_id）
        '''

    def store_soposts_answer_info(self, datas):
        sql = "insert into poi_soposts_answer_li(id,answer_body,vote,question_id) value (%s,%s,%s,%s)"
        try:
            self.statement_st.executemany(sql, datas)
            self.threadLocal.connect_st.commit()
        except Exception as e:
            self.threadLocal.connect_st.rollback()
            raise e

    '''
    批量存储so上的帖子信息数据
    datas 5000条数据  每一个是（question_id,title,body_markdown,view_count）
    '''
    def store_soposts_info(self, datas):
        sql = "insert into poi_soposts_li(question_id,question_title,question_body,viewcount) value (%s,%s,%s,%s)"
        try:
            self.statement_st.executemany(sql, datas)
            self.threadLocal.connect_st.commit()
        except Exception as e:
            self.threadLocal.connect_st.rollback()
            raise e

    '''
     批量存储方法信息数据
     datas 5000条数据  每一个是（id,qualified_name,short_description）
    '''
    def store_method_entityInfo(self,datas):
        sql = "insert into poi_mdentitys_li(id,method_qualified_name,description) value (%s,%s,%s)"
        try:
            self.statement_st.executemany(sql,datas)
            self.threadLocal.connect_st.commit()
        except Exception as e:
            self.threadLocal.connect_st.rollback()
            raise e


    def fetch_upvote_answer_body(self,flag):
        sql_2 = "select p.question_id,p.answer_body from poi_soposts_answer_li as p,TMP as a where p.question_id = a.question_id and a.m_vote = p.vote;"
        try:
            self.statement_st.execute(sql_2)
            datas = self.statement_st.fetchall()
            for data in datas:
                yield data
        except Exception as e:
            raise e
        return True

    def store_upvote_answer_body(self,data):
        sql = "update poi_soposts_li set answer_body_upvote = %s where question_id= %s"
        try:
            self.statement_st.execute(sql,(data[1],data[0]))
            self.threadLocal.connect_st.commit()
        except Exception as e:
            raise e

    def close_connect(self):
        if self.statement_st:
            self.statement_st.close()
        if self.threadLocal.connect_st:
            self.threadLocal.connect_st.close()

if __name__ == '__main__':
    threadLocal = threading.local()
    so = MySQLStoreSoDatas(threadLocal, MysqlConnectFactory.INFO_2)
    gen = so.fetch_upvote_answer_body("poi")
    for i in gen:
        print(i)
        break
