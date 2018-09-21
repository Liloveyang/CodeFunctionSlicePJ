#coding=utf-8

from MysqlConnectFactory import MysqlConnectFactory


'''
 Dtonpj(dependent on Project缩写) 用来存储poi所依赖的项目的title，body，answer等信息
'''


class PoiDtonpjStoreSoDatas(object):

    '''
    tag 说明的是poi所依赖的项目的名称
    '''
    def __init__(self, threadLocal, connect_info):
        # 利用多线程保护起来1.不用局部传参 2.避免资源浪费
        self.threadLocal = threadLocal
        self.threadLocal.connect_st = MysqlConnectFactory.get_connect(connect_info)
        self.statement_st = self.threadLocal.connect_st.cursor()


    '''
       批量存储so上的帖子信息数据
       每一个是（question_id,title,body_markdown,view_count）
    '''
    def store_Dtonpj_soposts_info(self, data,tag):
        sql = "insert into poi_dtonpj_soposts_li(question_id,question_title,question_body,tag) value (%s,%s,%s,%s)"
        try:
            print(data,tag)
            self.statement_st.execute(sql, (data[0],data[1],data[2],tag))
            self.threadLocal.connect_st.commit()
        except Exception as e:
            self.threadLocal.connect_st.rollback()
            raise e

    def close_connect(self):
        if self.statement_st:
            self.statement_st.close()
        if self.threadLocal.connect_st:
            self.threadLocal.connect_st.close()

    '''
          批量存储so上的帖子信息数据
          每一个是（question_id,title,body_markdown,view_count）
       '''

    def fetch_Dtonpj_soposts_question_id(self):
        sql = "select question_id from poi_dtonpj_soposts_li"
        try:
            self.statement_st.execute(sql)
            question_ids = self.statement_st.fetchall()
            for question_id in question_ids:
                yield question_id
            self.threadLocal.connect_st.commit()
        except Exception as e:
            self.threadLocal.connect_st.rollback()
            raise e

    def store_Dtonpj_soposts_answer_info(self,data):
        sql = "insert into poi_dtonpj_soposts_answer_li(question_id,answer_body,vote) value (%s,%s,%s)"
        try:
            self.statement_st.execute(sql,data)
            self.threadLocal.connect_st.commit()
        except Exception as e:
            self.threadLocal.connect_st.rollback()
            raise e

    def fetch_dtonpj_upvote_answer_body(self, flag):
        sql_2 = "select p.question_id,p.answer_body from poi_dtonpj_soposts_answer_li as p,DtonpjView as a where p.question_id = a.question_id and a.m_vote = p.vote;"
        try:
            self.statement_st.execute(sql_2)
            datas = self.statement_st.fetchall()
            for data in datas:
                yield data
        except Exception as e:
            raise e
        return True

    def store_dtonpj_upvote_answer_body(self, data):
        sql = "update poi_dtonpj_soposts_li set answer_body_upvote = %s where question_id= %s"
        try:
            self.statement_st.execute(sql, (data[1], data[0]))
            self.threadLocal.connect_st.commit()
        except Exception as e:
            raise e


if __name__ == '__main__':
    import threading
    threadLocal = threading.local()
    so = PoiDtonpjStoreSoDatas(threadLocal, MysqlConnectFactory.INFO_2)
    gen = so.fetch_Dtonpj_soposts_question_id()
    for i in gen:
        print(i)
        break