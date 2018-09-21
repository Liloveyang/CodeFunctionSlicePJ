#coding=utf-8
'''
处理所有关于第三方包的StackOverflow 帖子的title body answer upvote_answer
curvesapi-1.04.jar
commons-codec-1.10.jar
commons-logging-1.2.jar
stax-api-1.0-2.jar
junit-4.12.jar
jopt-simple-4.6.jar
ant-1.9.4.jar
ant-launcher-1.9.4.jar
commons-collections4-4.1.jar
commons-math3-3.2.jar
xmlsec-2.0.6.jar
xmlbeans-2.6.0.jar
bcpkix-jdk15on-1.54.jar
bcprov-jdk15on-1.54.jar
stax2-api-3.1.4.jar
woodstox-core-asl-4.4.1.jar
hamcrest-core-1.3.jar
jmh-core-1.15.jar
jmh-generator-annprocess-1.15.jar
slf4j-api-1.7.13.jar
stax-api-1.0.1.jar
'''

from MysqlConnectFactory import MysqlConnectFactory

TAGS = ['curves','commons-codec','commons-logging','stax','junit','jopt-simple','ant','ant-launcher','Commons Collections',
        'commons-math','xmlsec','xmlbeans','bcpkix-jdk15on','bcprov-jdk15on','woodstox-core-asl','hamcrest-core','jmh-core','jmh-generator-annprocess','slf4j']

'''
 Dtonpj(dependent on Project缩写) 用来获取poi所依赖的项目的title，body，answer等信息
'''
class PoiDtonpjSoDatas(object):

    '''
    tag 说明的是poi所依赖的项目的名称
    '''
    def __init__(self, threadLocal, connect_info):
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
    def __deal_soposts_info(self, statement,tag):
        tag = r"'%{tag}%'".format(tag=tag)
        sql_1 = "select question_id,title,body from post_view where tag like %s"%(tag)
        try:
            print(sql_1)
            statement.execute(sql_1)
            datas = statement.fetchall()
            for data in datas:
                yield data
        except Exception as e:
            raise e
        return True

    def fetch_answers(self,question_id):
        yield from self.__deal_answer_info(self.statement_so,question_id)

    '''
    处理方法全面，以及方法的描述信息 利用mysql的分页和python生成器实现每次5000条的处理，避免内存存不下
    '''
    def __deal_answer_info(self, statement,question_id):
        sql_1 = "select question_id,answer_body,score from post_answer_view where question_id = %s"
        try:
            #一个question_id 对应多个answers
            statement.execute(sql_1,question_id)
            datas = statement.fetchall()
            for data in datas:
                yield data
        except Exception as e:
            raise e
        return True

    def close_connect(self):
        if self.statement_so:
            self.statement_so.close()
        if self.threadLocal.connect_so:
            self.threadLocal.connect_so.close()

if __name__ == '__main__':
    import threading
    threadLocal = threading.local()
    so = PoiDtonpjSoDatas(threadLocal, MysqlConnectFactory.INFO_3)
    gen = so.fetch_answers("poi",'1300')
    for i in gen:
        print(i)