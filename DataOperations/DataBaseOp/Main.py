#coding=utf-8
from MysqlConnectFactory import MysqlConnectFactory
from MySQLSoDatasIp import MySQLSoDatasIp
from MySQLStoreSoDatas import MySQLStoreSoDatas
from ThirdPartyJarSoDatas import PoiDtonpjSoDatas,TAGS
from ThirdPartyStoreSoDatas import PoiDtonpjStoreSoDatas
import threading

#整个环境 就一个dict就行
threadLocal = threading.local()

'''
处理整个mysql的业务逻辑，从原始数据库搬迁数据到新的数据库！ 缺点没有考虑整个业务：取出，存入 这是一个事务！需要改进！
问题：如何跨数据库(跨主机进行事务处理)
处理的是poi项目中所有方法全名字和方法的解释说明
'''
def dealEntitys(connect_1,connect_2):
    soDatas = MySQLSoDatasIp(threadLocal,connect_1)
    storeDatas = MySQLStoreSoDatas(threadLocal,connect_2)
    fetchGenerator = soDatas.fetch_pj("poi")

    # 生成器的特性
    for datas in fetchGenerator:
        storeDatas.store_method_entityInfo(datas)
        print("正在处理dealEntity信息"+str(datas[-1][0]))
    #清理资源
    soDatas.close_connect()
    storeDatas.close_connect()
    print("method Entity信息处理完毕")

'''
处理整个mysql的业务逻辑，从原始数据库搬迁数据到新的数据库！ 缺点没有考虑整个业务：取出，存入 这是一个事务！需要改进！
问题：如何跨数据库(跨主机进行事务处理)
处理的是拿到so上面的所有关于apache-poi项目所发出的帖子
'''
def dealPosts(connect_1,connect_2):
    soDatas = MySQLSoDatasIp(threadLocal, connect_1)
    storeDatas = MySQLStoreSoDatas(threadLocal, connect_2)
    fetchGenerator = soDatas.fetch_soposts("poi")

    # 生成器的特性
    for datas in fetchGenerator:
        storeDatas.store_soposts_info(datas)
        print("正在处理dealPosts信息" + str(datas[-1][0]))

    # 清理资源
    soDatas.close_connect()
    storeDatas.close_connect()
    print("posts信息处理完毕")

'''
处理整个mysql的业务逻辑，从原始数据库搬迁数据到新的数据库！ 缺点没有考虑整个业务：取出，存入 这是一个事务！需要改进！
问题：如何跨数据库(跨主机进行事务处理)
处理的是拿到so上面所有关于poi项目发出的帖子的回复answer信息
'''
def dealAnswers(connect_1,connect_2):
    soDatas = MySQLSoDatasIp(threadLocal, connect_1)
    storeDatas = MySQLStoreSoDatas(threadLocal, connect_2)
    fetchGenerator = soDatas.fetch_soposts_answer("poi")

    # 生成器的特性
    for datas in fetchGenerator:
        storeDatas.store_soposts_answer_info(datas)
        print("dealAnswers" + str(datas[-1][0]))
    # 清理资源
    soDatas.close_connect()
    storeDatas.close_connect()
    print("answers信息处理完毕")

'''
处理整个mysql的业务逻辑，从原始数据库搬迁数据到新的数据库！ 缺点没有考虑整个业务：取出，存入 这是一个事务！需要改进！
问题：如何跨数据库(跨主机进行事务处理)
处理的是拿到so上面所有关于poi项目发出的帖子的回复answer信息的最主要
'''
def dealUpVoteBody(connect_2):
    storeDatas = MySQLStoreSoDatas(threadLocal, connect_2)
    fetchGenerator = storeDatas.fetch_upvote_answer_body("poi")

    # 生成器的特性
    for data in fetchGenerator:
        storeDatas.store_upvote_answer_body(data)
        print("dealup_Answers_body"+str(data))
    # 清理资源
    storeDatas.close_connect()
    print("dealup_Answers_body信息处理完毕")


'''
处理整个mysql的业务逻辑，从原始数据库搬迁数据到新的数据库！ 缺点没有考虑整个业务：取出，存入 这是一个事务！需要改进！
问题：如何跨数据库(跨主机进行事务处理)
处理的是拿到so上面所有关于poi依赖的项目发出的帖子的信息
'''
def dealDtonpjPosts(connect_2,connect_3,tag):
    soDatas = PoiDtonpjSoDatas(threadLocal, connect_3)
    storeDatas = PoiDtonpjStoreSoDatas(threadLocal, connect_2)
    fetchGenerator = soDatas.fetch_soposts("poi")

    # 生成器的特性
    for data in fetchGenerator:
        storeDatas.store_Dtonpj_soposts_info(data,tag)
    # 清理资源
    soDatas.close_connect()
    storeDatas.close_connect()
    print("DtonpjPosts信息处理完毕")

'''
处理整个mysql的业务逻辑，从原始数据库搬迁数据到新的数据库！ 缺点没有考虑整个业务：取出，存入 这是一个事务！需要改进！
问题：如何跨数据库(跨主机进行事务处理)
处理的是拿到so上面所有关于poi依赖的项目发出的帖子的answer 信息
'''
def dealDtonpjAnswers(connect_2,connect_3):
    soDatas = PoiDtonpjSoDatas(threadLocal, connect_3)
    storeDatas = PoiDtonpjStoreSoDatas(threadLocal, connect_2)
    question_id_generator = storeDatas.fetch_Dtonpj_soposts_question_id()

    # 生成器的特性
    for question_id in question_id_generator:
        answers_generator = soDatas.fetch_answers(question_id)
        for answer_info in answers_generator:
            storeDatas.store_Dtonpj_soposts_answer_info(answer_info)
    # 清理资源
    soDatas.close_connect()
    storeDatas.close_connect()
    print("DtonpjAnswers信息处理完毕")

'''
处理整个mysql的业务逻辑，从原始数据库搬迁数据到新的数据库！ 缺点没有考虑整个业务：取出，存入 这是一个事务！需要改进！
问题：如何跨数据库(跨主机进行事务处理)
处理的是拿到so上面所有关于poi依赖的项目发出的帖子的回复answer信息的最主要
'''
def dealDtonpjUpVoteBody(connect_2):
    storeDatas = PoiDtonpjStoreSoDatas(threadLocal, connect_2)
    fetchGenerator = storeDatas.fetch_dtonpj_upvote_answer_body("poi依赖的项目")

    # 生成器的特性
    for data in fetchGenerator:
        storeDatas.store_dtonpj_upvote_answer_body(data)
        print(data)
    # 清理资源
    storeDatas.close_connect()
    print("dealDtonpjUpVoteBody信息处理完毕")


if  __name__ =="__main__":
    # 数据库1是原始混杂数据库
    con1 = MysqlConnectFactory.INFO_1
    # 数据库2是新建的专用数据库
    con2 = MysqlConnectFactory.INFO_2
    # 数据库3是所有stackoverflow信息
    con3 = MysqlConnectFactory.INFO_3
    # tmp = [dealEntitys, dealPosts]
    #
    # for i in tmp:
    #     t = threading.Thread(target=i, args=(con1, con2))
    #     t.start()
    # t.join()
    # threading.Thread(target=dealAnswers, args=(con1, con2)).start()
    #threading.Thread(target=dealUpVoteBody, args=(con2,)).start()
    # for i in TAGS:
    #     t = threading.Thread(target=dealDtonpjPosts,args=(con2,con3,i))
    #     t.start()

    threading.Thread(target=dealDtonpjUpVoteBody, args=(con2,)).start()

