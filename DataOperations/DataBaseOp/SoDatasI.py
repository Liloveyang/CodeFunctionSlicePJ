#coding=utf-8
'''
 从stackOverflow中获取所有关于某一项目的数据接口
'''

import abc

'''
从stackoverflow 获取信息的接口
'''
class SoDatasI(object):

    __metaclass__ = abc.ABCMeta

    '''
      提取posets信息 从apache_poi_so_question
    '''
    @abc.abstractmethod
    def fetch_soposts(self, flag):
        pass

    '''
      提取posets的answer信息 从apache_poi_so_answer
    '''
    @abc.abstractmethod
    def fetch_soposts_answer(self, flag):
        pass

    '''
      提取项目的所有方法全名，及其方法的解释信息
    '''
    @abc.abstractmethod
    def fetch_from_pj(self, pj):
        pass




