import redis

from datetime import  datetime,timedelta

import os
import configparser





def test():
    # red = redis.Redis(host='192.168.118.71', password='', port=6379, db=1)
    # red.set('1','a')

    #
    # t = datetime.now()
    # t1 = t-timedelta(days=1)
    # print(t)
    # print(t1)
    #
    # t = t.strftime('%Y-%m-%d 08:00:00')
    # t1 = t1.strftime('%Y-%m-%d 08:00:00')
    #
    # print(t)
    # print(t1)

    dict={}
    dict["a"]

def getConfig():
    root_dir = os.path.dirname(os.path.abspath("\\"))
    # root_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    # root_dir = os.path.join(root_dir, file_Name)
    cf = configparser.ConfigParser()
    root_dir = os.path.join(root_dir, "config.ini")
    cf.read(root_dir)
    # dict = {}
    # dict["sql_server_database"] = cf.items("sql_server_database")
    # dict["GetData_config"] = cf.items("GetData_config")
    # dict["redis_config"] = cf.items("redis_config")
    # dict["sql_server_databaseEx"] = cf.items("sql_server_databaseEx")

    # t = datetime.now()
    # t1 = t - timedelta(days=1)
    # t = t.strftime('%Y%m%d 08:00:00')
    # t1 = t1.strftime('%Y-%m-%d 08:00:00')
    #
    # dict["GetData_config"][0] = 'start_Time', t1
    # dict["GetData_config"][1] = 'end_TIme', t

    # ret = cf.items('GetData_config')
    for i in ['aaa','bbb']:
        cf.set('GetData_config', 'mlb', i)
        cf.write(open(root_dir, 'w'))
    # return dict




    pass

def Print_to_CSV():
    # 获取配置
    # dict = getConfig()
    t = datetime.now()
    file_Name = t.strftime('%Y%m%d') + '_B419.CSV'
    print(file_Name)

def test1():
    for i in ['N','Y']:
        print(i)




if __name__ == '__main__':
    # Print_to_CSV()
    # test1()
    # getConfig()
    print("TEST")
    pass