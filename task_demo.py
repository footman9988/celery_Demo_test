
import os
import time
import redis
import pymssql
import configparser
import pandas as pd
import datetime
import numpy as np
import json

from proj import tasks







# 读取配置
def getConfig():
    root_dir = os.path.dirname(os.path.abspath("\\"))
    cf = configparser.ConfigParser()
    root_dir = os.path.join(root_dir,"config.ini")
    cf.read(root_dir)
    dict = {}
    dict["sql_server_database"] = cf.items("sql_server_database")
    dict["GetData_config"] = cf.items("GetData_config")
    dict["redis_config"] = cf.items("redis_config")
    dict["sql_server_databaseEx"] = cf.items("sql_server_databaseEx")
    return dict


# 直接从190 UOP服务器获取
def getDataForSql_v2():
    # 获取配置
    dict = getConfig()
    try:
        conn = pymssql.connect(server=dict["sql_server_databaseEx"][0][1], user=dict["sql_server_databaseEx"][1][1],
                               password=dict["sql_server_databaseEx"][2][1], database=dict["sql_server_databaseEx"][3][1])


        conn1 = pymssql.connect(server=dict["sql_server_database"][0][1], user=dict["sql_server_database"][1][1],
                               password=dict["sql_server_database"][2][1], database=dict["sql_server_database"][3][1])

        if conn and conn1:
            # 增加两个小时，因为外观检查，和check-in check-out 的时间与其他LINE 获取的时间相隔2个小时
            date_str = (datetime.datetime.fromtimestamp(
                time.mktime(time.strptime(dict["GetData_config"][1][1], "%Y-%m-%d %H:%M:%S"))) + datetime.timedelta(
                hours=2)).strftime(
                "%Y-%m-%d %H:%M:%S")

            sqlStr_190 = "SELECT Sn,test_station_name,station_id,result,start_time,stop_time,product " \
                         "FROM baTestRelatedItemsEx " \
                         "WHERE " \
                         "product='{2}' " \
                         "AND stop_time " \
                         "BETWEEN '{0}' AND '{1}' " \
                         "ORDER BY stop_time ASC ".format(dict["GetData_config"][0][1], date_str, dict["GetData_config"][4][1])


            sqlStr_cos = "SELECT ProductCode, SN, LineName, TestStationProductTask, TestTime, TestResult  " \
                         "FROM FATPYieldFile " \
                         "WHERE ProductCode='{0}'  " \
                         "AND TestTime " \
                         "BETWEEN '{1}' AND '{2}' " \
                         "ORDER BY TestTime ASC ".format(
                dict["GetData_config"][4][1], dict["GetData_config"][0][1], date_str)
        df = pd.DataFrame(
            columns=["SITE", "LINE", "PRODUCT_CODE", "SERIAL_NUMBER", "EQUIPMENT_TYPE", "STATION_ID", "START_TIME",
                     "RESULT",
                     "RETEST", "END_TIME"])

        df1 = pd.DataFrame(
            columns=["SITE", "LINE", "PRODUCT_CODE", "SERIAL_NUMBER", "EQUIPMENT_TYPE", "STATION_ID", "START_TIME",
                     "RESULT",
                     "RETEST", "END_TIME"])
        df4 = pd.read_sql(sqlStr_190,conn)

        # 是否是MLB获取
        ISMLB = dict["GetData_config"][6][1]
        if ISMLB == "Y":
            df4 = df4[df4['Sn'].str.len() == 17]
        else:
            df4 = df4[df4['Sn'].str.len() == 12]
        df['LINE'] = df4['station_id'].map(lambda x:x.split("_")[1])
        df['SITE'] = 'INFY'
        df['PRODUCT_CODE'] = df4['product']
        df['SERIAL_NUMBER'] = df4['Sn'].map(strQ2B)
        df['EQUIPMENT_TYPE'] = df4['test_station_name']
        df['STATION_ID'] = df4['station_id']
        df['START_TIME'] = df4['start_time'].map(lambda x:str(x))
        df['RESULT'] = df4['result']
        df['RETEST'] = ""
        df['END_TIME'] = df4['stop_time'].map(lambda  x:str(x))
        # 读取外观检查
        df5 = pd.read_sql(sqlStr_cos,conn1)
        # 是否是MLB获取
        if ISMLB == "Y":
            df5 = df5[df5['SN'].str.len() == 17]
        else:
            df5 = df5[df5['SN'].str.len() == 12]
        df1['LINE'] = df5['LineName'].map(lambda x:x.upper())
        df1['SITE'] = 'INFY'
        df1['PRODUCT_CODE'] = df5['ProductCode']
        df1['SERIAL_NUMBER'] = df5['SN'].map(lambda x:x.upper()).map(strQ2B)
        df1['EQUIPMENT_TYPE'] = df5['TestStationProductTask']
        df1['STATION_ID'] = ""
        df1['START_TIME'] = df5['TestTime'].map(lambda  x:str(x))
        df1['RESULT'] = df5['TestResult']
        df1['RETEST'] = ""
        df1['END_TIME'] = ""
        df = df.append(df1,ignore_index=True)


        # 生成测试数据
        Make_Test_Data(df)


        # 去掉不是main的line
        df = df[~df['LINE'].str.contains('REL')]
        df = df[~df['LINE'].str.contains('FR')]
        df = df[~df['LINE'].str.contains('FTFB')]
        df = df[~df['LINE'].str.contains('OCQ')]
        # 剔除开始时间在在报告期外的
        df = df[df.START_TIME >= dict["GetData_config"][0][1]]
        # 剔除外观检查1 外观检查2 在end_time 后的剔除
        df = df[~(((df.EQUIPMENT_TYPE == 'COSMETIC1') | (df.EQUIPMENT_TYPE == 'COSMETIC2')) & (df.START_TIME > dict["GetData_config"][1][1]))]
        start_Time = datetime.datetime.now()
        # 按照序列号分组
        # 按照SERIAL_NUMBER分组
        sn_gp = df.groupby("SERIAL_NUMBER")
        flag = True
        sum = 0
        for sn_gp_name, sn_gp_data in sn_gp:
            sum = sum + sn_gp_data.shape[0]

            # 分布式
            # if flag:
            #     tasks.TaskA.delay(sn_gp_name, sn_gp_data.to_json())
            #     flag = False
            # else:
            #     tasks.TaskB.delay(sn_gp_name, sn_gp_data.to_json())
            #     flag = True


            # 单机
            tasks.TaskA.delay(sn_gp_name, sn_gp_data.to_json())
        while True:
            time.sleep(5)
            sum_ret = Is_Over()
            if sum_ret == sum:
                break
        print("OK")




    except Exception as e:
        print(e)
    finally:
        return None
        conn.close()
        conn.close()
    pass



def getDataForSql_v3():
    # 获取配置
    dict = getConfig()
    try:
        # conn = pymssql.connect(server=dict["sql_server_databaseEx"][0][1], user=dict["sql_server_databaseEx"][1][1],
        #                        password=dict["sql_server_databaseEx"][2][1], database=dict["sql_server_databaseEx"][3][1])

        conn = pymssql.connect(server=dict["sql_server_database"][0][1], user=dict["sql_server_database"][1][1],
                               password=dict["sql_server_database"][2][1], database=dict["sql_server_database"][3][1])

        conn1 = pymssql.connect(server=dict["sql_server_database"][0][1], user=dict["sql_server_database"][1][1],
                               password=dict["sql_server_database"][2][1], database=dict["sql_server_database"][3][1])
        red = redis.Redis(host=dict["redis_config"][1][1], password=dict["redis_config"][2][1],
                          port=dict["redis_config"][3][1], db=dict["redis_config"][4][1])
        if conn and conn1:
            # 增加两个小时，因为外观检查，和check-in check-out 的时间与其他LINE 获取的时间相隔2个小时
            date_str = (datetime.datetime.fromtimestamp(
                time.mktime(time.strptime(dict["GetData_config"][1][1], "%Y-%m-%d %H:%M:%S"))) + datetime.timedelta(
                hours=2)).strftime(
                "%Y-%m-%d %H:%M:%S")

            # sqlStr_190 = "SELECT Sn,test_station_name,station_id,result,start_time,stop_time,product " \
            #              "FROM baTestRelatedItemsEx " \
            #              "WHERE stop_time " \
            #              "BETWEEN '{0}' AND '{1}' " \
            #              "ORDER BY stop_time ASC ".format(dict["GetData_config"][0][1],date_str)

            sqlStr_190 = "select distinct a.sn as Sn,a.test_station_name as test_station_name,a.station_id as station_id,a.result as result,a.start_time as start_time,a.stop_time as stop_time,c.MaterialCode as product " \
                     "from TestRelatedItems as a  " \
                     "inner join opPlanExecutMain as b on b.InternalCode=a.sn " \
                     "inner join plAssemblyPlanDetail as c on c.ID=b.AssemblyPlanDetailID " \
                     "where  " \
                     "c.MaterialCode like '%{2}%' " \
                     "and a.stop_time between '{0}'  " \
                     "and '{1}'  order by a.start_time asc ".format(dict["GetData_config"][0][1], date_str,
                                                                    dict["GetData_config"][2][1],
                                                                    )


            sqlStr_cos = "SELECT ProductCode, SN, LineName, TestStationProductTask, TestTime, TestResult  " \
                         "FROM FATPYieldFile " \
                         "WHERE ProductCode='{0}'  " \
                         "AND TestTime " \
                         "BETWEEN '{1}' AND '{2}' " \
                         "ORDER BY TestTime ASC ".format(
                dict["GetData_config"][4][1], dict["GetData_config"][0][1], date_str)
        df = pd.DataFrame(
            columns=["SITE", "LINE", "PRODUCT_CODE", "SERIAL_NUMBER", "EQUIPMENT_TYPE", "STATION_ID", "START_TIME",
                     "RESULT",
                     "RETEST", "END_TIME"])

        df1 = pd.DataFrame(
            columns=["SITE", "LINE", "PRODUCT_CODE", "SERIAL_NUMBER", "EQUIPMENT_TYPE", "STATION_ID", "START_TIME",
                     "RESULT",
                     "RETEST", "END_TIME"])
        df4 = pd.read_sql(sqlStr_190,conn)

        # 是否是MLB获取
        ISMLB = dict["GetData_config"][6][1]
        if ISMLB == "Y":
            df4 = df4[df4['Sn'].str.len() == 17]
        else:
            df4 = df4[df4['Sn'].str.len() == 12]


        df['LINE'] = df4['station_id'].map(lambda x:x.split("_")[1])
        df['SITE'] = 'INFY'
        df['PRODUCT_CODE'] = 'B282'
        df['SERIAL_NUMBER'] = df4['Sn'].map(strQ2B)
        df['EQUIPMENT_TYPE'] = df4['test_station_name']
        df['STATION_ID'] = df4['station_id']
        df['START_TIME'] = df4['start_time'].map(lambda x:str(x))
        df['RESULT'] = df4['result']
        df['RETEST'] = ""
        df['END_TIME'] = df4['stop_time'].map(lambda  x:str(x))
        # 读取外观检查
        df5 = pd.read_sql(sqlStr_cos,conn1)
        df1['LINE'] = df5['LineName'].map(lambda x:x.upper())
        df1['SITE'] = 'INFY'
        df1['PRODUCT_CODE'] = df5['ProductCode']
        df1['SERIAL_NUMBER'] = df5['SN'].map(lambda x:x.upper()).map(strQ2B)
        df1['EQUIPMENT_TYPE'] = df5['TestStationProductTask']
        df1['STATION_ID'] = ""
        df1['START_TIME'] = df5['TestTime'].map(lambda  x:str(x))
        df1['RESULT'] = df5['TestResult']
        df1['RETEST'] = ""
        df1['END_TIME'] = ""
        df = df.append(df1,ignore_index=True)

        # 生成测试数据
        Make_Test_Data(df)


        # 去掉不是main的line
        df = df[~df['LINE'].str.contains('REL')]
        df = df[~df['LINE'].str.contains('FR')]
        df = df[~df['LINE'].str.contains('FTFB')]
        df = df[~df['LINE'].str.contains('OCQ')]
        # 剔除开始时间在在报告期外的
        df = df[df.START_TIME >= dict["GetData_config"][0][1]]
        # 剔除外观检查1 外观检查2 在end_time 后的剔除
        df = df[~(((df.EQUIPMENT_TYPE == 'COSMETIC1') | (df.EQUIPMENT_TYPE == 'COSMETIC2')) & (df.START_TIME > dict["GetData_config"][1][1]))]
        start_Time = datetime.datetime.now()
        # 按照序列号分组
        # 按照SERIAL_NUMBER分组
        sn_gp = df.groupby("SERIAL_NUMBER")
        flag = True
        sum = 0
        for sn_gp_name, sn_gp_data in sn_gp:
            sum = sum + sn_gp_data.shape[0]
            if flag:
                tasks.TaskA.delay(sn_gp_name, sn_gp_data.to_json())
                flag = False
            else:
                tasks.TaskB.delay(sn_gp_name, sn_gp_data.to_json())
                flag = True
        while True:
            time.sleep(5)
            sum_ret = Is_Over()
            if sum_ret == sum:
                break
        print("OK")




    except Exception as e:
        print(e)
    finally:
        return None
        conn.close()
        conn.close()
    pass




# 可以用于加载数据测试
def Data_Test():
    config_dict = getConfig()
    # conn = pymssql.connect(server=config_dict["sql_server_database"][0][1],
    #                        user=config_dict["sql_server_database"][1][1],
    #                        password=config_dict["sql_server_database"][2][1],
    #                        database=config_dict["sql_server_database"][3][1])
    #
    # conn1 = pymssql.connect(server=config_dict["sql_server_databaseEx"][0][1],
    #                         user=config_dict["sql_server_databaseEx"][1][1],
    #                        password=config_dict["sql_server_databaseEx"][2][1],
    #                         database=config_dict["sql_server_databaseEx"][3][1])
    # if conn and conn1:
    #     cursor = conn.cursor()
    #     cursor1 = conn1.cursor()
    root_dir = os.path.dirname(os.path.abspath("\\"))
    root_dir_target = os.path.join(root_dir, "test20191129_B419.CSV")
    df = pd.read_csv(root_dir_target)
    df = df[~df['LINE'].str.contains('REL')]

    #去掉不是main的line
    df = df[~df['LINE'].str.contains('FR')]
    df = df[~df['LINE'].str.contains('FTFB')]
    df = df[~df['LINE'].str.contains('OCQ')]

    # 剔除开始时间在在报告期外的
    df = df[df.START_TIME >= config_dict["GetData_config"][0][1]]

    # 剔除外观检查1 外观检查2 在end_time 后的剔除
    df = df[~(((df.EQUIPMENT_TYPE == 'COSMETIC1') | (df.EQUIPMENT_TYPE == 'COSMETIC2')) & (
                df.START_TIME > config_dict["GetData_config"][1][1]))]

    # 可以用来测试异常的SN
    df = df[df.SERIAL_NUMBER == "FL6ZQ88FJMMJ"]



    df2 = pd.DataFrame(columns=["SITE", "LINE", "PRODUCT_CODE", "SERIAL_NUMBER", "EQUIPMENT_TYPE", "STATION_ID",
                                "START_TIME", "RESULT", "RETEST","END_TIME"])

    df3 = pd.DataFrame(columns=["SITE", "LINE", "PRODUCT_CODE", "SERIAL_NUMBER", "EQUIPMENT_TYPE", "STATION_ID",
                                "START_TIME", "RESULT", "RETEST","END_TIME"])

    start_Time = datetime.datetime.now()
    # 按照序列号分组
    # 按照SERIAL_NUMBER分组
    sn_gp = df.groupby("SERIAL_NUMBER")
    flag = True
    sum = 0
    for sn_gp_name, sn_gp_data in sn_gp:
        sum = sum + sn_gp_data.shape[0]



        tasks.Data_Filter_ForYeildEx(sn_gp_name,sn_gp_data.to_json())
        # tasks.TaskA(sn_gp_name,sn_gp_data)
        # tasks.TaskA.delay(sn_gp_name,sn_gp_data.to_json())
        # TaskA.delay(sn_gp_name,sn_gp_data.to_json())
        # if flag:
        #     tasks.TaskA.delay(sn_gp_name, sn_gp_data.to_json())
        #     flag = False
        # else:
        #     tasks.TaskB.delay(sn_gp_name, sn_gp_data.to_json())
        #     flag = True

    while True:
        time.sleep(5)
        sum_ret = Is_Over()
        if sum_ret == sum:
            break

    print("OK")


    pass
# 判断是否结束
def Is_Over():
    config_dict = getConfig()
    red = redis.Redis(host=config_dict["redis_config"][1][1], password=config_dict["redis_config"][2][1],
                      port=config_dict["redis_config"][3][1], db=config_dict["redis_config"][5][1])
    # ret =  red.hgetall("result1")
    ret = red.hvals("result1")
    ret_list = [int(i) for i in ret]
    sum = np.sum(ret_list, axis=0)
    print(sum)
    red.close()
    return sum


df_list = []
# 从数据库获取信息
def GetData_From_Redis():
    df2 = pd.DataFrame(columns=["SITE", "LINE", "PRODUCT_CODE", "SERIAL_NUMBER", "EQUIPMENT_TYPE", "STATION_ID",
                                "START_TIME", "RESULT", "RETEST","END_TIME"])
    config_dict = getConfig()
    red = redis.Redis(host=config_dict["redis_config"][1][1], password=config_dict["redis_config"][2][1],
                      port=config_dict["redis_config"][3][1], db=config_dict["redis_config"][4][1])
    ret = red.hvals("result")
    ret_list = [json.loads(i.decode())[0]  for i in ret]
    df = pd.DataFrame(ret_list)
    Print_to_CSV(df)
    pass



# 输出到CSV文件
def Print_to_CSV(data_frame):
    # 获取配置
    dict = getConfig()
    # 判断配置文件中是否有配置写入文件名，默认不写文件为test
    if dict["GetData_config"][5][1] == "":
        root_dir = os.path.dirname(os.path.abspath("\\"))
        root_dir = os.path.join(root_dir, "test.csv")
    else:
        root_dir = os.path.dirname(os.path.abspath("\\"))
        root_dir = os.path.join(root_dir, "SFS-INFY-" + dict["GetData_config"][5][1] + ".csv")
    # 输出到scv文件
    data_frame = data_frame.drop(["END_TIME"],axis=1)
    data_frame.to_csv(root_dir, index=False)


# 时间转换
def timestamp_to_str(timestamp=None, format='%Y-%m-%d %H:%M:%S'):
    if timestamp:
        time_tuple = time.localtime(timestamp)  # 把时间戳转换成时间元祖
        result = time.strftime(format, time_tuple)  # 把时间元祖转换成格式化好的时间
        return result
    else:
        return time.strptime(format)

# 生成数据
def Make_Test_Data(data_farme):
    root_dir = os.path.dirname(os.path.abspath("\\"))
    root_dir = os.path.join(root_dir, "test20191220_B419.CSV")
    data_farme.to_csv(root_dir, index=False)


def strQ2B(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code ==12288:
            inside_code = 32
        elif (inside_code >= 65281 and inside_code <= 65374):
            inside_code -= 65248
        rstring += chr(inside_code)
    return rstring

if __name__ == '__main__':





    # Data_Test()
    # Is_Over()


    # 生成测试文件
    getDataForSql_v2()

    GetData_From_Redis()
    pass