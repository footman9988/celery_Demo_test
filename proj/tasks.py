from __future__ import absolute_import

import os
import time
import redis
import pymssql
import configparser
import pandas as pd
import datetime
import json
import numpy as np
from datetime import  datetime, timedelta

from proj.celery import app1
from proj.celery import logger




# @app1.task
# def TaskA(sn_gp_name, sn_gp_data):
#     print("任务-----A正在执行....")
#     Data_Filter_ForYeildEx(sn_gp_name, sn_gp_data)


@app1.task
def TaskB(sn_gp_name, sn_gp_data):
    print("任务-----B正在执行....")
    Data_Filter_ForYeildEx(sn_gp_name, sn_gp_data)


@app1.task
def TaskA():
    print("添加任务.........")

    root_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    root_dir = os.path.join(root_dir, 'config.ini')
    # getDataForSql_v2()
    # GetData_From_Redis()

    # MLB
    cf = configparser.ConfigParser()
    cf.read(root_dir)
    for i in ['N','Y']:
        cf.set('GetData_config', 'mlb', i)
        cf.write(open(root_dir, 'w'))
        print(i)
        getDataForSql_v2()
        GetData_From_Redis()



# @app1.task
# def TaskA():
#     print("OK------------A")
#     TaskB.delay()
#
# @app1.task
# def TaskB():
#     print("OK------------B")






# FA站提交的时间就是测试时间，所以不需要判断有FA站的情况
def Data_Filter_ForYeildEx(sn_gp_name, sn_gp_data):
    sn_gp_data = pd.read_json(sn_gp_data)
    Save_To_Redis_2(sn_gp_name, sn_gp_data.shape[0])
    # 数据库配置
    config_dict = getConfig()
    conn = pymssql.connect(server=config_dict["sql_server_database"][0][1],
                           user=config_dict["sql_server_database"][1][1],
                           password=config_dict["sql_server_database"][2][1],
                           database=config_dict["sql_server_database"][3][1])

    conn1 = pymssql.connect(server=config_dict["sql_server_databaseEx"][0][1],
                            user=config_dict["sql_server_databaseEx"][1][1],
                            password=config_dict["sql_server_databaseEx"][2][1],
                            database=config_dict["sql_server_databaseEx"][3][1])
    Report_Time = config_dict["GetData_config"][1][1]

    if conn and conn1:
        cursor = conn.cursor()
        cursor1 = conn1.cursor()
        et_gp = sn_gp_data.groupby("EQUIPMENT_TYPE")
        # 获取有FA的站点信息
        check_In_Info = sn_gp_data[sn_gp_data["EQUIPMENT_TYPE"] == "FA-Check-In"]
        if check_In_Info.shape[0] > 0:
            for et_gp_name, et_gp_data in et_gp:
                redis_Key = sn_gp_name + et_gp_name
                my_Flag = True
                # 剔除check-in 和check-out　站
                if "FA-Check-In" in et_gp_data.values:
                    continue
                if "FA-Check-Out" in et_gp_data.values:
                    continue
                # 时间过滤
                if Check_Time_ForYeild(cursor, cursor1, et_gp_name, sn_gp_name, config_dict["GetData_config"][0][1]):
                    continue
                count = et_gp_data.shape[0]
                pass_count = et_gp_data[et_gp_data.RESULT == "PASS"].shape[0]
                # 如果同一SN 在同一station 所有的开始测试时间都在报告期外！则剔除。
                if et_gp_data[et_gp_data.START_TIME > Report_Time].shape[0] == count:
                    continue
                if count > 1:
                    et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1).loc[-1:, ['RETEST']].iloc[0, 0] = "RETEST"

                # 如果外观检查1，2 有一次FAIL 就直接保存
                if et_gp_data[((et_gp_data.EQUIPMENT_TYPE == 'COSMETIC1') | (et_gp_data.EQUIPMENT_TYPE == 'COSMETIC2')) & (et_gp_data.RESULT == 'FAIL')].shape[0] > 0:
                    ret2 = et_gp_data[
                        ((et_gp_data.EQUIPMENT_TYPE == 'COSMETIC1') | (et_gp_data.EQUIPMENT_TYPE == 'COSMETIC2')) & (
                                    et_gp_data.RESULT == 'FAIL')].tail(1)
                    Save_To_Redis(redis_Key, ret2)
                    continue
                # 判断该ATE机台测试改sn的时候是否在check-in　时间之前取最后一条,如果没有，则还是取第一条
                et_gp_data = et_gp_data.sort_values(by="START_TIME")
                checkIn_time_list = check_In_Info.START_TIME.tolist()
                # 当check——time——list 中有两条以上的check——in——time的时候判断结果
                if len(checkIn_time_list) > 1:
                    for check_In_Time in checkIn_time_list:
                        FA_sation_cout = et_gp_data[et_gp_data.START_TIME < check_In_Time].shape[0]
                        if FA_sation_cout > 0:
                            my_Flag = False
                            Save_To_Redis(redis_Key, et_gp_data[et_gp_data.START_TIME < check_In_Time].tail(1))
                            # df_list.append(et_gp_data[et_gp_data.START_TIME < check_In_Time].tail(1))
                            break
                    if my_Flag:
                        Save_To_Redis(redis_Key, et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1))
                        # df_list.append(et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1))
                    continue
                else:
                    # 当check——time——list 中有一条的check——in——time的时候判断结果
                    FA_sation_cout = et_gp_data[et_gp_data.START_TIME < checkIn_time_list[0]].shape[0]
                    if FA_sation_cout > 0:
                        Save_To_Redis(redis_Key, et_gp_data[et_gp_data.START_TIME < checkIn_time_list[0]].tail(1))
                        # Save_To_Redis_2(redis_Key)
                        # df_list.append(et_gp_data[et_gp_data.START_TIME < checkIn_time_list[0]].tail(1))
                        continue
                    Save_To_Redis(redis_Key, et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1))
                    # df_list.append(et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1))
                # 多条记录的情况下，如果开始时间在报告时间之后10点之前，会影响报告结果
                if et_gp_data[et_gp_data.START_TIME > Report_Time].shape[0] > 0:
                    ret = et_gp_data[et_gp_data.START_TIME > Report_Time].tail(1).loc[-1:, ['RESULT']].iloc[0, 0]
                    ret1 = et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1)
                    ret1.loc[-1:, ['RESULT']] = ret
                    Save_To_Redis(redis_Key, ret1)
                    # df_list.append(ret1)
                    continue
        else:
            for et_gp_name, et_gp_data in et_gp:
                redis_Key = sn_gp_name + et_gp_name
                # Save_To_Redis_2(redis_Key, et_gp_data.shape[0])
                if "FA-Check-Out" in et_gp_data.values:
                    continue
                # 时间过滤
                if Check_Time_ForYeild(cursor, cursor1, et_gp_name, sn_gp_name, config_dict["GetData_config"][0][1]):
                    continue
                et_gp_data = et_gp_data.sort_values(by="START_TIME")
                count = et_gp_data.shape[0]
                pass_count = et_gp_data[et_gp_data.RESULT == "PASS"].shape[0]
                # 如果同一SN 在同一station 所有的开始测试时间都在报告期外！则剔除。
                if et_gp_data[et_gp_data.START_TIME > Report_Time].shape[0] == count:
                    continue
                if count > 1:
                    et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1).loc[-1:, ['RETEST']].iloc[0, 0] = "RETEST"
                # 如果外观检查1，2 有一次FAIL 就直接保存
                if et_gp_data[((et_gp_data.EQUIPMENT_TYPE == 'COSMETIC1') | (et_gp_data.EQUIPMENT_TYPE == 'COSMETIC2')) & (et_gp_data.RESULT == 'FAIL')].shape[0] > 0:
                    ret2 = et_gp_data[
                        ((et_gp_data.EQUIPMENT_TYPE == 'COSMETIC1') | (et_gp_data.EQUIPMENT_TYPE == 'COSMETIC2')) & (
                                    et_gp_data.RESULT == 'FAIL')].tail(1)
                    Save_To_Redis(redis_Key, ret2)
                    continue


                # 修改逻辑为 在报告期内结果取最后一条也就是end——time的前一条，如果在end——time后，cut time前面，有测试结果，会影响到最后一条结果
                if et_gp_data[et_gp_data.START_TIME > Report_Time].shape[0] > 0:
                    # 取最接近报告结束时间的一条记录的结果
                    ret = et_gp_data[et_gp_data.START_TIME > Report_Time].tail(1).loc[-1:, ['RESULT']].iloc[0, 0]
                    # 把上面取到的结果赋值给最后条记录
                    ret1 = et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1)
                    ret1.loc[-1:, ['RESULT']] = ret
                    Save_To_Redis(redis_Key, ret1)
                    # df_list.append(ret1)
                    continue
                Save_To_Redis(redis_Key, et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1))
                # df_list.append(et_gp_data[et_gp_data.START_TIME <= Report_Time].tail(1))
        cursor.close()
        cursor1.close()
    conn.close()
    conn1.close()


# 读取配置
def getConfig():
    root_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    cf = configparser.ConfigParser()
    root_dir = os.path.join(root_dir,"config.ini")
    cf.read(root_dir)
    dict={}
    dict["sql_server_database"] = cf.items("sql_server_database")
    dict["GetData_config"] = cf.items("GetData_config")
    dict["redis_config"] = cf.items("redis_config")
    dict["sql_server_databaseEx"] = cf.items("sql_server_databaseEx")

    # t = datetime.now()
    # t1 = t-timedelta(days=1)
    # t = t.strftime('%Y-%m-%d 08:00:00')
    # t1 = t1.strftime('%Y-%m-%d 08:00:00')
    #
    # dict["GetData_config"][0] = 'start_Time', t1
    # dict["GetData_config"][1] = 'end_TIme', t
    return dict

# 保存redis
def Save_To_Redis(key,value):
    try:
        config_dict = getConfig()
        red = redis.Redis(host=config_dict["redis_config"][1][1], password=config_dict["redis_config"][2][1],
                          port=config_dict["redis_config"][3][1], db=config_dict["redis_config"][4][1])
        value["START_TIME"] = value["START_TIME"].map(lambda x:str(x))
        value = value.to_json(orient='records')
        red.hset('result', key, value)
    except:
        logger.info(key)
    pass



def Save_To_Redis_2(key,count):
    config_dict = getConfig()
    red = redis.Redis(host=config_dict["redis_config"][1][1], password=config_dict["redis_config"][2][1],
                      port=config_dict["redis_config"][3][1], db=config_dict["redis_config"][5][1])
    red.hset('result1', key, count)
    pass



# 时间判断
def Check_Time_ForYeild(cursor, cursor1, station_Name, sn, time):
    try:
        # todo 可以添加一个判断station_name 在对数据库查询外观检查，和check-in check-out
        if station_Name == "COSMETIC1" or station_Name == "COSMETIC2":
            sql_Str = "SELECT COUNT(*) FROM FATPYieldFile WHERE SN='{1}' AND TestStationProductTask='{0}' AND TestTime<'{2}'".format(
                station_Name, sn, time)
            cursor.execute(sql_Str)
            rows = cursor.fetchone()
            count = rows[0]
            if count != 0:
                return True
        else:
            sql_Str = "SELECT COUNT(*) FROM TestRelatedItems WHERE test_station_name='{0}' AND sn='{1}' AND start_time<'{2}'".format(
                station_Name, sn, time)

            sql_Str_190 = "SELECT COUNT(*) FROM baTestRelatedItemsEx WHERE Sn='{0}' AND test_station_name='{1}' AND start_time<'{2}'".format(
                sn, station_Name, time
            )
            cursor1.execute(sql_Str_190)
            rows1 = cursor1.fetchone()
            count1 = rows1[0]
            if count1 > 0:
                return True
            cursor.execute(sql_Str)
            rows = cursor.fetchone()
            count = rows[0]
            if count > 0:
                return True
    except:
        logger.info(sn)


    # 测试使用
    # sql_Str = "SELECT COUNT(*) FROM TestRelatedItems WHERE test_station_name='{0}' AND sn='{1}' AND start_time<'{2}'".format(
    #     station_Name, sn, time)
    #
    # sql_str_190 = "SELECT COUNT(*) FROM baTestRelatedItemsEx WHERE Sn='{0}' AND test_station_name='{1}' AND start_time<'{2}'".format(
    #     sn , station_Name, time
    # )
    # cursor1.execute(sql_str_190)
    # rows1 = cursor1.fetchone()
    # count1 = rows1[0]
    # if count1 > 0:
    #     return True

    # cursor.execute(sql_Str)
    # rows = cursor.fetchone()
    # count = rows[0]
    # if count > 0:
    #     return True


# 判断是否结束
def Is_Over():
    config_dict = getConfig()
    red = redis.Redis(host=config_dict["redis_config"][1][1], password=config_dict["redis_config"][2][1],
                      port=config_dict["redis_config"][3][1], db=config_dict["redis_config"][5][1])
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
    red.flushall()
    pass



# 输出到CSV文件
def Print_to_CSV(data_frame):
    # 获取配置
    dict = getConfig()
    root_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    # file_Name = (datetime.now()).strftime('%Y%m%d') + '_B419.CSV'
    # # 判断配置文件中是否有配置写入文件名，默认不写文件为test
    # if dict["GetData_config"][5][1] == "":
    #     root_dir = os.path.join(root_dir, "test.csv")
    # else:
    #     root_dir = os.path.join(root_dir, "SFS-INFY-" + file_Name)

    ISMLB = dict["GetData_config"][6][1]
    if ISMLB == 'Y':
        file_Name = (datetime.now()).strftime('%Y%m%d') + '_' +dict['GetData_config'][4][1] + '_MLB.csv'
    else:
        file_Name = (datetime.now()).strftime('%Y%m%d') + '_' +dict['GetData_config'][4][1] + '.csv'
    root_dir = os.path.join(root_dir, 'Data')
    root_dir = os.path.join(root_dir, "SFS-INFY-" + file_Name)

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
    dict = getConfig()
    ISMLB = dict["GetData_config"][6][1]
    if ISMLB == 'Y':
        file_Name = 'test' + (datetime.now()).strftime('%Y%m%d') + dict['GetData_config'][4][1] + '_MLB.csv'
    else:
        file_Name = 'test' + (datetime.now()).strftime('%Y%m%d') + dict['GetData_config'][4][1] + '.csv'
    # file_Name = 'test' + (datetime.now()).strftime('%Y%m%d') + '_B419.CSV'
    root_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    root_dir = os.path.join(root_dir, 'Data')
    root_dir = os.path.join(root_dir, file_Name)
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
            date_str = (datetime.fromtimestamp(
                time.mktime(time.strptime(dict["GetData_config"][1][1], "%Y-%m-%d %H:%M:%S"))) + timedelta(
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
        start_Time = datetime.now()
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
            TaskB.delay(sn_gp_name, sn_gp_data.to_json())
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


if __name__ == '__main__':
    # getDataForSql_v2()
    GetData_From_Redis()

    pass

