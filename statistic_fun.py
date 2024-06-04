'''
資料統計
'''
import pandas as pd
import sys
import traceback
# import os
from typing import Dict
from save_data import get_part_data, count_data, get_iot_count, \
    get_machine_healthy_val, read_config, link_Postgres
from datetime import date, timedelta
from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker


# 統計
def statistic_part(logger):
    now_date = date.today().strftime('%Y-%m-%d')
    config_msg = read_config('N', False, logger)
    engine = create_engine(
        f'postgresql://'
        f'{config_msg["user"]}:{config_msg["password"]}@'
        f'{config_msg["host"]}:{config_msg["port"]}/{config_msg["database"]}')
    # 讀取 machine_name&part_name組合
    try:
        mp_sql = """SELECT mt.machine_name, mt.part_name FROM machine_table as mt"""
        mp_data = pd.read_sql_query(mp_sql, engine)
        for m in range(len(mp_data)):
            # part_table
            ptm_sql = f"""SELECT 
                    pt.machine_name, pt.part_name, 
                    MAX(pt.insert_date) insert_date,
                    MAX(pt.init_val) init_val,
                    CAST(ROUND(AVG(pt.part_count), 0) AS INTEGER) mean_count,
                    CAST(ROUND(AVG(pt.part_day), 0) AS INTEGER) mean_day
                FROM part_table as pt
                WHERE pt.machine_name = '{mp_data.loc[m, 'machine_name']}'
                AND pt.part_name = '{mp_data.loc[m, 'part_name']}'
                AND pt.part_day != 0
                GROUP BY pt.machine_name, pt.part_name"""
            pt_mean_data = pd.read_sql_query(ptm_sql, engine)
            if pt_mean_data.empty:
                pt_sql = f"""SELECT *
                        FROM part_table as pt
                        WHERE pt.machine_name = '{mp_data.loc[m, 'machine_name']}'
                        AND pt.part_name = '{mp_data.loc[m, 'part_name']}'"""
            else:
                pt_sql = f"""SELECT *
                        FROM part_table as pt
                        WHERE pt.machine_name = '{mp_data.loc[m, 'machine_name']}'
                        AND pt.part_name = '{mp_data.loc[m, 'part_name']}'
                        AND pt.insert_date = '{pt_mean_data.loc[0, 'insert_date']}'"""
            pt_data = pd.read_sql_query(pt_sql, engine)
            # statistic
            s_dict = {}
            s_dict['machine_name'] = mp_data.loc[m, 'machine_name']
            s_dict['part_name'] = mp_data.loc[m, 'part_name']
            s_dict['system_date'] = now_date
            # 判斷 - part_day
            if pt_data.loc[0, 'part_day'] == 0:
                s_dict['mean_count'] = pt_data.loc[0, 'part_count']
                s_dict['mean_day'] = pt_data.loc[0, 'init_val']
                s_dict['used_count'] = pt_data.loc[0, 'part_count']
                used_day = (date.fromisoformat(
                    now_date) - pt_data.loc[0, 'insert_date']).days
                s_dict['used_day'] = used_day
                healthy_val = used_day / pt_data.loc[0, 'init_val']
                s_dict['part_healthy_val'] = healthy_val
                df_data = pd.DataFrame(s_dict, index=[0])
            else:
                s_dict['mean_count'] = pt_mean_data.loc[0, 'mean_count']
                s_dict['mean_day'] = pt_mean_data.loc[0, 'mean_day']
                s_dict['used_count'] = pt_data.loc[0, 'part_count']
                used_day = (date.fromisoformat(
                    now_date) - pt_data.loc[0, 'insert_date']).days
                s_dict['used_day'] = used_day
                healthy_val = used_day / pt_mean_data.loc[0, 'mean_day']
                s_dict['part_healthy_val'] = healthy_val
                df_data = pd.DataFrame(s_dict, index=[0])
            if healthy_val >= 0.9:
                up_sql = f"""UPDATE machine_table
                    SET part_healthy = 'R'
                    WHERE machine_name = '{mp_data.loc[m, 'machine_name']}'
                    AND part_name = '{mp_data.loc[m, 'part_name']}'"""
            elif healthy_val < 0.9 and healthy_val >= 0.7:
                up_sql = f"""UPDATE machine_table
                    SET part_healthy = 'Y'
                    WHERE machine_name = '{mp_data.loc[m, 'machine_name']}'
                    AND part_name = '{mp_data.loc[m, 'part_name']}'"""
            else:
                up_sql = f"""UPDATE machine_table
                    SET part_healthy = 'G'
                    WHERE machine_name = '{mp_data.loc[m, 'machine_name']}'
                    AND part_name = '{mp_data.loc[m, 'part_name']}'"""
            db_msg = read_config(up_sql, True, logger)
            m_data, db_msg = link_Postgres(**db_msg)
            if db_msg['state']:
                print('Update part healthy completed')
                logger.info('Update part healthy completed')
            else:
                print(db_msg['message'])
                logger.info(db_msg['message'])
            df_data.to_sql(
                'part_statistic_table', engine,
                if_exists='append', index=False)
        msg = 'Statistical calculations have been completed.'
        msg_dict = {'message': msg, 'state': True}
    except:
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        msg = f'[Error] Statistical calculations. {file_name}: {func_name} line at {line_num}'
        msg_dict = {'message': msg, 'state': False}
    return msg_dict


# 增加維修資料時同時修改part_table的insert_date
# 統計時會直接將日期進行相減獲得使用天數
def add_pair_data(add_dict: Dict, logger):
    try:
        config_msg = read_config('N', False, logger)
        engine = create_engine(
            f'postgresql://'
            f'{config_msg["user"]}:{config_msg["password"]}@'
            f'{config_msg["host"]}:{config_msg["port"]}/{config_msg["database"]}')
        df_data = pd.DataFrame(add_dict, index=range(1))

        # 讀取 part_table 在相同的machine_name&part_name的init_val
        re_day = pd.to_datetime(df_data['repair_date'], format='%Y-%m-%d').dt.date
        # 依據machine_name&part_name計算part_table資料數
        num_sql = count_data(add_dict["machine_name"], add_dict["part_name"])
        num = pd.read_sql_query(num_sql, engine)
        # 依據資料數量
        query = get_part_data(int(num.iloc[0, 0]),
                              add_dict["machine_name"],
                              add_dict["part_name"])
        df2 = pd.read_sql_query(query, engine)
        # 累計天數 = re_day - insert
        part_day = (date.fromisoformat(
            f'{df_data.loc[0, "repair_date"]}') - df2.loc[0, 'insert_date']).days
        # 如果num>1表示 m2已經於sql計算累積的天數與count的中位數
        if int(num.iloc[0, 0]) <= 1:
            df2['part_day'] = part_day
            # 擷取iot資料於re_day前一天累積圈數
            before_date = (
                    date.fromisoformat(f'{df_data.loc[0, "repair_date"]}') - timedelta(days=1)).strftime("%Y-%m-%d")
            iot_count_sql = get_iot_count(
                None, before_date, add_dict["machine_name"],
                int(num.iloc[0, 0]))
            iot_count = pd.read_sql_query(iot_count_sql, engine)
            if iot_count.empty:
                df2['part_count'] = 0
            else:
                df2['part_count'] = int(iot_count.loc[0, 'sum_count'])
        else:
            df2['part_day'] = part_day
            before_date = (date.fromisoformat(f'{df_data.loc[0, "repair_date"]}') - timedelta(days=1)).strftime("%Y-%m-%d")
            insert_date = date.fromisoformat(f'{df2.loc[0, "insert_date"]}').strftime("%Y-%m-%d")
            iot_count_sql = get_iot_count(
                insert_date, before_date, add_dict["machine_name"],
                int(num.iloc[0, 0]))
            iot_count = pd.read_sql_query(iot_count_sql, engine)
            if iot_count.empty:
                df2['part_count'] = 0
            else:
                df2['part_count'] = int(iot_count.loc[0, 'sum_count'])
        # re_day 取代 insert
        df2['insert_date'] = re_day
        df2.to_sql('part_table', engine, if_exists='append', index=False)
        msg = 'Finished statistic and update part table data'
        msg_dict = {'message': msg, 'state': True}
    except:
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        msg = f'[Error] {file_name}: {func_name} at line {line_num}'
        msg_dict = {'message': msg, 'state': False}
    return msg_dict


def machine_healthy_val(logger):
    try:
        config_msg = read_config('N', False, logger)
        engine = create_engine(
            f'postgresql://'
            f'{config_msg["user"]}:{config_msg["password"]}@'
            f'{config_msg["host"]}:{config_msg["port"]}/{config_msg["database"]}')
        mh_sql = get_machine_healthy_val()
        df_data = pd.read_sql_query(mh_sql, engine)
        del df_data['healthy_val']
        df_data.to_sql(
            'machine_state_table', engine, if_exists='replace', index=False)
        msg = 'Add the statistic results have been completed.'
        msg_dict = {'message': msg, 'state': True}
    except:
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        msg = f'[Error] Statistical calculations. {file_name}: {func_name} line at {line_num}'
        msg_dict = {'message': msg, 'state': False}
    return msg_dict


# 定期執行統計與匯入新的又得馬設備資料
def update_main(logger):
    # 更新device list
    device_dict = insert_device(logger)
    # 新增又得馬資料
    # insert_dict = insert_huan_jia_data(logger)
    # 統計
    sp_dict = statistic_part(logger)
    # 新增健康統計結果
    mhv_dict = machine_healthy_val(logger)


