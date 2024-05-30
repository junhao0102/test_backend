"""
儲存資料
"""

import pandas as pd
import sys
import traceback
import configparser
import os
import copy
import requests
from typing import Dict
import psycopg2 as psy
from datetime import date, timedelta

path = os.getcwd()


def read_config(sql_code: str, create, logger):
    config = configparser.ConfigParser()
    config_path = os.path.join(path, "config", "config.ini")
    config.read(config_path, encoding="utf-8")
    msg = {
        "host": config["DB_path"]["host"],
        "database": config["DB_path"]["database"],
        "user": config["DB_path"]["user"],
        "password": config["DB_path"]["password"],
        "sql": sql_code,
        "port": config["DB_path"]["port"],
        "create": create,
        "logger": logger,
    }
    return msg


def link_Postgres(
    host: str,
    database: str,
    user: str,
    password: str,
    sql: str,
    port: str,
    create,
    logger,
):
    try:
        conn = psy.connect(
            host=host, database=database, user=user, password=password, port=port
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        if create:
            conn.commit()
            msg = f"Created table or insert data successfully"
            db_msg = {"message": msg, "state": True}
            data = None
            logger.info(msg)
        else:
            row = cursor.fetchall()
            # 判斷是否為空
            if not row:
                data = pd.DataFrame()
            else:
                data = pd.DataFrame([tuple(x) for x in row])
                df = [None] * len(cursor.description)
                for i in range(len(cursor.description)):
                    desc = cursor.description[i]
                    df[i] = desc[0]
                data.columns = df
            msg = f"Imported data completed"
            db_msg = {"message": msg, "state": True}

        cursor.close()
        conn.close()
    except:
        data = "error"
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        if func_name == "link_Postgres":
            msg = "Input request body error"
        else:
            msg = "Linking DB error"
        logger.info(f"[Error] {msg}; {file_name}. {func_name} at line {line_num}.")
        db_msg = {"message": msg, "state": False}
    return data, db_msg


# ----------------Step 1. 新增DB資料表---------------
# 新增維修紀錄表
def create_repair_table():
    sql = """CREATE TABLE repair_table(
    machine_name varchar(30),
    part_name varchar(30),
    repair_date date,
    repair_man varchar(30),
    director varchar(30),
    remark varchar(255));"""
    return sql


# 機台 - 總表
def create_machine_table():
    sql = """CREATE TABLE machine_table(
        machine_name varchar(30),
        device_id int,
        init_val int,
        part_name varchar(30), 
        use_state varchar(30),
        part_healthy varchar(30)
        );"""
    return sql


# 機台 - 紀錄UI新增資料 (device_id 可能不存在)
def create_machine_list():
    sql = """CREATE TABLE machine_list(
        machine_name varchar(30),
        device_id int,
        remark varchar(255), 
        CONSTRAINT unique_machine_name UNIQUE (machine_name)
        );"""
    return sql


# 又得馬資料表
def create_device_table():
    sql = """CREATE TABLE device_table(
            id int,
            serverId int,
            machineId int,
            enabled int,
            name varchar(30),
            aliasId varchar(30),
            comment varchar(30),
            createdAt varchar(30),
            updateAt varchar(30));"""
    return sql


# 零件管理表
def create_part_table():
    sql = """CREATE TABLE part_table(
    machine_name varchar(30),
    part_name varchar(30),
    insert_date date,
    init_val int,
    part_count int,
    part_day int);"""
    return sql


# 零件列表
def create_part_list():
    sql = """CREATE TABLE part_list(
    id int,
    part_name varchar(30),
    use_state varchar(30),
    remark varchar(255), 
    CONSTRAINT unique_part_id UNIQUE (id), 
    CONSTRAINT unique_part_name UNIQUE (part_name)
    );"""
    return sql


# 零件統計表
def create_part_statistic_table():
    sql = """CREATE TABLE part_statistic_table(
    machine_name varchar(30),
    part_name varchar(30),
    system_date date,
    mean_count int,
    mean_day int,
    used_count int,
    used_day int,
    part_healthy_val float);"""
    return sql


# 機台健康狀態表
def create_machine_state_table():
    sql = """CREATE TABLE machine_state_table(
    machine_name varchar(30),
    system_date date,
    mean_count int,
    mean_day int,
    machine_healthy_state varchar(30));"""
    return sql


# 又得馬資料表
def create_huan_jia_table():
    sql = """CREATE TABLE huan_jia(
        device_id int,
        start_at date,
        end_at date,
        count int);"""
    return sql


# 檢查資料表是否存在
def check_table(table_name):
    sql = f"""select * from {table_name};"""
    return sql


# ---- 新增DB表單 ----
def create_main(logger):
    # 新增資料表表單
    create_list = [
        "repair_table",
        "machine_table",
        "part_table",
        "part_statistic_table",
        "machine_state_table",
        "create_huan_jia_table",
        "create_part_list",
        "create_device_table",
        "create_machine_list",
    ]
    save_msg = []
    try:
        check = 0
        for item in create_list:
            db_sql = check_table(item)
            msg = read_config(db_sql, False, logger)
            data, db_msg = link_Postgres(**msg)
            if db_msg["state"]:
                save_msg.append(f"The {item} already exists.")
                logger.info(f"The {item} already exists.")
                pass
            else:
                if item == create_list[0]:
                    c_sql = create_repair_table()
                elif item == create_list[1]:
                    c_sql = create_machine_table()
                elif item == create_list[2]:
                    c_sql = create_part_table()
                elif item == create_list[3]:
                    c_sql = create_part_statistic_table()
                elif item == create_list[4]:
                    c_sql = create_machine_state_table()
                elif item == create_list[5]:
                    c_sql = create_huan_jia_table()
                elif item == create_list[6]:
                    c_sql = create_part_list()
                elif item == create_list[7]:
                    c_sql = create_device_table()
                else:
                    c_sql = create_machine_list()
                msg2 = read_config(c_sql, True, logger)
                d2, _msg = link_Postgres(**msg2)
                if _msg["state"]:
                    save_msg.append(f"Created the {item} data")
                    logger.info(f"Created the {item} data")
                    check = check + 1
                else:
                    save_msg.append(_msg["message"])
                    logger.info(_msg["message"])
        if check < 7:
            build_msg = {"message": save_msg, "state": False}
        else:
            build_msg = {"message": save_msg, "state": True}
    except:
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        b_msg = f"[Error] Build data base table; {file_name}. {func_name} at line {line_num}."
        logger.info(b_msg)
        build_msg = {"message": b_msg, "state": False}
    return build_msg


# ----------------Step 2. 新增資料 ---------------
# 新增維修資料(from UI)
def insert_repair_data(df_data: Dict):
    n_sql = f"""INSERT INTO repair_table(
        machine_name, part_name, repair_date, repair_man, director, remark)
    VALUES(
        '{df_data["machine_name"]}', '{df_data["part_name"]}', 
        '{df_data["repair_date"]}', '{df_data["repair_man"]}', 
        '{df_data["director"]}', '{df_data["remark"]}')"""
    return n_sql


# 新增機台資料(from UI)
def insert_machine_data(df_data: Dict, logger):
    # 新增machine list data
    m_list_sql = f"""INSERT INTO machine_list(
                machine_name, device_id, remark)
            VALUES(
                '{df_data["machine_name"]}', {df_data["device_id"]}, 
                '{df_data["remark"]}')"""
    msg1 = read_config(m_list_sql, True, logger)
    df, df_msg = link_Postgres(**msg1)
    if df_msg["state"]:
        # 讀取 part list table
        p_sql = """SELECT pl.part_name, pl.use_state FROM part_list as pl"""
        msg = read_config(p_sql, False, logger)
        part_list_table, db_msg = link_Postgres(**msg)
        name_list = part_list_table["part_name"].tolist()
        state_list = part_list_table["use_state"].tolist()
        insert_date = date.today().strftime("%Y-%m-%d")
        # 新增總表資料
        for idx, pn in enumerate(name_list):
            # 新增 machine table data
            n_sql = f"""INSERT INTO machine_table(
                machine_name, device_id, init_val, part_name, use_state, part_healthy)
            VALUES(
                '{df_data["machine_name"]}', {df_data["device_id"]}, 
                {df_data["init_val"]}, '{pn}', '{state_list[idx]}', 'G')"""
            msg2 = read_config(n_sql, True, logger)
            d2, _msg2 = link_Postgres(**msg2)
            # 新增資料到 零件表
            part_sql = f"""INSERT INTO part_table(
                machine_name, part_name, insert_date, init_val, part_count, part_day)
            VALUES(
                '{df_data["machine_name"]}', '{pn}', '{insert_date}', 
                {df_data["init_val"]}, 0, 0)"""
            msg3 = read_config(part_sql, True, logger)
            d3, _msg3 = link_Postgres(**msg3)

            if _msg2["state"] and _msg3["state"]:
                logger.info(f"Created the machine name and device id")
            else:
                save_msg = {"message": [_msg2["message"], _msg3["message"]]}
                logger.info(save_msg)
        return {"message": "Created the machine name and device id", "state": True}
    else:
        return {"message": df_msg["message"], "state": False}


# 新增零件(from UI)
def insert_part_data(df_data: Dict):
    n_sql = f"""INSERT INTO part_table(
        machine_name, part_name, insert_date, 
        init_val, part_count, part_day, remark)
    VALUES(
        '{df_data["machine_name"]}', '{df_data["part_name"]}', '{df_data["insert_date"]}',
        '{df_data["init_val"]}', '0', '0', '{df_data["remark"]}')"""
    return n_sql


# 新增零件列表
def insert_part_list(df_data: Dict):
    n_sql = f"""INSERT INTO part_list(id, part_name, use_state, remark)
        VALUES('{df_data["id"]}',
            '{df_data["part_name"]}',
            '{df_data["use_state"]}', '{df_data["remark"]}')"""
    return n_sql


# 讀取零件列表
def read_part_list(logger):
    sql = """SELECT * FROM part_list"""
    msg = read_config(sql, False, logger)
    part_list, db_msg = link_Postgres(**msg)
    df_dict = part_list.to_dict()
    read_msg = {
        "state": db_msg["state"],
        "data": [df_dict],
        "message": db_msg["message"],
    }
    return read_msg


# 初始零件列表
def init_part_data(logger):
    name = [
        "儲紗輪(第一段)",
        "儲紗輪(第二段)",
        "儲紗輪(第三段)",
        "儲紗輪(第四段)",
        "送紗皮帶(第一段)",
        "送紗皮帶(第二段)",
        "送紗皮帶(第三段)",
        "送紗皮帶(第四段)",
        "羅拉捲布皮帶",
        "馬達皮帶",
        "主機傳動皮帶",
        "捲布齒輪",
    ]
    init = {}
    build_msg = {}
    build_num = 0
    for idx, item in enumerate(name):
        init_part = {"id": idx + 1, "part_name": item, "use_state": "Y", "remark": ""}
        msg = insert_main(init_part, "part_list", logger)
        init[item] = msg
        if msg["state"]:
            build_num = build_num + 1
    build_msg["init"] = init
    if build_num < len(name) - 1:
        build_msg["message"] = "[Error] insert the initial part data"
        build_msg["state"] = False
    else:
        build_msg["message"] = "Created the initial part data"
        build_msg["state"] = True
    return build_msg


# # 又得馬資料
# def insert_huan_jia_data(logger):
#     # 擷取docker compose 設定
#     api_base_url = "https://host.docker.internal:443"
#     api_endpoint = os.environ["API_COUNT_ENDPOINT"]
#     access_token = os.environ["API_TOKEN"]
#     headers = {
#         "Authorization": "Bearer" + access_token,
#         "Content-Type": "application/json",
#     }
#     # 日期
#     today = date.today().strftime("%Y-%m-%d")
#     # 擷取 目前 device table
#     device_sql = """SELECT dt."id" FROM device_table as dt"""
#     msg = read_config(device_sql, False, logger)
#     d_data, db_msg = link_Postgres(**msg)
#     id_list = d_data["id"].tolist()
#     if db_msg["state"]:
#         for device_id in id_list:
#             # data_response = requests.get(
#             #     'https://raw.githubusercontent.com/api/device/running-count/',
#             #     params={"deviceId": device_id, "date": f"'{today}'"})
#             data_response = requests.get(
#                 api_base_url + api_endpoint,
#                 headers=headers,
#                 verify=False,
#                 params={"deviceId": device_id, "date": f"'{today}'"},
#             )

#             df_dict = data_response.json()
#             if df_dict["status"] == "ok":
#                 # for item in range(len(df_dict['data'])):
#                 df_d1 = df_dict["data"][0]
#                 df_d2 = pd.DataFrame(df_d1, index=[0])
#                 date_list = ["startAt", "endAt", "createdAt", "updateAt"]
#                 for d in date_list:
#                     p_date = pd.to_datetime(df_d2[d])
#                     df_d2[d] = p_date.iloc[0].strftime("%Y-%m-%d")
#                 del_list = ["id", "createdAt", "updateAt"]
#                 for de in del_list:
#                     del df_d2[de]
#                 df_d2.columns = ["device_id", "start_at", "end_at", "count"]
#                 # 匯入DB
#                 n_sql = f"""INSERT INTO huan_jia(
#                         device_id, start_at, end_at, count)
#                     VALUES(
#                         '{df_d2.iloc[0, 0]}', '{df_d2.iloc[0, 1]}', 
#                         '{df_d2.iloc[0, 2]}', '{df_d2.iloc[0, 3]}')"""
#                 msg2 = read_config(n_sql, True, logger)
#                 d2, _msg = link_Postgres(**msg2)
#                 if _msg["state"]:
#                     logger.info(f"Created the iot data")
#                     insert_dict = {"message": "Created the iot data", "state": True}
#                 else:
#                     logger.info(_msg)
#                     insert_dict = {"message": "[Error] insert iot data", "state": False}
#                 return insert_dict
#             else:
#                 error_msg = {
#                     "message": "[Error] input status does not ok or other input error",
#                     "state": False,
#                 }
#                 logger.info(error_msg)
#                 return error_msg
#     else:
#         error_msg = {"message": db_msg["message"], "state": False}
#         logger.info(error_msg)
#         return error_msg


# 又得馬 - 機台裝置假設資料
def insert_device_data(df2, enabled):
    # 匯入DB
    n_sql = f"""INSERT INTO device_table(
                    id, serverid, machineid, enabled, name, 
                    aliasid, comment, createdat, updateat)
                VALUES(
                    {df2.iloc[0, 0]}, {df2.iloc[0, 1]}, 
                    {df2.iloc[0, 2]}, {enabled}, 
                    '{df2.iloc[0, 4]}', '{df2.iloc[0, 5]}', 
                    '{df2.iloc[0, 6]}', '{df2.iloc[0, 7]}',
                    '{df2.iloc[0, 8]}')"""
    return n_sql, True


# 新增又得馬裝置資料
def insert_device(logger):
    # 擷取docker compose 設定
    api_base_url = "https://host.docker.internal:443"
    api_endpoint = os.environ["API_DEVICE_ENDPOINT"]
    access_token = os.environ["API_TOKEN"]
    headers = {
        "Authorization": "Bearer" + access_token,
        "Content-Type": "application/json",
    }
    print(api_base_url + api_endpoint)
    # 設置讀取 又得馬API程式
    data_response = requests.get(
        api_base_url + api_endpoint, headers=headers, verify=False
    )
    device_dict = data_response.json()
    if device_dict["status"] == "ok":
        for i in range(len(device_dict["data"])):
            df1 = copy.deepcopy(device_dict["data"][i])
            df2 = pd.DataFrame(df1, index=[0])
            if df2.iloc[0, 3]:
                enabled = 1
            else:
                enabled = 0
            n_sql, state = insert_device_data(df2, enabled)
            if state:
                msg = read_config(n_sql, True, logger)
                d2, _msg = link_Postgres(**msg)
                logger.info(f"Created the device data: row {i}")
            else:
                pass
                logger.info(f"[Error] insert device data: row {i}")
        return {"message": "Created the device data", "state": True}
    else:
        return {"message": "[Error] insert device data", "state": False}


# 新增資料主程式
def insert_main(df_dict: Dict, insert_name: str, logger):
    input_state = True
    state = True
    if insert_name == "repair":
        n_sql = insert_repair_data(df_dict)
    elif insert_name == "part_list":
        n_sql = insert_part_list(df_dict)
    else:
        input_state = False
        state = False
        n_sql = None
    if input_state:
        msg2 = read_config(n_sql, True, logger)
        d2, _msg = link_Postgres(**msg2)
        if _msg["state"]:
            save_msg = f"Created the {insert_name} data"
            logger.info(f"Created the {insert_name} table")
        else:
            state = False
            save_msg = _msg["message"]
            logger.info(_msg["message"])
    else:
        save_msg = "API input error"
        state = False
        logger.info("[Error] API input error")
    return {"message": save_msg, "state": state}


# -------------- Get data ---------------
# 擷取又得馬資料
def get_iot_data(logger):
    sql = """select * from huan_jia;"""
    msg = read_config(sql, False, logger)
    iot_data, db_msg = link_Postgres(**msg)
    return iot_data, db_msg


# 讀 machine table
def get_machine_data(logger):
    sql = """select * from machine_table;"""
    msg = read_config(sql, False, logger)
    m_data, db_msg = link_Postgres(**msg)
    return m_data, db_msg


# 讀取 machine table join iot data
# def get_mt_and_iot(logger):
#     sql = """SELECT mt.machine_name, ht.device_id, ht.end_at, ht.count
#     FROM machine_table as mt
#     FULL JOIN huan_jia as ht
#     ON mt.device_id=ht.device_id
#     WHERE mt.machine_name is NOT NULL;"""
#     msg = read_config(sql, False, logger)
#     m_data, db_msg = link_Postgres(**msg)
#     return m_data, db_msg


# 讀取 iot + machine + part table
# machine name 有新增到的才進行
# def get_imp_data(now_date, machine_name, part_name):
#     sql = f"""SELECT t4.machine_name, t4.part_name, MAX(t4.init_val) as init_val, Max(t4.insert_date) as insert_date, MAX(t4.h_date) as h_date, sum(t4.count)
#     FROM (
#         SELECT DISTINCT t3.h_date, t3.machine_name, t3.part_name, t3."count", t3.init_val, t3.insert_date
#         FROM (
#         SELECT
#             t2.*
#         FROM
#             (
#             SELECT
#                 t1.*,
#                 pt.part_name,
#                 pt.insert_date,
#                 pt.init_val
#             FROM
#                 (
#                 SELECT
#                     mt.machine_name,
#                     ht.count,
#                     ht.end_at as h_date
#                 FROM
#                     machine_table AS mt
#                     FULL JOIN huan_jia AS ht ON mt.device_id = ht.device_id
#                     WHERE mt.machine_name IS NOT NULL) t1
#             FULL JOIN (SELECT pt.machine_name, pt.part_name,
#                             MAX(pt.insert_date) as insert_date, MAX(pt.init_val) as init_val
#                         FROM part_table as pt
#                         GROUP BY pt.machine_name, pt.part_name)
#                         AS pt ON t1.machine_name = pt.machine_name) t2 ) t3
#         WHERE machine_name = '{machine_name}'
#         AND part_name = '{part_name}'
#         AND h_date < '{now_date}'
#         AND h_date >= insert_date) t4
#         GROUP BY t4.machine_name, t4.part_name"""
#     return sql


# 讀取維修資料
def get_repair_data(logger):
    sql = """SELECT rt.repair_date, rt.machine_name, rt.part_name
        FROM repair_table as rt"""
    msg = read_config(sql, False, logger)
    m_data, db_msg = link_Postgres(**msg)
    return m_data, db_msg


# 讀取 part_table 在指定的 machine_name & part_name有多少筆資料
# 如果資料筆數 > 1 則需要換讀取方式
def get_part_data(num, machine_name, part_name):
    if num <= 1:
        sql = f"""SELECT
        pt.machine_name, pt.part_name,
        MAX ( pt.insert_date ) AS insert_date,
        MAX ( pt.init_val ) AS init_val
        FROM part_table AS pt
        WHERE pt.machine_name = '{machine_name}'
        AND pt.part_name = '{part_name}'
        GROUP BY
            pt.machine_name,
            pt.part_name"""
    else:
        sql = f"""SELECT
        pt.machine_name, pt.part_name,
        MAX ( pt.insert_date ) AS insert_date,
        MAX ( pt.init_val ) AS init_val,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY pt.part_day) AS  mean_day,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY pt.part_count) AS mean_count
        FROM part_table AS pt
        WHERE pt.part_day != 0
        AND pt.machine_name = '{machine_name}'
        AND pt.part_name = '{part_name}'
        GROUP BY
            pt.machine_name,
            pt.part_name"""
    return sql


# 計算 part_table 資料數
def count_data(machine_name, part_name):
    sql = f"""SELECT COUNT(*) as num
        FROM part_table as pt
        WHERE pt.machine_name = '{machine_name}'
        AND pt.part_name='{part_name}'"""
    return sql


# 取得指定的 machine_name & part_name在iot有多少count
# 根據時間範圍加總機台運轉圈數
def get_iot_count(insert_date, before_date, machine_name, num):
    if num <= 1:
        sql = f"""SELECT t2.machine_name, t2.device_id, sum(t2."count") sum_count
            FROM (
            SELECT t1.machine_name, t1.device_id, ht.start_at, ht.end_at, ht.count
            FROM(
            SELECT ml.machine_name, ml.device_id
            FROM machine_list as ml) t1
            FULL JOIN huan_jia as ht on ht.device_id = t1.device_id
            WHERE t1.machine_name = '{machine_name}'
            AND ht.end_at < '{before_date}')t2
            GROUP BY t2.machine_name, t2.device_id"""
    else:
        sql = f"""SELECT t2.machine_name, t2.device_id, sum(t2."count") sum_count
            FROM (
            SELECT t1.machine_name, t1.device_id, ht.start_at, ht.end_at, ht.count
            FROM(
            SELECT ml.machine_name, ml.device_id
            FROM machine_list as ml) t1
            FULL JOIN huan_jia as ht on ht.device_id = t1.device_id
            WHERE t1.machine_name = '{machine_name}'
            AND ht.end_at < '{before_date}'
            AND ht.end_at > '{insert_date}')t2
            GROUP BY t2.machine_name, t2.device_id"""
    return sql


# 依據各零件進行判斷設備健康度
def get_machine_healthy_val():
    sql = """SELECT
        t1.*,
    CASE
        WHEN t1.healthy_val >= 0.9 THEN
        'R' 
        WHEN t1.healthy_val < 0.9 
        AND t1.healthy_val >= 0.7 THEN
            'Y' ELSE'G' 
            END AS machine_healthy_state 
    FROM
        (
        SELECT
            pst.machine_name,
            MAX ( pst.system_date ) system_date,
            MAX ( pst.mean_count ) mean_count,
            MAX ( pst.mean_day ) mean_day,
            MAX ( pst.part_healthy_val ) healthy_val 
        FROM
            part_statistic_table AS pst 
        GROUP BY
        pst.machine_name 
    ) t1"""
    return sql


# ------------ 讀取資料 -----------------
# 讀取 machine table
def read_machine_table(logger):
    try:
        df_dict = {}
        sql = """SELECT * FROM machine_table"""
        msg = read_config(sql, False, logger)
        m_data, db_msg = link_Postgres(**msg)
        df_data = m_data.to_dict()
        df_dict["data"] = [df_data]
        msg = "Finished reading data"
        df_dict["message"] = msg
        df_dict["state"] = True
    except:
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        msg = f"[Error] {file_name}: {func_name} at line {line_num}"
        df_dict = {"message": msg, "state": False}
        logger.info(msg)
    return df_dict


def read_machine_list_data(logger):
    try:
        df_dict = {}
        sql = """SELECT * FROM machine_list"""
        msg = read_config(sql, False, logger)
        m_data, db_msg = link_Postgres(**msg)
        df_data = m_data.to_dict()
        df_dict["data"] = [df_data]
        msg = "Finished reading data"
        df_dict["message"] = msg
        df_dict["state"] = True
    except:
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        msg = f"[Error] {file_name}: {func_name} at line {line_num}"
        df_dict = {"message": msg, "state": False}
        logger.info(msg)
    return df_dict


# 讀取資料 - machine_state_table
def read_machine_state(logger):
    try:
        # 取出最大日期
        mst1_sql = """SELECT mst.machine_name, MAX(mst.system_date) system_date
                FROM machine_state_table AS mst
                GROUP BY mst.machine_name"""
        msg1 = read_config(mst1_sql, False, logger)
        m1_data, db_msg1 = link_Postgres(**msg1)
        if db_msg1["state"]:
            date_list = m1_data["system_date"].tolist()
            healthy_val = []
            for idx, d in enumerate(date_list):
                mst2_sql = f"""SELECT mst.machine_healthy_state
                    FROM machine_state_table AS mst
                    WHERE mst.machine_name = '{m1_data.loc[idx, 'machine_name']}'
                    AND mst.system_date = '{d}'"""
                msg2 = read_config(mst2_sql, False, logger)
                m2_data, db_msg2 = link_Postgres(**msg2)
                healthy_val.append(m2_data.iloc[0, 0])
            data = {
                "machine_name": m1_data["machine_name"].tolist(),
                "healthy_state": healthy_val,
            }
            result_dict = {
                "data": data,
                "message": "Read the results have been completed.",
                "state": True,
            }
        else:
            result_dict = db_msg1
    except:
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        msg = f"[Error] {file_name}: {func_name} at line {line_num}"
        result_dict = {"data": None, "message": msg, "state": False}
    return result_dict


def read_repair_data(logger):
    sql = f"""SELECT *
        FROM repair_table as rt"""
    df_dict = {}
    try:
        msg = read_config(sql, False, logger)
        m_data, db_msg = link_Postgres(**msg)
        df_dict["data"] = m_data.to_dict(orient="records")
        df_dict["message"] = "Read repair data OK!"
        df_dict["state"] = True
    except:
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        msg = f"[Error] {file_name}: {func_name} at line {line_num}"
        df_dict = {"data": None, "message": msg, "state": False}
    return df_dict


# ------------- 修改 --------------
# 修改零件名稱
def update_part_item(up_dict: Dict, logger):
    sql = f"""UPDATE part_list 
    SET part_name = '{up_dict['part_name']}', remark = '{up_dict['remark']}'
    WHERE id = {up_dict['id']}"""
    db_msg = read_config(sql, True, logger)
    m_data, db_msg = link_Postgres(**db_msg)
    if db_msg["state"]:
        msg = f'Update the id = {up_dict["id"]}: {up_dict["part_name"]} completed'
        msg_dict = {"message": msg, "state": True}
        logger.info("Update part item completed")
    else:
        msg_dict = db_msg
        logger.info(db_msg["message"])
    return msg_dict


# 修改 machine table init val and use state
def update_machine_table(up_dict: Dict, logger):
    if up_dict["update_target"] == "init_val":
        sql = f"""UPDATE machine_table
                SET init_val = {up_dict['init_val']}
                WHERE part_name = '{up_dict['part_name']}' 
                AND machine_name = '{up_dict['machine_name']}'"""
    elif up_dict["update_target"] == "use_state":
        sql = f"""UPDATE machine_table
                SET use_state = '{up_dict['use_state']}'
                WHERE part_name = '{up_dict['part_name']}' 
                AND machine_name = '{up_dict['machine_name']}'"""
    else:
        sql = "N"
    if sql != "N":
        db_msg = read_config(sql, True, logger)
        m_data, db_msg = link_Postgres(**db_msg)
        if db_msg["state"]:
            msg = f'Update the {up_dict["update_target"]} completed'
            msg_dict = {"message": msg, "state": True}
            logger.info(f'Update the {up_dict["update_target"]} completed')
        else:
            msg_dict = db_msg
            logger.info(db_msg["message"])
    else:
        msg_dict = {
            "message": f'[Error] input target ({up_dict["update_target"]}) error. check the update target',
            "state": False,
        }
        logger.info(
            f'[Error] input target ({up_dict["update_target"]}) error. check the update target'
        )
    return msg_dict


# ------------- 刪除 --------------
# 刪除零件列表中的零件
def del_part_item(del_dict: Dict, logger):
    sql = f"""DELETE FROM part_list
        WHERE id = {del_dict['id']} AND part_name = '{del_dict['part_name']}'"""
    db_msg = read_config(sql, True, logger)
    m_data, db_msg = link_Postgres(**db_msg)
    if db_msg["state"]:
        msg = f'Delete the id = {del_dict["id"]}: {del_dict["part_name"]} completed'
        msg_dict = {"message": msg, "state": True}
        logger.info("Delete part item completed")
    else:
        msg_dict = db_msg
        logger.info(db_msg["message"])
    return msg_dict


# 刪除維修資料
def del_repair_data(del_dict: Dict, logger):
    sql = f"""DELETE FROM repair_table 
    WHERE machine_name = '{del_dict['machine_name']}'
    AND part_name = '{del_dict['part_name']}'
    AND repair_date = '{del_dict['repair_date']}'
    AND repair_man = '{del_dict['repair_man']}'
    AND director = '{del_dict['director']}'"""
    db_msg = read_config(sql, True, logger)
    m_data, db_msg = link_Postgres(**db_msg)
    if db_msg["state"]:
        msg = "Delete repair data completed"
        msg_dict = {"message": msg, "state": True}
        logger.info("Delete repair data completed")
    else:
        msg_dict = db_msg
        logger.info(db_msg["message"])
    return msg_dict


# 刪除機台管理資料
def del_machine_table_data(del_dict: Dict, logger):
    sql = f"""DELETE FROM machine_table
            WHERE machine_name = '{del_dict['machine_name']}' 
            AND part_name = '{del_dict['part_name']}'"""
    db_msg = read_config(sql, True, logger)
    m_data, db_msg = link_Postgres(**db_msg)
    if db_msg["state"]:
        msg = f'Delete the machine_name = {del_dict["machine_name"]} & part_name = {del_dict["part_name"]}'
        msg_dict = {"message": msg, "state": True}
        logger.info("Delete data of machine table completed")
    else:
        msg_dict = db_msg
        logger.info(db_msg["message"])
    return msg_dict


# 刪除 machine list data & machine table 相對應的資料
def del_machine_list_data(del_dict: Dict, logger):
    # delete machine list data
    ml_sql = f"""DELETE FROM machine_list
                WHERE machine_name = '{del_dict['machine_name']}' 
                AND device_id = {del_dict['device_id']}"""
    db_msg1 = read_config(ml_sql, True, logger)
    m1_data, d1_msg = link_Postgres(**db_msg1)
    if d1_msg["state"]:
        logger.info("Delete data of machine list completed")
        # delete machine table data
        mt_sql = f"""DELETE FROM machine_table
                    WHERE machine_name = '{del_dict['machine_name']}' 
                    AND device_id = {del_dict['device_id']}"""
        db_msg2 = read_config(mt_sql, True, logger)
        m2_data, d2_msg = link_Postgres(**db_msg2)
        if d2_msg["state"]:
            msg = f'Delete the machine_name({del_dict["machine_name"]}) and device_id({del_dict["device_id"]})'
            msg_dict = {"message": msg, "state": True}
            logger.info("Delete data of machine table completed")
            return msg_dict
        else:
            msg_dict = d2_msg
            logger.info(d2_msg["message"])
            return msg_dict
    else:
        msg_dict = d1_msg
        logger.info(d1_msg["message"])
        return msg_dict


# ----------linking data -----------
# 回傳現有可配對的device id
def use_device_id(logger):
    try:
        df_dict = {}
        # 比對 device_table & machine_list 並取出不包含 machine_list 中的 device_id
        d_sql = """
            SELECT dt.id as device_id FROM device_table as dt
            EXCEPT
            SELECT ml.device_id FROM machine_list as ml"""
        msg = read_config(d_sql, False, logger)
        m_data, db_msg = link_Postgres(**msg)
        df_data = m_data.to_dict()
        df_dict["data"] = [df_data]
        msg = "Finished reading data"
        df_dict["message"] = msg
        df_dict["state"] = True
    except:
        cl, exc, tb = sys.exc_info()  # 取得Call Stack
        last_call_stack = traceback.extract_tb(tb)[-1]  # 取得Call Stack的最後一筆資料
        file_name = last_call_stack[0]  # 取得發生的檔案名稱
        line_num = last_call_stack[1]  # 取得發生的行號
        func_name = last_call_stack[2]  # 取得發生的函數名稱
        msg = f"[Error] {file_name}: {func_name} at line {line_num}"
        df_dict = {"message": msg, "state": False}
        logger.info(msg)
    return df_dict
