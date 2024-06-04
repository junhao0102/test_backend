'''
fast api
'''
import os
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import uvicorn

# initial
from save_data import create_main, init_part_data, insert_main

# delete
from save_data import del_part_item, del_machine_table_data, del_machine_list_data

# update
from save_data import update_part_item, update_machine_table

# read
from save_data import read_part_list, read_machine_table, read_machine_list_data, \
read_machine_state, read_repair_data, del_repair_data

# read_part_msg

# insert
from save_data import insert_machine_data
from statistic_fun import add_pair_data

# linking
from save_data import use_device_id

# export
from statistic_fun import statistic_part, machine_healthy_val


# 定期執行
from statistic_fun import update_main

from datetime import date
import schedule
import time
import json
from utility import set_logger

logger = set_logger('api_logger')
path = os.getcwd()
app = FastAPI()
job = None  # 排程工作
job_tag = []  # 排程工作的標間


# ----- step 1. 新增DB資料表 -----
@app.get("/create/new_table")
async def create_table():
    create_msg = create_main(logger)
    return create_msg


@app.get("/create/init_part")
async def init_part_list():
    init_msg = init_part_data(logger)
    return init_msg


# -------------- 新增資料 -----------------
# 新增零件列表
class NewPartItem(BaseModel):
    id: int
    part_name: str
    use_state: str
    remark: str


@app.post("/insert/new_part")
async def insert_new_part(item: NewPartItem):
    api_dict = item.dict()
    insert_msg = insert_main(api_dict, 'part_list', logger)
    print(insert_msg)
    return insert_msg


# 新增維修資料
class NewRepair(BaseModel):
    machine_name: str
    part_name: str
    repair_date: date
    repair_man: str
    director: str
    remark: str


@app.post("/insert/new_repair")
async def insert_new_repair(item: NewRepair):
    api_dict = item.dict()
    insert_msg = insert_main(api_dict, 'repair', logger)
    msg_dict = add_pair_data(api_dict, logger)
    if insert_msg['state'] and msg_dict['state']:
        insert_dict = {'message': [{'insert_message': insert_msg['message']},
                                   {'update_message': msg_dict['message']}],
                       'state': True}
    else:
        insert_dict = {'message': [{'insert_message': insert_msg['message']},
                                   {'update_message': msg_dict['message']}],
                       'state': False}
    return insert_dict


# 新增機台資料
class NewMachine(BaseModel):
    machine_name: str
    device_id: int
    init_val: int
    remark: str


@app.post("/insert/new_machine")
async def insert_new_machine(item: NewMachine):
    api_dict = item.dict()
    insert_msg = insert_machine_data(api_dict, logger)
    return insert_msg


# ----------------------讀取資料-------------------
# -----讀又得馬裝置資料--> 假設資料，等又得馬給API就可以刪除
# 讀取 machine table
@app.post("/read/machine_table")
async def read_mt():
    msg_dict = read_machine_table(logger)
    return msg_dict


# 讀取 machine list
@app.post("/read/machine_list")
async def read_ml():
    msg_dict = read_machine_list_data(logger)
    return msg_dict


# 讀取零件列表
@app.get("/read/part_list")
async def read_part_list_all():
    read_msg = read_part_list(logger)
    return read_msg


# 讀取設備健康度資料
@app.post("/read/machine_healthy")
async def read_machine_healthy():
    df_dict = read_machine_state(logger)
    return df_dict


@app.post("/read/repair_data")
async def repair_data():
    df_dict = read_repair_data(logger)
    return df_dict


# ----------------  修改 -----------------
# 修改零件名稱
class UpdatePartItem(BaseModel):
    id: int
    part_name: str
    remark: str


@app.post("/update/update_part_item")
async def update_p_item(item: UpdatePartItem):
    api_dict = item.dict()
    msg_dict = update_part_item(
        api_dict, logger)
    return msg_dict


# 修改零件名稱或狀態或初始值
class UpdateMachineTable(BaseModel):
    machine_name: str
    init_val: int
    part_name: str
    use_state: str
    update_target: str


@app.post("/update/update_m_table")
async def update_mt(item: UpdateMachineTable):
    api_dict = item.dict()
    msg_dict = update_machine_table(api_dict, logger)
    return msg_dict


# --------- 刪除資料 ------------
# 重置 – 刪除符合target條件的所有資料表 (需修改)
class DelData(BaseModel):
    target: str
    machine_name: str
    part_name: str


class DelRepairData(BaseModel):
    machine_name: str
    part_name: str
    repair_date: str
    repair_man: str
    director: str


@app.post("/del/del_repair_data")
async def del_rep_data(item: DelRepairData):
    api_dict = item.dict()
    msg_dict = del_repair_data(api_dict, logger)
    return msg_dict


# 刪除零件列表
class DelPartItem(BaseModel):
    id: int
    part_name: str


@app.post("/del/del_part_item")
async def del_p_item(item: DelPartItem):
    api_dict = item.dict()
    msg_dict = del_part_item(
        api_dict, logger)
    return msg_dict


# 刪除 machine table data
class DelMachineTableData(BaseModel):
    machine_name: str
    part_name: str


@app.post("/del/machine_table_data")
async def del_mt_data(item: DelMachineTableData):
    api_dict = item.dict()
    msg_dict = del_machine_table_data(api_dict, logger)
    return msg_dict


# 刪除 machine list data
class DelMachineListData(BaseModel):
    machine_name: str
    device_id: int


@app.post("/del/machine_list_data")
async def del_mlist_data(item: DelMachineListData):
    api_dict = item.dict()
    msg_dict = del_machine_list_data(api_dict, logger)
    return msg_dict


# -------------------- linking data ---------
# 回傳現有可配對的device id
@app.post("/link/use_device_id")
async def link_device_id():
    msg_dict = use_device_id(logger)
    return msg_dict


# ----------- 執行 ---------
@app.post("/export/statistic_data")
async def export_statistic_data():
    msg_dict = statistic_part(logger)
    return msg_dict


@app.post("/export/machine_healthy_val")
async def export_add_result():
    msg_dict = machine_healthy_val(logger)
    return msg_dict


@app.post("/export/insert_device")
async def export_insert_device():
    msg_dict = insert_device(logger)
    return msg_dict


# @app.post("/export/insert_huan_jia")
# async def export_insert_huan_jia_data():
#     msg_dict = insert_huan_jia_data(logger)
#     return msg_dict


# -------------------- 定時重新執行 ---------
def greet():
    update_main(logger)


def schedule_train_model(tag):
    global job, job_tag
    job = schedule.every().day.at("01:00").do(greet).tag(tag)
    job_tag.append(tag)
    while len(schedule.get_jobs()) != 0:
        schedule.run_pending()
        # print(schedule.get_jobs())
        time.sleep(1)


@app.get('/stop_schedule/{tag}')
def stop_schedule(tag: str = None):
    global job, job_tag
    print(job)
    print(job_tag)
    if not (job is None):
        schedule.clear(tag)
        job_tag.remove(tag)
    else:
        print('There is no job.')
    print('stop job!')
    return {"message": "Stop scheduling."}


@app.get('/list_schedule/')
def list_schedule():
    global job_tag
    print(job_tag)
    return {"jobs": json.dumps(job_tag)}


@app.get('/schedule_retrain/{tag}')
def schedule_retrain_every(background_tasks: BackgroundTasks, tag: str):
    background_tasks.add_task(schedule_train_model, tag)
    return {"message": "Start retraining."}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
