#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time: 2021/8/3 18:05

import pyodbc
import os
from db_format import traversal_file, read_file
import re


class DbSql:
    """数据库"""
    def __init__(self, server, database, user, password):
        self.server = server
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        self.conn = pyodbc.connect(DRIVER='{SQL Server}', SERVER=self.server, DATABASE=self.database, UID=self.user,
                              PWD=self.password)
        self.cursor = self.conn.cursor()

    def query(self, sql, commit=True):
        self.cursor.execute(sql)
        if commit:
            # 查询语句不加commit，否则返回数据会报pyodbc.Error: ('HY010', '[HY010] [Microsoft][ODBC SQL Server Driver]函数序列错误 (0) (SQLFetch)')
            self.conn.commit()
        # self.cursor.commit()

    def fetchall(self):
        """返回查询到的多条记录，二维元组"""
        return self.cursor.fetchall()

    def fetchone(self):
        """返回查询到的最上面一条记录，单个元组；可多次调用依次返回，直至为空"""
        return self.cursor.fetchone()

    def close(self):
        self.cursor.close()
        self.conn.close()


def storage(root_dir_db, logger):
    """存储数据"""
    server_local = 'MACBOOKFA3E\SQLEXPRESS'
    database_local = 'test'
    user_local = 'sa'
    password_local = 'lmj19990506.'

    server_target = '10.7.6.92'
    database_target = 'HFData2'
    user_target = 'temp'
    password_target = 'XJTLU12345shixi'
    table_name_target = 'temp_lmj'
    # -----------------------------------------------
    # 准备工作
    # 父目录
    parent_dir = os.path.split(os.path.abspath(__file__))[0]
    print(parent_dir)
    # 获得所有文件
    file_names = traversal_file(root_dir_db)
    # 连接数据库
    db_local = DbSql(server_local, database_local, user_local, password_local)
    db_local.connect()
    logger.critical('连接本地数据库成功！')
    db_target = DbSql(server_target, database_target, user_target, password_target)
    db_target.connect()
    logger.critical('连接目标数据库成功！')
    # 开始执行操作
    file_name_sum = len(file_names)
    logger.info('准备进行存储数据库操作，本次文件数{}'.format(file_name_sum))
    error=[]
    for i, file_name in enumerate(file_names):
        logger.info('已完成文件数：{}，总文件数：{}；开始存储数据文件：{}'.format(i, file_name_sum, file_name))
        data = read_file(file_name)
        data_sum = len(data)
        time_min = min(data, key=lambda x: x[0])[0]
        time_max = max(data, key=lambda x: x[0])[0]
        symbol = data[0][1]
        # absolute_file_name = '{}\{}'.format(parent_dir, file_name)
        absolute_file_name=file_name
        upper_path=os.path.split(file_name)[0]
        futures_market=os.path.split(upper_path)[1]
        name=os.path.split(file_name)[1][0:-4]
        # 如果是201909-201911,则：
        # name = os.path.split(file_name)[1][2:-4]

        # 查询数据是否存在，存在则删除
        implementation = True
        sql = """SELECT time FROM {} WHERE filename='{}' and futures_market='{}' AND time>='{}' AND time<='{}'""".format(table_name_target,

                                                                                           name,futures_market, time_min, time_max)
        print(sql)
        db_target.query(sql, False)
        rows = db_target.fetchall()
        if rows:
            rows_sum = len(rows)
            logger.info('添加前查询到数据库存在重复数据，条数：{}，尝试删除。'.format(rows_sum))
            # 删除数据
            sql = """DELETE FROM {} WHERE filename='{}' and futures_market='{}' and time>='{}' AND time<='{}'""".format(table_name_target,
                                                                                              name,futures_market, time_min, time_max)
            print(sql)
            db_target.query(sql)
            # 再次查询确认数据被删除
            sql = """SELECT time FROM {} WHERE filename='{}' and futures_market='{}' AND time>='{}' AND time<='{}'""".format(
                table_name_target, name,futures_market, time_min, time_max)
            db_target.query(sql, False)
            rows = db_target.fetchall()
            if rows:
                logger.warning('错误！添加前数据库重复数据删除失败，将不添加文件：'.format(file_name))
                error.append(file_name)
                implementation = False
            else:
                logger.info('数据库内重复数据被删除')
        if implementation:
            # 开始添加数据
            logger.info('开始添加数据，条数：{}'.format(data_sum))
            sql = """EXEC master..xp_cmdshell 'BCP "{}..{}" in "{}" -c -S"10.7.6.92" -U"temp" -P"XJTLU12345shixi" -t","'""".format(
                database_target, table_name_target, absolute_file_name)
            print(sql)
            db_local.query(sql)
            # 检查数据添加完成
            sql = """SELECT time FROM {} WHERE filename='{}' and futures_market='{}' AND time>='{}' AND time<='{}'""".format(
                table_name_target, name,futures_market, time_min, time_max)
            db_target.query(sql, False)
            rows = db_target.fetchall()
            rows_sum = len(rows)
            if rows_sum == data_sum:
                logger.info('数据添加完成，检查数据库添加条数正确，为：{}'.format(rows_sum))
            else:
                logger.warning('错误！数据添加条数不对，目标条数应为{}，实际为{}'.format(data_sum, rows_sum))
    db_local.close()
    logger.critical('关闭本地数据连接')
    db_target.close()
    logger.critical('关闭目标数据连接')
    print('没有导入的文件:\n')
    for name in error:
        print(name+'\n')

if __name__ == '__main__':
    from log import log

    logger = log()
    root_dir_db = 'C:\\中金所期货1分钟-20210730_db'
    storage(root_dir_db, logger)
