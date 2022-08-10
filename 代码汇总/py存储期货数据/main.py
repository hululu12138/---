#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time: 2021/8/3 13:26

from db_format import DbFormat
from db_sql import storage
from log import log


def main():
    logger = log()
    root_dir = '202008'
    # 转换所有数据库格式化文件
    db_format = DbFormat(root_dir, logger)
    db_format.run()
    # 存储与检查数据
    root_dir_db = root_dir + '_db'
    storage(root_dir_db, logger)


main()
