#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time: 2021/8/3 17:39

import os
import csv


def save_file(file_name, content, mode='w', encoding='utf8'):
    """写文件"""
    # 写文件前新建目录
    dir_name = os.path.dirname(file_name)
    if dir_name:
        try:
            if os.path.exists(dir_name) is False:
                os.makedirs(dir_name)
        except Exception as e:
            print("无法新建目录{}；异常信息：{}".format(dir_name, str(e)))
    with open(file_name, mode, encoding=encoding, newline='') as csv_file:
        writer = csv.writer(csv_file)
        # # 写入单行
        # writer.writerow(content)
        # 写入多行
        writer.writerows(content)


def read_file(file_name, mode='r', encoding='utf8'):
    """读文件"""
    with open(file_name, mode, encoding=encoding) as csv_file:
        reader = csv.reader(csv_file)
        return list(reader)


def split_file(file_name):
    """拆分文件名，获得写入数据库格式所需的元素"""
    file_path, temp_filename = os.path.split(file_name)
    filename, extension = os.path.splitext(temp_filename)
    upper_path = os.path.split(file_path)[1]
    return upper_path, filename

def traversal_file(root_dir):
    """遍历目录下文件"""
    file_names = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            file_name = os.path.join(root, file)
            file_names.append(file_name)
    return file_names

class DbFormat:
    """转换符合数据格式的文件"""
    def __init__(self, root_dir, logger):
        self.root_dir = root_dir
        self.logger = logger

    def get_db_format(self, file_name):
        """获得单个写入数据库格式的文件"""
        format_data = []
        data = read_file(file_name)
        upper_path, filename = split_file(file_name)
        for i, a_data in enumerate(data):
            if i > 0:
                a_data.insert(1, filename)
                a_data.insert(2, upper_path)
                a_data.append(filename)
                format_data.append(a_data)
        file_name_db = '{}_db\\{}\\{}.csv'.format(self.root_dir, upper_path, filename)
        save_file(file_name_db, format_data)

    def run(self):
        file_names = traversal_file(self.root_dir)
        file_name_sum = len(file_names)
        for i, file_name in enumerate(file_names):
            self.logger.info('转换符合数据库格式文件，已完成数：{}，总文件数：{}；开始处理文件{}'.format(i,
                                                                           file_name_sum, file_name))
            self.get_db_format(file_name)


if __name__ == '__main__':
    from log import log

    logger = log()
    root_dir = '202008'
    # 转换所有符合数据库格式化文件
    db_format = DbFormat(root_dir, logger)
    db_format.run()