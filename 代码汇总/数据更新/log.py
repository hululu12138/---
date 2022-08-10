# #!/usr/bin/python3
# # -*- coding: utf-8 -*-
# # @Time: 2019/3/18 13:29
import logging
import datetime
import os

# 定义默认路径格式
_filefmt = os.path.join("logs", "%Y-%m-%d.log")


class MyLoggerHandler(logging.Handler):
    """日志文件的类"""
    def __init__(self, filefmt=None):
        """初始化，设置日志路径格式"""
        if filefmt is None:
            self.filefmt =_filefmt
        else:
            self.filefmt = filefmt
        logging.Handler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        # 确定当天日志路径
        _filePath = datetime.datetime.now().strftime(self.filefmt)
        # 返回目录
        _dir = os.path.dirname(_filePath)
        try:
            if os.path.exists(_dir) is False:
                os.makedirs(_dir)
        except Exception as e:
            print("创建目录失败！filepath is" + _filePath)
            print(e)
            pass
        try:
            _fobj = open(_filePath, 'a')
            _fobj.write(msg)
            _fobj.write("\n")
            # 文件关闭后会自动刷新缓冲区，但有时你需要在关闭前刷新它
            _fobj.flush()
            _fobj.close()
        except Exception as e:
            print("写入文件失败！filepath is" + _filePath)
            print(e)
            pass


def log():
    """日志"""
    logger = logging.getLogger(__name__)
    # 基础过滤等级
    logger.setLevel(level=logging.DEBUG)
    # 日志，自定义的handler类
    handler = MyLoggerHandler()
    # 日志文件过滤等级
    handler.setLevel(logging.INFO)
    # 日志文件输出格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # 控制台
    console = logging.StreamHandler()
    # 控制台过滤等级
    console.setLevel(logging.INFO)
    # 控制台输出格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    # 将日志文件和控制台加入
    logger.addHandler(handler)
    logger.addHandler(console)
    return logger


if __name__ == '__main__':
    import time
    logging.basicConfig()
    logger = logging.getLogger("logger")
    logger.setLevel(logging.INFO)
    handler = MyLoggerHandler()
    handler.setLevel(logging.INFO)
    # 日志信息格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    i = 1
    while True:
        logger.error('log...' + str(i))
        i += 1
        time.sleep(5)
