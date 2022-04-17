# coding=utf-8
import logging
import os

from factor_table.utils.logger import LoggerHelper

LOG_DIR = os.path.dirname(os.path.abspath('./'))

file_path = os.path.join(LOG_DIR, 'DEBUG.log')
Logger = LoggerHelper(file_path=file_path, log_level=logging.INFO)
if __name__ == '__main__':
    # print(file_path)
    # Logger.warn('test')
    # Logger.info('test2')
    # Logger.debug('test3')
    # Logger.critical('test3')
    # Logger.sql('test3')
    # Logger.status('tst5')
    pass
