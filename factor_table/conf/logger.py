# coding=utf-8
import logging, os
from WealthScore.utils.logger import LoggerHelper
from WealthScore.conf import Config
import WealthScore

BASE_DIR = os.path.dirname(os.path.abspath(WealthScore.__path__[0]))
LOG_DIR = os.path.join(BASE_DIR, 'logs')

file_path = os.path.join(LOG_DIR, Config.settings.get('LogFile', 'proj.log'))
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
