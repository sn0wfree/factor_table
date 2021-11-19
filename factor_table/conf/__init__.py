# coding=utf-8
import os
import sys
from glob import glob

import yaml

from factor_table.utils.singleton import singleton

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, str(BASE_DIR))

LOG_DIR = os.path.join(BASE_DIR, 'logs')
config_file = os.path.join(BASE_DIR, 'config.yml')


# Config = Configs(conf_file=config_file)

def detect_file_full_path(conf_file: str = 'config.yml'):
    # if system() == 'Windows':
    #     sep = '\\'
    # else:
    #     sep = '/'
    sep = os.sep
    path1 = os.path.abspath('.')
    path2 = os.path.abspath(os.path.dirname(os.getcwd()))
    path3 = os.path.abspath(os.path.join(os.getcwd(), '../..'))
    if path1 + sep + conf_file in glob(path1 + sep + '*'):
        target_path = path1 + sep
    elif path2 + sep + conf_file in glob(path2 + sep + '*'):
        target_path = path2 + sep
    elif path3 + sep + conf_file in glob(path3 + sep + '*'):
        target_path = path3 + sep
    else:
        raise FileNotFoundError('connot locate {}'.format(conf_file))
    return target_path + conf_file


def load_settings(conf_file: str = 'config.yml', method='auto'):
    if method == 'auto':
        settings = detect_file_full_path(conf_file=conf_file)
    else:
        config_file = os.path.join(BASE_DIR, conf_file)
        settings = config_file
    if settings.endswith(('yml', 'yaml')):
        with open(settings, 'rb') as f:
            return yaml.safe_load(f)
    else:
        raise ValueError('only accept yaml or yml!')
    pass


@singleton
class Configs(object):
    def __init__(self, conf_file='config.yml'):
        self.settings = load_settings(conf_file=conf_file)
        print('config loaded!')

    def __getattr__(self, item):
        return self.settings[item]

    def __getitem__(self, item):
        return self.settings[item]

    def keys(self):
        return self.settings.keys()

    def load_file(self, key):
        return detect_file_full_path(self.settings[key])

    # def get_conn(self, key='conn'):
    #     conn_settings = self.settings.get(key, None)
    #     if conn_settings is not None:
    #         from WealthScore.conf.conn import Conn
    #         return Conn(conn_settings)
    #     else:
    #         raise ValueError(f'cannot find key as {key} settings!')


# F:\代码\财富分\SourceCode\project_name\trunk\WealthScore\config.yml
if __name__ == '__main__':
    pass
