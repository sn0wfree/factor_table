# coding=utf-8
import os
import sys
from glob import glob

import yaml

from factor_table.utils.singleton import singleton
from factor_table import conf

# setup Base dir
DEFAULT_DIR = conf.__path__[0]
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.insert(0, str(DEFAULT_DIR))

default_config_file = os.path.join(DEFAULT_DIR, 'config.yml')

with open(default_config_file, 'rb') as f:
    default_config_dict = yaml.safe_load(f)


# base_config_file = os.path.join(BASE_DIR, 'config.yml')


# DATETIME_FORMAT = '%Y-%m-%d'

def check_full_path_file_exists(conf_file: str, curr_path):
    return curr_path + os.sep + conf_file in glob(curr_path + os.sep + '*')


@singleton
class Configs(object):
    @staticmethod
    def detect_file_full_path(conf_file: str = 'config.yml', raise_error='raise'):
        curr_path = os.getcwd()  # current path
        f_path = os.path.abspath(os.path.dirname(curr_path))
        gf_path = os.path.abspath(os.path.dirname(f_path))

        if check_full_path_file_exists(conf_file, curr_path):

            return os.path.join(curr_path, conf_file)
        elif f_path + os.sep + conf_file in glob(f_path + os.sep + '*'):
            return os.path.join(f_path, conf_file)
        elif gf_path + os.sep + conf_file in glob(gf_path + os.sep + '*'):
            return os.path.join(gf_path, conf_file)
        else:
            if raise_error != 'raise':
                return None
            else:
                raise FileNotFoundError(
                    f'Cannot locate {conf_file} at the following path: {",".join([curr_path, f_path, gf_path])}')

    @classmethod
    def load_settings(cls, conf_file: str = 'config.yml', raise_error='raise'):
        settings_path = cls.detect_file_full_path(conf_file=conf_file, raise_error=raise_error)
        # if method == 'auto' else os.path.join(BASE_DIR,conf_file)

        if settings_path is not None and settings_path.endswith(('yml', 'yaml')):
            with open(settings_path, 'rb') as f:
                return yaml.safe_load(f)
        else:
            if raise_error != 'raise':
                return {}
            else:
                raise ValueError('only accept yaml or yml!')

    def __init__(self, conf_file='config.yml', raise_error='raise'):
        self.settings = default_config_dict
        self.settings.update(self.load_settings(conf_file=conf_file, raise_error=raise_error))
        print('config loaded!')

    def __getattr__(self, item):
        return self.settings[item]

    def __getitem__(self, item):
        return self.settings[item]

    def keys(self):
        return self.settings.keys()

    def load_file(self, key):
        return self.detect_file_full_path(self.settings[key])


# Config = Configs(conf_file=config_file)

if __name__ == '__main__':
    # print(os.getcwd(), os.path.abspath('.'))
    # curr_path = os.getcwd()
    # f_path = os.path.abspath(os.path.dirname(curr_path))
    # gf_path = os.path.abspath(os.path.join(os.getcwd(), '../..'))
    # conf_file: str = 'config.yml'
    # print(os.path.join(curr_path, conf_file))
    pass
