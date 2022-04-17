# coding=utf-8
from functools import wraps

import pandas as pd

from factor_table.conf.logger import Logger
from factor_table.core.FactorPool import FactorPool
from factor_table.helper.FactorTools import SaveTools

from factor_table.conf.config import Configs

CONF = Configs(raise_error='no_raise')
DATETIME_FORMAT = CONF.DATETIME_FORMAT
DEFAULT_CIK_DT = CONF.DEFAULT_CIK_DT  # 'cik_dt'
DEFAULT_CIK_ID = CONF.DEFAULT_CIK_ID  # 'cik_id'


class MetaFactorTable(SaveTools):
    @staticmethod
    def _extract_kwargs_info(key: str, default: str, kw_dict: dict, use_default=False):
        """
        extract cik_dt, cik_iid from key
        :param key:
        :param default:
        :param kw_dict:
        :return:
        """
        if use_default:
            return kw_dict.get(key, default)
        else:
            return kw_dict[key]

    __slots__ = ['_factors', '_cleaned', '_transformed', '_transformed_data', '_status']

    def __init__(self, maxlen=None, **config):
        self._cleaned = False
        self._transformed = False
        self._transformed_data = {}  # store transformed data
        self._factors = FactorPool(maxlen=maxlen)
        self._config = config  # store config

    @wraps(FactorPool.show_factors)
    def show_factors(self, *args, **kwargs):
        return self._factors.show_factors(*args, **kwargs)

    @wraps(FactorPool.add_factor)
    def append(self, *args, **kwargs):
        self._factors.add_factor(*args, **kwargs)

    @wraps(FactorPool.add_factor)
    def add_factor(self, *args, **kwargs):
        return self._factors.add_factor(*args, **kwargs)

    # def add_elements(self, *elements):
    #     for element in elements:
    #         self.__factors.add_factor(FactorCreator.load_from_element(element))

    def add_factor_from_config(self, *configs):
        for config in configs:
            self.add_factor(**config)

    # # @wraps(FactorPool.save)
    # def save(self, *args, force=True, **kwargs):
    #     # force = self._status.get('force', False)
    #     return self.__factors.save(self.__factors, *args, force=force, **kwargs)

    # # @wraps(FactorPool.load)
    # def load(self, *args, **kwargs):
    #     return self.__factors.load(self, *args, **kwargs)

    # @property
    # def _obj_type(self):
    #     h = [f._obj_type for f in self.__factors]
    #     return h

    @property
    def _enable_update(self):
        if self._status.get('strict_update', True):
            sql_count = list(filter(lambda x: not x.startswith(('SQL_', 'db_table_')), self._factors._obj_type))
        else:
            sql_count = self._factors._obj_type
        return len(sql_count) == 0

    @Logger.deco(level='info', timer=True, extra_name='__FactorTable__')
    def fetch(self, cik_dts=None, cik_ids=None, show_process=False, delay=False, skip_cache=True, **kwargs):
        """

        :param delay:
        :param show_process:
        :param reduced: whether use reduce form
        :param _cik_dts: set up dts
        :param _cik_ids:  set up ids
        :param add_limit: use force limit columns
        :return:
        """

        cik_dts = self._factors.available_cik_dts if cik_dts is None else cik_dts

        cik_ids = self._factors.available_cik_ids if cik_ids is None else cik_ids

        fetched = self._factors.fetch_iter(_cik_dt_data=cik_dts, _cik_id_data=cik_ids, reduced=False,
                                           add_limit=False, show_process=show_process, skip_cache=skip_cache)

        return fetched if delay else pd.concat(fetched, axis=1).drop_duplicates()

    @Logger.deco(level='info', timer=True, extra_name='__FactorTable__')
    def save(self, store_path: str, cik_cols=['cik_dt', 'cik_id'], auto_save=False, cik_dt_data=None,
             cik_id_data=None, if_exists='replace'):
        cik_cols = ['cik_dt', 'cik_id'] if cik_cols is None else cik_cols
        self.raw_save(
            self, store_path, cik_cols=cik_cols, auto_save=auto_save, cik_dts=cik_dt_data,
            cik_ids=cik_id_data, if_exists=if_exists)

    def load(self, store_path, cik_dts=None, cik_cols=['cik_dt', 'cik_id']):
        for cols, data in self.raw_load(store_path, cik_dts=cik_dts, cik_cols=cik_cols):
            self.add_factor(cols, data, cik_dt=cik_cols[0], cik_id=cik_cols[0], factor_names=[cols], db_type='DF')
            print(cols)
            print(1)


class FactorTable(MetaFactorTable):
    __slots__ = ['_factors', '_cleaned', '_transformed', '_transformed_data']

    def get_transform(self, key):
        if self._transformed:
            return self._transformed_data[key]
        else:
            raise ValueError('should transform first!')

    def transform_matrix(self, cik_dt_data=None, cik_id_data=None, to='matrix', **kwargs):
        """
        
        transform to matrix form
        """
        cik_dt_data = pd.to_datetime(self._factors.available_cik_dts) if cik_dt_data is None else pd.to_datetime(
            cik_dt_data)

        cik_id_data = self._factors.available_cik_ids if cik_id_data is None else cik_id_data
        data_iter = self.fetch(cik_dt_data, cik_id_data, reduced=False, delay=True, add_limit=False)

        for d in data_iter:

            # dt_mask = d[DEFAULT_CIK_DT].isin(cik_dt_data)
            # id_mask = d[DEFAULT_CIK_ID].isin(cik_id_data)

            for col in d.columns.tolist():  # 遍历factors

                self._transformed_data[col] = d[col].reset_index().pivot_table(
                    index=DEFAULT_CIK_DT,
                    columns=DEFAULT_CIK_ID,
                    values=col).reindex(
                    index=cik_dt_data,
                    columns=cik_id_data)

        # holder = {col: data[col].reset_index().pivot_table(index='cik_dts', columns='cik_ids', values=col) for col in
        #           cols}

        self._transformed = True

    def clean_cache(self):
        """
        clean redundant factors
        """
        if self._transformed:
            self._factors = FactorPool()
            self._cleaned = True
        else:
            raise ValueError('You should transform first!')

    def __getitem__(self, item):
        """
        get matrix form item from factor table
        """
        if self._transformed:
            return self._transformed_data[item]
        else:
            raise ValueError('transform first!')


if __name__ == '__main__':
    # f_ = FactorTable()
    # import numpy as np
    #
    # np.random.seed(1)
    # from factor_table.core.FactorCreator import FactorCreator
    #
    # np.random.seed(1)
    # dts = pd.date_range('2011-01-01', '2022-12-31', )[:1000]
    # ids = np.random.randint(1, 10000, size=(1000, 1)).ravel().tolist()
    # h1 = []
    # h2 = []
    # for i in ids:
    #     df = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v1', 'v2'])
    #     df2 = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v3', 'v4'])
    #     df['cik_dt'] = dts
    #     df['cik_id'] = i
    #     df2['cik_dt'] = dts
    #     df2['cik_id'] = i
    #     h1.append(df)
    #     h2.append(df2)
    # df_1 = pd.concat(h1)
    # df_2 = pd.concat(h2)
    # del df, df2
    #
    # # f1 = FactorCreator('test', df_1, 'cik_dt', 'cik_id', factor_names=['v1', 'v2'])
    # f2 = FactorCreator('test2', df_2, 'cik_dt', 'cik_id', factor_names=['v3', 'v4'])
    # f_.add_factor('test', df_1, 'cik_dt', 'cik_id', factor_names=['v1', 'v2'])
    # print(1, f_.show_factors())
    # f_.add_factor(f2)
    # # f_.add_factor('test3', df3, 'cik_dt', 'cik_id', factor_names=['v5', 'v6'])
    #
    # print(2, f_.show_factors())
    # f_.transform_matrix()
    # print(1)
    pass
