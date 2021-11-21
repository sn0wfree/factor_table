# coding=utf-8
from functools import wraps
from typing import Union

import pandas as pd

from factor_table.core.FactorPool import FactorPool
from factor_table.core.FactorTools import SaveTools
from factor_table.core.Factors import FactorCreator


class __MetaFactorTable__(SaveTools):
    @staticmethod
    def generate_alias(factor_names: Union[list,tuple], as_alias: Union[list, tuple, str] = None):
        """
        convert alias to list of alias, only accept list, tuple, str

        :param factor_names: list of factor names
        :param as_alias: alias of factor names
        :return: list of alias
        """
        if as_alias is None:  # if not entered, use factor_names as alias
            alias = len(factor_names) * [None]
        elif isinstance(as_alias, str):  # if entered as str, use self as alias
            if ',' in as_alias:
                alias = as_alias.split(',')
            else:
                alias = [as_alias]
        elif isinstance(as_alias, (list, tuple)):
            if len(as_alias) != len(factor_names):
                raise ValueError('as_alias is not match factor_names')
            else:
                alias = as_alias
        else:
            raise TypeError('alias only accept list tuple or str!')
        return alias

    @staticmethod
    def generate_factor_names(factor_names: Union[list, tuple, str]):
        """
        convert factor_names to list of factor names, only accept list, tuple, str

        :param factor_names: list of factor names
        :return: list of factor names
        """
        if isinstance(factor_names, str):  # if entered as str, use self as alias
            if ',' in factor_names:
                factor_names = factor_names.split(',')
            else:
                factor_names = [factor_names]
        # if entered as list or tuple, use self[list] as alias
        elif isinstance(factor_names, (list, tuple)):
            pass
        else:
            raise TypeError('columns only accept list tuple or str!')
        return factor_names

    @staticmethod
    def check_cik_dt(cik_dt, default_cik_dt):
        """

        :param cik_dt:
        :param default_cik_dt:
        :return:
        """
        if cik_dt is None:
            cik_dt = default_cik_dt

        return cik_dt

    @staticmethod
    def check_cik_id(cik_id, default_cik_id):
        """


        :param cik_id:
        :param default_cik_id:
        :return:

        """

        if cik_id is None:
            cik_id = default_cik_id
        return cik_id

    @staticmethod
    def _extract_kwargs_info(key: str, default: str, kw_dict: dict, use_default=False):
        """
        extract cik_dt, cik_iid from key
        :param key:
        :param default:
        :param kw_dict:
        :return:
        """
        if key is None:
            key = default
        if key in kw_dict:
            return kw_dict[key]
        else:
            if use_default:
                return default
            raise ValueError(f'{key} is not in kw_dict')


class __FactorTable__(__MetaFactorTable__):
    __slots__ = ['__factors', '_cleaned', '_transformed', '__transformed_data', '_status']

    def __init__(self, **config):
        self._cleaned = False
        self._transformed = False
        self.__transformed_data = {}  # store transformed data
        self.__factors = FactorPool()
        self._status = config  # store config

    @wraps(FactorPool.show_factors)
    def show_factors(self, *args, **kwargs):
        return self.__factors.show_factors(*args, **kwargs)

    @wraps(FactorCreator.__new__)
    def append(self, *args, **kwargs):
        self.__factors.append(*args, **kwargs)

    @wraps(FactorCreator.__new__)
    def add_factor(self, *args, **kwargs):
        return self.__factors.add_factor(*args, **kwargs)

    def add_elements(self, *elements):
        for element in elements:
            self.__factors.add_factor(FactorCreator.load_from_element(element))

    def add_factor_from_config(self, *configs):
        for config in configs:
            self.__factors.add_factor(**config)

    # # @wraps(FactorPool.save)
    # def save(self, *args, force=True, **kwargs):
    #     # force = self._status.get('force', False)
    #     return self.__factors.save(self.__factors, *args, force=force, **kwargs)

    # # @wraps(FactorPool.load)
    # def load(self, *args, **kwargs):
    #     return self.__factors.load(self, *args, **kwargs)

    @property
    def _obj_type(self):
        h = [f._obj_type for f in self.__factors]
        return h

    @property
    def _enable_update(self):
        if self._status.get('strict_update', True):
            sql_count = list(filter(lambda x: not x.startswith(('SQL_', 'db_table_')), self._obj_type))
        else:
            sql_count = self._obj_type
        return len(sql_count) == 0

    @property
    def cik_dts(self):
        h = []
        for f in self.__factors:
            h.extend(f.get_cik_dts())
        return sorted(set(h))

    @property
    def cik_ids(self):
        h = []
        for f in self.__factors:
            h.extend(f.get_cik_ids())
        return sorted(set(h))

    def fetch(self, cik_dts=None, cik_ids=None,  show_process=False, delay=False,force=False,**kwargs):
        """

        :param delay:
        :param show_process:
        :param reduced: whether use reduce form
        :param _cik_dts: set up dts
        :param _cik_ids:  set up ids
        :param add_limit: use force limit columns
        :return:
        """
        if cik_dts is None:
            cik_dts = self.cik_dts
        if cik_ids is None:
            cik_ids = self.cik_ids

        fetched = self.__factors.fetch_iter(_cik_dts=cik_dts, _cik_ids=cik_ids, reduced=False,
                                            add_limit=False, show_process=show_process,force=force)
        
        print(fetched)

        return fetched if delay else pd.concat(fetched, axis=1).drop_duplicates()


class FactorTable(__FactorTable__):
    __slots__ = ['__factors', '_cleaned', '_transformed', '__transformed_data']

    def transform_matrix(self, to='matrix', **kwargs):
        """
        
        transform to matrix form
        """
        cik_dts = self.cik_dts
        cik_ids = self.cik_ids
        data = self.fetch(cik_dts, cik_ids, reduced=False,delay=True, add_limit=False)
        holder={}
        for d in data:
            for col in d.columns.tolist():
                holder[col] = d[col].reset_index().pivot_table(index='cik_dts', columns='cik_ids', values=col)
        
        # holder = {col: data[col].reset_index().pivot_table(index='cik_dts', columns='cik_ids', values=col) for col in
        #           cols}

        self.__transformed_data = holder
        self._transformed = True

    def clean_cache(self):
        """
        clean redundant factors
        """
        if self._transformed:
            self.__factors = FactorPool()
            self._cleaned = True
        else:
            raise ValueError('You should transform first!')

    def __getitem__(self, item):
        """
        get matrix form item from factor table
        """
        if self._transformed:
            return self.__transformed_data[item]
        else:
            raise ValueError('transform first!')


if __name__ == '__main__':
    pass
