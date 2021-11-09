# coding=utf-8
import os
import warnings
from collections import Callable
from dataclasses import dataclass
from functools import lru_cache

import pandas as pd

from factor_table.utils.check_file_type import filetype

from factor_table.helper import CoreIndexKeys,FactorElement,FactorInfo



class Factor(object):
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', 'element']

    def __init__(self, name, obj, cik_dt: str, cik_id: str, factor_names: (list, tuple, str),
                 as_alias: (list, tuple, str) = None, db_table: str = None, **kwargs):

        self._obj = obj
        self._cik = CoreIndexKeys(cik_dt, cik_id)
        self._factor_name_list = self.generate_factor_names(factor_names)
        self._alias = self.generate_alias(self._factor_name_list, as_alias)

        self.element = FactorElement(name, obj, cik_dt, cik_id, self._factor_name_list,
                                     as_alias=self._alias, db_table=db_table, kwargs=kwargs)

        self._db_table = db_table
        self._name = name
        self._kwargs = kwargs
        self._obj_type = 'Meta'

    def __rename_col__(self):
        for f, a in zip(self._factor_name_list, self._alias):
            if a is None:
                pass
            else:
                yield f, a

    def factor_info(self):
        return FactorInfo(self._obj,  # 'db_table' # df
                          self._cik.dts,  # 'dts'
                          self._cik.ids,  # 'ids'
                          ','.join(self._factor_name_list),  # 'origin_factor_names'
                          ','.join(map(lambda x: str(x), self._alias)) if self._alias is not None else None,  # 'alias'
                          '',  # sql
                          self._obj_type,  # via
                          ''  # info
                          )

    @staticmethod
    def generate_factor_names(factor_names: (list, tuple, str)):
        if isinstance(factor_names, str):
            if ',' in factor_names:
                factor_names = factor_names.split(',')
            else:
                factor_names = [factor_names]
        elif isinstance(factor_names, (list, tuple)):
            factor_names = list(factor_names)
        else:
            raise ValueError('columns only accept list tuple str!')
        return factor_names

    @staticmethod
    def generate_alias(factor_names: list, as_alias: (list, tuple, str) = None):
        if as_alias is None:
            alias = [None for _ in factor_names]
        elif isinstance(as_alias, str):
            if ',' in as_alias:
                alias = as_alias.split(',')
                if len(alias) != len(factor_names):
                    raise ValueError('as_alias is not match factor_names')
                else:
                    alias = [factor_names[idx] if alia is None else alia for idx, alia in enumerate(alias)]
            else:
                alias = [as_alias]
            if len(alias) != len(factor_names):
                raise ValueError('as_alias is not match factor_names')

        elif isinstance(as_alias, (list, tuple)):
            if len(as_alias) != len(factor_names):
                raise ValueError('as_alias is not match factor_names')
            else:
                alias = [factor_names[idx] if alia is None else alia for idx, alia in enumerate(as_alias)]
                # print(1)

                # alias = list(as_alias)
        else:
            raise ValueError('alias only accept list tuple str!')
        return alias

    def __str__(self):
        return str(self.factor_info())

    # ('db_table', 'dts', 'iid', 'origin_factor_names', 'alias', 'sql', 'via', 'info')

    def get_cik_dts(self, df: pd.DataFrame):
        """

        :param df:
        :return:
        """
        raise NotImplementedError('get_cik_dts!')
        return df[self._cik.dts].dt.strftime('%Y-%m-%d').unique().tolist()

    def get_cik_ids(self, df: pd.DataFrame):
        """

        :param df:
        :return:
        """
        raise NotImplementedError('get_cik_ids!')
        return df[self._cik.ids].unique().tolist()

    def _df_get_(self, df: pd.DataFrame, cik_dt_list: list, cik_id_list: list):
        dt_mask = df[self._cik.dts].isin(cik_dt_list)
        id_mask = df[self._cik.ids].isin(cik_id_list)
        mask = dt_mask & id_mask
        cols = [self._cik.dts, self._cik.ids] + self._factor_name_list
        return df[mask][cols].rename(
            columns={self._cik.dts: 'cik_dts', self._cik.ids: 'cik_ids', **dict(self.__rename_col__())})


class __FactorDF__(Factor):  # only store a cross section data
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', 'element']

    def __init__(self, name: str, df: pd.DataFrame, cik_dt: str, cik_id: str, factor_names: str,
                 as_alias: (list, tuple, str) = None, **kwargs):
        super(__FactorDF__, self).__init__(name, df, cik_dt, cik_id, factor_names,
                                           as_alias=as_alias, **kwargs)
        self._obj_type = 'DF'

    def get_cik_dts(self, **kwargs):
        return self._obj[self._cik.dts].dt.strftime('%Y-%m-%d').unique().tolist()

    def get_cik_ids(self, **kwargs):
        return self._obj[self._cik.ids].unique().tolist()

    def get(self, cik_dt_list, cik_id_list, **kwargs):
        return self._df_get_(self._obj, cik_dt_list, cik_id_list)
        # self.get = partial(self._df_get_, df=self._obj)


class __FactorH5__(Factor):  # only store a cross section data
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', 'element']

    def __init__(self, key: str, h5_path: str, cik_dt: str, cik_id: str, factor_names: list, *args,
                 as_alias: (list, tuple, str) = None, **kwargs):
        super(__FactorH5__, self).__init__(key, h5_path, cik_dt, cik_id, factor_names, *args,
                                           as_alias=as_alias, **kwargs)
        if filetype(h5_path) == 'HDF5':
            self._obj_type = 'H5'
        else:
            raise ValueError(f'{h5_path} is not a HDF5 file !')

    def factor_info(self):
        return FactorInfo(self._obj,  # 'db_table' = H5_path
                          self._cik.dts,  # 'dts'
                          self._cik.ids,  # 'ids'
                          ','.join(self._factor_name_list),  # 'origin_factor_names'
                          ','.join(map(lambda x: str(x), self._alias)) if self._alias is not None else None,  # 'alias'
                          self._name,  # sql name = key
                          self._obj_type,  # via # H5
                          ''  # info
                          )

    def get_cik_dts(self, **kwargs):
        # TODO 性能点
        h5_path = self._obj
        key = self._name
        df = pd.read_hdf(h5_path, key)
        return df[self._cik.dts].dt.strftime('%Y-%m-%d').unique().tolist()

    def get_cik_ids(self, **kwargs):
        # TODO 性能点
        h5_path = self._obj
        key = self._name
        df = pd.read_hdf(h5_path, key)
        return df[self._cik.ids].unique().tolist()

    def get(self, cik_dt_list, cik_id_list, **kwargs):
        # TODO 性能点
        h5_path = self._obj
        key = self._name
        df = pd.read_hdf(h5_path, key)
        return self._df_get_(df, cik_dt_list, cik_id_list)


class __FactorSQL__(Factor):  # only store a cross section data
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', '_src',
                 'db_type', 'element']

    def __init__(self, db_table_sql: str, query: object, cik_dt: str, cik_id: str, factor_names: str, *args,
                 as_alias: (list, tuple, str) = None, **kwargs):
        super(__FactorSQL__, self).__init__(db_table_sql, query, cik_dt, cik_id, factor_names, *args,
                                            as_alias=as_alias, **kwargs)
        db_type: str = kwargs.get('db_type', 'ClickHouse')  # 默认是ClickHouse
        self.db_type = db_type
        if db_table_sql.lower().startswith('select '):
            self._obj_type = f'SQL_{db_type}'
        else:
            self._obj_type = f'db_table_{db_type}'
        self._db_table = db_table_sql
        if hasattr(query, 'src'):
            self._src = query.src
        else:
            warnings.warn('query dont have src property! will set as random src! please check src!')
            self._src = str(np.random.random())

    def factor_info(self):
        if self._obj_type.startswith('SQL_'):
            return FactorInfo(self._db_table,  # 'db_table' = db_table_sql
                              self._cik.dts,  # 'dts'
                              self._cik.ids,  # 'iid'
                              ','.join(self._factor_name_list),  # 'origin_factor_names'
                              ','.join(map(lambda x: str(x), self._alias)) if self._alias is not None else None,
                              # 'alias'
                              self._db_table,  # sql
                              self._obj_type,  # via # SQL
                              self._src  # info
                              )
        else:
            return FactorInfo(self._db_table,  # 'db_table' = db_table_sql
                              self._cik.dts,  # 'dts'
                              self._cik.ids,  # 'iid'
                              ','.join(self._factor_name_list),  # 'origin_factor_names'
                              ','.join(map(lambda x: str(x), self._alias)) if self._alias is not None else None,
                              # 'alias'
                              '',  # sql
                              self._obj_type,  # via # db_table
                              self._src  # info
                              )

    @staticmethod
    def get_sql_create(cik_dt_list, cik_id_list, _factor_name_list, _alias, _cik, _db_table, _obj_type):
        f_names_list = [f if (a is None) or (f == a) else f"{f} as {a}" for f, a in
                        zip(_factor_name_list, _alias)]
        cols_str = ','.join(f_names_list)

        cik_id_cond = "'" + "' , '".join(cik_id_list) + "'"
        cik_dt_cond = "'" + "' , '".join(cik_dt_list) + "'"
        conditions = f"cik_dts in ({cik_dt_cond}) and cik_iid in ({cik_id_cond})"

        if _obj_type.startswith('SQL'):
            sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.iid}  as cik_iid  from ({_db_table}) where {conditions}"
            sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from ({_db_table})"
            sql_cik_iid = f"select distinct {_cik.iid} as cik_iid from ({_db_table})"
        else:
            sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.iid} as cik_iid  from {_db_table} where {conditions}"
            sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from {_db_table}"
            sql_cik_iid = f"select distinct {_cik.iid} as cik_iid from {_db_table}"

        return sql, sql_cik_dts, sql_cik_iid

    def get_cik_dts(self, **kwargs):
        # TODO 性能点
        func = getattr(self, 'get_sql_create')
        sql, sql_cik_dts, sql_cik_ids = func(None, None, self._factor_name_list, self._alias, self._cik,
                                             self._db_table, self._obj_type)

        return self._obj(sql_cik_dts).dt.strftime('%Y-%m-%d').unique().tolist()

    def get_cik_ids(self, **kwargs):
        # TODO 性能点
        func = getattr(self, 'get_sql_create')
        sql, sql_cik_dts, sql_cik_ids = func(None, None, self._factor_name_list, self._alias, self._cik,
                                             self._db_table, self._obj_type)
        return self._obj(sql_cik_ids).unique().tolist()

    def get(self, cik_dt_list, cik_id_list, **kwargs):
        func = getattr(self, 'get_sql_create')
        sql, sql_cik_dts, sql_cik_ids = func(cik_dt_list, cik_id_list, self._factor_name_list, self._alias, self._cik,
                                             self._db_table, self._obj_type)
        return self._obj(sql)


def get_mysql_sql_create(cik_dt_list, cik_id_list, _factor_name_list, _alias, _cik, _db_table, _obj_type):
    """

    :param cik_dt_list:
    :param cik_id_list:
    :param _factor_name_list:
    :param _alias:
    :param _cik:
    :param _db_table:
    :param _obj_type:
    :return:
    """
    f_names_list = [f if (a is None) or (f == a) else f"{f} as {a}" for f, a in
                    zip(_factor_name_list, _alias)]
    cols_str = ','.join(f_names_list)

    cik_id_cond = "'" + "' , '".join(cik_id_list) + "'"
    cik_dt_cond = "'" + "' , '".join(cik_dt_list) + "'"
    conditions = f"cik_dts in ({cik_dt_cond}) and cik_iid in ({cik_id_cond})"
    if _obj_type.startswith('SQL'):
        sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.iid}  as cik_iid  from ({_db_table}) where {conditions}"
        sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from ({_db_table})"
        sql_cik_iid = f"select distinct {_cik.iid} as cik_iid from ({_db_table})"
    else:
        sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.iid}  as cik_iid  from {_db_table} where {conditions}"
        sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from {_db_table}"
        sql_cik_iid = f"select distinct {_cik.iid} as cik_iid from {_db_table}"
    return sql, sql_cik_dts, sql_cik_iid


def get_clickhouse_sql_create(cik_dt_list, cik_id_list, _factor_name_list, _alias, _cik, _db_table, _obj_type):
    f_names_list = [f if (a is None) or (f == a) else f"{f} as {a}" for f, a in
                    zip(_factor_name_list, _alias)]
    cols_str = ','.join(f_names_list)

    cik_id_cond = "'" + "' , '".join(cik_id_list) + "'"
    cik_dt_cond = "'" + "' , '".join(cik_dt_list) + "'"
    conditions = f"cik_dts in ({cik_dt_cond}) and cik_iid in ({cik_id_cond})"

    if _obj_type.startswith('SQL'):
        sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.iid}  as cik_iid  from ({_db_table}) where {conditions}"
        sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from ({_db_table})"
        sql_cik_iid = f"select distinct {_cik.iid} as cik_iid from ({_db_table})"
    else:
        sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.iid}  as cik_iid  from {_db_table} where {conditions}"
        sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from {_db_table}"
        sql_cik_iid = f"select distinct {_cik.iid} as cik_iid from {_db_table}"

    return sql, sql_cik_dts, sql_cik_iid


__FactorSQLClickHouse__ = type('__FactorSQLClickHouse__', (__FactorSQL__,),
                               {'get_sql_create': staticmethod(lru_cache(get_clickhouse_sql_create))})

__FactorSQLMySQL__ = type('__FactorSQLMySQL__', (__FactorSQL__,),
                          {'get_sql_create': staticmethod(lru_cache(get_mysql_sql_create))})


class FactorCreator(type):
    @staticmethod
    def load_from_element(element):
        if isinstance(element, FactorElement):
            return FactorCreator(element.name, element.obj, element.cik_dt, element.cik_id, element.factor_names,
                                 as_alias=element.as_alias, db_table=element.db_table, **element.kwargs)
        else:
            raise ValueError('element must be FactorElement class!')

    def __instancecheck__(cls, instance):
        return isinstance(instance, Factor)

    def __new__(cls, name, obj, cik_dt: str, cik_id: str, factor_names: (list, tuple, str),
                as_alias: (list, tuple, str) = None, db_table: str = None, **kwargs):
        """

        :param name:  db_table_sql, df name or  h5_key
        :param obj:  query,df or h5_path
        :param cik_dt:  cik_dt
        :param cik_id:  cik_id
        :param factor_names:  factor names
        :param args:
        :param as_alias:    factor alias name
        :param db_table:  db_table
        :param kwargs:
        """
        if isinstance(obj, pd.DataFrame):
            _obj = __FactorDF__(name, obj, cik_dt, cik_id, factor_names,
                                as_alias=as_alias, db_table=db_table, **kwargs)
        elif isinstance(obj, str) and os.path.isfile(obj) and obj.lower().endswith(
                '.h5'):  # obj is path and end with .h5
            _obj = __FactorH5__(name, obj, cik_dt, cik_id, factor_names,
                                as_alias=as_alias, db_table=db_table, **kwargs)
        elif isinstance(obj, Callable):  # obj is executable query function

            db_type = kwargs.get('db_type', None)
            if db_type is None:
                raise ValueError('FactorSQL must have db_type to claim database type!')
            elif db_type == 'ClickHouse':
                _obj = __FactorSQLClickHouse__(name, obj, cik_dt, cik_id, factor_names,
                                               as_alias=as_alias, db_table=db_table, **kwargs)
            else:
                raise ValueError(f'unknown db_type:{db_type}')
        else:
            raise ValueError('unknown info')
        _obj.__class__.__name__ = 'Factor'
        # _obj.__class__.__mro__ = FactorCreator.__mro__

        return _obj
        # return _obj


if __name__ == '__main__':
    import numpy as np

    df = pd.DataFrame(np.random.random(size=(1000, 4)), columns=['cik_dts', 'cik_iid', 'v1', 'v2'])
    f1 = FactorCreator('test', df, 'cik_dts', 'cik_iid', factor_names=['v1', 'v2'])
    # FactorCreator.load_from_element(f1.element)
    # print(isinstance(f1, Factor))
    # print(isinstance(f1, FactorCreator))
    # print(f1.__class__.__mro__)
    # print(FactorCreator.__class__.__mro__)
    pass
