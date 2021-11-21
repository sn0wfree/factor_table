# coding=utf-8

from typing import Union
import warnings
from collections import Callable
# from numpy import np

import pandas as pd

# from factor_table.factor_engine import Factor
from factor_table.helper import CoreIndexKeys, FactorElement, FactorInfo
from factor_table.helper.transform_dt_format import transform_dt_format


class Factor(object):
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', 'element',
                 '_cache']

    def __init__(self, name, obj, cik_dt: str, cik_id: str, factor_names: Union[list, tuple, str],
                 as_alias: Union[list, tuple, str] = None, db_table: str = None, **kwargs):

        self._obj = obj
        self._cik = CoreIndexKeys(cik_dt, cik_id)
        self._factor_name_list = self.generate_factor_names(factor_names)
        self._alias = self.generate_alias(self._factor_name_list, as_alias)

        self.element = FactorElement(name, obj, cik_dt, cik_id, self._factor_name_list,
                                     as_alias=self._alias, db_table=db_table, kwargs=kwargs)

        self._cache = None

        self._db_table = db_table
        self._name = name
        self._kwargs = kwargs
        self._obj_type = 'Meta'

    def _cache_func(self, data: pd.DataFrame):
        if self._cache is None:
            self._cache = data
        else:
            self._cache = pd.concat([self._cache, data])

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
    def generate_factor_names(factor_names: Union[list, tuple, str]):
        if isinstance(factor_names, str):
            factor_names = factor_names.split(',') if ',' in factor_names else [factor_names]
        elif isinstance(factor_names, (list, tuple)):
            factor_names = list(factor_names)
        else:
            raise ValueError('columns only accept list tuple str!')
        return factor_names

    @staticmethod
    def generate_alias(factor_names: list, as_alias: Union[list, tuple, str] = None):
        if as_alias is None:
            alias = [None for _ in factor_names]
        elif isinstance(as_alias, str):
            alias = as_alias.split(',') if ',' in as_alias else [as_alias]
            if len(alias) != len(factor_names):
                raise ValueError('as_alias is not match factor_names')
            else:
                # replace None as factor_names
                alias = [factor_names[idx] if alia is None else alia for idx, alia in enumerate(alias)]
        elif isinstance(as_alias, (list, tuple)):
            if len(as_alias) != len(factor_names):
                raise ValueError('as_alias is not match factor_names')
            else:
                alias = [factor_names[idx] if alia is None else alia for idx, alia in enumerate(as_alias)]
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
        # return df[self._cik.dts].dt.strftime('%Y-%m-%d').unique().tolist()

    def get_cik_ids(self, df: pd.DataFrame):
        """

        :param df:
        :return:
        """
        raise NotImplementedError('get_cik_ids!')
        # return df[self._cik.ids].unique().tolist()

    def get(self, *args, **kwargs):
        raise NotImplementedError('get!')

    def _df_get_(self, df: pd.DataFrame, cik_dt_list: list, cik_id_list: list):
        # print(df)

        data_cols = df.columns.tolist()
        dt_col='cik_dts' if 'cik_dts' in data_cols else self._cik.dts    
        id_col='cik_ids' if 'cik_ids' in data_cols else self._cik.ids
        
        df[dt_col] = transform_dt_format(df, col=dt_col)
        dt_mask = df[dt_col].isin(cik_dt_list) if cik_dt_list is not None else True
        id_mask = df[id_col].isin(cik_id_list) if cik_id_list is not None else True
        mask = dt_mask & id_mask
               
        cols = [dt_col, id_col] # + self._factor_name_list
        # check factor_name_list
        for f in self._factor_name_list:
            status =( f in data_cols , f.lower() in data_cols , f.upper() in data_cols)
            if status ==(True, False, False):
                cols.append(f)
            elif status == (False, True, False):
                cols.append(f.lower())
            elif status == (False, False, True):
                cols.append(f.upper())
            else:
                raise ValueError(f'{f} or {f.lower()} or {f.upper()} not found!')


            

        if isinstance(mask, bool):
            data = df[cols].rename(
                columns={dt_col: 'cik_dts',id_col: 'cik_ids', **dict(self.__rename_col__())})
        else:
            data = df[mask][cols].rename(
                columns={dt_col: 'cik_dts', id_col: 'cik_ids', **dict(self.__rename_col__())})

        self._cache_func(data)
        # self._cache = data
        return data

    def update(self, *args, **kwargs):
        raise NotImplementedError('only FactorSQL can update!')


class __FactorSQL__(Factor):  # only store a cross section data
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', '_src',
                 'db_type', 'element', '_cache']

    def __init__(self, db_table_sql: str, query: object, cik_dt: str, cik_id: str, factor_names: Union[list, tuple, str], *args,
                 as_alias: Union[list, tuple, str] = None, **kwargs):
        super(__FactorSQL__, self).__init__(db_table_sql, query, cik_dt, cik_id, factor_names, *args,
                                            as_alias=as_alias, **kwargs)
        db_type: str = kwargs.get('db_type', 'SQL') 
        self.db_type = db_type
        self._db_table = db_table_sql
        self._obj_type = f'SQL_{db_type}' if db_table_sql.lower().startswith('select ') else f'db_table_{db_type}'

        self._src = query.src if hasattr(query, 'src') else None
        if self._src is None:
            warnings.warn('query dont have src property! will set as random src! please check src!')

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
        if len(cik_dt_list) <= 10 :
            if len(cik_dt_list) > 1:
                cik_dt_cond = f"{_cik.dts} in ( '" + "' , '".join(cik_dt_list) + "')"
            else:
                cik_dt_cond = f"{_cik.dts} = '" +   "' , '".join(cik_dt_list) +  "'"
        else:
            cik_dt_cond = f"{_cik.dts} > '{min(cik_dt_list)}'  "

        if cik_id_list is not None:
            if len(cik_id_list) <= 10:
                if len(cik_id_list) > 1:
                    cik_id_cond = f"{_cik.ids} in ( '" + "' , '".join(cik_id_list) + "')"
                else:
                    cik_id_cond = f"{_cik.ids}= '" + "' , '".join(cik_id_list) +  "'"
                conditions = f"{cik_dt_cond} and {cik_id_cond}"
            else:
                conditions = f"{cik_dt_cond} "
        else:
            cik_id_cond = None
            conditions = f"{cik_dt_cond} "
            # conditions = f"cik_dts in ({cik_dt_cond}) "

        if _obj_type.startswith('SQL'):
            sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids}  as cik_ids  from ({_db_table}) where {conditions}"
            sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from ({_db_table})"
            sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from ({_db_table})"
        else:
            sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids} as cik_ids  from {_db_table} where {conditions}"
            sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from {_db_table}"
            sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from {_db_table}"

        return sql, sql_cik_dts, sql_cik_ids

    @staticmethod
    def _update_sql_mode(dt, _factor_name_list, _alias, _cik, _db_table, _obj_type):
        f_names_list = [f if (a is None) or (f == a) else f"{f} as {a}" for f, a in
                        zip(_factor_name_list, _alias)]
        cols_str = ','.join(f_names_list)
        if dt is not None:
            conditions = f"cik_dts > {dt} "
        else:
            conditions = '1'
        if _obj_type.startswith('SQL'):
            sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids}  as cik_ids  from ({_db_table}) where {conditions}"
            # sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from ({_db_table})"
            # sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from ({_db_table})"
        else:
            sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids} as cik_ids  from {_db_table} where {conditions}"
            # sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from {_db_table}"
            # sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from {_db_table}"

        return sql

    def get_cik_dts(self, force=False, **kwargs):
        
        if self._cache is not None and not force:
            
            print(f'get cik_dts from cache!')
            
            dt_data = self._cache.reset_index()#['cik_dts']
        else:
            print(f'get cik_dts from db!')
            # dt_data = self._obj[self._cik.dts]
            # TODO 性能点
            func = getattr(self, 'get_sql_create')
            sql, sql_cik_dts, sql_cik_ids = func('', '', self._factor_name_list, self._alias, self._cik,
                                                 self._db_table, self._obj_type)

            dt_data = self._obj(sql_cik_dts)
        

        return transform_dt_format(dt_data, col='cik_dts').dt.strftime('%Y%m%d').unique().tolist()

    def get_cik_ids(self, force=False, **kwargs):
        
        if self._cache is not None and not force:
            print(f'get cik_ids from cache!')
            # dt_data = self._cache[self._cik.ids]
            return self._cache.reset_index()['cik_ids'].unique().tolist()
        else:
            print('get cik_ids from db!')
            # dt_data = self._obj[self._cik.dts]
            # TODO 性能点
            func = getattr(self, 'get_sql_create')
            sql, sql_cik_dts, sql_cik_ids = func('', '', self._factor_name_list, self._alias, self._cik,
                                                 self._db_table, self._obj_type)
            return self._obj(sql_cik_ids)['cik_ids'].unique().tolist()

    def get(self, cik_dt_list, cik_id_list, force=False, **kwargs):
        if self._cache is not None and not force:
            data = self._cache
        else:
            func = getattr(self, 'get_sql_create')
            sql, sql_cik_dts, sql_cik_ids = func(cik_dt_list, cik_id_list, self._factor_name_list, self._alias, self._cik,
                                                self._db_table, self._obj_type)
            # print(sql)

            data = self._obj(sql)
        # print(data)
            
        return self._df_get_(data, cik_dt_list, cik_id_list)

    def update(self, cik_dt_list=None, callback=None, force=False, **kwargs):
        if self._cache is None and not force:
            raise BufferError('cache have not been loaded! please fetch first!')

        if self._cache is None:
            if force:
                max_cik_dts = None
            else:
                raise BufferError('cache have not been loaded! please fetch first!')
        else:
            max_cik_dts = self._cache['cik_dts'].max()

        sql = self._update_sql_mode(max_cik_dts, self._factor_name_list, self._alias, self._cik,
                                    self._db_table, self._obj_type)
        try:
            data = self._obj(sql)
        except Exception as e:
            if callback is not None and isinstance(callback, Callable):
                data = callback(sql)
            else:
                raise e
        self._cache_func(data)
        # return self._df_get_(data, cik_dt_list, cik_id_list)


if __name__ == '__main__':
    pass
