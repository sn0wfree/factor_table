# coding=utf-8
import warnings
from typing import Union

import pandas as pd

from factor_table.conf.logger import Logger
from factor_table.helper.FactorTools import CoreIndexKeys, FactorInfo
from factor_table.helper.transform_dt_format import transform_dt_format  # , DATETIME_FORMAT
from factor_table.conf.config import Configs

CONF = Configs(raise_error='no_raise')
DATETIME_FORMAT = CONF.DATETIME_FORMAT
DEFAULT_CIK_DT = CONF.DEFAULT_CIK_DT #'cik_dt'
DEFAULT_CIK_ID = CONF.DEFAULT_CIK_ID #'cik_id'

class FactorNameTools(object):
    @staticmethod
    def generate_factor_names(factor_names: Union[list, tuple, str]):
        """
        convert factor_names to list of factor names, only accept list, tuple, str

        :param factor_names: list of factor names
        :return: list of factor names
        """
        if isinstance(factor_names, str):
            factor_names = factor_names.split(',') if ',' in factor_names else [factor_names]
        elif isinstance(factor_names, (list, tuple)):
            factor_names = list(factor_names)
        else:
            raise ValueError('columns only accept list,tuple or str!')
        return factor_names

    @staticmethod
    def generate_alias(factor_names: list, as_alias: Union[list, tuple, str] = None):
        """
        convert alias to list of alias, only accept list, tuple, str

        :param factor_names: list of factor names
        :param as_alias: alias of factor names
        :return: list of alias
        """
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


class Factor(object):
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_f_id', '_kwargs',
                 '_db_type', '_src']

    def __init__(self, f_id, obj_or_df, cik_dt_col: str, cik_id_col: str, factor_names: Union[list, tuple, str],
                 as_alias: Union[list, tuple, str] = None, **kwargs):
        self._f_id = f_id  # 存储id名称或者sql
        self._obj = obj_or_df  # 存储对象或者dataframe
        self._obj_type = 'Meta'  # 识别关键factor类别
        self._cik = CoreIndexKeys(cik_dt_col, cik_id_col)  # 存储关键核心列（时间列名，id列表）
        self._factor_name_list = FactorNameTools.generate_factor_names(factor_names)  # 因子变量列表
        self._alias = FactorNameTools.generate_alias(self._factor_name_list, as_alias)  # 因子变量别名
        self._kwargs = kwargs  # 存储关键词参数
        self._db_type: str = kwargs.get('db_type', 'DataFrame')
        self._src = None

    @staticmethod
    def _create_renamed_columns_dict(factor_name_list, alias):
        """

        :param factor_name_list:
        :param alias:
        :return:
        """

        return dict(filter(lambda x: x[0] is None, zip(factor_name_list, alias)))

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
        raise NotImplementedError('get func should be rewritten!')

    @staticmethod
    def _get_exists_columns(target_cols, factor_name_list, data_cols):
        """

        :param target_cols: columns
        :param factor_name_list:  因子名称列表
        :param data_cols:  存储的数据列表
        :return:
        """

        for f in factor_name_list:
            norm_status, lower_status, upper_status = (f in data_cols, f.lower() in data_cols, f.upper() in data_cols)
            if norm_status:
                target_cols.append(f)
            elif lower_status:
                target_cols.append(f.lower())
            elif upper_status:
                target_cols.append(f.upper())
            else:
                raise ValueError(f'{f} or {f.lower()} or {f.upper()} not found!')
        return target_cols

    @Logger.deco(level='info', timer=True, extra_name='Factor')
    def _df_get_(self, df: pd.DataFrame, cik_dt_list: list, cik_id_list: list):
        # print(df)

        data_cols = df.columns.tolist()
        dt_col = DEFAULT_CIK_DT if DEFAULT_CIK_DT in data_cols else self._cik.dts
        id_col = DEFAULT_CIK_ID if DEFAULT_CIK_ID in data_cols else self._cik.ids

        df[dt_col] = transform_dt_format(df, col=dt_col)
        dt_mask = df[dt_col].isin(cik_dt_list) if cik_dt_list is not None else True
        id_mask = df[id_col].isin(cik_id_list) if cik_id_list is not None else True

        # check factor_name_list

        cols = self._get_exists_columns([dt_col, id_col], self._factor_name_list, data_cols)
        mask = dt_mask & id_mask
        if isinstance(mask, bool):
            data = df[cols].rename(
                columns={dt_col: DEFAULT_CIK_DT, id_col: DEFAULT_CIK_ID,
                         **self._create_renamed_columns_dict(self._factor_name_list, self._alias)})
        else:
            data = df[mask][cols].rename(
                columns={dt_col: DEFAULT_CIK_DT, id_col: DEFAULT_CIK_ID,
                         **self._create_renamed_columns_dict(self._factor_name_list, self._alias)})

        return data

    def update(self, *args, **kwargs):
        raise NotImplementedError('only FactorSQL can update!')

    def __str__(self):
        return str(self.info())

    # @property
    def info(self):
        # ['f_id_or_sql', 'cik_dt_col', 'cik_id_col', 'factor_names', 'alias', 'obj_type', 'db_type', 'src']

        # f_id_or_sql, cik_dt,cik_id,factor_names,alias,obj_type,db_type,src
        return FactorInfo(self._f_id,  # f_id_or_sql
                          self._cik.dts,  # cik_dt
                          self._cik.ids,  # cik_id
                          ','.join(self._factor_name_list),  # factor_names
                          ','.join(map(lambda x: str(x), self._alias)) if self._alias is not None else None,  # alias
                          self._obj_type,  # obj_type
                          self._db_type,  # db_type
                          self._src)  # src

        pass


class FactorSQL(Factor):  # only store a cross section data
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', '_src',
                 'db_type', ]

    def __init__(self, db_table_sql: str, query: object, cik_dt: str, cik_id: str,
                 factor_names: Union[list, tuple, str],
                 as_alias: Union[list, tuple, str] = None, **kwargs):
        super(FactorSQL, self).__init__(db_table_sql, query, cik_dt, cik_id, factor_names,
                                        as_alias=as_alias, **kwargs)
        self._f_id: str = db_table_sql
        self._obj = query
        self._obj_type: str = f'SQL_{self.db_type}' if db_table_sql.lower().startswith(
            'select ') else f'db_table_{self.db_type}'
        self.db_type: str = kwargs.get('db_type', 'SQL')
        # 识别关键factor类别
        if hasattr(query, 'src'):
            self._src = query.src
        else:
            self._src = None
            warnings.warn('query dont have src property! will set as random src! please check src!')

    @staticmethod
    def _build_cik_id_where_clause(cik_id_list: (list, tuple), _cik, max_in_num=100):
        if cik_id_list is not None:
            if len(cik_id_list) == 1:
                cik_id_cond = f"{_cik.ids}= '" + "' , '".join(cik_id_list) + "'"
                # conditions = f"{cik_dt_cond} and {cik_id_cond}"
            elif len(cik_id_list) <= max(max_in_num, 2):
                cik_id_cond = f"{_cik.ids} in ( '" + "' , '".join(cik_id_list) + "')"
                # conditions = f"{cik_dt_cond} and {cik_id_cond}"
            else:
                warnings.warn(
                    f'cik_id_list owns {len(cik_id_list)} length! It is too long to build SQL, will reduce to None')
                cik_id_cond = None
                # conditions = f"{cik_dt_cond} "
        else:
            cik_id_cond = None
        return cik_id_cond

    @staticmethod
    def _build_cik_dt_where_clause(cik_dt_list: (list, tuple, None), _cik, max_in_num=100):
        if isinstance(cik_dt_list, list):  # 一定有cik_dt_list
            if len(cik_dt_list) == 1:  # 一定有cik_dt_list
                cik_dt_cond = f"{_cik.dts} = '" + "' , '".join(cik_dt_list) + "'"
            elif len(cik_dt_list) <= max(max_in_num, 2):
                cik_dt_cond = f"{_cik.dts} in ( '" + "' , '".join(cik_dt_list) + "')"
            else:
                cik_dt_cond = f"{_cik.dts} >= '{min(cik_dt_list)}' and  {_cik.dts} <= '{max(cik_dt_list)} "
        elif cik_dt_list is None:
            cik_dt_cond = None
        else:
            raise ValueError('cik_dt_list should be defined as list only first!')
        return cik_dt_cond

    @staticmethod
    def _build_f_names_list(factor_name_list, alias):
        f_names_list = [f if (a is None) or (f == a) else f"{f} as {a}" for f, a in zip(factor_name_list, alias)]
        cols_str = ','.join(f_names_list)
        return f_names_list, cols_str

    @staticmethod
    def _build_cond_clause(cik_id_cond: str, cik_dt_cond: str, other_conds: str = None):
        if cik_id_cond is None:
            if cik_dt_cond is not None:
                conditions = f"{cik_dt_cond}"
            else:
                conditions = None
        else:
            if cik_dt_cond is None:
                conditions = f"{cik_id_cond}"
            else:
                conditions = f"{cik_dt_cond} and {cik_id_cond}"

        if conditions is None:
            if other_conds is not None:
                return f'where {other_conds}'
            else:
                return None
        elif conditions is not None:
            if other_conds is not None:
                return f'where {conditions} and {other_conds}'
            else:
                return f'where {conditions}'
        else:
            raise ValueError(f'got wrong condition: {conditions}')

    @classmethod
    def get_sql_create(cls, cik_dt_list, cik_id_list, _factor_name_list, _alias, _cik, _db_table, _obj_type,
                       max_in_num=100, other_conds=None, **kwargs):
        # f_names_list = [f if (a is None) or (f == a) else f"{f} as {a}" for f, a in zip(_factor_name_list, _alias)]
        # cols_str = ','.join(f_names_list)
        f_names_list, cols_str = cls._build_f_names_list(_factor_name_list, _alias)
        # create cik_dt condition
        cik_dt_cond = cls._build_cik_dt_where_clause(cik_dt_list, _cik, max_in_num=max_in_num)
        # create cik_id condition
        cik_id_cond = cls._build_cik_id_where_clause(cik_id_list, _cik, max_in_num=max_in_num)
        conditions = cls._build_cond_clause(cik_id_cond, cik_dt_cond, other_conds=other_conds)
        # conditions = f"cik_dts in ({cik_dt_cond}) "

        if _obj_type.startswith('SQL'):

            sql_without_cond = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids}  as cik_ids  from ({_db_table}) "

        else:
            sql_without_cond = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids} as cik_ids  from {_db_table} "
        sql = sql_without_cond + conditions if conditions is not None else sql_without_cond
        sql_cik_dts = cls._build_cik_dts_sql(_db_table, _cik)
        sql_cik_ids = cls._build_cik_ids_sql(_db_table, _cik)
        return sql, sql_cik_dts, sql_cik_ids

    @staticmethod
    def _build_cik_dts_sql(_db_table, _cik):
        sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from ({_db_table})"
        return sql_cik_dts

    @staticmethod
    def _build_cik_ids_sql(_db_table, _cik):
        sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from {_db_table}"
        return sql_cik_ids

    # @classmethod
    # def _update_sql_mode(cls, dt, _factor_name_list, _alias, _cik, _db_table, _obj_type):
    #     # f_names_list = [f if (a is None) or (f == a) else f"{f} as {a}" for f, a in
    #     #                 zip(_factor_name_list, _alias)]
    #     # cols_str = ','.join(f_names_list)
    #     f_names_list, cols_str = cls._build_f_names_list(_factor_name_list, _alias)
    #     if dt is not None:
    #         conditions = f"cik_dts > {dt} "
    #     else:
    #         conditions = '1'
    #     if _obj_type.startswith('SQL'):
    #         sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids}  as cik_ids  from ({_db_table}) where {conditions}"
    #         # sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from ({_db_table})"
    #         # sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from ({_db_table})"
    #     else:
    #         sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids} as cik_ids  from {_db_table} where {conditions}"
    #         # sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from {_db_table}"
    #         # sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from {_db_table}"
    #
    #     return sql

    def get_cik_dts(self, force=False, **kwargs):

        print(f'get cik_dts from db!')
        # dt_data = self._obj[self._cik.dts]
        # TODO 性能点
        # sql_func = getattr(self, 'get_sql_create')
        query_func = self._obj
        # sql = sql_func('', '', self._factor_name_list, self._alias, self._cik,
        #            self._db_table, self._obj_type)
        sql_cik_dts = self._build_cik_dt_where_clause(self._db_table, self._cik)
        dt_data = query_func(sql_cik_dts)

        return transform_dt_format(dt_data, col='cik_dts').dt.strftime(DATETIME_FORMAT).unique().tolist()

    def get_cik_ids(self, force=False, **kwargs):

        print('get cik_ids from db!')
        # dt_data = self._obj[self._cik.dts]
        # TODO 性能点
        # func = getattr(self, 'get_sql_create')
        query_func = self._obj
        sql_cik_ids = self._build_cik_id_where_clause(self._db_table, self._cik)
        # sql, sql_cik_dts, sql_cik_ids = func('', '', self._factor_name_list, self._alias, self._cik,
        #                                      self._db_table, self._obj_type)
        return query_func(sql_cik_ids)['cik_ids'].unique().tolist()

    def get(self, cik_dt_list, cik_id_list, force=False, other_conds=None, **kwargs):

        sql_func = getattr(self, 'get_sql_create')
        query_func = self._obj
        sql = sql_func(cik_dt_list, cik_id_list, self._factor_name_list, self._alias,
                       self._cik,
                       self._db_table, self._obj_type, other_conds=other_conds, **kwargs)
        # print(sql)

        data = query_func(sql)
        # print(data)

        return self._df_get_(data, cik_dt_list, cik_id_list)

    # def update(self, cik_dt_list=None, callback=None, force=False, **kwargs):
    #     if self._cache is None and not force:
    #         raise BufferError('cache have not been loaded! please fetch first!')
    #
    #     if self._cache is None:
    #         if force:
    #             max_cik_dts = None
    #         else:
    #             raise BufferError('cache have not been loaded! please fetch first!')
    #     else:
    #         max_cik_dts = self._cache['cik_dts'].max()
    #
    #     sql = self._update_sql_mode(max_cik_dts, self._factor_name_list, self._alias, self._cik,
    #                                 self._db_table, self._obj_type)
    #     try:
    #         data = self._obj(sql)
    #     except Exception as e:
    #         if callback is not None and isinstance(callback, Callable):
    #             data = callback(sql)
    #         else:
    #             raise e
    #     self._cache_func(data)
    # return self._df_get_(data, cik_dt_list, cik_id_list)


if __name__ == '__main__':
    # FactorCreator.load_from_element(f1.element)
    # print(isinstance(f1, Factor))
    # print(isinstance(f1, FactorCreator))
    # print(f1.__class__.__mro__)
    # print(FactorCreator.__class__.__mro__)
    pass
