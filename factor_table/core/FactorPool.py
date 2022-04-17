# coding=utf-8
from collections import deque
from functools import wraps

import pandas as pd

from factor_table.core.FactorCreator import FactorCreator
from factor_table.helper.FactorTools import CoreIndexKeys, FactorInfo
from factor_table.core.Factors import Factor
# from factor_table.helper import FactorElement
from factor_table.helper.FactorTools import SaveTools
from factor_table.utils.process_bar import process_bar

# db_table, dts, iid, via, info
COLS = ['f_id_or_sql', 'cik_dt_col', 'cik_id_col'] + ['obj_type', 'db_type', 'src']
FACTOR_NAME_COLS = 'factor_names'
ALIAS_COLS = 'alias'

# f_id_or_sql: str
# cik_dt_col: str
# cik_id_col: str
# factor_names: str
# alias: str
# obj_type: str
# db_type: str
# src: str

"""
factor_pool 主要是装载factor的容器，没有其他功能
"""


def check_iterable(func):
    @wraps(func)
    def _deco(*args, **kwargs):
        if not hasattr(args[0], '__iter__'):
            raise TypeError('factor_pool must be iterable!')
        return func(*args, **kwargs)

    return _deco


class FactorPool(deque):
    __slots__ = ('_cik_id_data', '_cik_dt_data')

    @property
    def _obj_type(self):
        f_info = self.show_factors()
        return f_info['obj_type']

    @property
    def available_cik_dts(self):
        h = []
        for f in self:
            temp = f.get_cik_dts()
            h.extend(temp)
        return sorted(set(h))

    @property
    def available_cik_ids(self):
        h = []
        for f in self:
            h.extend(f.get_cik_ids())
        return sorted(set(h))

    def __init__(self, maxlen=None):
        super(FactorPool, self).__init__(maxlen=maxlen)
        self._cik_id_data = None  # 存储cik_id data
        self._cik_dt_data = None  # 存储cik_dt data

    def add_factor(self, *args, **kwargs):
        factor_ori = args[0]
        # todo check factor type
        if isinstance(factor_ori, Factor):
            self.append(factor_ori)
        # elif isinstance(factor_ori, FactorElement):
        #     factor = FactorCreator.load_from_element(factor_ori)
        #     # name, obj, cik_dt: str, cik_id: str, factor_names: (list, tuple, str),
        #     #                 as_alias: (list, tuple, str) = None, db_table: str = None, **kwargs
        #     self.append(factor)
        else:
            factor = FactorCreator(*args, **kwargs)
            self.append(factor)

    @staticmethod
    @check_iterable
    def _style_factor_split(factor_pool, _obj_type: str):
        return filter(lambda x: x._obj_type == _obj_type, factor_pool)

    @classmethod
    def merge_df_factor(cls, factor_pool):
        return cls._style_factor_split(factor_pool, 'DF')

    @classmethod
    def merge_h5_factor(cls, factor_pool):
        return cls._style_factor_split(factor_pool, 'H5')

    @staticmethod
    def merge_sql_factor(factor_pool):
        if not hasattr(factor_pool, '__iter__'):
            raise TypeError('factor_pool must be iterable!')

        factors = list(filter(lambda x: x._obj_type.startswith(('SQL_', 'db_table')), factor_pool))

        # f_id_or_sql: str
        # cik_dt_col: str
        # cik_id_col: str
        # factor_names: str
        # alias: str
        # obj_type: str
        # db_type: str
        # src: str

        factors_info = pd.DataFrame(list(map(lambda x: x.info(), factors)), columns=FactorInfo.__slots__)
        for (f_id_or_sql, cik_dt_col, cik_id_col, factor_names, alias, obj_type, db_type,
             src), df in factors_info.groupby(COLS):  # merge same source factors
            obj = factors[df.index[0]]._obj
            # merge same source data
            factor_name_and_alias = df[[FACTOR_NAME_COLS, ALIAS_COLS]].apply(lambda x: ','.join(x))
            origin_factor_names = factor_name_and_alias[FACTOR_NAME_COLS].split(',')
            alias = factor_name_and_alias[ALIAS_COLS].split(',')
            # use set will disrupt the order
            # we need keep the origin order
            back = list(zip(origin_factor_names, alias))
            disrupted = list(set(back))
            disrupted.sort(key=back.index)

            origin_factor_names_new, alias_new = zip(*disrupted)
            alias_new = list(map(lambda x: x if x != 'None' else None, alias_new))

            # add_factor process have checked
            # db_table_sql: str, query: object, cik_dt: str, cik_id: str, factor_names: str, *args,
            #                  as_alias: (list, tuple, str) = None
            # name, obj, cik_dt: str, cik_id: str, factor_names: (list, tuple, str), *args,
            # as_alias: (list, tuple, str) = None, db_table: str = None, ** kwargs
            # name, obj, cik_dt: str, cik_id: str, factor_names: (list, tuple, str),
            # as_alias: (list, tuple, str) = None, db_table: str = None
            res = FactorCreator(f_id_or_sql, obj, cik_dt_col, cik_id_col, origin_factor_names_new,
                                as_alias=alias_new)
            yield res

    def show_factors(self, to_df=True, **kwargs):
        factor_info_iter = map(lambda x: x.info(), self)
        return pd.DataFrame(list(factor_info_iter)) if to_df else list(factor_info_iter)

    def merge_factors(self, reduce=True):
        # todo unstack factor name to check whether factor exists duplicates!!
        # factors = FactorPool()
        if reduce:
            for factor in self.merge_df_factor(self):
                yield factor
            for factor in self.merge_h5_factor(self):
                yield factor
            for factor in self.merge_sql_factor(self):
                yield factor
        else:
            for factor in self:
                yield factor

    # def _fetch_iter_with_element(self, force=False) -> object:
    #     """
    #
    #     :param _cik_dts:
    #     :param _cik_ids:
    #     :return:
    #     """
    #     fetched = ((f.element, f.get(self._cik_dts, self._cik_ids, skip_cache=force).set_index(['cik_dts', 'cik_ids']))
    #                for f in self)
    #     return fetched
    @staticmethod
    def check_cik_data(cik, self_cik, check_name: str = 'cik_dts'):
        """

        :param cik:
        :param self_cik:
        :param check_name:
        :return:
        """
        if cik is None:
            if self_cik is None:
                raise KeyError(f'{check_name}(either default approach or fetch) both are not setup!')
            else:
                cik_data = self_cik
        else:
            cik_data = cik
        return cik_data

    def fetch_iter(self, _cik_dt_data=None, _cik_id_data=None, reduced=False, add_limit=False, show_process=False,
                   skip_cache=False) -> object:
        """

        :param show_process:
        :param reduced: whether use reduce form
        :param _cik_dts: set up dts
        :param _cik_ids:  set up ids
        :param add_limit: use force limit columns
        :return:
        """

        cik_dt_data = self.check_cik_data(_cik_dt_data, self.available_cik_dts, check_name='cik_dt')
        cik_id_data = self.check_cik_data(_cik_id_data, self.available_cik_ids, check_name='cik_id')

        factors = self.merge_factors(reduced=reduced) if reduced else self

        if show_process:
            fetched = (f.get(cik_dt_data, cik_id_data, skip_cache=skip_cache).set_index(['cik_dt', 'cik_id']) for
                       f in
                       process_bar(factors))
        else:
            fetched = (f.get(cik_dt_data, cik_id_data, skip_cache=skip_cache).set_index(['cik_dt', 'cik_id']) for
                       f in
                       factors)
        return fetched

    def fetch(self, _cik_dts=None, _cik_ids=None, merge=True, reduced=False, add_limit=False, show_process=False,
              skip_cache=False):
        """

        :param merge:
        :param show_process:
        :param reduced: whether use reduce form
        :param _cik_dts: set up dts
        :param _cik_ids:  set up ids
        :param add_limit: use force limit columns
        :return:
        """

        fetched = self.fetch_iter(_cik_dts=_cik_dts, _cik_ids=_cik_ids, reduced=reduced, add_limit=add_limit,
                                  show_process=show_process, skip_cache=skip_cache)

        return pd.concat(fetched, axis=1) if merge else list(fetched)

    @staticmethod
    def _filter_no_duplicated_fetched(fetched, old_f_ind, name, cik_cols=['cik_dt', 'cik_id']):
        """

        :param fetched:
        :param old_f_ind:
        :param name:
        :param cik_cols:
        :return:
        """
        SaveTools._check_stored_var_consistent(old_f_ind, name)
        # dts
        cik_dts_list = old_f_ind[cik_cols[0]].values.tolist()
        dt_mask = ~fetched[cik_cols[0]].isin(cik_dts_list)
        # ids
        cik_ids_list = old_f_ind[cik_cols[1]].values.tolist()
        id_mask = ~fetched[cik_cols[1]].isin(cik_ids_list)
        return fetched[dt_mask & id_mask]

    # @staticmethod
    # def _build_key(name, f_ind, cik_cols):
    #     # f_ind_df = f_ind.copy(deep=True)
    #     cik_dts = sorted(f_ind[cik_cols[0]].values.tolist())  # record dts
    #
    #     key = SaveTools._create_key_name(name, cik_dts, None)
    #     return key

    def _update_obj(self, obj_dict: dict):  # 重构连接器
        for f in self:
            if f._name in obj_dict.keys():
                f.update_element_obj(obj_dict[f._name])

    # def optimize(self, store_path, cik_cols=['cik_dts', 'cik_ids']):
    #     """
    #     optimize data
    #     :param store_path:
    #     :param cik_cols:
    #     :return:
    #     """
    #     raise NotImplementedError('optimize')
    #     pass


if __name__ == '__main__':
    f_ = FactorPool()
    import numpy as np

    np.random.seed(1)
    np.random.seed(1)
    dts = pd.date_range('2011-01-01', '2022-12-31', )[:1000]

    df = pd.DataFrame(np.random.random(size=(1000, 3)), columns=['cik_id', 'v1', 'v2'])
    df['cik_dt'] = dts
    df2 = pd.DataFrame(np.random.random(size=(1000, 3)), columns=['cik_id', 'v3', 'v4'])
    df2['cik_dt'] = dts
    df3 = pd.DataFrame(np.random.random(size=(1000, 3)), columns=['cik_id', 'v5', 'v6'])
    df3['cik_dt'] = dts
    f1 = FactorCreator('test', df, 'cik_dt', 'cik_id', factor_names=['v1', 'v2'])
    f2 = FactorCreator('test2', df2, 'cik_dt', 'cik_id', factor_names=['v3', 'v4'])

    f_.add_factor('test', df, 'cik_dt', 'cik_id', factor_names=['v1', 'v2'])
    print(1, f_.show_factors())
    f_.add_factor(f2)
    # f_.add_factor('test3', df3, 'cik_dt', 'cik_id', factor_names=['v5', 'v6'])

    print(2, f_.show_factors())
    print(f_.available_cik_dts)

    # f2 = FactorUnit('test2', df, 'cik_dts', 'cik_iid', factor_names=['v3'])
    # from ClickSQL import BaseSingleFactorTableNode
    #
    # src = 'clickhouse://default:Imsn0wfree@47.104.186.157:8123/system'
    # node = BaseSingleFactorTableNode(src)
    #
    # # f3 = FactorUnit('test.test2', query, 'cik_dts', 'cik_iid', factor_names=['v3'])
    #
    # f2 = FactorPool()
    #
    # f2.add_factor('EDGAR_LOG.parsed_edgar_log', node, 'date', 'ip', 'size,crawler')
    # f2.add_factor('EDGAR_LOG.parsed_edgar_log', node, 'date', 'ip', 'code')
    # f2.add_factor('select * from EDGAR_LOG.parsed_edgar_log', node, 'date', 'ip', 'idx,cik,noagent')
    # f2_data = f2.merge_factors().fetch(_cik_dts=['20170430'], _cik_iids=['104.155.127.aha'])
    # config = {}
    # config['name'] = name  # str
    # config['obj'] = obj  # object
    # config['cik_dt'] = cik_dt  # str
    # config['cik_id'] = cik_id  # str
    #
    # config['factor_names'] = factor_names  # (list, tuple, str)
    # config['as_alias'] = as_alias  # (list, tuple, str)
    # config['db_table'] = db_table  # str

    pass
