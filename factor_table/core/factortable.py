# coding=utf-8


from functools import wraps
from typing import Union

import pandas as pd

from factor_table.core.FactorPool import FactorPool


class __MetaFactorTable__(object):
    @staticmethod
    def generate_alias(factor_names: Union[list,], as_alias: Union[list, tuple, str] = None):
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


        :param cik_iid:
        :param default_cik_iid:
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
    def __init__(self, **kwargs):
        self._checked = False
        # False if 'strict_cik' not in kwargs.keys() else kwargs['strict_cik']
        # self._strict_cik = self._extract_kwargs_info('strict_cik', False, kwargs, use_default=True)
        # self._store_path = self._extract_kwargs_info('store_path', None, kwargs, use_default=True)
        # auto_load = self._extract_kwargs_info('auto_load', False, kwargs, use_default=True)

        # 'cik_dt' if 'cik_dt' not in kwargs.keys() else kwargs['cik_dt']  # default use cik_dt as cik_dt
        # cik_dt, cik_id = self._extract_kwargs_info('cik_dt', 'cik_dt', kwargs,
        #                                            use_default=True), self._extract_kwargs_info('cik_id',
        #                                                                                         'cik_id',
        #                                                                                         kwargs,
        #                                                                                         use_default=True)
        # self._cik_cols = CIK(cik_dt, cik_id)

        # self._cik_dts = None
        # self._cik_ids = None
        # self._checked = True
        # self.__auto_check_cik__()  # check default dt and iid whether set up
        self._factors = FactorPool()
        # if auto_load:
        #     self.load(self._store_path)

    @wraps(FactorPool.show_factors)
    def show_factors(self, *args, **kwargs):
        return self._factors.show_factors(*args, **kwargs)

    @wraps(FactorPool.show_factors)
    def append(self, *args, **kwargs):
        self._factors.append(*args, **kwargs)

    @wraps(FactorPool.add_factor)
    def add_factor(self, *args, **kwargs):
        return self._factors.add_factor(*args, **kwargs)

    @wraps(FactorPool.save)
    def save(self, *args, **kwargs):
        return self._factors.save(*args, **kwargs)

    @wraps(FactorPool.load)
    def load(self, *args, **kwargs):
        return self._factors.load(*args, **kwargs)

    @property
    def cik_dts(self):
        h = []
        for f in self._factors:
            h.extend(f.get_cik_dts())
        return sorted(set(h))

    @property
    def cik_ids(self):
        h = []
        for f in self._factors:
            h.extend(f.get_cik_ids())
        return sorted(set(h))

    def fetch(self, _cik_dts, _cik_ids, reduced=True, add_limit=False, show_process=True, delay=False):
        """

        :param delay:
        :param show_process:
        :param reduced: whether use reduce form
        :param _cik_dts: set up dts
        :param _cik_ids:  set up ids
        :param add_limit: use force limit columns
        :return:
        """
        # if not add_limit:
        #     if _cik_dts is not None:
        #         self._cik_dts = _cik_dts
        #     else:
        #         if self._cik_dts is None:
        #             raise KeyError(
        #                 'cik_dts(either default approach or fetch) both are not setup!')
        #
        #     if _cik_ids is not None:
        #         self._cik_ids = _cik_ids
        #     else:
        #         if self._cik_ids is None:
        #             raise KeyError(
        #                 'cik_ids(either default approach or fetch) both are not setup!')
        # query_func = self._node
        fetched = self._factors.fetch_iter(_cik_dts=_cik_dts, _cik_ids=_cik_ids, reduced=reduced,
                                           add_limit=add_limit, show_process=show_process)
        if delay:
            return fetched
        else:
            result = pd.concat(fetched, axis=1)
            # columns = result.columns.tolist()
            return result


# class __FactorTableOld__(__MetaFactorTable__):
#     __Name__ = "基础因子库单因子表"

#     def __init__(self, *args, **kwargs):
#         # super(FatctorTable, self).__init__(*args, **kwargs)
#         # TODO _node 改成适配器模式，而不是写死的
#         self._node = BaseSingleQueryBaseNode(*args, **kwargs)

#         cik_dt = 'cik_dt' if 'cik_dt' not in kwargs.keys(
#         ) else kwargs['cik_dt']  # default use cik_dt as cik_dt
#         cik_iid = 'cik_iid' if 'cik_iid' not in kwargs.keys(
#         ) else kwargs['cik_iid']  # default use cik_iid as cik_iid
#         self._cik = CIK(cik_dt, cik_iid)
#         self._cik_data = None
#         self._checked = False
#         self.__auto_check_cik__()  # check default dt and iid whether set up
#         self._factors = _FactorPool()

#         self._strict_cik = False if 'strict_cik' not in kwargs.keys(
#         ) else kwargs['strict_cik']
#         # self.append = self.add_factor

#         self._cik_dts = None
#         self._cik_iids = None

#     def __auto_check_cik__(self):
#         if not self._checked and (self._cik.dts is None or self._cik.iid is None):
#             raise NotImplementedError('cik(dts or iid) is not setup!')
#         else:
#             self._checked = True

#     def _setup_cik(self, cik_dt_col: str, cik_iid_col: str):
#         """
#         设置 cik 列名
#         :param cik_dt_col:
#         :param cik_iid_col:
#         :return:
#         """

#         self._cik = CIK(cik_dt_col, cik_iid_col)

#     # def getDB(self, db):
#     #     self.db = db

#     def add_factor(self, db_table: str, factor_names: Union(list, tuple, str), cik_dt=None, cik_iid=None,
#                    cik_dt_format='datetime',

#                    as_alias: Union(list, tuple, str) = None):
#         conds = '1'  # not allow to set conds
#         cik_dt, cik_iid = self.check_cik_dt(cik_dt=cik_dt, default_cik_dt=self._cik.dts), self.check_cik_iid(
#             cik_iid=cik_iid, default_cik_iid=self._cik.iid)

#         res = self._factors.add_factor(db_table, factor_names, cik_dt=cik_dt, cik_iid=cik_iid,
#                                        cik_dt_format=cik_dt_format,
#                                        conds=conds, as_alias=as_alias)
#         if isinstance(res, tuple):
#             self._factors.append(res)
#         elif isinstance(res, list):
#             self._factors.extend(res)
#         else:
#             raise ValueError('res is not list or tuple')

#     def show_factors(self, reduced=False, to_df=True):
#         return self._factors.show_factors(reduced=reduced, to_df=to_df)

#     def __iter__(self):
#         query_func = self._node
#         return self._factors.fetch_iter(query_func, self.cik_dt, self.cik_iid, reduced=True,
#                                         add_limit=False)

#     def head(self, reduced=True, ):
#         """
#         quick look top data
#         :param reduced:
#         :return:
#         """

#         return self.fetch(reduced=reduced, add_limit=True)

#     def fetch(self, _cik_dts=None, _cik_iids=None, reduced=True, add_limit=False, ):
#         """

#         :param reduced: whether use reduce form
#         :param _cik_dts: set up dts
#         :param _cik_iids:  set up iids
#         :param add_limit: use force limit columns
#         :return:
#         """
#         if not add_limit:
#             if _cik_dts is not None:
#                 self._cik_dts = _cik_dts
#             else:
#                 if self._cik_dts is None:
#                     raise KeyError(
#                         'cik_dts(either default approach or fetch) both are not setup!')

#             if _cik_iids is not None:
#                 self._cik_iids = _cik_iids
#             else:
#                 if self._cik_iids is None:
#                     raise KeyError(
#                         'cik_iids(either default approach or fetch) both are not setup!')
#         query_func = self._node
#         fetched = self._factors.fetch_iter(query_func, self.cik_dt, self.cik_iid, reduced=reduced,
#                                            add_limit=add_limit)

#         result = pd.concat(fetched, axis=1)
#         # columns = result.columns.tolist()

#         return result

#     @property
#     def cik_dt(self):
#         dt_format = "%Y%m%d"
#         if self._cik_dts is None:
#             return "  1 "
#         else:
#             cik_dts_str = "','".join(map(lambda x: x.strftime(
#                 dt_format), pd.to_datetime(self._cik_dts)))
#             return f" toYYYYMMDD(cik_dt) in ('{cik_dts_str}') "

#     def set_cik_dt(self, cik_dt: list):
#         self._cik_dts = cik_dt

#     @property
#     def cik_iid(self):
#         if self._cik_iids is None:
#             return "  1 "
#         else:
#             cik_iid_str = "','".join(map(lambda x: x, self._cik_iids))
#             return f" cik_iid in ('{cik_iid_str}') "

#     def set_cik_iid(self, cik_iid: list):
#         self._cik_iids = cik_iid
#         pass

class FactorTable(__FactorTable__):
    pass


if __name__ == '__main__':
    pass
