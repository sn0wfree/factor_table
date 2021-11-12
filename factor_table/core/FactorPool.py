# coding=utf-8
from collections import deque

import pandas as pd

from factor_table.core.FactorTools import SaveTools
from factor_table.core.Factors import Factor as __Factor__, FactorCreator
from factor_table.helper import FactorInfo, FactorElement
from factor_table.utils.process_bar import process_bar

# db_table, dts, iid, via, info
COLS = ['db_table', 'dts', 'ids'] + ['via', 'info']
FACTOR_NAME_COLS = 'origin_factor_names'
ALIAS_COLS = 'alias'


class FactorPool(deque, SaveTools):
    def __init__(self, ):
        super(FactorPool, self).__init__()
        self._cik_ids = None
        self._cik_dts = None

    """
    @dataclass
class FactorElement(object):
    name: (str, object)
    obj: (object,)
    cik_dt: str
    cik_id: str
    factor_names: (list, tuple, str)
    as_alias: (list, tuple, str)
    db_table: str
    kwargs: dict
    """

    def add_factor(self, *args, **kwargs):
        factor_ori = args[0]
        # todo check factor type
        if isinstance(factor_ori, __Factor__):
            self.append(factor_ori)
        elif isinstance(factor_ori, FactorElement):
            factor = FactorCreator.load_from_element(factor_ori)
            # name, obj, cik_dt: str, cik_id: str, factor_names: (list, tuple, str),
            #                 as_alias: (list, tuple, str) = None, db_table: str = None, **kwargs
            self.append(factor)
        else:
            factor = FactorCreator(*args, **kwargs)
            self.append(factor)

    @staticmethod
    def merge_df_factor(factor_pool):
        if not hasattr(factor_pool, '__iter__'):
            raise TypeError('factor_pool must be iterable!')
        for df in filter(lambda x: x._obj_type == 'DF', factor_pool):
            yield df

    @staticmethod
    def merge_h5_factor(factor_pool):
        if not hasattr(factor_pool, '__iter__'):
            raise TypeError('factor_pool must be iterable!')
        for h5 in filter(lambda x: x._obj_type == 'H5', factor_pool):
            yield h5

    @staticmethod
    def merge_sql_factor(factor_pool):
        if not hasattr(factor_pool, '__iter__'):
            raise TypeError('factor_pool must be iterable!')

        factors = list(filter(lambda x: x._obj_type.startswith(('SQL_', 'db_table')), factor_pool))

        factors_info = pd.DataFrame(list(map(lambda x: x.factor_info(), factors)), columns=FactorInfo.__slots__)
        for (db_table, dts, iid, via, info), df in factors_info.groupby(COLS):  # merge same source factors
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
            res = FactorCreator(db_table, factors[df.index[0]]._obj, dts, iid, origin_factor_names_new, via,
                                as_alias=alias_new, db_table=db_table)
            yield res

    def show_factors(self, reduced=False, to_df=True):
        factor_info_iter = map(lambda x: x.factor_info(), self)
        if to_df:
            factors_info = pd.DataFrame(list(factor_info_iter))
        else:
            factors_info = list(factor_info_iter)
        return factors_info

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

    def _fetch_iter_with_element(self, _cik_dts, _cik_ids) -> object:

        fetched = ((f.element, f.get(self._cik_dts, self._cik_ids).set_index(['cik_dts', 'cik_ids'])) for f in self)
        return fetched

    def fetch_iter(self, _cik_dts=None, _cik_ids=None, reduced=False, add_limit=False, show_process=False):
        """

        :param show_process:
        :param reduced: whether use reduce form
        :param _cik_dts: set up dts
        :param _cik_ids:  set up ids
        :param add_limit: use force limit columns
        :return:
        """
        if not add_limit:
            if _cik_dts is not None:
                self._cik_dts = _cik_dts
            else:
                if self._cik_dts is None:
                    raise KeyError('cik_dts(either default approach or fetch) both are not setup!')

            if _cik_ids is not None:
                self._cik_ids = _cik_ids
            else:
                if self._cik_ids is None:
                    raise KeyError('cik_ids(either default approach or fetch) both are not setup!')

        factors = self.merge_factors() if reduced else self

        if show_process:
            fetched = (f.get(self._cik_dts, self._cik_ids).set_index(['cik_dts', 'cik_ids']) for f in
                       process_bar(factors))
        else:
            fetched = (f.get(self._cik_dts, self._cik_ids).set_index(['cik_dts', 'cik_ids']) for f in factors)
        return fetched

    def fetch(self, _cik_dts=None, _cik_ids=None, merge=True, reduced=False, add_limit=False, show_process=False):
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
                                  show_process=show_process)
        if merge:
            result = pd.concat(fetched, axis=1)
        # columns = result.columns.tolist()
        else:
            result = list(fetched)
        return result

    @staticmethod
    def _filter_no_duplicated_fetched(fetched, old_f_ind, name, cik_cols=['cik_dts', 'cik_ids']):
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

    @staticmethod
    def _build_key(name, f_ind, cik_cols):
        # f_ind_df = f_ind.copy(deep=True)
        cik_dts = sorted(f_ind[cik_cols[0]].values.tolist())  # record dts
        # cik_ids = sorted(f_ind[cik_cols[1]].values.tolist())  # record iid

        # f_ind_df['var'] = name
        # f_ind_df['store_loc'] = key
        key = SaveTools._create_key_name(name, cik_dts, None)
        return key

    # @property
    # def _update_info(self):
    #     if self._enable_update:
    #         d = [f.element for f in self.__factors]
    #         return d
    #     else:
    #         raise None

    def _update_obj(self, obj_dict: dict):  # 重构连接器
        for f in self:
            if f._name in obj_dict.keys():
                f.update_element_obj(obj_dict[f._name])

    def optimize(self, store_path, cik_cols=['cik_dts', 'cik_ids']):
        """
        optimize data
        :param store_path:
        :param cik_cols:
        :return:
        """
        raise NotImplementedError('optimize')
        pass


if __name__ == '__main__':
    # f_ = FactorPool()
    # np.random.seed(1)
    # df = pd.DataFrame(np.random.random(size=(1000, 4)), columns=['cik_dts', 'cik_iid', 'v1', 'v2'])
    # f1 = FactorUnit('test', df, 'cik_dts', 'cik_iid', factor_names=['v1', 'v2'])
    #
    # df = pd.DataFrame(np.random.random(size=(1000, 3)), columns=['cik_dts', 'cik_iid', 'v3'])
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
    config = {}
    config['name'] = name  # str
    config['obj'] = obj  # object
    config['cik_dt'] = cik_dt  # str
    config['cik_id'] = cik_id  # str

    config['factor_names'] = factor_names  # (list, tuple, str)
    config['as_alias'] = as_alias  # (list, tuple, str)
    config['db_table'] = db_table  # str

    pass
