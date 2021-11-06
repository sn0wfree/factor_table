# coding=utf-8
from collections import deque
import pandas as pd
import numpy as np
from factor_table.core.Factors import __Factor__, FactorUnit, FactorInfo
from factor_table.utils.process_bar import process_bar

COLS = list(FactorInfo._fields[:3]) + list(FactorInfo._fields[-2:])
FACTOR_NAME_COLS = FactorInfo._fields[3]
ALIAS_COLS = FactorInfo._fields[4]


class FactorPool(deque):
    def __init__(self, ):
        super(FactorPool, self).__init__()
        self._cik_iids = None
        self._cik_dts = None

    def add_factor(self, *args, **kwargs):
        factor = args[0]
        # todo check factor type
        if isinstance(factor, __Factor__):
            self.append(factor)
        else:
            factor = FactorUnit(*args, **kwargs)
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

        factors_info = pd.DataFrame(list(map(lambda x: x.factor_info(), factors)))
        # print(factors_info, factors_info.index)

        # factors.extend(sql_f)  # dataframe have not reduced!
        for (db_table, dts, iid, via, info), df in factors_info.groupby(COLS): # merge same source factors
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
            res = FactorUnit(db_table, factors[df.index[0]]._obj, dts, iid, origin_factor_names_new, via,
                             as_alias=alias_new,
                             db_table=db_table)
            yield res

    def merge_factors(self):
        # todo unstack factor name to check whether factor exists duplicates!!
        factors = FactorPool()
        factors.extend(list(self.merge_sql_factor()))
        factors.extend(list(self.merge_df_factor()))
        factors.extend(list(self.merge_h5_factor()))
        return factors

    def fetch(self, _cik_dts=None, _cik_iids=None, reduced=True, add_limit=False, ):
        """

        :param reduced: whether use reduce form
        :param _cik_dts: set up dts
        :param _cik_iids:  set up iids
        :param add_limit: use force limit columns
        :return:
        """
        if not add_limit:
            if _cik_dts is not None:
                self._cik_dts = _cik_dts
            else:
                if self._cik_dts is None:
                    raise KeyError('cik_dts(either default approach or fetch) both are not setup!')

            if _cik_iids is not None:
                self._cik_iids = _cik_iids
            else:
                if self._cik_iids is None:
                    raise KeyError('cik_iids(either default approach or fetch) both are not setup!')

        
        factors = self.merge_factors() if reduced else self
        

        fetched = [f.get(self._cik_dts, self._cik_iids).set_index(['cik_dts', 'cik_iid']) for f in process_bar(factors)]

        result = pd.concat(fetched, axis=1)
        # columns = result.columns.tolist()

        return result


if __name__ == '__main__':
    # f_ = FactorPool()
    # np.random.seed(1)
    # df = pd.DataFrame(np.random.random(size=(1000, 4)), columns=['cik_dts', 'cik_iid', 'v1', 'v2'])
    # f1 = FactorUnit('test', df, 'cik_dts', 'cik_iid', factor_names=['v1', 'v2'])
    #
    # df = pd.DataFrame(np.random.random(size=(1000, 3)), columns=['cik_dts', 'cik_iid', 'v3'])
    # f2 = FactorUnit('test2', df, 'cik_dts', 'cik_iid', factor_names=['v3'])
    from ClickSQL import BaseSingleFactorTableNode

    src = 'clickhouse://default:Imsn0wfree@47.104.186.157:8123/system'
    node = BaseSingleFactorTableNode(src)

    # f3 = FactorUnit('test.test2', query, 'cik_dts', 'cik_iid', factor_names=['v3'])

    f2 = FactorPool()

    f2.add_factor('EDGAR_LOG.parsed_edgar_log', node, 'date', 'ip', 'size,crawler')
    f2.add_factor('EDGAR_LOG.parsed_edgar_log', node, 'date', 'ip', 'code')
    f2.add_factor('select * from EDGAR_LOG.parsed_edgar_log', node, 'date', 'ip', 'idx,cik,noagent')
    f2_data = f2.merge_factors().fetch(_cik_dts=['20170430'],_cik_iids=['104.155.127.aha'])
    pass
