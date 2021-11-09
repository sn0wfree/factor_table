# coding=utf-8
from collections import deque
import os,warnings
import pandas as pd

from factor_table.core.FactorTools import SaveTools
from factor_table.core.Factors import Factor as __Factor__, FactorCreator
from factor_table.helper import FactorInfo
from factor_table.utils.process_bar import process_bar

# db_table, dts, iid, via, info
COLS = ['db_table', 'dts', 'ids'] + ['via', 'info']
FACTOR_NAME_COLS = 'origin_factor_names'
ALIAS_COLS = 'alias'


class FactorPool(deque):
    def __init__(self, ):
        super(FactorPool, self).__init__()
        self._cik_ids = None
        self._cik_dts = None

    def add_factor(self, *args, **kwargs):
        factor = args[0]
        # todo check factor type
        if isinstance(factor, __Factor__):
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
        # print(factors_info, factors_info.index)

        # factors.extend(sql_f)  # dataframe have not reduced!
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
                                as_alias=alias_new,
                                db_table=db_table)
            yield res

    def show_factors(self, reduced=False, to_df=True):
        factor_info_iter = map(lambda x: x.factor_info(), self)
        if to_df:
            factors_info = pd.DataFrame(list(factor_info_iter))
        else:
            factors_info = list(factor_info_iter)
        return factors_info

    def merge_factors(self):
        # todo unstack factor name to check whether factor exists duplicates!!
        # factors = FactorPool()

        for factor in self.merge_df_factor(self):
            yield factor
        for factor in self.merge_h5_factor(self):
            yield factor
        for factor in self.merge_sql_factor(self):
            yield factor
        # factors.extend(list(self.merge_sql_factor(self)))
        # factors.extend(list(self.merge_df_factor(self)))
        # factors.extend(list(self.merge_h5_factor(self)))
        # return factors

    def fetch_iter(self, _cik_dts=None, _cik_ids=None, reduced=True, add_limit=False, show_process=True):
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

    def fetch(self, _cik_dts=None, _cik_ids=None, reduced=True, add_limit=False, show_process=True):
        """

        :param show_process:
        :param reduced: whether use reduce form
        :param _cik_dts: set up dts
        :param _cik_ids:  set up ids
        :param add_limit: use force limit columns
        :return:
        """

        fetched = self.fetch_iter(_cik_dts=_cik_dts, _cik_ids=_cik_ids, reduced=reduced, add_limit=add_limit,
                                  show_process=show_process)

        result = pd.concat(fetched, axis=1)
        # columns = result.columns.tolist()

        return result

    @staticmethod
    def filter_no_duplicated_fetched(fetched, old_f_ind, name, cik_cols=['cik_dts', 'cik_ids']):
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

    def save(self, store_path, _cik_dts=None, _cik_ids=None, reduced=True, cik_cols=['cik_dts', 'cik_ids']):
        """
        store factor pool

        will create f_ind table to store


        :param cik_cols:
        :param store_path:
        :param _cik_dts:
        :param _cik_ids:
        :param reduced:
        :return:
        """

        fetched = self.fetch(_cik_dts=_cik_dts, _cik_ids=_cik_ids, reduced=reduced).reset_index()
        cols = sorted(filter(lambda x: x not in cik_cols, fetched.columns.tolist()))
        name = ','.join(cols)

        def build_key(name, f_ind, cik_cols):
            # f_ind_df = f_ind.copy(deep=True)
            cik_dts = sorted(f_ind[cik_cols[0]].values.tolist())  # record dts
            cik_ids = sorted(f_ind[cik_cols[1]].values.tolist())  # record iid

            # f_ind_df['var'] = name
            # f_ind_df['store_loc'] = key
            key = SaveTools._create_key_name(name, cik_dts, cik_ids)
            return key

        with pd.HDFStore(store_path, mode="a", complevel=3, complib=None, fletcher32=False, ) as h5_conn:
            # check f_ind
            keys = h5_conn.keys()
            if '/f_ind' not in keys:
                # create new one
                f_ind = fetched[cik_cols]
                # cik_dts = sorted(f_ind[cik_cols[0]].values.tolist())  # record dts
                # cik_iid = sorted(f_ind[cik_cols[1]].values.tolist())  # record iid
                # key = SaveTools._create_key_name(name, cik_dts, cik_iid)
                # f_ind['var'] = name
                # f_ind['store_loc'] = key
                key = build_key(name, f_ind, cik_cols)
                f_ind['var'] = name
                f_ind['store_loc'] = key
                h5_conn['f_ind'] = f_ind
                h5_conn[key] = fetched
            else:
                # update old one
                old_f_ind = h5_conn['f_ind']
                old_name = old_f_ind['var'].unique()[0]
                if name != old_name:
                    raise ValueError(f'f_ind.var got two or more diff vars！ new[{name}] vs old[{old_name}]')
                new_fetched = self.filter_no_duplicated_fetched(fetched, old_f_ind, name, cik_cols=cik_cols)
                if new_fetched.empty:
                    warnings.warn(f'new_fetched is empty! nothing to update!')
                else:
                    f_ind = fetched[cik_cols]
                    # cik_dts = sorted(new_fetched[cik_cols[0]].values.tolist())
                    # cik_iid = sorted(new_fetched[cik_cols[1]].values.tolist())

                    key = build_key(name, new_fetched, cik_cols)  # SaveTools._create_key_name(name, cik_dts, cik_iid)
                    f_ind['store_loc'] = key
                    f_ind['var'] = name

                    h5_conn['f_ind'] = new_f_ind = pd.concat([old_f_ind, f_ind])
                    h5_conn[key] = new_fetched

    def load(self, store_path, cik_dts=None, cik_ids=None, cik_cols=['cik_dts', 'cik_ids']):

        if not os.path.exists(store_path):
            raise FileNotFoundError(f'h5:{store_path} not exists! please check!')
        with pd.HDFStore(store_path, mode="a", complevel=3, complib=None, fletcher32=False, ) as h5_conn:
            keys = h5_conn.keys()
            if '/f_ind' not in keys:
                raise ValueError(f'h5:{store_path} has no f_ind key! please check!')
            f_ind = h5_conn['f_ind']
            var = f_ind['var'].unique().tolist()
            if len(var) > 1: raise ValueError(f'f_ind.var got two or more diff vars:{var}')
            name = var[0]
            var = name.split(',')

            if cik_dts is None and cik_ids is None:
                store_loc_list = f_ind['store_loc'].unique()
                df = pd.concat([h5_conn[loc] for loc in store_loc_list])
                f1 = FactorCreator(name, df, cik_cols[0], cik_cols[1], factor_names=var)
                self.add_factor(f1)
            else:
                selected_cik_dts = f_ind[cik_cols[0]].unique() if cik_dts is None else cik_dts
                selected_cik_iid = f_ind[cik_cols[1]].unique() if cik_ids is None else cik_ids

                dts_mask = f_ind[cik_cols[0]].isin(selected_cik_dts)
                ids_mask = f_ind[cik_cols[1]].isin(selected_cik_iid)

                store_loc_list = f_ind[dts_mask & ids_mask]['store_loc'].unique()
                df = pd.concat([h5_conn[loc] for loc in store_loc_list])

                dts_mask = df[cik_cols[0]].isin(selected_cik_dts)
                ids_mask = df[cik_cols[1]].isin(selected_cik_iid)

                f1 = FactorCreator(name, df[dts_mask & ids_mask], cik_cols[0], cik_cols[1], factor_names=var)
                self.add_factor(f1)


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
    f2_data = f2.merge_factors().fetch(_cik_dts=['20170430'], _cik_iids=['104.155.127.aha'])
    pass
