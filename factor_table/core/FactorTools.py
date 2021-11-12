# coding=utf-8
import hashlib
import os
import pickle
import warnings

import pandas as pd

from factor_table.core.Factors import FactorCreator


class SaveTools(object):
    @staticmethod
    def _hash_func(s, encoding='utf-8'):
        return hashlib.md5(str(s).encode(encoding)).hexdigest()

    @classmethod
    def _create_key_name(cls, name: str, cik_dts: (str, list), cik_iid: (str, list)):
        """

        :param name:
        :param cik_dts:
        :param cik_iid:
        :return:
        """
        cik_dts_hash = cls._hash_func(cik_dts)  # hashlib.md5(str(cik_dts).encode('utf-8')).hexdigest()
        cik_iid_hash = cls._hash_func(cik_iid)  # hashlib.md5(str(cik_iid).encode('utf-8')).hexdigest()
        key = name + "-" + cik_dts_hash + '-' + cik_iid_hash
        return key

    @staticmethod
    def _filter_fetched(fetched, cik_dts, cik_iid, cik=None):
        if cik is None:
            cik = ['cik_dts', 'cik_iid']
        dt_mask = ~fetched[cik[0]].isin(cik_dts)
        iid_mask = ~fetched[cik[1]].isin(cik_iid)
        return fetched[dt_mask & iid_mask]

    @staticmethod
    def _check_stored_var_consistent(old_f_ind, name):
        """

        :param old_f_ind:
        :param name:
        :return:
        """
        old_name = old_f_ind['var'].unique()  # should one string
        if len(old_name) > 1:  # if got 2 or more var,that means something wrong at this file
            raise ValueError('f_ind.var got two or more diff vars')
        old_name = old_name[0]  # get all variables which has joined with comma
        if name == old_name:  # check all variable should only store same var
            pass
        else:
            raise ValueError(f'stored var:{old_name} diff from {name}')

    @staticmethod
    def save(factor_pool: object, store_path, _cik_dts=None, _cik_ids=None, cik_cols=['cik_dts', 'cik_ids'],
             force=False):
        """
        store factor pool

        will create f_ind table to store
        1.  f_ind 存储 因子 cik 的序列的最大值（min_dt,max_dt）
        2.  config 存储关键因素，重建链接--配置文件
        3.  key(因子) -> dataframe
        4.

        :param cik_cols:
        :param store_path:
        :param _cik_dts:
        :param _cik_ids:

        :return:
        """

        # c = pickle.dumps(elements)

        # fetched = self.fetch_iter(_cik_dts=_cik_dts, _cik_ids=_cik_ids, reduced=False)  # .reset_index()
        # cols = sorted(filter(lambda x: x not in cik_cols, fetched.columns.tolist()))
        # name = ','.join(cols)

        with pd.HDFStore(store_path, mode="a", complevel=6, complib=None, fletcher32=False, ) as h5_conn:
            # check f_ind
            keys = h5_conn.keys()
            elements = []
            if '/f_ind' in keys:
                ori_f_ind = h5_conn['f_ind']
            else:
                warnings.warn('not found f_ind! will create new one')
                ori_f_ind = pd.DataFrame()
            for element, fetched_data in factor_pool._fetch_iter_with_element(_cik_dts, _cik_ids):
                elements.append(element)
                print(element.name)
                f_ind = fetched_data.reset_index()[cik_cols[:1]]
                max_f_ind = f_ind.max().values[0]
                min_f_ind = f_ind.min().values[0]

                # check index
                key = factor_pool._build_key(element.name, f_ind, cik_cols)
                f_info = pd.DataFrame(pd.Series([max_f_ind, min_f_ind, element.name, key],
                                                index=['max_cik_dts', 'min_cik_dts', 'var', 'store_loc'])).T

                # f_ind['var'] = element.name
                # f_ind['store_loc'] = key

                if '/' + key in keys and not force:
                    raise ValueError(f'duplicates data {key} | max_f_ind:{max_f_ind} , min_f_ind:{min_f_ind}')
                else:
                    fetched_data.to_hdf(h5_conn, key, format="table", append=True, data_columns=cik_cols)
                ori_f_ind = pd.concat([ori_f_ind, f_info])
                # h5_conn.put(key, fetched_data, format="table", append=True, data_columns=cik_cols)
            # if '/elements' not in keys:
            h5_conn['f_ind'] = ori_f_ind
            h5_conn['elements'] = pd.Series([pickle.dumps(elements)])
            # ele =
            # ele.to_hdf(h5_conn, 'elements', format="table", append=True)
            # else:
            #     old_elements = h5_conn['elements']

    @staticmethod
    def load(target_ft, store_path, cik_dts=None, cik_ids=None, cik_cols=['cik_dts', 'cik_ids']):

        if not os.path.exists(store_path):
            raise FileNotFoundError(f'h5:{store_path} not exists! please check!')
        with pd.HDFStore(store_path, mode="a", complevel=6, complib=None, fletcher32=False, ) as h5_conn:
            keys = h5_conn.keys()
            if '/elements' not in keys:
                raise ValueError(f'h5:{store_path} has no elements key! please check!')

            if '/f_ind' not in keys:
                raise ValueError(f'h5:{store_path} has no f_ind key! please check!')
            f_ind = h5_conn['f_ind']
            ele = h5_conn['elements']
            elements = pickle.loads(ele[0])
            # var = f_ind['var'].unique().tolist()
            # if len(var) > 1: raise ValueError(f'f_ind.var got two or more diff vars:{var}')
            for element in elements:

                # name = element.name
                if cik_dts is None:
                    mask = f_ind['var'].isin([element.name])
                    store_loc_list = f_ind[mask]['store_loc'].unique().tolist()
                    df = pd.concat([h5_conn[loc] for loc in store_loc_list])
                    df = df[df[cik_cols[1]].isin(cik_ids)] if cik_ids is not None else df
                    f1 = FactorCreator.load_from_element(element)
                    f1._cache_func(df)

                    target_ft.add_factor(f1)
                else:
                    selected_cik_dts = f_ind[cik_cols[0]].unique() if cik_dts is None else cik_dts
                    # selected_cik_ids = f_ind[cik_cols[1]].unique() if cik_ids is None else cik_ids
                    # filter f_ind index
                    dts_mask = f_ind[cik_cols[0]].isin(selected_cik_dts)
                    # ids_mask = f_ind[cik_cols[1]].isin(selected_cik_ids)
                    store_loc_list = f_ind[dts_mask]['store_loc'].unique().tolist()
                    df = pd.concat([h5_conn[loc] for loc in store_loc_list])
                    dts_mask = df[cik_cols[0]].isin(selected_cik_dts)
                    ids_mask = df[cik_cols[1]].isin(cik_ids) if cik_ids is not None else 1
                    f1 = FactorCreator.load_from_element(element)
                    f1._cache_func(df[dts_mask & ids_mask])
                    # f1 = FactorCreator(name, df[dts_mask & ids_mask], cik_cols[0], cik_cols[1], factor_names=var)
                    target_ft.add_factor(f1)


class FactorTools(SaveTools): pass


# @classmethod
# def save(cls, fetched, store_path, _cik_dts=None, _cik_ids=None, cik_cols=['cik_dts', 'cik_iid']):
#     """
#     store factor pool
#
#     will create f_ind table to store
#
#
#     :param store_path:
#     :param _cik_dts:
#     :param _cik_ids:
#     :return:
#     """
#
#     var_cols = sorted(filter(lambda x: x not in cik_cols, fetched.columns.tolist()))  # sorted variables
#     name = ','.join(var_cols)  # have join all variable with comma
#     # load data
#     with pd.HDFStore(store_path, mode="a", complevel=3, complib=None, fletcher32=False, ) as h5_conn:
#         # check f_ind
#         keys = h5_conn.keys()
#         if '/f_ind' not in keys:  # check index table whether exists
#             # create new one
#             f_ind = fetched[cik_cols]
#             cik_dts, cik_iid = sorted(f_ind['cik_dts'].values.tolist()), sorted(f_ind['cik_iid'].values.tolist())
#
#             key = cls._create_key_name(name, cik_dts, cik_iid)
#             f_ind['var'] = name  # string
#             f_ind['store_loc'] = key
#             h5_conn['f_ind'] = f_ind
#             h5_conn[key] = fetched
#         else:
#             # update old one
#             old_f_ind = h5_conn['f_ind']
#             cls._check_stored_var_consistent(old_f_ind, name)  # name must be string
#             cik_dts = old_f_ind['cik_dts'].values.tolist()
#             cik_iid = old_f_ind['cik_iid'].values.tolist()
#             new_fetched = cls._filter_fetched(fetched, cik_dts, cik_iid, cik=cik_cols)
#             if new_fetched.empty:
#                 print(f'new_fetched is empty! nothing to update!')
#             else:
#                 cik_dts = sorted(new_fetched['cik_dts'].values.tolist())
#                 cik_iid = sorted(new_fetched['cik_iid'].values.tolist())
#                 f_ind = fetched[cik_cols]
#                 key = cls._create_key_name(name, cik_dts, cik_iid)
#                 f_ind['store_loc'] = key
#                 f_ind['var'] = name
#                 new_f_ind = pd.concat([old_f_ind, f_ind])
#                 h5_conn['f_ind'] = new_f_ind
#                 h5_conn[key] = new_fetched


if __name__ == '__main__':
    pass
