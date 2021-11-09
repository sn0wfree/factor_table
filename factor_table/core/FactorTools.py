# coding=utf-8
import hashlib
from collections import deque
from itertools import chain

import pandas as pd

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


class FactorTools(SaveTools):

    @classmethod
    def save(cls, fetched, store_path, _cik_dts=None, _cik_ids=None, cik_cols=['cik_dts', 'cik_iid']):
        """
        store factor pool

        will create f_ind table to store


        :param store_path:
        :param _cik_dts:
        :param _cik_ids:
        :return:
        """

        var_cols = sorted(filter(lambda x: x not in cik_cols, fetched.columns.tolist()))  # sorted variables
        name = ','.join(var_cols)  # have join all variable with comma
        # load data
        with pd.HDFStore(store_path, mode="a", complevel=3, complib=None, fletcher32=False, ) as h5_conn:
            # check f_ind
            keys = h5_conn.keys()
            if '/f_ind' not in keys:  # check index table whether exists
                # create new one
                f_ind = fetched[cik_cols]
                cik_dts, cik_iid = sorted(f_ind['cik_dts'].values.tolist()), sorted(f_ind['cik_iid'].values.tolist())

                key = cls._create_key_name(name, cik_dts, cik_iid)
                f_ind['var'] = name  # string
                f_ind['store_loc'] = key
                h5_conn['f_ind'] = f_ind
                h5_conn[key] = fetched
            else:
                # update old one
                old_f_ind = h5_conn['f_ind']
                cls._check_stored_var_consistent(old_f_ind, name)  # name must be string
                cik_dts = old_f_ind['cik_dts'].values.tolist()
                cik_iid = old_f_ind['cik_iid'].values.tolist()
                new_fetched = cls._filter_fetched(fetched, cik_dts, cik_iid, cik=cik_cols)
                if new_fetched.empty:
                    print(f'new_fetched is empty! nothing to update!')
                else:
                    cik_dts = sorted(new_fetched['cik_dts'].values.tolist())
                    cik_iid = sorted(new_fetched['cik_iid'].values.tolist())
                    f_ind = fetched[cik_cols]
                    key = cls._create_key_name(name, cik_dts, cik_iid)
                    f_ind['store_loc'] = key
                    f_ind['var'] = name
                    new_f_ind = pd.concat([old_f_ind, f_ind])
                    h5_conn['f_ind'] = new_f_ind
                    h5_conn[key] = new_fetched


if __name__ == '__main__':
    pass
