# coding=utf-8
import hashlib
import os,time

import pandas as pd

def _build_key(data, _cik_dts: list, cik_cols=['cik_dts', 'cik_ids']):
    cols = sorted(filter(lambda x: x not in cik_cols, data.columns.tolist()))
    return '|'.join(cols) + '@' + f"{min(_cik_dts)}_{max(_cik_dts)}"

class KeyInfoDict(dict):
    def batch_key_append(self, keys):
        for key in keys:
            cols_str, dts_str = key.split('@')
            # cols = tuple(cols_str.split('|'))
            dt_s, dt_e = dts_str.split('_')
            self.key_append(key, (key, cols_str, int(dt_s), int(dt_e)))

    def key_append(self, key, data):
        if key in self.keys():
            self[key].append(data)
        else:
            self[key] = [data]

    @property
    def extend_dts(self):
        holder = []
        # info = pd.DataFrame(self.values(), columns=['key', 'cols_str', 'start', 'end'])
        for key, cols, start, end in self.values():
             
            d = pd.DataFrame(pd.date_range(str(start), str(end)), columns=['dt'])
            d['key'] = key
            d['cols_str'] = cols
            holder.append(d)
        return pd.concat(holder).drop_duplicates()

    def search_key(self, cols_str: str=None, start=None, end=None):
        info = self.extend_dts
    
        mask1 = info['cols_str'] == cols_str if cols_str is not None else True               
        mask_dt1 = info['dt'] >=  info['dt'].min()  if start is None else info['dt'] >= pd.to_datetime(str(start))                               
        mask_dt2 = info['dt'] <= info['dt'].max() if end is None else info['dt'] <= pd.to_datetime(str(end))               
        selected = info[mask1 & mask_dt1 & mask_dt2]
        return selected.groupby('cols_str')['key'].unique().to_dict()


class CacheViews(dict):
    pass
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
    def raw_save(factor_table: object, store_path: str, cik_cols=['cik_dts', 'cik_ids'], auto_save=True, cik_dts=None,
                 cik_ids=None, if_exists='replace'):
        """
        store factor pool

        1. use start dt and end dt as key
        2. use variable names as file_name



        :param cik_cols:
        :param store_path:


        :return:
        """

        # c = pickle.dumps(elements)

        # fetched = self.fetch_iter(_cik_dts=_cik_dts, _cik_ids=_cik_ids, reduced=False)  # .reset_index()
        # cols = sorted(filter(lambda x: x not in cik_cols, fetched.columns.tolist()))

        if not os.path.exists(store_path) and auto_save:
            raise ValueError(f"{store_path} is not exists!")
        _cik_dts = factor_table.cik_dts if cik_dts is None else cik_dts
        _cik_ids = factor_table.cik_ids if cik_ids is None else cik_ids
        if not hasattr(factor_table, 'fetch'):
            raise AttributeError('input factor_table object has not fetch Attribute!')

        

        with pd.HDFStore(store_path, mode="a", complevel=6, complib=None, fletcher32=False, ) as h5_conn:
            # check f_ind

            fetched = factor_table.fetch(_cik_dts=_cik_dts, _cik_ids=_cik_ids, reduced=False, add_limit=False,
                                         show_process=True, delay=True)  # .reset_index()
            keys = sorted(map(lambda x: x[1:], h5_conn.keys()))

            for data in fetched:
                name = _build_key(data, _cik_dts, cik_cols=cik_cols)
                if if_exists == 'replace':
                    h5_conn[name] = data
                elif if_exists == 'ignore':
                    if name not in keys:
                        h5_conn[name] = data
                elif if_exists == 'error':
                    if name in keys:
                        raise KeyError(f'{name} at {store_path} exists!')
                    else:
                        h5_conn[name] = data
                else:
                    raise ValueError('if_exists must be replace, ignore or error')

                # if name not in keys:

    @staticmethod
    def raw_load(store_path, cik_dts=None, cik_cols=['cik_dts', 'cik_ids']):
        if not os.path.exists(store_path):
            raise FileNotFoundError(f'h5:{store_path} not exists! please check!')
        with pd.HDFStore(store_path, mode="a", complevel=6, complib=None, fletcher32=False, ) as h5_conn:

            keys = sorted(map(lambda x: x[1:], h5_conn.keys()))
            if not keys:
                raise ValueError(f'{store_path} is empty!')
            key_info = KeyInfoDict()
            key_info.batch_key_append(keys)
            if cik_dts is None:
                start = None
                end =None
            else:
                start = min(cik_dts)
                end = max(cik_dts)
            for cols, key_list in key_info.search_key(None, start=start, end=end):
                data = pd.concat([ h5_conn[key]  for key in key_list])
                yield cols,data
    @classmethod
    def raw_merge(cls,store_path, cik_cols=['cik_dts', 'cik_ids'],del_method='remove',**kwargs):

        
        with pd.HDFStore(store_path+'.merge', mode="a", complevel=6, complib=None, fletcher32=False, ) as h5_conn2:
            for cols, data in cls.raw_load(cls,store_path,cik_dts=None, cik_cols=cik_cols):
                _cik_dts = data[cik_cols[0]].unique().tolist()
                name = _build_key(data, _cik_dts, cik_cols=cik_cols)
                h5_conn2[name] = data
        
        # repalce file old file by merged file
        if os.path.exists(store_path) and os.path.exists(store_path+'.merge'):
            if del_method =='remove':
                os.remove(store_path)
            else:
                os.rename(store_path,store_path+f'.old.{int(time.time())}')
            os.rename(store_path+'.merge', store_path)
        else:
            raise ValueError(f'{store_path} or {store_path+".merge"} is not exists!')

        # os.remove(store_path)


        # return pd.concat(datas.values(), axis=1)
        
                
                # if '/elements' not in keys:
            #     raise ValueError(f'h5:{store_path} has no elements key! please check!')
            #
            # if '/f_ind' not in keys:
            #     raise ValueError(f'h5:{store_path} has no f_ind key! please check!')
            # f_ind = h5_conn['f_ind']
            # ele = h5_conn['elements']
            # elements = pickle.loads(ele[0])
            # # var = f_ind['var'].unique().tolist()
            # # if len(var) > 1: raise ValueError(f'f_ind.var got two or more diff vars:{var}')
            # for element in elements:
            #
            #     # name = element.name
            #     if cik_dts is None:
            #         mask = f_ind['var'].isin([element.name])
            #         store_loc_list = f_ind[mask]['store_loc'].unique().tolist()
            #         df = pd.concat([h5_conn[loc] for loc in store_loc_list])
            #         df = df[df[cik_cols[1]].isin(cik_ids)] if cik_ids is not None else df
            #         f1 = FactorCreator.load_from_element(element)
            #         f1._cache_func(df)
            #
            #         target_ft.add_factor(f1)
            #     else:
            #         selected_cik_dts = f_ind[cik_cols[0]].unique() if cik_dts is None else cik_dts
            #         # selected_cik_ids = f_ind[cik_cols[1]].unique() if cik_ids is None else cik_ids
            #         # filter f_ind index
            #         dts_mask = f_ind[cik_cols[0]].isin(selected_cik_dts)
            #         # ids_mask = f_ind[cik_cols[1]].isin(selected_cik_ids)
            #         store_loc_list = f_ind[dts_mask]['store_loc'].unique().tolist()
            #         df = pd.concat([h5_conn[loc] for loc in store_loc_list])
            #         dts_mask = df[cik_cols[0]].isin(selected_cik_dts)
            #         ids_mask = df[cik_cols[1]].isin(cik_ids) if cik_ids is not None else 1
            #         f1 = FactorCreator.load_from_element(element)
            #         f1._cache_func(df[dts_mask & ids_mask])
            #         # f1 = FactorCreator(name, df[dts_mask & ids_mask], cik_cols[0], cik_cols[1], factor_names=var)
            #         target_ft.add_factor(f1)


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
