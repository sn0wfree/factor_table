# coding=utf-8
import pandas as pd

from factor_table.factor_engine import Factor
from factor_table.helper import FactorInfo
from factor_table.helper.transform_dt_format import transform_dt_format
from factor_table.utils.check_file_type import filetype


class __FactorH5__(Factor):  # only store a cross section data
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', 'element',
                 '_cache']

    def __init__(self, key: str, h5_path: str, cik_dt: str, cik_id: str, factor_names: list, *args,
                 as_alias: (list, tuple, str) = None, **kwargs):
        super(__FactorH5__, self).__init__(key, h5_path, cik_dt, cik_id, factor_names, *args,
                                           as_alias=as_alias, **kwargs)
        if filetype(h5_path) == 'HDF5':
            self._obj_type = 'H5'
        else:
            raise ValueError(f'{h5_path} is not a HDF5 file !')

    def factor_info(self):
        return FactorInfo(self._obj,  # 'db_table' = H5_path
                          self._cik.dts,  # 'dts'
                          self._cik.ids,  # 'ids'
                          ','.join(self._factor_name_list),  # 'origin_factor_names'
                          ','.join(map(lambda x: str(x), self._alias)) if self._alias is not None else None,  # 'alias'
                          self._name,  # sql name = key
                          self._obj_type,  # via # H5
                          ''  # info
                          )

    def get_cik_dts(self, force=False, **kwargs):
        # TODO 性能点
        if self._cache is not None and not force:
            df = self._cache.reset_index()  # [self._cik.dts]
        else:
            h5_path = self._obj
            key = self._name
            df = pd.read_hdf(h5_path, key)  # [self._cik.dts]
        dt_ = transform_dt_format(df, col='cik_dts')
        return dt_.dt.strftime('%Y-%m-%d').unique().tolist()
        # return df[self._cik.dts].dt.strftime('%Y-%m-%d').unique().tolist()

    def get_cik_ids(self, force=False, **kwargs):
        if self._cache is not None and not force:
            return self._cache.reset_index()[self._cik.ids].unique().tolist()
        else:
            # TODO 性能点
            h5_path = self._obj
            key = self._name
            df = pd.read_hdf(h5_path, key)
            return df[self._cik.ids].unique().tolist()

    def get(self, cik_dt_list, cik_id_list, force=False, **kwargs):
        if self._cache is not None and not force:
            return self._cache
        # TODO 性能点
        h5_path = self._obj
        key = self._name
        df = pd.read_hdf(h5_path, key)
        return self._df_get_(df, cik_dt_list, cik_id_list)


if __name__ == '__main__':
    pass
