# coding=utf-8
from functools import lru_cache

import pandas as pd

from factor_table.core.Factors import Factor
from factor_table.helper.FactorTools import FactorInfo
from factor_table.helper.transform_dt_format import transform_dt_format
from factor_table.utils.check_file_type import filetype

from factor_table.conf.config import Configs

CONF = Configs(raise_error='no_raise')
DATETIME_FORMAT = CONF.DATETIME_FORMAT
class FactorH5(Factor):  # only store a cross section data
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', 'element',
                 '_cache']
    """
     f_id, obj_or_df, cik_dt_col: str, cik_id_col: str, factor_names: Union[list, tuple, str],
                 as_alias: Union[list, tuple, str] = None, **kwargs
    """

    def __init__(self, f_id: str, h5_path: str, cik_dt: str, cik_id: str, factor_names: list,
                 as_alias: (list, tuple, str) = None, **kwargs):
        super(FactorH5, self).__init__(f_id, h5_path, cik_dt, cik_id, factor_names,
                                       as_alias=as_alias, **kwargs)
        if filetype(h5_path) == 'HDF5':
            self._obj_type = 'H5'
        else:
            raise ValueError(f'{h5_path} is not a HDF5 file !')

    def factor_info(self):
        # f_id_or_sql, cik_dt,cik_id,factor_names,alias,obj_type,db_type,src

        return FactorInfo(self._f_id,  # f_id_or_sql
                          self._cik.dts,  # cik_dt
                          self._cik.ids,  # cik_id
                          ','.join(self._factor_name_list),  # factor_names
                          ','.join(map(lambda x: str(x), self._alias)) if self._alias is not None else None,  # alias
                          self._obj_type,  # obj_type
                          self._db_type,  # db_type
                          self._src)  # src

    @lru_cache(maxsize=200)
    def get_cik_dts(self, force=False, dt_col='cik_dt', **kwargs):
        # TODO 性能点

        h5_path = self._obj
        key = self._name
        df = pd.read_hdf(h5_path, key)  # [self._cik.dts]
        dt_ = transform_dt_format(df, col=dt_col)
        return dt_.dt.strftime(DATETIME_FORMAT).unique().tolist()
        # return df[self._cik.dts].dt.strftime('%Y-%m-%d').unique().tolist()

    @lru_cache(maxsize=200)
    def get_cik_ids(self, force=False, **kwargs):

        # TODO 性能点
        h5_path = self._obj
        key = self._name
        df = pd.read_hdf(h5_path, key)
        return df[self._cik.ids].unique().tolist()

    def get(self, cik_dt_list, cik_id_list, force=False, **kwargs):

        # TODO 性能点
        h5_path = self._obj
        key = self._name
        df = pd.read_hdf(h5_path, key)
        return self._df_get_(df, cik_dt_list, cik_id_list)


if __name__ == '__main__':
    pass
