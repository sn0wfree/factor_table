# coding=utf-8
import pandas as pd

from factor_table.factor_engine import Factor
from factor_table.helper.transform_dt_format import transform_dt_format


class __FactorDF__(Factor):  # only store a cross section data
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs', 'element',
                 '_cache']

    def __init__(self, name: str, df: pd.DataFrame, cik_dt: str, cik_id: str, factor_names: str,
                 as_alias: (list, tuple, str) = None, **kwargs):
        super(__FactorDF__, self).__init__(name, df, cik_dt, cik_id, factor_names,
                                           as_alias=as_alias, **kwargs)
        self._obj_type = 'DF'

    def get_cik_dts(self, force=False, **kwargs):
        if self._cache is not None and not force:
            dt_data = self._cache.reset_index()  # [self._cik.dts]
        else:
            dt_data = self._obj  # [self._cik.dts]

        dt_ = transform_dt_format(dt_data, col='cik_dts')
        return dt_.dt.strftime('%Y-%m-%d').unique().tolist()

    def get_cik_ids(self, force=False, **kwargs):

        if self._cache is not None and not force:
            dt_data = self._cache.reset_index()[self._cik.ids]
        else:
            dt_data = self._obj[self._cik.ids]

        return dt_data.unique().tolist()

    def get(self, cik_dt_list, cik_id_list, force=False, **kwargs):
        if self._cache is not None and not force:
            return self._cache
        return self._df_get_(self._obj, cik_dt_list, cik_id_list)
        # self.get = partial(self._df_get_, df=self._obj)


if __name__ == '__main__':
    pass
