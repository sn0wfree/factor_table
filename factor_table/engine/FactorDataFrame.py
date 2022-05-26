# coding=utf-8
import pandas as pd

from factor_table.core.Factors import Factor
from factor_table.helper.transform_dt_format import transform_dt_format
from factor_table.conf.config import Configs

CONF = Configs(raise_error='no_raise')
DATETIME_FORMAT = CONF.DATETIME_FORMAT


class FactorDF(Factor):  # only store a cross section data
    __slots__ = ['_obj', '_cik', '_factor_name_list', '_alias', '_obj_type', '_db_table', '_name', '_kwargs',
                 ]

    def __init__(self, name: str, df: pd.DataFrame, cik_dt: str, cik_id: str, factor_names: str,
                 as_alias: (list, tuple, str) = None, **kwargs):
        super(FactorDF, self).__init__(name, df, cik_dt, cik_id, factor_names,
                                       as_alias=as_alias, **kwargs)
        self._obj_type = 'DF'

    def get_cik_dts(self, skip_cache=True, **kwargs):
        # dt_data = self._obj[self._cik.dts]
        dt_ = transform_dt_format(self._obj, col=self._cik.dts).drop_duplicates()
        c = dt_.dt.strftime(DATETIME_FORMAT).tolist()
        return c

    def get_cik_ids(self, skip_cache=True, **kwargs):
        dt_data = self._obj[self._cik.ids]
        return dt_data.unique().tolist()

    def get(self, cik_dt_list, cik_id_list, skip_cache=False, **kwargs):
        return self._df_get_(self._obj, cik_dt_list, cik_id_list)
        # self.get = partial(self._df_get_, df=self._obj)


if __name__ == '__main__':
    pass
