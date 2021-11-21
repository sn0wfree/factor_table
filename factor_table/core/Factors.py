# coding=utf-8
import os
from collections import Callable
from typing import Union

import pandas as pd

from factor_table.factor_engine import Factor, __FactorSQL__
from factor_table.factor_engine.FactorDataFrame import __FactorDF__
from factor_table.factor_engine.FactorH5 import __FactorH5__
from factor_table.factor_engine.FactorSQLClickHouse import __FactorSQLClickHouse__
from factor_table.factor_engine.FactorSQLMySQL import __FactorSQLMySQL__
from factor_table.helper import FactorElement


class FactorCreator(type):
    @staticmethod
    def load_from_element(element):
        if isinstance(element, FactorElement):
            return FactorCreator(element.name, element.obj, element.cik_dt, element.cik_id, element.factor_names,
                                 as_alias=element.as_alias, db_table=element.db_table, **element.kwargs)
        else:
            raise ValueError('element must be a FactorElement class!')

    def __instancecheck__(cls, instance):
        return isinstance(instance, Factor)

    def __new__(cls, name, obj, cik_dt: str, cik_id: str, factor_names: Union[list, tuple, str],
                as_alias: Union[list, tuple, str] = None, db_table: str = None, **kwargs):
        """

        :param name:  db_table_sql, df name or  h5_key
        :param obj:  query,df or h5_path
        :param cik_dt:  cik_dt
        :param cik_id:  cik_id
        :param factor_names:  factor names
        :param args:
        :param as_alias:    factor alias name
        :param db_table:  db_table
        :param kwargs:
        """
        if isinstance(obj, pd.DataFrame):
            _obj = __FactorDF__(name, obj, cik_dt, cik_id, factor_names,
                                as_alias=as_alias, db_table=db_table, **kwargs)
        elif isinstance(obj, str) and obj.lower().endswith('.h5'):  # obj is path and end with .h5
            if not os.path.isfile(obj):
                raise FileNotFoundError(f'{obj} not found!')
            _obj = __FactorH5__(name, obj, cik_dt, cik_id, factor_names,
                                as_alias=as_alias, db_table=db_table, **kwargs)
        elif isinstance(obj, Callable):  # obj is executable query function
            db_type = kwargs.get('db_type', None)
            if db_type is None:
                raise ValueError('FactorSQL must have db_type to claim database type!')
            elif db_type == 'SQL':
                _obj = __FactorSQL__(name, obj, cik_dt, cik_id, factor_names,
                                     as_alias=as_alias, db_table=db_table, **kwargs)
            elif db_type == 'MySQL':
                _obj = __FactorSQLMySQL__(name, obj, cik_dt, cik_id, factor_names,
                                     as_alias=as_alias, db_table=db_table, **kwargs)
            elif db_type == 'ClickHouse':
                _obj = __FactorSQLClickHouse__(name, obj, cik_dt, cik_id, factor_names,
                                               as_alias=as_alias, db_table=db_table, **kwargs)
            else:
                raise ValueError(f'unknown db_type:{db_type}')
        else:
            raise ValueError('Unknown info! only accept DF,F5,SQL or ClickHouse')
        _obj.__class__.__name__ = 'Factor'
        return _obj
        


if __name__ == '__main__':
    import numpy as np

    df = pd.DataFrame(np.random.random(size=(1000, 4)), columns=['cik_dts', 'cik_iid', 'v1', 'v2'])
    f1 = FactorCreator('test', df, 'cik_dts', 'cik_iid', factor_names=['v1', 'v2'])
    # FactorCreator.load_from_element(f1.element)
    # print(isinstance(f1, Factor))
    # print(isinstance(f1, FactorCreator))
    # print(f1.__class__.__mro__)
    # print(FactorCreator.__class__.__mro__)
    pass
