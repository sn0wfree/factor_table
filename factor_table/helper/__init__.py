# coding=utf-8
from collections import namedtuple
from dataclasses import dataclass

import pandas as pd


@dataclass
class CoreIndexKeyData:
    dts: pd.Series
    ids: pd.Series


@dataclass
class CoreIndexKeys(object):
    __slots__ = ['dts', 'ids']
    dts: str
    ids: str


@dataclass
class FactorInfo(object):
    __slots__ = ['db_table', 'dts', 'ids', 'origin_factor_names', 'alias', 'sql', 'via', 'info']
    db_table: (str, object)
    dts: str
    ids: str
    origin_factor_names: (str, list, tuple)
    alias: (str, list, tuple)
    sql: (str, object)
    via: str
    info: str


@dataclass
class FactorElement(object):
    name: (str, object)
    obj: (object,)
    cik_dt: str
    cik_id: str
    factor_names: (list, tuple, str)
    as_alias: (list, tuple, str)
    db_table: str
    kwargs: dict


if __name__ == '__main__':
    pass
