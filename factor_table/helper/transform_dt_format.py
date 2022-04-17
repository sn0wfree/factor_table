# coding=utf-8

import pandas as pd


def transform_dt_format(dts1: pd.DataFrame, col='cik_dt'):
    name = dts1.dtypes[col].name
    if name == 'object':
        return pd.to_datetime(dts1[col])
    elif name.startswith('int'):
        return pd.to_datetime(dts1[col].astype(str))
    elif name.startswith('datetime'):
        return dts1[col]
    else:
        raise TypeError('unknown cik_dts type! only accept str,int or datetime')


if __name__ == '__main__':
    pass
