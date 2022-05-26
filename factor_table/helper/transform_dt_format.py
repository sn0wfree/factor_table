# coding=utf-8

import pandas as pd

import datetime


def detect_and_transform_dt_format(alist):
    first = alist[0]
    if isinstance(first, int):
        return pd.to_datetime(map(lambda x: str(x), alist), format='%Y%m%d')
    elif isinstance(first, str):
        if first.isnumeric():
            return pd.to_datetime(alist, format='%Y%m%d')
        else:
            return pd.to_datetime(alist)
    elif isinstance(first, datetime.datetime):
        return alist
    else:
        raise TypeError('unknown cik_dts type! only accept str,int or datetime')


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
    # def detect_dt_format(dt_df, col='cik_dt'):
    #     first = dt_df[col][0]
    #     if isinstance(first, (str, int)):
    #         return transform_dt_format(dt_df, col)
    #     elif isinstance(first, int):
    #         pass
    #
    #
    # dts = pd.DataFrame(pd.date_range('2021-01-01', '2022-01-01'), columns=['cik_dt'])
    # dt0 = dts['cik_dt'][0]
    # import datetime, time
    #
    # t0 = time.time()
    # c = all(map(lambda dt0: isinstance(dt0, datetime.datetime), dts['cik_dt']))
    # t1 = time.time()
    # c1 = transform_dt_format(dts)
    # t2 = time.time()
    #
    # print(t1 - t0, t2 - t1)
    pass
