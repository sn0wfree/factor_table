# coding=utf-8
from dataclasses import dataclass

import pandas as pd


@dataclass
class CoreIndexKeyData:
    dts: pd.Series
    ids: pd.Series


if __name__ == '__main__':
    pass
