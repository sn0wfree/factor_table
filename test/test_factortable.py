# coding=utf-8
import os.path
import unittest

import numpy as np
import pandas as pd

from factor_table.core import FactorCreator, FactorTable


class MyTestCase(unittest.TestCase):
    def test_load_from_dataframe(self):
        np.random.seed(1)
        df = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v1', 'v2'])
        df['cik_dts'] = pd.date_range(start=20200101, periods=1000, freq='D')
        df['cik_ids'] = np.random.randint(0, 2000, 1000)
        f1 = FactorCreator('test', df, cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'])
        self.assertEqual(f1._obj_type, 'DF')  # add assertion here
        self.assertNotEqual(f1._obj_type, 'H5')  # add assertion here

    def test_load_from_dataframe2_add_alias(self):
        np.random.seed(12)
        df = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v1', 'v2'])
        # , 'cik_iid',
        df['cik_dts'] = pd.date_range(start=20200101, periods=1000, freq='D')
        df['cik_ids'] = np.random.randint(0, 2000, 1000)

        f1 = FactorCreator('test', df, cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=['renamed_v1', 'renamed_v2'])
        self.assertEqual(f1._obj_type, 'DF')  # add assertion here
        self.assertNotEqual(f1._obj_type, 'H5')  # add assertion here

    def test_load_from_h5(self):
        np.random.seed(13)
        df = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v1', 'v2'])
        # , 'cik_iid',
        df['cik_dts'] = pd.date_range(start=20200101, periods=1000, freq='D')
        df['cik_ids'] = np.random.randint(0, 2000, 1000)

        df.to_hdf('test.h5', 'test2')
        f1 = FactorCreator('test2', 'test.h5', cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           )
        self.assertNotEqual(f1._obj_type, 'DF')  # add assertion here
        self.assertEqual(f1._obj_type, 'H5')

    def test_load_from_h5_add_alias(self):
        np.random.seed(14)
        df = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v1', 'v2'])
        # , 'cik_iid',
        df['cik_dts'] = pd.date_range(start=20200101, periods=1000, freq='D')
        df['cik_ids'] = np.random.randint(0, 2000, 1000)

        df.to_hdf('test.h5', 'test2')
        f1 = FactorCreator('test2', 'test.h5', cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=['renamed_v1', 'renamed_v2'])
        self.assertNotEqual(f1._obj_type, 'DF')  # add assertion here
        self.assertEqual(f1._obj_type, 'H5')

    def test_load_from_2_diff_type(self):
        np.random.seed(15)
        df = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v1', 'v2'])
        # , 'cik_iid',
        df['cik_dts'] = pd.date_range(start=20200101, periods=1000, freq='D')
        df['cik_ids'] = np.random.randint(0, 2000, 1000)

        f1 = FactorCreator('test', df, cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=['renamed_v1', 'renamed_v2'])
        self.assertEqual(f1._obj_type, 'DF')

        df2 = df.copy(deep=True)
        df2['cik_ids'] = df2['cik_ids'] + 1

        df2.to_hdf('test.h5', 'test2')
        f2 = FactorCreator('test2', 'test.h5', cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=['renamed_v1', 'renamed_v2'])
        self.assertEqual(f2._obj_type, 'H5')

    def test_add_factor(self):
        np.random.seed(15)
        df = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v1', 'v2'])
        # , 'cik_iid',
        df['cik_dts'] = pd.date_range(start='2020-01-01', periods=1000, freq='D')
        df['cik_ids'] = np.random.randint(0, 1500, 1000)

        f1 = FactorCreator('test', df, cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=['renamed_v1', 'renamed_v2'])

        df2 = df.copy(deep=True)
        # df2['cik_ids'] = np.random.randint(0, 1500, 1000)

        df2.to_hdf('test.h5', 'test2')
        f2 = FactorCreator('test2', 'test.h5', cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=[None, 'renamed_v4'])

        ft = FactorTable()
        ft.add_factor(f1)
        ft.add_factor(f2)

        cik_dt_list = df['cik_dts'].dt.strftime("%Y-%m-%d")
        cik_id_list = list(set(df['cik_ids'].unique().tolist() + df2['cik_ids'].unique().tolist()))
        result = ft.fetch(cik_dt_list, cik_id_list, show_process=False, reduced=False)
        self.assertListEqual(result.columns.tolist(), ['renamed_v1', 'renamed_v2', 'v1', 'renamed_v4'])

    def test_save(self):
        store_path = 'test_save.h5'
        exists = os.path.exists(store_path)

        np.random.seed(15)
        df = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v1', 'v2'])
        # , 'cik_iid',
        df['cik_dts'] = pd.date_range(start='2020-01-01', periods=1000, freq='D')
        df['cik_ids'] = np.random.randint(0, 1500, 1000)

        f1 = FactorCreator('test', df, cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=['renamed_v1', 'renamed_v2'])

        df2 = df.copy(deep=True)
        # df2['cik_ids'] = np.random.randint(0, 1500, 1000)

        df2.to_hdf('test.h5', 'test2')
        f2 = FactorCreator('test2', 'test.h5', cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=[None, 'renamed_v4'])

        ft = FactorTable()
        ft.add_factor(f1)
        ft.add_factor(f2)

        cik_dt_list = df['cik_dts'].dt.strftime("%Y-%m-%d")
        cik_id_list = list(set(df['cik_ids'].unique().tolist() + df2['cik_ids'].unique().tolist()))
        if not exists:
            ft.save(store_path, _cik_dts=cik_dt_list, _cik_ids=cik_id_list)
            self.assertEqual(True, os.path.exists(store_path))
        else:
            with self.assertWarns(UserWarning):
                ft.save(store_path, _cik_dts=cik_dt_list, _cik_ids=cik_id_list)

        # result = ft.fetch(cik_dt_list, cik_id_list, show_process=False, reduced=False)
        # self.assertListEqual(result.columns.tolist(), ['renamed_v1', 'renamed_v2', 'v1', 'renamed_v4'])

    def test_load_no_exists_err(self):
        store_path = 'test_save2.h5'
        ft2 = FactorTable()
        with self.assertRaises(FileNotFoundError):
            ft2.load(store_path)

    def test_load(self):
        np.random.seed(15)
        df = pd.DataFrame(np.random.random(size=(1000, 2)), columns=['v1', 'v2'])
        # , 'cik_iid',
        df['cik_dts'] = pd.date_range(start='2020-01-01', periods=1000, freq='D')
        df['cik_ids'] = np.random.randint(0, 1500, 1000)

        f1 = FactorCreator('test', df, cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=['renamed_v1', 'renamed_v2'])

        df2 = df.copy(deep=True)
        # df2['cik_ids'] = np.random.randint(0, 1500, 1000)

        df2.to_hdf('test.h5', 'test2')
        f2 = FactorCreator('test2', 'test.h5', cik_dt='cik_dts', cik_id='cik_ids', factor_names=['v1', 'v2'],
                           as_alias=[None, 'renamed_v4'])

        ft = FactorTable()
        ft.add_factor(f1)
        ft.add_factor(f2)

        # cik_dt_list = df['cik_dts'].dt.strftime("%Y-%m-%d")
        # cik_id_list = list(set(df['cik_ids'].unique().tolist() + df2['cik_ids'].unique().tolist()))

        store_path = 'test_save.h5'
        ft2 = FactorTable()
        ft2.load(store_path)
        self.assertListEqual(ft.cik_dts, ft2.cik_dts)
        self.assertListEqual(ft.cik_ids, ft2.cik_ids)
        print(1)


if __name__ == '__main__':
    unittest.main()
