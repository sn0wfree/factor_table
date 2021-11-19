# coding=utf-8
import pandas as pd


class LoadFileSettings(object):
    @staticmethod
    def load_csv(file_path):
        return pd.read_csv(file_path)

    @staticmethod
    def load_xslx(file_path):
        return pd.read_excel(file_path)


cosetting = dict(time_weight_dict={'PROFIT_WEIGHTED': {'PROFIT_RATE_1M': 1 / (1 + 1),
                                                       'PROFIT_RATE_3M': 1 / (1 + 3),
                                                       'PROFIT_RATE_6M': 1 / (1 + 6)}},

                 perf_dim_col=['SEL_TIME_CAL', 'SEL_STOCK_CAL', 'PROFIT_RATE_1M', 'PROFIT_RATE_3M', 'PROFIT_RATE_6M'],
                 dt_id_rl_cols=['TRADE_DT', 'CLIENT_ID', 'RISK_LEVEL'],
                 asset_allocation_col=['ASSET_QY', 'ASSET_GS', 'ASSET_CASH', 'ASSET_OTHER'],
                 recom_weight=None,
                 RISK_LEVEL_COL='RISK_LEVEL',
                 RISK_SCORE_NAME='ScoreRisk',
                 Pref_SCORE_NAME='ScorePref',

                 total_score_weight={'ScoreRisk': 0.5, 'ScorePref': 0.5},

                 dim_weight={'PROFIT_WEIGHTED': 0.8, 'SEL_TIME_CAL': 0.1, 'SEL_STOCK_CAL': 0.1})
if __name__ == '__main__':
    import yaml

    pass
