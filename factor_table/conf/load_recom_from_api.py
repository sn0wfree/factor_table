# coding=utf-8
import json

import pandas as pd
import requests

from WealthScore.conf import Config


def mapping_url(rl_list, url_base):
    for rl in rl_list:
        f1 = url_base.format(rl=rl)
        yield rl, f1


# def load_recom():
#     func = requests.get
#     rl_list = [1, 2, 3, 4, 5]
#     tasks_list = mapping_url(rl_list, url_base)
#     for risk_level, url in tasks_list:
#         res = func(url)
#
#         yield risk_level, res
#     pass


"""
CASE T.ASSET_TYPE
                WHEN '现金管理类' THEN
                'A1'
                WHEN '固定收益类' THEN
                'A2'
                WHEN '权益类' THEN
                'A3'
                WHEN '其他类' THEN
                'A4'
                ELSE
"""
replace_col = {"A1": '现金类',
               "A2": '固收类',
               "A3": '权益类',
               "A4": '其他类'
               }


def get_recom_info(risk_level, url_base=Config['recom_api']):
    resp = requests.get(url_base.format(rl=risk_level))

    res = json.loads(resp.content)
    res = pd.DataFrame(res['attach']['list'])
    res['ctaAssertType'] = res['ctaAssertType'].replace(replace_col)
    return res


def reshape_recom_info(res):
    r3 = res.rename(columns={'ctaAssertType': 'asset_cate', 'ctaRiskLevel': 'risk_level', 'ctaStaRatio': 'occup'})
    r3['risk_level_name'] = r3['risk_level'].replace(
        {'1': '低风险承受', '2': '中低风险承受', '3': '中风险承受', '4': '中高险承受', '5': '高风险承受'})
    r3['risk_level'] = r3['risk_level'].replace({'1': 'C1', '2': 'C2', '3': 'C3', '4': 'C4', '5': 'C5'})
    return r3[['risk_level', 'risk_level_name', 'asset_cate', 'occup']]


def get_recom_single(risk_level, url_base=Config['recom_api']):
    res = get_recom_info(risk_level, url_base=url_base)
    r4 = reshape_recom_info(res)
    return r4


def get_recom_general(url_base=Config['recom_api']):
    resc = get_recom_single(1, url_base=url_base)
    resc['risk_level'] = 'general'
    resc['risk_level_name'] = '通用'
    return resc


def get_recom(url_base=Config['recom_api'], **kwargs):
    red = [get_recom_single(i, url_base=url_base) for i in [1, 2, 3, 4, 5]]
    risk_level_df = pd.concat(red + [get_recom_general()])
    asset_translate = {'权益类': 'ASSET_QY', '固收类': "ASSET_GS", '现金类': "ASSET_CASH", '其他类': "ASSET_OTHER"}
    risk_level_df['asset_cate'] = risk_level_df['asset_cate'].replace(asset_translate)
    risk_level_df = risk_level_df[['risk_level', 'asset_cate', 'occup']].set_index(
        ['risk_level', 'asset_cate']).unstack(-1)
    risk_level_df.columns = risk_level_df.columns.levels[-1]

    return risk_level_df


if __name__ == '__main__':
    recom = get_recom()

    print(recom)

    pass
