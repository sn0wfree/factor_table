# # -*- coding: utf-8 -*-
# """
# Created on Wed Jun 17 15:16:00 2020
#
# @author: user
# """
#
# import logging
# from gjdata import PostgresDB
# from gjdata import OracleDB
# from project_name import settings
# gpdb = PostgresDB(settings.DATABASES['GP'])
# db = OracleDB(settings.DATABASES['DW'])
#
# logger = logging.getLogger("proj")
#
#
# def process(init_date):
#   pass
#
#
#
# def finish(init_date):
#    # ------------------------ 数据完成标志位 ------------------------  #
#     data_complete = dict()
#     data_complete['init_date'] = init_date
#     data_complete['occurdate'] = None
#     data_complete['object_id'] = 'm_cfw.t_cust_prefer_label_h'
#     data_complete['object_name'] = 'm_cfw.t_cust_prefer_label_h'
#     data_complete['data_status'] = '1'
#     data_complete['dw_clt_date'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#     db.insert_one("t_data_complete", data=data_complete, schema="comm_data")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
