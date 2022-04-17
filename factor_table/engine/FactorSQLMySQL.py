# coding=utf-8

from factor_table.core.Factors import FactorSQL


def get_mysql_sql_create(cik_dt_list, cik_id_list, _factor_name_list, _alias, _cik, _db_table, _obj_type,
                         max_in_num=1000, other_conds=None):
    """

    :param cik_dt_list:
    :param cik_id_list:
    :param _factor_name_list:
    :param _alias:
    :param _cik:
    :param _db_table:
    :param _obj_type:
    :return:
    """
    f_names_list, cols_str = FactorSQL._build_f_names_list(_factor_name_list, _alias)
    # create cik_dt condition
    cik_dt_cond = FactorSQL._build_cik_dt_where_clause(cik_dt_list, _cik, max_in_num=max_in_num)
    # create cik_id condition
    cik_id_cond = FactorSQL._build_cik_id_where_clause(cik_id_list, _cik, max_in_num=max_in_num)
    conditions = FactorSQL._build_cond_clause(cik_id_cond, cik_dt_cond, other_conds=other_conds)

    if _obj_type.startswith('SQL'):
        sql_without_cond = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids}  as cik_ids  from ({_db_table}) "

    else:
        sql_without_cond = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids} as cik_ids  from {_db_table} "
    sql = sql_without_cond + conditions if conditions is not None else sql_without_cond
    sql_cik_dts = _build_cik_dts_sql(_db_table, _cik)
    sql_cik_ids = _build_cik_ids_sql(_db_table, _cik)
    return sql, sql_cik_dts, sql_cik_ids


def _build_cik_dts_sql(_db_table, _cik):
    sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from ({_db_table})"
    return sql_cik_dts


def _build_cik_ids_sql(_db_table, _cik):
    sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from {_db_table}"
    return sql_cik_ids


FactorSQLMySQL = type('FactorSQLMySQL', (FactorSQL,),
                      {'get_sql_create': staticmethod(get_mysql_sql_create),

                       })

if __name__ == '__main__':
    pass
