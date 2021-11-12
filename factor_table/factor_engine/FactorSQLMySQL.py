# coding=utf-8

from factor_table.factor_engine import __FactorSQL__


def get_mysql_sql_create(cik_dt_list, cik_id_list, _factor_name_list, _alias, _cik, _db_table, _obj_type):
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
    f_names_list = [f if (a is None) or (f == a) else f"{f} as {a}" for f, a in
                    zip(_factor_name_list, _alias)]
    cols_str = ','.join(f_names_list)
    if len(cik_dt_list) <= 10:
        cik_dt_cond = f"{_cik.dts} in ( '" + "' , '".join(cik_dt_list) + "')"
    else:
        cik_dt_cond = f"{_cik.dts} > '{min(cik_dt_list)}'  "

    if cik_id_list is not None:
        if len(cik_id_list) <= 10:
            cik_id_cond = f"{_cik.ids} in ('" + "' , '".join(cik_id_list) + "') "
            conditions = f"{cik_dt_cond} and {cik_id_cond}"
        else:
            conditions = f"{cik_dt_cond} "
    else:
        cik_id_cond = None
        conditions = f"{cik_dt_cond} "
    if _obj_type.startswith('SQL'):
        sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids}  as cik_ids  from ({_db_table}) where {conditions}"
        sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from ({_db_table})"
        sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from ({_db_table})"
    else:
        sql = f"select {cols_str},{_cik.dts} as cik_dts, {_cik.ids}  as cik_ids  from {_db_table} where {conditions}"
        sql_cik_dts = f"select distinct {_cik.dts} as cik_dts from {_db_table}"
        sql_cik_ids = f"select distinct {_cik.ids} as cik_ids from {_db_table}"
    return sql, sql_cik_dts, sql_cik_ids


__FactorSQLMySQL__ = type('__FactorSQLMySQL__', (__FactorSQL__,),
                          {'get_sql_create': staticmethod(get_mysql_sql_create)})

if __name__ == '__main__':
    pass
