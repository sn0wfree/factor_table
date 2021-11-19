# coding=utf-8
def singleton(cls):
    _instance = {}

    def inner(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]

    return inner

class GeneralConn(object):
    def __init__(self, config, odb):
        self.gp_conn = odb(config)

    def execute_sql(self, func):
        def deco(*args, **kwargs):
            ret = func(*args, **kwargs)
            if isinstance(ret, str) and ret.lower().startswith(('select ', 'desc ', 'show ', 'insert ')):
                return self.query(ret)
            else:
                return ret

        return deco

    def __call__(self, sql, ):
        return self.query(sql)

    def query(self, sql):
        return self.gp_conn.read_sql(sql)

    # @timer
    # def insert(self, db: str, table: str, data: pd.DataFrame, chunk_size=10000, method='normal', core=2):
    #     if method == 'normal':
    #         for d in process_bar(chunk(data, chunks=chunk_size)):
    #             self.gp_conn.insert_dataframe(table=table, data=d, schema=db)
    #     else:
    #         print('multiprocessing!')
    #         tasks = chunk(data, chunks=chunk_size)
    #         func = lambda x: self.gp_conn.insert_dataframe(table=table, data=x, schema=db)
    #         boost_up(func, tasks, core=core, method='Thread', star=False)

    def close(self, ):
        self.gp_conn.close()


@singleton
class SingletonConn(GeneralConn):
    pass


# Connections = GeneralConn()
if __name__ == '__main__':
    pass
