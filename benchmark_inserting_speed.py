import time
from pprint import pprint

from sqlalchemy import Column, Integer, String, Table, create_engine

from sqlalchemy.ext.declarative import declarative_base
from sqlutil.sqlalchemy_methods import get_tables_by_reflection


def get_sqlalchemy_base_engine(dburl='sqlite:///sqlalchemy.db'):
    sqlalchemy_base = declarative_base()
    Base = sqlalchemy_base

    sqlalchemy_engine = create_engine(dburl, echo=False)
    Base.metadata.bind = sqlalchemy_engine
    Base.metadata.create_all(sqlalchemy_engine)
    return Base,sqlalchemy_engine


def benchmark_sqlalchemy_core(
        dburl='sqlite:///sqlalchemy.db',
        n = 1000_000,

):
    sqlalchemy_base, sqlalchemy_engine = get_sqlalchemy_base_engine(dburl=dburl)

    columns = [Column('id', Integer, primary_key=True)] + [Column(colname, String(255)) for colname in ['name']]
    table_name = 'testtable'
    tables = get_tables_by_reflection(sqlalchemy_base.metadata, sqlalchemy_engine)
    pprint(tables)
    if table_name in tables:
        table = tables[table_name]
        table.drop(sqlalchemy_engine) #TODO: not working for mysql ?
    table = Table(table_name, sqlalchemy_base.metadata, *columns, extend_existing=True)
    print('creating table %s' % table.name)
    table.create()
    t0 = time.time()
    sqlalchemy_engine.execute(
        table.insert(),
        [{"name": 'NAME ' + str(i)} for i in range(n)]
    )
    print("database-url '%s' : inserting %d records took: %0.2f secs" % (dburl, n, time.time() - t0))


if __name__ == '__main__':

    benchmark_sqlalchemy_core(dburl='postgresql://postgres:postgres@localhost:5432/postgres')
    benchmark_sqlalchemy_core(dburl='mysql+mysqlconnector://mysql:mysql@localhost:3306/mysql')
    benchmark_sqlalchemy_core(dburl='sqlite:///sqlalchemy.db')
