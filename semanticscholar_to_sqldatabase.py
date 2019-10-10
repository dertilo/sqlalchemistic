import multiprocessing
from pprint import pprint
from time import time

from multiprocessing_pooling import consume_parallel
from sqlutil.sqlalchemy_methods import count_rows, get_tables_by_reflection
from sqlutil.sqlalchemy_methods import create_sqlalchemy_base_engine
from util import util_methods, data_io
from sqlalchemy import Integer, Float, Table, String, Column

def populate_table(sqlalchemy_engine,table, data_g,batch_size=1000):
    data_g = (fill_missing_with_Nones(l, column_names) for l in data_g)
    with sqlalchemy_engine.connect() as conn:
        util_methods.consume_batchwise(lambda rows:conn.execute(table.insert(), rows), data_g, batch_size)

def fill_missing_with_Nones(l, column_names):
    return {k: l.get(k, None) for k in column_names + ['id']}

def build_consumer_supplier(limit,batch_size): ## called in main
    def consumer_supplier(): ## called once on worker
        # print(multiprocessing.current_process())
        sqlalchemy_base, sqlalchemy_engine = create_sqlalchemy_base_engine(
            dburl='postgresql://postgres:postgres@localhost:5432/postgres')

        table_name = 'semanticscholar'
        tables = get_tables_by_reflection(sqlalchemy_base.metadata, sqlalchemy_engine)
        assert table_name in tables
        table = tables[table_name]
        column_names = [c.name for c in table.columns]

        def consumer(file:str):## called multiple times on worker
            data_g = (fill_missing_with_Nones(d, column_names) for d in
                      data_io.read_jsonl(file, limit=limit, num_to_skip=0))
            populate_table(sqlalchemy_engine, table, data_g, batch_size=batch_size)

        return consumer
    return consumer_supplier

if __name__ == "__main__":
    import os

    path = '/docker-share/data/semantic_scholar'

    files = [path + '/' + file_name for file_name in os.listdir(path) if file_name.startswith('s2') and file_name.endswith('.gz')]
    max_num_processes=10
    these_files = files[:max_num_processes]
    limit = 100_000
    batch_size = 1_000

    column_names = ['title','paperAbstract','year']
    columns = [Column('id', String, primary_key=True)] + [Column(colname, String(),nullable=True) for colname in column_names]

    sqlalchemy_base, sqlalchemy_engine = create_sqlalchemy_base_engine(dburl='postgresql://postgres:postgres@localhost:5432/postgres')

    table_name = 'semanticscholar'
    tables = get_tables_by_reflection(sqlalchemy_base.metadata, sqlalchemy_engine)
    pprint(tables)

    def run_benchmark(num_processes=0):
        if table_name in tables:
            table = tables[table_name]
            table.drop(sqlalchemy_engine)
        table = Table(table_name, sqlalchemy_base.metadata, *columns, extend_existing=True)
        table.create()
        print('populating: %s with %d processes' % (table_name,num_processes))
        start = time()
        if num_processes==0:
            consumer = build_consumer_supplier(limit=limit, batch_size=batch_size)()
            [consumer(file) for file in these_files ]
        else:
            consume_parallel(these_files,
                             build_consumer_supplier(limit=limit, batch_size=batch_size),
                             num_processes=num_processes)

        numrows = count_rows(sqlalchemy_engine, table)
        dur = time() - start
        print('with %d processes inserted %d rows in %0.2f secs with speed of %0.2f rows/sec' % (
            num_processes, numrows, dur, float(numrows) / dur))

    run_benchmark(0)
    run_benchmark(10)
