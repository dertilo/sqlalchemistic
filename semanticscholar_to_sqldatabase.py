import multiprocessing
import pandas
from time import time, sleep
from typing import List

from sqlutil.sqlalchemy_methods import count_rows, get_tables_by_reflection, insert_if_not_existing, get_rows
from sqlutil.sqlalchemy_methods import create_sqlalchemy_base_engine
from util import util_methods, data_io
from sqlalchemy import Integer, Float, Table, String, Column, Boolean, select

from util.consume_with_pool import pool_consume

def populate_table(conn,table,data_g,batch_size=1000):
    column_names = [c.name for c in table.columns]
    data_g = (fill_missing_with_Nones(l, column_names) for l in data_g)
    counter = 0
    for batch in util_methods.iterable_to_batches(data_g,batch_size):
        conn.execute(table.insert(), batch)
        counter+=len(batch)
        yield counter

def fill_missing_with_Nones(l, column_names):
    return {k: l.get(k, None) for k in column_names + ['id']}

def build_consumer_supplier(table_name,limit,batch_size,dburl='postgresql://postgres:postgres@localhost:5432/postgres'): ## called in main
    def consumer_supplier(): ## called once on worker
        sqlalchemy_base, sqlalchemy_engine = create_sqlalchemy_base_engine(dburl)
        tables = get_tables_by_reflection(sqlalchemy_base.metadata, sqlalchemy_engine)
        table = tables[table_name]
        column_names = [c.name for c in table.columns]
        state_table = get_state_table(tables,table_name)

        def consumer(file:str):## called multiple times on worker
            with sqlalchemy_engine.connect() as conn:
                assert isinstance(file,str)
                num_to_skip = next(get_rows(conn, select([state_table]).where(state_table.c.file == file)))['line']
                print('%s skipping %d lines in file %s'%(multiprocessing.current_process(),num_to_skip,file))
                data_g = (fill_missing_with_Nones(d, column_names) for d in
                          data_io.read_jsonl(file, limit=limit, num_to_skip=num_to_skip))

                for count in populate_table(conn, table, data_g, batch_size=batch_size):
                    conn.execute(state_table.update().values(line=count+num_to_skip).where(state_table.c.file==file))

                if limit is None:
                    conn.execute(state_table.update().values(done=True).where(state_table.c.file == file))

        return consumer

    return consumer_supplier

def get_state_table(tables,table_name):
    return tables[table_name + 'state']

def create_or_expand_state_table(sqlalchemy_base, sqlalchemy_engine, table_name, files:List[str], from_scratch=False):
    tables = get_tables_by_reflection(sqlalchemy_base.metadata, sqlalchemy_engine)

    state_table_name = table_name + 'state'
    columns = [Column('file', String, primary_key=True), Column('line', Integer()),
               Column('done', Boolean())]
    state_table = Table(state_table_name, sqlalchemy_base.metadata, *columns, extend_existing=True)

    print(list(tables.keys()))
    if state_table_name in tables:
        if from_scratch:
            state_table = tables[state_table_name]
            print(state_table)
            try:
                state_table.drop(sqlalchemy_engine)
            except: #TODO: why is drop failing?
                pass
            state_table.create()
            print('recreated state-table')
        else:
            state_table = tables[state_table_name]
    else:
        state_table.create()
        print('created state-table')

    rows = [{'file':file,'line':0,'done':False} for file in files]
    insert_if_not_existing(sqlalchemy_engine,state_table,rows,primary_key_col='file')
    return state_table

def run_table_population(files, dburl='postgresql://postgres:postgres@localhost:5432/postgres', num_processes=1,
                         num_to_insert=10_000, benchmark_mode=False, batch_size=1_00):
    sqlalchemy_base, sqlalchemy_engine = create_sqlalchemy_base_engine(dburl)

    def table_supplier():
        table_name = 'semanticscholar'
        column_names = ['title', 'paperAbstract', 'year']
        columns = [Column('id', String, primary_key=True)] + [Column(colname, String(), nullable=True) for colname
                                                              in column_names]
        table = Table(table_name, sqlalchemy_base.metadata, *columns, extend_existing=True)
        return table, table_name

    tables = get_tables_by_reflection(sqlalchemy_base.metadata, sqlalchemy_engine)
    table, table_name = table_supplier()

    state_table = create_or_expand_state_table(sqlalchemy_base, sqlalchemy_engine, table_name, files,
                                               from_scratch=benchmark_mode)
    files = [f[0] for f in
             sqlalchemy_engine.execute(select([state_table.c.file]).where(state_table.c.done == False))]

    if table_name in tables:
        if benchmark_mode:
            try:
                tables[table_name].drop(sqlalchemy_engine)
            except:#TODO: why is drop failing?
                pass
            table.create()
        else:
            table = tables[table_name]
    else:
        table.create()

    print('populating: %s with %d processes' % (table_name, num_processes))
    start = time()
    if num_processes == 1:
        consumer = build_consumer_supplier(table_name, limit=num_to_insert, batch_size=batch_size)()
        [consumer(file) for file in files]
    else:
        pool_consume(
            data=files,
            consumer_supplier=build_consumer_supplier(table_name, limit=num_to_insert, batch_size=batch_size),
            num_processes=num_processes)

    if benchmark_mode:
        numrows = count_rows(sqlalchemy_engine, table)
        dur = time() - start
        speed = float(numrows) / dur
        print('with %d processes inserted %d rows in %0.2f secs with speed of %0.2f rows/sec' % (
            num_processes, numrows, dur, speed))
    else:
        speed = None  # due to skipping lines speed cannot be calculated
    return speed


if __name__ == "__main__":
    import os

    dburl = 'postgresql://postgres:postgres@localhost:5432/postgres'
    path = '/docker-share/data/semantic_scholar'
    files = [path + '/' + file_name for file_name in os.listdir(path) if file_name.startswith('s2') and file_name.endswith('.gz')]
    these_files = files[:12]

    benchmark_fun = lambda n:run_table_population(
                                     files=these_files,
                                     dburl=dburl,
                                     num_processes=n,
                                     num_to_insert=100_000,
                                     benchmark_mode=True,
                                     batch_size=1000)

    df = pandas.DataFrame(data=[{'num-cores':n,'speed':benchmark_fun(n)} for n in [1,2,4,8,12]])
    ax = df.plot.bar(x='num-cores',y='speed')
    ax.set_xlabel('number of processes')
    ax.set_ylabel('insertation speed in rows/sec')
    from matplotlib import pyplot as plt
    plt.savefig('benchmarking_insertation_speed.png')


