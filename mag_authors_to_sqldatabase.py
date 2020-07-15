from sqlalchemy import Table, String, Column
from sqlutil.sqlalchemy_methods import create_sqlalchemy_base_engine
from semanticscholar_to_sqldatabase import run_table_population

if __name__ == "__main__":
    def build_table(sqlalchemy_base):
        table_name = 'authors'
        column_names = ['name', 'normalized_name','orgs','org','position','n_pubs','n_citation','h_index','tags.t','tags.w','pubs.i','pubs.r']
        columns = [Column('id', String, primary_key=True)] + [Column(colname, String(), nullable=True) for colname
                                                              in column_names]
        table = Table(table_name, sqlalchemy_base.metadata, *columns, extend_existing=True)
        return table

    import os

    dburl = 'postgresql://postgres:postgres@localhost:5432/postgres'
    # dburl = 'postgresql://postgres:postgres@guntherhamachi:5432/postgres'
    # path = '/home/tilo/gunther/data/MAG_authors'
    path = '/docker-share/data/MAG_authors'
    files = [path + '/' + file_name for file_name in os.listdir(path) if file_name.startswith('aminer_authors') and file_name.endswith('.gz')]
    these_files = files

    run_table_population(
        build_table=build_table,
        files=these_files,
        dburl=dburl,
        num_processes=8,
        num_to_insert=None,
        benchmark_mode=False,
        batch_size=1000)




