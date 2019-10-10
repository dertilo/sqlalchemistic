# sqlalchemistic

### benchmarking insertation speed for different databases

output of [benchmark_inserting_speed.py](benchmark_inserting_speed.py)

    database-url 'postgresql://postgres:postgres@localhost:5432/postgres' : inserting 1000000 records took: 116.57 secs
    database-url 'mysql+mysqlconnector://mysql:mysql@localhost:3306/mysql' : inserting 1000000 records took: 16.92 secs
    database-url 'sqlite:///sqlalchemy.db' : inserting 1000000 records took: 3.90 secs

* postgres: 116 secs
* mysql: 17 secs
* sqlite: 4.9 secs

-> postgres is a fat elephant! 