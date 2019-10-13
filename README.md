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

### semantic scholar to postgres
#### benchmarking insertation speed 
* inserting ~24 million rows  
![speed](images/benchmarking_insertation_speed.png)


        with 1 processes inserted 23925694 rows in 5808.88 secs with speed of 4118.81 rows/sec
        with 2 processes inserted 23925694 rows in 3176.49 secs with speed of 7532.12 rows/sec
        with 4 processes inserted 23925694 rows in 1833.92 secs with speed of 13046.24 rows/sec
        with 8 processes inserted 23925694 rows in 1189.98 secs with speed of 20105.95 rows/sec
        with 12 processes inserted 23925694 rows in 1054.88 secs with speed of 22680.99 rows/sec