repo for SmokedDuck benchmark scripts and output results


## Install PostgresSQL

Source: https://www.geeksforgeeks.org/install-postgresql-on-linux/
```
$ sudod su postgres

$ psql

$ SELECT version(); 

                                                     version
------------------------------------------------------------------------------------------------------------------
 PostgreSQL 11.18 (Debian 11.18-0+deb10u1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 8.3.0-6) 8.3.0, 64-bit
(1 row)
```

## Install ProvSQL

 or use docker: https://www.simplilearn.com/tutorials/docker-tutorial/how-to-install-docker-on-ubuntu
```
$ git clone git@github.com:PierreSenellart/provsql.git
$ apt get libpq-dev, libboost-dev, postgresql-server-dev-11
$ cd provsql 
$ make
$ make install
$ sudo vim /etc/postgresql/11/main/postgresql.conf
add: ishared_preload_libraries = 'provsql'

$ sudo service postgresql restart

### Use PorvSQL
$ createdb provsqltest
$ psql
$ # \set ECHO none
$ # CREATE EXTENSION "uuid-ossp";
$ #CREATE EXTENSION provsql;
$ # CREATE SCHEMA provsql_test;
$ # create table zipf1 (idx int, z int, v float);
$ # copy zipf1 from '/home/.../zipfan_g10_card10000000_a1.csv' csv header;
$ # select add_provenance('zipf1');
$ # explain analyze select z, count(*) from zipf1 group by z;

```

## GProm setting:

$ apt-get install libreadline-dev ant default-jdk
