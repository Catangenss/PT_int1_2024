FROM mysql:8.0

WORKDIR /db

COPY my.cnf /etc/mysql/conf.d/my.cnf
COPY world.sql /docker-entrypoint-initdb.d
COPY employees.sql /docker-entrypoint-initdb.d

COPY test_db-master .

RUN chown -R mysql:mysql /docker-entrypoint-initdb.d

CMD ["mysqld"]