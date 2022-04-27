#!/bin/sh

source ./conf.sh

cql_remove_query="
DROP KEYSPACE $kspace_name;"

cql_create_query="
CREATE KEYSPACE $kspace_name WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE $kspace_name;
CREATE TABLE $table1_name (
    id int,
    author text,
    song_name text,
    release_year int,
    PRIMARY KEY (id)
);
CREATE TABLE $table2_name (
    id int,
    name text,
    producer text,
    release_year int,
    PRIMARY KEY (id)
);
DESCRIBE TABLES;"

function main() {
    if [ "$1" == "remove" ]; then
        docker exec "$cql_node" cqlsh -e "$cql_remove_query"
    else
        docker exec "$cql_node" cqlsh -e "$cql_create_query"
    fi
}

main $@
