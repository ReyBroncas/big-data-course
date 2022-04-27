#!/bin/sh

source ./conf.sh

cql_create_query="
USE $kspace_name;
INSERT INTO $table1_name (id, author, song_name, release_year)
VALUES (0, 'Post Malone', 'Candy Paint', 2017);

INSERT INTO $table1_name (id, author, song_name, release_year)
VALUES (1, 'Rasmus Faber', 'Be Real', 2018);

INSERT INTO $table2_name (id, name, producer, release_year)
VALUES (0, 'The Martian', 'Ridley Scott', 2015);

INSERT INTO $table2_name (id, name, producer, release_year)
VALUES (1, 'The Gentleman', 'Guy Ritchie', 2019);

SELECT * FROM $table1_name;
SELECT * FROM $table2_name;"

function main() {
    docker exec "$cql_node" cqlsh -e "$cql_create_query"
}

main
