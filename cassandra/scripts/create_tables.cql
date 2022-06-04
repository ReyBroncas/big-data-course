CREATE KEYSPACE IF NOT EXISTS lab_8
    WITH REPLICATION = {
        'class' : 'SimpleStrategy', 'replication_factor' : 1
    };

USE lab_8;

CREATE TABLE IF NOT EXISTS "topTransactions" (
    "amount" float,
    "nameOrig" text,
    "oldbalanceOrg" float,
    "newbalanceOrig" float,
    "nameDest" text,
    "oldbalanceDest" float,
    "newbalanceDest" float,
    PRIMARY KEY ("nameOrig","amount"))
WITH CLUSTERING ORDER BY ("amount" DESC);

CREATE TABLE IF NOT EXISTS "fraudTransactions" (
    "amount" float,
    "nameOrig" text,
    "oldbalanceOrg" float,
    "newbalanceOrig" float,
    "nameDest" text,
    "oldbalanceDest" float,
    "newbalanceDest" float,
    "isFraud" boolean,
    PRIMARY KEY ("nameOrig","isFraud"))
WITH CLUSTERING ORDER BY ("isFraud" DESC);

CREATE TABLE IF NOT EXISTS "incomingTransactions" (
    "amount" float,
    "nameDest" text,
    "transaction_date" timestamp,
    PRIMARY KEY ("nameDest", "transaction_date"))
WITH CLUSTERING ORDER BY ("transaction_date" DESC);
