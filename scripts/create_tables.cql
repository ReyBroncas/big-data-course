CREATE KEYSPACE IF NOT EXISTS hw4_bek 
    WITH REPLICATION = {
        'class' : 'SimpleStrategy', 'replication_factor' : 1 
    };

USE hw4_bek;

CREATE TABLE IF NOT EXISTS reviewByProductId (
    marketplace	text,
    customer_id	bigint,
    review_id text,
    product_id text,
    product_parent bigint,
    product_title text,
    product_category text,
    star_rating	int,
    helpful_votes int,
    total_votes	int,
    vine boolean,
    verified_purchase boolean,
    review_headline	text,
    review_body	text,
    review_date date,
    PRIMARY KEY ((product_id), star_rating))
WITH CLUSTERING ORDER BY (star_rating DESC);

CREATE TABLE IF NOT EXISTS reviewByCustomerId (
    marketplace	text,
    customer_id	bigint,
    review_id text,
    product_id text,
    product_parent bigint,
    product_title text,
    product_category text,
    star_rating	int,
    helpful_votes int,
    total_votes	int,
    vine boolean,
    verified_purchase boolean,
    review_headline	text,
    review_body	text,
    review_date date,
    PRIMARY KEY (customer_id));

CREATE TABLE IF NOT EXISTS productForGivenTimePeriod (
    marketplace	text,
    customer_id	bigint,
    review_id text,
    product_id text,
    product_parent bigint,
    product_title text,
    product_category text,
    star_rating	int,
    helpful_votes int,
    total_votes	int,
    vine boolean,
    verified_purchase boolean,
    review_headline	text,
    review_body	text,
    review_date date,
    PRIMARY KEY ((product_id), review_date, verified_purchase))
WITH CLUSTERING ORDER BY (review_date DESC, verified_purchase DESC);


CREATE TABLE IF NOT EXISTS reviewByCustomerIdForGivenTimePeriod (
    marketplace	text,
    customer_id	bigint,
    review_id text,
    product_id text,
    product_parent bigint,
    product_title text,
    product_category text,
    star_rating	int,
    helpful_votes int,
    total_votes	int,
    vine boolean,
    verified_purchase boolean,
    review_headline	text,
    review_body	text,
    review_date date,
    PRIMARY KEY ((customer_id), review_date, star_rating))
WITH CLUSTERING ORDER BY (review_date DESC, star_rating DESC);
