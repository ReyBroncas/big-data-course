CREATE KEYSPACE hw3_bek WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE hw3_bek;

CREATE TABLE IF NOT EXISTS reviewByProductId (
    marketplace	text,
    customer_id	bigint,
    review_id bigint,
    product_id bigint,
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
    review_id bigint,
    product_id bigint,
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
    PRIMARY KEY ((customer_id), star_rating))
WITH CLUSTERING ORDER BY (star_rating DESC);

CREATE TABLE IF NOT EXISTS customerReviewCountByProductId (
    product_id bigint,
    customer_id	bigint,
    count int,
    PRIMARY KEY ((product_id), count))
WITH CLUSTERING ORDER BY (count DESC);
