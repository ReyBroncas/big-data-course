USE hw4_bek;

COPY reviewByProductId (
    marketplace,
    customer_id,
    review_id,
    product_id,
    product_parent,
    product_title,
    product_category,
    star_rating,
    helpful_votes,
    total_votes,
    vine,
    verified_purchase,
    review_headline,
    review_body,
    review_date ) 
FROM '/data/dataset.tsv' 
WITH DELIMITER='\t' AND HEADER=FALSE AND CHUNKSIZE=1 AND MAXBATCHSIZE=10;

COPY reviewByCustomerId (
    marketplace,
    customer_id,
    review_id,
    product_id,
    product_parent,
    product_title,
    product_category,
    star_rating,
    helpful_votes,
    total_votes,
    vine,
    verified_purchase,
    review_headline,
    review_body,
    review_date ) 
FROM '/data/dataset.tsv' 
WITH DELIMITER='\t' AND HEADER=FALSE AND CHUNKSIZE=1 AND MAXBATCHSIZE=10;

COPY productForGivenTimePeriod (
    marketplace,
    customer_id,
    review_id,
    product_id,
    product_parent,
    product_title,
    product_category,
    star_rating,
    helpful_votes,
    total_votes,
    vine,
    verified_purchase,
    review_headline,
    review_body,
    review_date ) 
FROM '/data/dataset.tsv' 
WITH DELIMITER='\t' AND HEADER=FALSE AND CHUNKSIZE=1 AND MAXBATCHSIZE=10;

COPY reviewByCustomerIdForGivenTimePeriod (
    marketplace,
    customer_id,
    review_id,
    product_id,
    product_parent,
    product_title,
    product_category,
    star_rating,
    helpful_votes,
    total_votes,
    vine,
    verified_purchase,
    review_headline,
    review_body,
    review_date ) 
FROM '/data/dataset.tsv' 
WITH DELIMITER='\t' AND HEADER=FALSE AND CHUNKSIZE=1 AND MAXBATCHSIZE=10;
