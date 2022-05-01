USE hw3_bek;

SELECT * FROM reviewByProductId WHERE product_id=<product_id>;

SELECT * FROM reviewByProductId WHERE product_id=<product_id> AND star_rating=<star_rating>;

SELECT * FROM reviewByCustomerId WHERE customer_id=<customer_id>;

SELECT customer_id FROM customerReviewCountByProductId WHERE product_id=<product_id> AND count><N>;

SELECT * FROM reviewByCustomerId WHERE customer_id=<customer_id> AND star_rating>=<star_rating>;
