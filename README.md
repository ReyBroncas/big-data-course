# big-data-course

### Explanation

## 1
the product_id is the PARTITION KEY in reviewByProductId table, making this query an optimal one
because by using a consistent hashing technique, cassandra momentarily identifies 
the exact node and exact partition range within a node in the cluster.

## 2
the product_id is the PARTITION key, and the star_rating is the CLUSTERING KEY in reviewByProductId table,
it's an efficient query because the clustering key provides the sort order of 
the data stored within a partition, and retrieval of the sorted data is very efficient

## 3
the customer_id is the PARTITION KEY in reviewByCustomerId table.
the same idea as with the query #1

## 4
the customerReviewCountByProductId is a table where the count of customer reviews is maintained, 
the product_id is the PARTITION KEY and the count is a CLUSTERING KEY by which the table is ordered;
thus the query is optimal.

## 5
the customer_id is the PARTITION KEY, and star_rating is the CLUSTERING KEY in reviewByCustomerId table,
similar to the idea in query #2

--- 
