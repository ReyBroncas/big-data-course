# big-data-course

## Setup

- install/run:
    `./cassandra.sh run`
- delete/stop
    `./cassandra.sh stop`

--- 

#### Examples

- Return all reviews for specified `product_id`:
    ![](/res/example_1.png)

- Return all reviews for specified `product_id` with given `star_rating`
    ![](/res/example_2.png)

- Return all reviews for specified customer_id
    ![](/res/example_3.png)

- Return N most reviewed items (by # of reviews) for a given period of time
    ![](/res/example_4.png)

- Return N most productive customers (by # of reviews written for verified purchases) for a given period
    ![](/res/example_5.png)

- Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period
    ![](/res/example_6.png)

- Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period
    ![](/res/example_7.png)

