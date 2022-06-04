# big-data-course

## Setup

- run cassandra & kafka
    `./run-kafka-cluster.sh && ./run-cassandra-cluster.sh`
- run consumer:
    `./task.sh consumer`
- run producer:
    `./task.sh producer`
- run rest-service:
    `./task.sh rest-service`
- delete/stop
    `./shutdown-cluster.sh`

--- 

#### Example

- example:

    ![](/res/img1.png)

- docker containers status:

    ![](/res/img2.png)

- rest-service API usage:

    ![](/res/img3.png)
