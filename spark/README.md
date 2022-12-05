# Spark PageRank
Calculate the PageRank using Spark. Provided both iteration approach and tolerance approach.

## Prerequisites
- Maven 3.6.3
- Java 11
- Spark 3.3.1

## How to run
- compile and build the jar, run `mvn package`
- run one of the following command
    - for single worker, use `local`, for multiple workers, use `local[x]`

    ``` bash
    spark-submit --class com.pinhaog2.cs511.PageRankIteration --master local[4] target/spark-1.0-SNAPSHOT.jar 2002 40
    spark-submit --class com.pinhaog2.cs511.PageRankTolerance --master local[4] target/spark-1.0-SNAPSHOT.jar 2002 1e-10
    ```