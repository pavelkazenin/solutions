# Introduction
The _Joins_ project demontrates various techniques to join two datasets using Apache Spark framework:

* PairRDD `combineByKey`
* PairRDD `groupByKey`
* SQL `groupBy`
* SQL `select`

The code reads two datasets `customers` and `orders` from '|' separated CSV files, joins them on `customer_id` field, 
computes the number of orders for each customer and writes `results` dataset as CSV '|' separated file.

# Development environment

* Java 11
* Scala 2.12.15
* Spark 3.3.0
* Maven 3.8.6
* IntelliJ IDEA 2022.2.2 w/ Scala plugin 2022.2.17

# Input and Output Datasets

Input and output datasets are pipe '|' separated CSV files with headers:

```
Dataset      Column Name     Column Type     Description
------------ --------------- --------------- ------------------------
customera    customer_id     integer         Customer ID     
customers    first_name      string          First name
customers    last_name       string          Last name
customers    street          string          Street address
customers    city            string          City
customers    state           string          State
customers    zip             string          Zip code

orders       order_id        integer         Order ID
orders       customer_id     integer         Customer ID
orders       product_id      integer         Product ID
orders       quantity        integer         Quantity of items in the order
orders       order_date      timestamp       Order date

results      customer_id     integer         Customer ID
results      first_name      integer         First Name
results      last_name       integer         Last Name
results      count           integer         Number of orders
```

# Code compilation
To build the program run maven command in the directory where `pom.xml` file is:

`$ mvn clean install` 

It will compile the code, run unit scala tests and create "uber jar" executable

`target/joins-1.0-SNAPSHOT.jar` 

and copy the jar file into your local maven repository `~/.m2/repository`

# Sample Execution

To run the program, copy uber jar file `joins-1.0-SNAPSHOT.jar` into your 
working directory and enter `spark-submit` command:

`$ spark-submit --class com.pavelkazenin.joins.JobRunner joins-1.0-SNAPSHOT.jar [options]`

In addition to normal Spark options like `-master`, `--conf`, etc, the program takes the following 
parameters:

```
-mode       operation mode, can be:
            generate  - generates random customers and orders datasets
            sql       - uses Spark SQL interpreter to parse SELECT command
            join      - uses Spark SQL join and groupBy methods
            group     - uses Spark PairRDD groouByKey method
            combine   - uses Spark PairRDD combineByKey method
           
-customers  directory for customers CSV files
-orders     directory for orders CSV files
-results    directory for results CSV files


-records    number of records in the customers and orders datasets,
            applicable to the generate mode only


-partitions number of partitions in the customers and orders datasets,
            applicable to the generate mode only
```

# Sample Input and Output datasets

* Customers:
```
customer_id|first_name|last_name|street|city|state|zip
2001|FirstName1|LastName1|Street Address 1|City1|S1|11111
```

* Orders:
```
order_id|customer_id|product_id|quantity|order_date
200101|2001|10001|1|2022-02-04 13:30:21
```

* Results
```
customer_id|first_name|last_name|count
2002|FirstName2|LastName2|4
```
