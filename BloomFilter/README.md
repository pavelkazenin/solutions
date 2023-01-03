# Introduction
The _BloomFilter_ project demonstrates Guava BloomFilter applications in Apache Spark Batch and Streaming ETLs

The ETL code generates `customers` initial and incremental datasets, builds and persists BloomFilter using 
Guava's BloomFilter class and measures False Positive Probability (FPP) of the filter.

# Development environment

* Java 11
* Scala 2.12.15
* Spark 3.3.0
* Maven 3.8.6
* IntelliJ IDEA 2022.2.2 w/ Scala plugin 2022.2.17

# Input Datasets and Output result

Input initial and incremental datasets are pipe '|' separated CSV files with headers:

```
Column Name     Column Type     Description
--------------- --------------- ------------------------
customer_id     integer         Customer ID     
first_name      string          First name
last_name       string          Last name
street          string          Street address
city            string          City
state           string          State
zip             string          Zip code
```

# Code compilation
To build the program run maven command in the directory where `pom.xml` file is:

`$ mvn clean install` 

It will compile the code, run unit scala tests and create "uber jar" executable

`target/bloomfilter-1.0-SNAPSHOT.jar` 

and copy the jar file into your local maven repository `~/.m2/repository`

# Sample ETL Execution

To run the program, copy uber jar file `bloomfilter-1.0-SNAPSHOT.jar` into your 
working directory and enter `spark-submit` command:

`$ spark-submit --class com.pavelkazenin.bloomfilter.JobRunner joins-1.0-SNAPSHOT.jar [options]`

In addition to normal Spark options like `-master`, `--conf`, etc, the program takes the following 
parameters:

```
-mode       operation mode, can be:
            generate  - generates initial and incremental customers datasets
            build     - builds and persists Bloom Filter for initial dataset
            filter    - apply Bloom Filter to incremental dataset in Batch mode
            stream    - apply Bloom Filter to incremental dataset in Stream mode
            
           
-initdata   directory for initial customers dataset
-incrdata   directory for incremental customers dataset
-filter     path to filter file
-fpp        false positive probability of the filter

-records    number of records in the customers initial and incremental datasets

-partitions number of partitions in the customers initial and incremental datasets
```

# Sample ETL Input and output results

* Customers:
```
customer_id|first_name|last_name|street|city|state|zip
2001|FirstName1|LastName1|Street Address 1|City1|S1|11111
```

* Results
```
==> Filter capacity 1000000, filter fpp 0.001000, measured fpp 0.000599
```

# Sample Streaming execution

* Customers:
```
customer_id|first_name|last_name|street|city|state|zip
2001|FirstName1|LastName1|Street Address 1|City1|S1|11111
```

* Console output
```
-------------------------------------------
Batch: 1
-------------------------------------------
+-----------+-------------+------------+--------------------+--------+---------+-------+
|customer_id|   first_name|   last_name|              street|    city|    state|    zip|
+-----------+-------------+------------+--------------------+--------+---------+-------+
|       2001|FirstName2001|LastName12001|Street Address 2001|City2001|State2001|Zip2001|
+-----------+-------------+------------+--------------------+--------+---------+-------+
only showing top 20 rows
```