## Abstract

Writing a Spark Dataframe to MySQL is something you might want to do for a number of reasons. This is a demo project to show how this can be done in your localhost using JDBC connection. At first, Data is read from MySQL Database in Apache Spark Dataframe API and after data transformation write two tables back to MySQL Database.

## About Data

Data include the date-wise records of covid cases around the world till 2020-10-15. Check "data.txt" file to see schema.

## Methodology
* Load dependencies as shown in "built.sbt". 
* Creating JDBC connection to connect MySQL server.
* Loading table into Dataframe.
* Finding top 5 most covid infected countries.
* Creating a dataframe with normalized value.
* Filtering data for India.
* Writing both dataframe into MySQL Database.
