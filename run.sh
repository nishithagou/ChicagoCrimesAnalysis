#!/usr/bin/env sh

## Task 1 Command Line Code
spark-submit --class edu.ucr.cs.cs167.project3_code.BeastScala --master "local[*]" target/project3_code-1.0-SNAPSHOT.jar Chicago_Crimes_10k.csv Chicago_Crimes_ZIP

## Task 2 Command Line Code
## NOTE : Command line gives exception: NoClassDefFoundError: org/apache/spark/beast/SparkSQLRegistration$
## 	  we dont know how to add the depency through the command line, but it works in intellij.
spark-submit --class edu.ucr.cs.cs167.project3_code.BeastScala2 --master "local[*]" target/project3_code-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP

## Task 3 Command Line Code
## NOTE : Command line gives exception: NoClassDefFoundError: org/apache/spark/beast/SparkSQLRegistration$
## 	  we dont know how to add the depency through the command line, but it works in intellij.
spark-submit --class edu.ucr.cs.cs167.project3_code.BeastScala3 --master "local[*]" target/project3_code-1.0-SNAPSHOT.jar 01/01/2014 01/01/2019