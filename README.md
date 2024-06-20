# CASARNDA
wget https://dlcdn.apache.org/cassandra/4.0.12/apache-cassandra-4.0.12-bin.tar.gz
tar -xvf apache-cassandra-4.0.12-bin.tar.gz

vi ~/.bashrc

export CASSANDRA_HOME=/home/hadoop/apache-cassandra-4.0.12

export PATH=$PATH:$CASSANDRA_HOME/bin

:wq

source ~/.bashrc

//start cassanra server first (don't close this terminal)

cassandra -f

//to practice cassandra use this command open in another terminal (java 8 and python 2.7 required if ur using in windows)

cqlsh (in another terminal)

if u run cqlsh without start cassandra -f u ll get this error

Connection error: ('Unable to connect to any servers', {'127.0.0.1': error(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})

[
#if u get such error first start casssandra first using (cassandra -f ) next start cqlsh in another terminal
#if u get cqlsh> in terminal cqssandra working fine

// in mysql /oracle u have ... database ... same way cassandra use keyspace.
https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md
https://github.com/datastax/spark-cassandra-connector/blob/master/doc/15_python.md

CREATE KEYSPACE cassdb

    WITH REPLICATION = {
        'class': 'SimpleStrategy', 
        'replication_factor': 3
    };

CREATE KEYSPACE tutorialspoint

WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor' : 3};

DESCRIBE KEYSPACES

USE cassdb
// in cassandra keyspace is mandatory ... cassandra always revolves around this primary key concept.
#if u want to create table , its mandatory use primary key otherwise its impossible to do anything in cassandra.

create table asl (

id INT PRIMARY KEY,
name varchar,
city varchar

);

create table asl1 (

id INT,
name varchar,
city varchar

);

insert into asl (id, name, city) values(1,'venu','hyderabad');

insert into asl (id, name, city) values(2,'sita','hyderabad');

insert into asl (id, name, city) values(3,'subbu','hyderabad');

insert into asl (id, name, city) values(4,'brahma','hyderabad');

insert into asl (id, name, city) values(5,'nirmal','bangalore');

insert into asl (id, name, city) values(6,'nischal','bangalore');

insert into asl (id, name, city) values(7,'pramod','bangalore');

insert into asl (id, name, city) values(8,'anu','hyderabad');

insert into asl (id, name, city) values(9,'ali','chennai');

insert into asl (id, name, city) values(10,'koti','chennai');

insert into asl (id, name, city) values(11,'venkat','chennai');
	
select * from asl where city='hyderabad';

//cassandra if u want to do anything u can do it only on primery key (id) other columns no use ..

 update asl set name='nnnnnnn' where id=5;
 
//if u want to do anything u can do only on top of primary key column. other columns impossible to do anything;

wget https://mvnrepository.com/artifact/com.twitter/jsr166e/1.1.0

  CREATE TABLE emp (
  
  empID int,
  deptID int,
  first_name varchar,
  last_name varchar,
  PRIMARY KEY (empID, deptID));
 
 
 
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (104, 15, 'sita', 'smith');
  
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (105, 16, 'venu', 'smitha');
  
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (106, 17, 'vignesh', 'sam');
  
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (106, 18, 'john', 'smith');
  
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (107, 19, 'andrew', 'amit');
  
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (108, 20, 'somesh', 'lendra');
  
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (109, 21, 'sachin', 'moman');
  
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (110, 22, 'kohli', 'desire');
  
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (111, 23, 'rohan', 'rohit');
  
  INSERT INTO emp (empID, deptID, first_name, last_name)
  VALUES (112, 24, 'hardik', 'pandya');

site:mvnrepository.com <something-error>
site:mvnrepository.com org.reactivestreams.Publisher

#if u want to join asl, emp tables its impossible. There is no join concepts in Cassandra.

#https://stackoverflow.com/questions/17248232/how-to-do-a-join-queries-with-2-or-more-tables-in-cassandra-cql

DataStax is a vendor to cassandra. modified cassandra little bit and released their own product called DataStax cassandra. its 90% same like cassandra.

wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.11/3.1.0/spark-cassandra-connector_2.11-3.1.0.jar

sudo mv spark-cassandra-connector_2.12-3.1.0.jar  /usr/lib/spark/jars/

if u r using emr spark 2.4.8 simply use these dependencies working fine

if u use spark 3.1.2 use these dependencies

https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.14.1/java-driver-core-4.14.1.jar

wget https://mvnrepository.com/artifact/com.datastax.oss/java-driver-shaded-guava/25.1-jre/java-driver-shaded-guava-25

https://mvnrepository.com/artifact/com.typesafe/config/1.4.2

https://mvnrepository.com/artifact/com.datastax.oss/native-protocol/

https://mvnrepository.com/artifact/org.reactivestreams/reactive-streams/1.0.3


wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.1.0/spark-cassandra-connector_2.12-3.1.0.jar

sudo mv spark-cassandra-connector_2.11-3.1.0.jar  /usr/lib/spark/jars/

https://mvnrepository.com/artifact/com.datastax.oss/java-driver-query-builder/4.15.0

.write.mode("append").partitionBy("name").saveAsTable("myCatalog.cassdb.joinempasl")
	
////////////sample code

from pyspark.sql import *

from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("spark.sql.catalog.myCatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")

ddf=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option("table", "asl").load()

edf=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option("table", "emp").load()

ddf.show()

edf.show()

jdf = ddf.join(edf, edf.first_name==ddf.name, "inner").drop("name")

jdf.show()

jdf.write.mode("append").partitionBy("first_name").saveAsTable("myCatalog.cassdb.joinempasl")

#here myCatalog.cassdb.joinempasl ... cassdb is a cassandra keyspacename (u created earlier use it)...joinempasl is a table name means what table i want to specify in cassandra .. give any name 

#partitionBy ... its primary key (in cassandra primary key is mandatory so use it ..

native-protocol-1.5.1.jar

config-1.4.2.jar                       pycass.py

java-driver-core-4.14.1.jar            reactive-streams-1.0.3.jar

java-driver-query-builder-4.15.0.jar   spark-cassandra-connector_2.12-3.1.0.jar

java-driver-shaded-guava-25.1-jre.jar  spark-cassandra-connector-driver_2.12-3.1.0.jar

import sys

import os

import findspark

findspark.init()

os.environ["JAVA_HOME"]="/usr/lib/jvm/java"

os.environ["SPARK_HOME"]="/usr/lib/spark"

from pyspark.sql import *

from pyspark.sql.functions import *

import re

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

adf=spark.read.format("org.apache.spark.sql.cassandra").option("table","asl").option("keyspace","cassdb").load()

adf.show()
edf=spark.read.format("org.apache.spark.sql.cassandra").option("table","emp").option("keyspace","cassdb").load()

edf.show()

#join=adf.join(edf,edf.first_name==adf.name,"fullouter").drop("first_name").na.fill("No Data",["city","name"]).na.fill(0,["id"])

join=adf.join(edf,edf.first_name==adf.name,"fullouter").drop("first_name").na.fill("No Data").na.fill(0)

#inner ... common records, .. leftouter ... get all left side elements but right side u ll get nulls mostly/other unmatched records

#rightouter  get all records from right side and mismatched records ll get nulls

#fullouter ... get all records from left and right and display all records from left and right

#if both dataframe having same column name at that time use join=adf.join(edf,"name","inner")

#by default its inner join ... or use join=adf.join(edf,edf.first_name==adf.name, "inner")
join.show()

join.write.mode("append").format("org.apache.spark.sql.cassandra").option("table","aslempjoin").option("keyspace","cassdb").save()//////////////////////////////////////////


create table employees_tbl (

id INT AUTO_INCREMENT PRIMARY KEY,
name varchar(20),
dept varchar(10),
salary int(10)

);

insert into employees_tbl values(100,'Michael','Sales',5500);

insert into employees_tbl values(200,'Cassandra','Technology',6000);

insert into employees_tbl values(300,'Samuel','Technology',7000);

insert into employees_tbl values(400,'John','Marketing',9500);

insert into employees_tbl values(500,'Beth','Technology',6500);

insert into employees_tbl values(600,'Peter','HR',5000);

insert into employees_tbl values(700,'Romeo','Legal',5400);


usecase 2

CREATE KEYSPACE venuks

     WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
     
use venuks

CREATE TABLE temperature_by_day (

   weatherstation_id text,
   date text,
   event_time timestamp,
   temperature float,
   PRIMARY KEY ((weatherstation_id,date),event_time)
   
);

INSERT INTO temperature_by_day(weatherstation_id,date,event_time,temperature)
VALUES ('1234WXYZ','2016-04-03','2016-04-03 07:01:00',73);
 
INSERT INTO temperature_by_day(weatherstation_id,date,event_time,temperature)
VALUES ('1234WXYZ','2016-04-03','2016-04-03 07:02:00',70);
 
INSERT INTO temperature_by_day(weatherstation_id,date,event_time,temperature)
VALUES ('1234WXYZ','2016-04-04','2016-04-04 07:01:00',73);
 
INSERT INTO temperature_by_day(weatherstation_id,date,event_time,temperature)
VALUES ('1234WXYZ','2016-04-04','2016-04-04 07:02:00',74);

SELECT weatherstation_id, date, MAX(temperature) FROM temperature_by_day GROUP BY weatherstation_id, date;

SELECT weatherstation_id, date, MAX(temperature) FROM temperature_by_day GROUP BY weatherstation_id, date, event_time;


usecase 3
//Cassandra Aggregates - min, max, avg, group by

CREATE TABLE Emp_record
 (
  E_id int PRIMARY KEY,
  E_score int,
  E_name text,
  E_city text
 );
INSERT INTO Emp_record(E_id, E_score, E_name, E_city)
       values (101, 85, 'ashish', 'Noida');
       
INSERT INTO Emp_record(E_id, E_score, E_name, E_city)
       values (102, 90, 'ankur', 'meerut');
       
INSERT INTO Emp_record(E_id, E_score, E_name, E_city)
       values (103, 99, 'shivang', 'gurugram');
       
INSERT INTO Emp_record(E_id, E_score, E_name, E_city)
       values (104, 85, 'abi', 'meerut');
       
INSERT INTO Emp_record(E_id, E_score, E_city)
       values (105, 95, 'mumbai');

select * from Emp_record;  

SELECT COUNT(*) FROM Emp_record;

SELECT COUNT(E_name) FROM Emp_record;

SELECT MIN(E_score) FROM Emp_record;

SELECT MAX(E_score) FROM Emp_record;

SELECT SUM(E_score)  FROM Emp_record;

SELECT AVG(E_score) FROM Emp_record;

ucse case 4

CREATE TABLE IF NOT EXISTS driver (

  driver_name TEXT,
  password TEXT,
  mobile INT,
  current_position TEXT,
  skill SET<TEXT>,
  PRIMARY KEY (driver_name)
);

CREATE INDEX IF NOT EXISTS driver_skill_index ON tranz.driver (skill);

CREATE INDEX IF NOT EXISTS driver_current_position_index ON tranz.driver (current_position);

-- See Question 2, Read 6.

CREATE INDEX driver_password_index ON driver (password);

-- 4.1 Create a table for vehicles. Table name: `vehicle`. Columns: `vehicle_id` (string, unique, if not exists), `status` (string), type (string)

CREATE TABLE IF NOT EXISTS vehicle (

  vehicle_id TEXT,
  status TEXT,
  type TEXT,
  PRIMARY KEY (vehicle_id)
);

-- We will filter vehicles by status. See Question 2, Read 5.

CREATE INDEX vehicle_status_index on vehicle (status);

-- 6.1 Create a table for timetables. Table name: time_table. Columns: line_name (unique, if not exists, string), service_no (number, asc within line_name), station_name (string), latitude (double), longitude (double), time (int), distance (double),  Notes: time are departure times, except the last (destination) time, it is arrival time. Sorted asc by time.

CREATE TABLE IF NOT EXISTS time_table (
  line_name TEXT,
  service_no INT,
  station_name TEXT,
  longitude DOUBLE,
  latitude DOUBLE,
  time INT,
  distance DOUBLE,
  PRIMARY KEY (line_name, service_no, time)
) WITH CLUSTERING ORDER BY (service_no ASC, time ASC);

-- 7.1 Need a vehicle_usage table for logging vehicle usage. Administrator can create this table with the following columns: vehicle_id, total_distance (counter).

CREATE TABLE IF NOT EXISTS vehicle_usage (
  vehicle_id TEXT,
  total_distance COUNTER,
  PRIMARY KEY (vehicle_id)
);

-- 8. Recording data points after the vehicle's engine started.

-- 8.1 Administrator create a table. Table name: data_point. Columns: day (int), sequence (timestamp), latitude (double), longitude (double), speed (double)

CREATE TABLE IF NOT EXISTS data_point (
  day INT,
  sequence TIMESTAMP,
  longitude DOUBLE,
  latitude DOUBLE,
  speed DOUBLE,
  PRIMARY KEY (day, sequence, longitude, latitude)
);

-- Question 2, 1. Read the number of working days of a driver. (Payroll will use this information.). App collects this information in a separate table. Table name: driver_working_days, Columns: driver_name (unique, string), working_day (counter). This is a counter table and the app will update the counter, when the driver starts to work.

CREATE TABLE IF NOT EXISTS driver_working_days (
  driver_name TEXT,
  working_day COUNTER,
  PRIMARY KEY (driver_name)
);

CONSISTENCY QUORUM;
INSERT INTO driver
  (driver_name, current_position, mobile, password, skill )
  VALUES ('milan', 'Upper Hutt', 211111, 'mm77', { 'Matangi' })
  IF NOT EXISTS;

CONSISTENCY QUORUM;

INSERT INTO driver
  (driver_name, current_position, mobile, password, skill )
  VALUES ('pavle', 'Upper Hutt', 213344, 'pm33', { 'Ganz Mavag', 'Guliver' })
  IF NOT EXISTS;

CONSISTENCY QUORUM;

INSERT INTO driver
  (driver_name, current_position, mobile, password, skill )
  VALUES ('pondy', 'Wellington', 216677, 'pondy', { 'Matangi', 'Kiwi Rail' })
  IF NOT EXISTS;

CONSISTENCY QUORUM;
INSERT INTO driver
  (driver_name, current_position, mobile, password, skill )
  VALUES ('fred', 'Taita', 210031, 'f5566f', { 'Gulliver', 'Ganz Mavag' })
  IF NOT EXISTS;

CONSISTENCY QUORUM;
INSERT INTO driver
  (driver_name, current_position, mobile, password, skill )
  VALUES ('jane', 'Waikanae', 213141, 'jjjj', { 'Matangi' })
  IF NOT EXISTS;

-- 3.1 Drivers can change their password. They provide `old_password` and `new_password`. Update the driver's row with `new_password` only if the `old_password` equal with the stored `password`. If the conditions apply, `password` will be equal with `new_password`.
CONSISTENCY QUORUM;

UPDATE driver
  SET password = 'dhy@@EE3#'
  WHERE driver_name = 'pondy'
  IF password = 'pondy';

-- 3.2 Drivers can update their `current_position`: (with city name string) `'Wellington'` OR (with vehicle) `vehicle_id` OR (with not available string constant) `'not_available'`. The update process managed by the app, based on the driver's skill and the location of the train. See Question 2, Read 5.

CONSISTENCY QUORUM;

UPDATE driver
  SET current_position = 'Petone'
  WHERE driver_name = 'pavle';


-- 3.3 Drivers can add new skill to `skill` column. Skill column type is `SET<string>`.

CONSISTENCY QUORUM;
UPDATE driver
  SET skill = skill + {'Ganz Mavag'}
  WHERE driver_name = 'milan';


-- 3.4 App updates a counter log table for payrol. See Question 2, Read 1.

CONSISTENCY QUORUM;
UPDATE driver_working_days
  SET working_day = working_day + 1
  WHERE driver_name = 'jane';

-- 4.2 Seed the initial vehicles data.
/*
=============================================
vehicle_id        status          type
---------------------------------------------
FA1122            Upper Hutt      Matangi
FP8899            maintenance     Ganz Mavag
FA4864            Wellington      Matangi
KW3300            Wellington      KiwiRail
=============================================
*/

CONSISTENCY QUORUM;

INSERT INTO vehicle
  (vehicle_id, status, type )
  VALUES ('FA1122', 'Upper Hutt', 'Matangi')
  IF NOT EXISTS;

CONSISTENCY QUORUM;

INSERT INTO vehicle
  (vehicle_id, status, type )
  VALUES ('FP8899', 'maintenance', 'Ganz Mavag')
  IF NOT EXISTS;

CONSISTENCY QUORUM;

INSERT INTO vehicle
  (vehicle_id, status, type )
  VALUES ('FA4864', 'Wellington', 'Matangi')
  IF NOT EXISTS;

CONSISTENCY QUORUM;

INSERT INTO vehicle
  (vehicle_id, status, type )
  VALUES ('KW3300', 'Wellington', 'KiwiRail')
  IF NOT EXISTS;

-- 5. App automatically updates the `status` of a `vehicle`. Station name, like `Wellington` OR `in_use` OR `maintenance` OR `out_of_order`.

-- 5.1 Status will be updated based on timetable (departure). `Status` will be the departure station name. See Question 2, Read 4.

CONSISTENCY QUORUM;

UPDATE vehicle
  SET status = 'Wellington'
  WHERE vehicle_id = 'FA1122';

--   5.2 `Status` will be updated to `in_use`, when driver turns on the engine.

CONSISTENCY QUORUM;

UPDATE vehicle
  SET status = 'in_use'
  WHERE vehicle_id = 'KW3300';

-- 5.3 Status will be updated when the driver turns off the engine on the destination station. `status` will be the destination station name.

-- A log event will be called also, see Question 1, Update 7.2.

CONSISTENCY QUORUM;

UPDATE vehicle
  SET status = 'Upper Hut'
  WHERE vehicle_id = 'FA4864';

-- 6.2 Seed `time_table`.
/*
============================================================================================
line_name,               service_no, station_name, longitude, latitude, time, distance
--------------------------------------------------------------------------------------------
'Hutt Valley Line (north bound)', 1, 'Wellington', 174.7762, -41.2865, 605, 0
'Hutt Valley Line (north bound)', 1, 'Petone', 174.8851, -41.227, 617, 8.3
'Hutt Valley Line (north bound)', 1, 'Waterloo', 174.9081, -41.2092, 625, 13.3
'Hutt Valley Line (north bound)', 1, 'Taita', 174.9608, -41.1798, 634, 19.0
'Hutt Valley Line (north bound)', 1, 'Silverstream', 175.010276, -41.147283, 642, 26.5
'Hutt Valley Line (north bound)', 1, 'Upper Hutt', 175.0708, -41.1244, 650, 34.3
'Hutt Valley Line (north bound)', 11, 'Wellington', 174.7762, -41.2865, 1935, 0
'Hutt Valley Line (north bound)', 11, 'Petone', 174.8851, -41.227, 1947, 8.3
'Hutt Valley Line (north bound)', 11, 'Woburn', 174.911249, -41.220841, 1950, 11.0
'Hutt Valley Line (north bound)', 11, 'Waterloo', 174.9081, -41.2092, 1955, 13.3
'Hutt Valley Line (north bound)', 11, 'Naenae', 174.946160, -41.197805, 2001, 16.9
'Hutt Valley Line (north bound)', 11, 'Taita', 174.9608, -41.1798, 2010, 19.0
'Hutt Valley Line (north bound)', 11, 'Silverstream', 175.010276, -41.147283, 2019, 26.5
'Hutt Valley Line (north bound)', 11, 'Upper Hutt', 175.0708, -41.1244, 2025, 34.3
'Hutt Valley Line (south bound)', 2, 'Upper Hutt', 175.0708, -41.1244, 700, 0
'Hutt Valley Line (south bound)', 2, 'Silverstream', 175.010276, -41.147283, 708, 7.8
'Hutt Valley Line (south bound)', 2, 'Taita', 174.9608, -41.1798, 716, 15.03
'Hutt Valley Line (south bound)', 2, 'Woburn', 174.911249, -41.220841, 725, 23.3
'Hutt Valley Line (south bound)', 2, 'Wellington', 174.7762, -41.2865, 745, 34.3
'Hutt Valley Line (south bound)', 12, 'Upper Hutt', 175.0708, -41.1244, 1900, 0
'Hutt Valley Line (south bound)', 12, 'Silverstream', 175.010276, -41.147283, 1907, 7.8
'Hutt Valley Line (south bound)', 12, 'Taita', 174.9608, -41.1798, 1918, 15.03
'Hutt Valley Line (south bound)', 12, 'Naenae', 174.946160, -41.197805, 1927, 17.04
'Hutt Valley Line (south bound)', 12, 'Waterloo', 174.9081, -41.2092, 2028, 21.0
'Hutt Valley Line (south bound)', 12, 'Woburn', 174.911249, -41.220841, 2030, 23.3
'Hutt Valley Line (south bound)', 12, 'Petone', 174.8851, -41.227, 2035, 26.0
'Hutt Valley Line (south bound)', 12, 'Wellington', 174.7762, -41.2865, 2050, 34.3
'Waikanae Line (north bound)', 5, 'Wellington', 174.7762, -41.2865, 1025, 0
'Waikanae Line (north bound)', 5, 'Paekakariki', 174.951, -40.9881, 1059, 33.1
'Waikanae Line (north bound)', 5, 'Paraparaumu', 175.0084, -40.9142, 1118, 51.3
'Waikanae Line (north bound)', 5, 'Waikanae', 175.0668, -40.8755, 1139, 62.8
===========================================================================================
*/

-- Based on consistency requirement, timetable data can be eventually consistent.

CONSISTENCY ONE;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 1, 'Wellington', 174.7762, -41.2865, 605, 0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 1, 'Petone', 174.8851, -41.227, 617, 8.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 1, 'Waterloo', 174.9081, -41.2092, 625, 13.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 1, 'Taita', 174.9608, -41.1798, 634, 19.0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 1, 'Silverstream', 175.010276, -41.147283, 642, 26.5) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 1, 'Upper Hutt', 175.0708, -41.1244, 650, 34.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 11, 'Wellington', 174.7762, -41.2865, 1935, 0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 11, 'Petone', 174.8851, -41.227, 1947, 8.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 11, 'Woburn', 174.911249, -41.220841, 1950, 11.0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 11, 'Waterloo', 174.9081, -41.2092, 1955, 13.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 11, 'Naenae', 174.946160, -41.197805, 2001, 16.9) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 11, 'Taita', 174.9608, -41.1798, 2010, 19.0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 11, 'Silverstream', 175.010276, -41.147283, 2019, 26.5) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (north bound)', 11, 'Upper Hutt', 175.0708, -41.1244, 2025, 34.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 2, 'Upper Hutt', 175.0708, -41.1244, 700, 0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 2, 'Silverstream', 175.010276, -41.147283, 708, 7.8) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 2, 'Taita', 174.9608, -41.1798, 716, 15.03) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 2, 'Woburn', 174.911249, -41.220841, 725, 23.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 2, 'Wellington', 174.7762, -41.2865, 745, 34.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 12, 'Upper Hutt', 175.0708, -41.1244, 1900, 0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 12, 'Silverstream', 175.010276, -41.147283, 1907, 7.8) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 12, 'Taita', 174.9608, -41.1798, 1918, 15.03) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 12, 'Naenae', 174.946160, -41.197805, 1927, 17.04) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 12, 'Waterloo', 174.9081, -41.2092, 2028, 21.0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 12, 'Woburn', 174.911249, -41.220841, 2030, 23.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 12, 'Petone', 174.8851, -41.227, 2035, 26.0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Hutt Valley Line (south bound)', 12, 'Wellington', 174.7762, -41.2865, 2050, 34.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Waikanae Line (north bound)', 5, 'Wellington', 174.7762, -41.2865, 1025, 0) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Waikanae Line (north bound)', 5, 'Paekakariki', 174.951, -40.9881, 1059, 33.1) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Waikanae Line (north bound)', 5, 'Paraparaumu', 175.0084, -40.9142, 1118, 51.3) IF NOT EXISTS;

INSERT INTO time_table (line_name, service_no, station_name, longitude, latitude, time, distance) VALUES ('Waikanae Line (north bound)', 5, 'Waikanae', 175.0668, -40.8755, 1139, 62.8) IF NOT EXISTS;

-- 7. Recording the travelled distance of a vehicle.

-- 7.2 This log will run after the app updated the vehicle `status`. See Question 1, Update 5.3. Distance information comes from Question 2, Read 7.

-- Vehicle related data has to be strongly consistent.

CONSISTENCY QUORUM;

UPDATE vehicle_usage
  SET total_distance = total_distance + 34
  WHERE vehicle_id = 'FA1122';

-- 8. Recording data points after the vehicle's engine started.

-- 8.2 The app creates a new log entry in this table in every 10 seconds, when the vehicle's engine is on.

/*
================================================
day,      sequence,    longitude, latitude, speed
------------------------------------------------
20170322, 10:37:50+1300, 174.77, -41.2262, 29.1
20170326, 10:07:40+1300, 175, -41.2012, 70.1
20170326, 10:02:10+1300, 175.07, -41.1255, 40.5
20170326, 10:49:40+1300, 174.8, -41.968, 30.8
20170326, 10:48:40+1300, 176.06, -41.3, 38
20170326, 10:48:10+1300, 175.89, -41.523, 67.6
20170326, 10:47:40+1300, 175.44, -40.081, 54
20170326, 10:47:10+1300, 174.8, -40.478, 36
================================================
*/

-- "Reading Data Point and other data may be eventually consistent."

CONSISTENCY ONE;

INSERT INTO data_point (day, sequence, longitude, latitude, speed) VALUES (20170322, '2017-03-22 10:37:50+1300', 174.77, -41.2262, 29.1);

INSERT INTO data_point (day, sequence, longitude, latitude, speed) VALUES (20170326, '2017-03-26 10:07:40+1300', 175, -41.2012, 70.1);

INSERT INTO data_point (day, sequence, longitude, latitude, speed) VALUES (20170326, '2017-03-26 10:02:10+1300', 175.07, -41.1255, 40.5);

INSERT INTO data_point (day, sequence, longitude, latitude, speed) VALUES (20170326, '2017-03-26 10:49:40+1300', 174.8, -41.968, 30.8);

INSERT INTO data_point (day, sequence, longitude, latitude, speed) VALUES (20170326, '2017-03-26 10:48:40+1300', 176.06, -41.3, 38);

INSERT INTO data_point (day, sequence, longitude, latitude, speed) VALUES (20170326, '2017-03-26 10:48:10+1300', 175.89, -41.523, 67.6);

INSERT INTO data_point (day, sequence, longitude, latitude, speed) VALUES (20170326, '2017-03-26 10:47:40+1300', 175.44, -40.081, 54);

INSERT INTO data_point (day, sequence, longitude, latitude, speed) VALUES (20170326, '2017-03-26 10:47:10+1300', 174.8, -40.478, 36);

-- ANSWERS FOR QUESTION 6

-- 1. Read the number of working days of a driver. (Payroll will use this information.). App collects this information in a separate table. Table name: `driver_working_days`, Columns: `driver_name` (unique, string), `working_day` (counter). This is a counter table and the app will update the counter, when the driver starts to work.

CONSISTENCY QUORUM;

SELECT * FROM driver_working_days WHERE driver_name = 'jane';

-- 2. Read timetable data for showing timetable for passengers. Requested columns from `time_table` table: `line_name`, `station_name`, `time`.

CONSISTENCY ONE;

SELECT line_name, station_name, time FROM time_table;

-- 3. Application can list `station_name`, `service_no`, `time` from `time_table`. `desc` sorted by `time`.

-- The sorting is ASC in this table, the data will be provided as ordered by ASC.

CONSISTENCY ONE;

SELECT station_name, service_no, time FROM time_table;

-- 4. The iPhone app, which is on the train can read `station_name`, `time`, `line_name`, `service_no`. The iPhone app connected with a train with `line_name` and `service_no`. If the `line_name`, `service_no` and `time` matches, we can update the vehicle status. See Question 1, Update 5.1

CONSISTENCY ONE;

SELECT station_name, time, line_name, service_no FROM time_table WHERE line_name = 'Hutt Valley Line (north bound)' AND service_no = 11 LIMIT 1;

-- 5. The application runs a query to list trains on a station. The application reads from `driver` table driver's `current_position` and check their `skill` values. If the list of skills contains the `vehicle`'s `type`, the driver will get a text message and the driver will be allocated to this train. The driver's `current_postion` will be updated. See Question 1, Update 3.2.

CONSISTENCY QUORUM;

SELECT * FROM vehicle WHERE status = 'Wellington';

-- Unfortunatelly, the created indexes are not enough, we still have to use ALLOW FILTERING.
-- Custom filter table could help, but some reason, it is not really working, some error always appeared.

CONSISTENCY QUORUM;

SELECT * FROM driver WHERE current_position = 'Wellington' and skill CONTAINS 'Matangi' ALLOW FILTERING;

-- 6. The app authentication service reads from the database, from `driver` table, `password` column for checking password, which provided by the driver after she/he entered in the vehicle.

-- Return the `driver_name` if the provided authentication data is right, otherwise no matching data, return an empty table.

CONSISTENCY QUORUM;

SELECT driver_name FROM driver WHERE driver_name = 'pondy' and password = 'dhy@@EE3#';

-- 7. For logging `vehicle_usage`, the app has to be able to read distances from `time_table`. See Question 1, Update 7.2.

-- Need Allow filtering. The `station_name` cannot be primary_key because of time is ordered by ASC.

CONSISTENCY ONE;

SELECT distance FROM time_table WHERE line_name = 'Hutt Valley Line (north bound)' AND service_no = 1 AND station_name = 'Upper Hutt' ALLOW FILTERING;

-- 8. Readings from `data_point` table:

-- 8.1 Last entry of a service, based on `line_name` and `service_no`.

-- Cassandra stores timestamps in UTC format, cqlsh convert timezones only if pytz python package is installed.

-- Please install with `pip install pytz`.

-- Launch cqlsh with the following command: `TZ=Pacific/Auckland ccm node1 cqlsh`

CONSISTENCY ONE;

SELECT * FROM data_point WHERE day = 20170326 ORDER BY sequence DESC LIMIT 1;

-- 8.2 List of entries in a time interval (`start_time`, `end_time`). List all the entries, where `sequence` between the given time intervals.

CONSISTENCY ONE;

SELECT * FROM data_point WHERE day = 20170326 and sequence >= '2017-03-26 10:47:40+1300' and sequence <= '2017-03-26 10:48:40+1300' ORDER BY sequence DESC;

-- 8.3 Find a data point in `data_point` table. It can provide a `time` and `latitude`.

CONSISTENCY ONE;

SELECT dateof(maxtimeuuid(sequence)) as time, latitude FROM data_point WHERE day = 20170326 ORDER BY sequence DESC LIMIT 1;

-- With results from the above query find the previous and next station in `time_table`. List city names as `north_neighbour` and `south_neighbour`.

CONSISTENCY ONE;

SELECT station_name as north_neighbour from time_table WHERE latitude > -41.968 AND time > 1049 AND service_no IN (5) LIMIT 1 ALLOW FILTERING;

-- There isn't any city which is southern from this coordinate, so this query return with empty table.

CONSISTENCY ONE;

SELECT station_name as south_neighbour from time_table WHERE latitude < -41.968 AND time < 1049 AND service_no IN (5) LIMIT 1 ALLOW FILTERING;






-- 1. Read the number of working days of a driver. (Payroll will use this information.). App collects this information in a separate table. Table name: `driver_working_days`, Columns: `driver_name` (unique, string), `working_day` (counter). This is a counter table and the app will update the counter, when the driver starts to work.

CONSISTENCY QUORUM;

SELECT * FROM driver_working_days WHERE driver_name = 'jane';

-- 2. Read timetable data for showing timetable for passengers. Requested columns from `time_table` table: `line_name`, `station_name`, `time`.

CONSISTENCY ONE;

SELECT line_name, station_name, time FROM time_table;

-- 3. Application can list `station_name`, `service_no`, `time` from `time_table`. `desc` sorted by `time`.

-- The sorting is ASC in this table, the data will be provided as ordered by ASC.

CONSISTENCY ONE;

SELECT station_name, service_no, time FROM time_table;

-- 4. The iPhone app, which is on the train can read `station_name`, `time`, `line_name`, `service_no`. The iPhone app connected with a train with `line_name` and `service_no`. If the `line_name`, `service_no` and `time` matches, we can update the vehicle status. See Question 1, Update 5.1

CONSISTENCY ONE;

SELECT station_name, time, line_name, service_no FROM time_table WHERE line_name = 'Hutt Valley Line (north bound)' AND service_no = 11 LIMIT 1;

-- 5. The application runs a query to list trains on a station. The application reads from `driver` table driver's `current_position` and check their `skill` values. If the list of skills contains the `vehicle`'s `type`, the driver will get a text message and the driver will be allocated to this train. The driver's `current_postion` will be updated. See Question 1, Update 3.2.

CONSISTENCY QUORUM;

SELECT * FROM vehicle WHERE status = 'Wellington';

-- Unfortunatelly, the created indexes are not enough, we still have to use ALLOW FILTERING.
-- Custom filter table could help, but some reason, it is not really working, some error always appeared.

CONSISTENCY QUORUM;

SELECT * FROM driver WHERE current_position = 'Wellington' and skill CONTAINS 'Matangi' ALLOW FILTERING;

-- 6. The app authentication service reads from the database, from `driver` table, `password` column for checking password, which provided by the driver after she/he entered in the vehicle.

-- Return the `driver_name` if the provided authentication data is right, otherwise no matching data, return an empty table.

CONSISTENCY QUORUM;

SELECT driver_name FROM driver WHERE driver_name = 'pondy' and password = 'dhy@@EE3#';

-- 7. For logging `vehicle_usage`, the app has to be able to read distances from `time_table`. See Question 1, Update 7.2.

-- Need Allow filtering. The `station_name` cannot be primary_key because of time is ordered by ASC.

CONSISTENCY ONE;

SELECT distance FROM time_table WHERE line_name = 'Hutt Valley Line (north bound)' AND service_no = 1 AND station_name = 'Upper Hutt' ALLOW FILTERING;

-- 8. Readings from `data_point` table:

-- 8.1 Last entry of a service, based on `line_name` and `service_no`.

-- Cassandra stores timestamps in UTC format, cqlsh convert timezones only if pytz python package is installed.

-- Please install with `pip install pytz`.

-- Launch cqlsh with the following command: `TZ=Pacific/Auckland ccm node1 cqlsh`

CONSISTENCY ONE;

SELECT * FROM data_point WHERE day = 20170326 ORDER BY sequence DESC LIMIT 1;

-- 8.2 List of entries in a time interval (`start_time`, `end_time`). List all the entries, where `sequence` between the given time intervals.

CONSISTENCY ONE;

SELECT * FROM data_point WHERE day = 20170326 and sequence >= '2017-03-26 10:47:40+1300' and sequence <= '2017-03-26 10:48:40+1300' ORDER BY sequence DESC;

-- 8.3 Find a data point in `data_point` table. It can provide a `time` and `latitude`.

CONSISTENCY ONE;

SELECT dateof(maxtimeuuid(sequence)) as time, latitude FROM data_point WHERE day = 20170326 ORDER BY sequence DESC LIMIT 1;

-- With results from the above query find the previous and next station in `time_table`. List city names as `north_neighbour` and `south_neighbour`.

CONSISTENCY ONE;

SELECT station_name as north_neighbour from time_table WHERE latitude > -41.968 AND time > 1049 AND service_no IN (5) LIMIT 1 ALLOW FILTERING;

-- There isn't any city which is southern from this coordinate, so this query return with empty table.

CONSISTENCY ONE;

SELECT station_name as south_neighbour from time_table WHERE latitude < -41.968 AND time < 1049 AND service_no IN (5) LIMIT 1 ALLOW FILTERING;


CREATE TABLE device_check (device_id int, checked_at timestamp, is_power boolean, is_locked boolean, PRIMARY KEY (device_id, checked_at));

INSERT INTO device_check
  (device_id, checked_at, is_power, is_locked)
values
  (1, '2013-01-01T09:00+1300', true, true);
  
INSERT INTO device_check
  (device_id, checked_at, is_power, is_locked)
values
  (2, '2013-01-01T09:10+1300', true, true);
  
INSERT INTO device_check
  (device_id, checked_at, is_power, is_locked)
values
  (3, '2013-01-01T09:10+1300', true, false);
  
INSERT INTO device_check
  (device_id, checked_at, is_power, is_locked)
values
  (1, '2013-02-01T09:00+1300', true, false);
  
INSERT INTO device_check
  (device_id, checked_at, is_power, is_locked)
values
  (2, '2013-02-01T09:10+1300', true, false);
  
INSERT INTO device_check
  (device_id, checked_at, is_power, is_locked)
values
  (3, '2013-02-01T09:10+1300', true, true);
 
 
 




////////////////////// common errors////////////////////
Connection error: ('Unable to connect to any servers', {'127.0.0.1': error(111, "Tried connecting to [('127.0.0.1', 9042)]. Last error: Connection refused")})

#it means  cassandra not started

# to solve this issue in another termina execute "cassandra -f"


//error 2
if u get this  either spark cassandra jar not available or version problem.

.ClassNotFoundException: Failed to find data source: cssandra. Please find packages at http://spark.apache.org/third-party-projects.html

///error 3: anywhere if u get ends with "V" its version issue. scala version issue may be ur using scala 12 pls change to scala 11
https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.11/2.4.2

//Caused by: java.lang.NoClassDefFoundError: com/twitter/jsr166e/LongAdder
if u get anywhere NoClassDefFoundError means its dependency issue. so download this dependency from mvnrepository
https://repo1.maven.org/maven2/com/twitter/jsr166e/1.1.0/jsr166e-1.1.0.jar


An error occurred while calling o72.load.
: java.lang.NoSuchMethodError: scala.Product.$init$(Lscala/Product;)V


pls use emr version 6.4.0 only (spark 3.1.2 only)

wget https://dlcdn.apache.org/cassandra/4.0.10/apache-cassandra-4.0.10-bin.tar.gz

wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.1.0/spark-cassandra-connector_2.12-3.1.0.jar

wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-driver_2.12/3.1.0/spark-cassandra-connector-driver_2.12-3.1.0.jar

wget https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.15.0/java-driver-core-4.15.0.jar

wget https://repo1.maven.org/maven2/com/datastax/oss/java-driver-shaded-guava/25.1-jre/java-driver-shaded-guava-25.1-jre.jar

wget https://repo1.maven.org/maven2/com/datastax/oss/java-driver-query-builder/4.15.0/java-driver-query-builder-4.15.0.jar

wget https://repo1.maven.org/maven2/com/typesafe/config/1.4.2/config-1.4.2.jar

wget https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.14.1/java-driver-core-4.14.1.jar

wget https://repo1.maven.org/maven2/com/datastax/oss/native-protocol/1.5.1/native-protocol-1.5.1.jar

 wget https://repo1.maven.org/maven2/org/reactivestreams/reactive-streams/1.0.3/reactive-streams-1.0.3.jar


sample code:

from pyspark.sql import *

from pyspark.sql.functions import *

spark=SparkSession.builder.appName("test").master("local[*]").enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")

adf=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option("table","asl").load()

edf=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option("table","emp").load()

adf.show()

edf.show()

print("inner join");

#by default it's inner join

joindf= adf.join(edf, adf.name==edf.first_name).drop(edf.first_name)

joindf.show()

print("left outer join")

ljoindf= adf.join(edf, adf.name==edf.first_name,"left_outer").drop(edf.first_name)

ljoindf.show()

print("right outer join")

rjoindf= adf.join(edf, adf.name==edf.first_name,"right_outer").drop(edf.first_name)

rjoindf.show()

print("corss join")

cjoindf = adf.crossJoin(edf)

cjoindf.show(100)

print("full outer join")

fjoindf= adf.join(edf, adf.name==edf.first_name,"full_outer").drop(edf.first_name).na.fill(0).na.fill("0")

fjoindf.show()

cjoindf.write.mode("append").partitionBy("id").saveAsTable("mycatalog.cassdb.empcrossjoinasl")

