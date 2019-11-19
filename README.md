# Project: Data Warehouse

## Intro

The goal of the project is to define, create and populate a Data Warehouse backed by a RedShift cluster.

The raw data is loaded from Amazon S3 and transformed into a suitable data model which enables
the Sparkify team to design and execute their analytics queries.

The README presents the following:
1. Data Model (and how the data is loaded)
2. Create and destroy the Redshift cluster
3. How to execute the ETL

## Data Model

### Staging tables

Two staging tables are used to load and store the raw data (JSON files in S3 Bucket):
1. STAGING_EVENTS (events about the users)
2. STAGING_SONGS (data about the songs)

**Data Loading**

The data is loaded without transformations at this stage using the COPY command.  
Note: while STAGING_SONGS columns matched the JSON attributes this was not the case for STAGING_EVENTS.  
In order to ensure the desired mapping the JSONPaths file has been used to explicitly define the names.


### Fact & Dimensions tables

The schema is designed around the Star Schema principle with one fact table and several dimensions:
1. SONG_PLAYS (fact)
2. USERS
3. SONGS
4. ARTISTS
5. TIME

KEYDISTs and SORTKEYs have been defined to improve the type of queries which the Sparkify team will run.

Creating a KEYDIST on song_plays.artist_id and artists.artist_id I was able to decrease the 
Query 'popular artists by month' (see below) response time from 17-15 sec to 2-3 sec.

SORTKEYs have been defined on colums when sorting or filtering is likely to happen, for example on 
time.start_time as analytics would probably focus on most recent data.
  

**Data Loading**

The data is loaded using INSERT INTO SELECT statement: for each table an INSERT statement is defined
selecting from the staging tables (joining them when necessary).  
During this step the data is cleaned (only loading relevant data about what/when users listen) and processed (distinct of key columns,
extract parts of a timestamp).

## RedShift Cluster

The 'Infrastructure as Code' approach is embraced to create the necessary AWS resources.

The 'infra' folder contains the code to:
- create and start the cluster (including role and policy)
- enable incoming traffic
- check status of the cluster
- stop and destroy the cluster

## Execute the ETL

1. Config dwh.cfg accordingly
2. Execute 'start.py'
3. Execute 'check_cluster_status.py': run this several times until the output confirms the Cluster is available
```
Cluster IS AVAILABLE
#######
DWH_ENDPOINT :: xxxxxxxxx
DWH_ROLE_ARN :: xxxxxxxxx
VPC_ID :: xxxxxxx
```
Make a note of the output as they must be used in the next step.

4. Edit 'dwh.cfg' to define the following variables:
```
HOST=<value of DWH_ENDPOINT>
ARN=<value of DWH_ROLE_ARN>
VPC_ID=<value of VPC_ID>
```

5. Execute 'enable_incoming_traffic.py' and verify the connection to the database is successful

6. Execute 'create_tables.sql' to drop/create all tables

7. Execute 'etl.py' to run the pipeline

The database will be (eventually) loaded with the data and ready to receive the analytic queries of the team.

**Delete the Cluster**
 
The cluster can be deleted executing the 'stop.py' Python file (or via the AWS Console)
 
## Analytics

# Query 'number of users per level and gender'
```
SELECT COUNT(*) AS total, us.level AS level, us.gender AS gender FROM users us 
GROUP BY us.level, us.gender
LIMIT 10
```

# Query 'popular artists by month'
```
SELECT COUNT(ar.name), ar.name, month, year 
FROM song_plays sp JOIN time ti ON sp.start_time=ti.start_time
JOIN artists ar ON sp.artist_id=ar.artist_id
GROUP BY month, year, ar.name
ORDER BY count(ar.name) DESC
LIMIT 10
```