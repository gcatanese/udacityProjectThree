# Project: Data Warehouse

## Intro

The goal of the project is to define, create and populate a Data Warehouse backed by a RedShift cluster.

The raw data is loaded from Amazon S3 and transformed into a suitable data model which enables
the Sparkify team to design and execute their analytics queries.

The README presents the following:
1. Data Model (and how the data is loaded)
2. Create and destroy the Redshift cluster
3. Execute the ETL

## Data Model

### Staging tables

Two staging tables are used to load and store the raw data (JSON files in S3 Bucket):
1. STAGING_EVENTS (events about users)
2. STAGING_SONGS (data about the songs)

**Data Loading**

The data is loaded without transformations at this stage using the COPY command.
Note: while STAGING_SONGS column matched the JSON attribute this was not the case for STAGING_EVENTS.
In order to overcome this problem the JSONPaths file has been used to explicitly define the mapping.


### Fact & Dimensions tables

The final schema is designed around the Star Schema principle with one fact table and several dimensions:
1. SONG_PLAYS (fact)
2. USERS
3. SONGS
4. ARTISTS
5. TIME

sort key? dist key?

**Data Loading**

The data is loaded using INSERT INTO SELECT statement: for each table an INSERT statement is defined
selecting from the staging tables (joining them when necessary).
During this step the data is cleaned (avoid loading incomplete data) and processed (distinct of key columns,
extract parts of a timestamp).

## RedShift Cluster

The 'Infrastructure as Code' approach is embraced to create the necessary AWS resources.
The 'infra' directory provides the code to:
- create and start the cluster (including role and policy)
- enable incoming traffic
- check status of the cluster
- stop and destroy the cluster

## Execute the ETL

