# DATA WAREHOUSE

## SUMMARY

The purpose of this project is to take source data describing the use of a music database and transform it so it can be queried using Amazon Redshift. 

### Extract / Transform / Load

The project is split into three files. 

1. sql_queries - Holds the individual queries that handles data extraction, transformation, and loading
2. create_tables - Creates a connection to AWS and relevant services, then runs the sql_queries in groups by delete, create, and insert
3. etl - Holds the configuration data and executes the functions in create_tables

#### Extraction Pipeline

Data is extracted from Amazon S3 files in JSON format and loaded into two staging tables, split by events and song data. Tables are created in a star scheman with a:

1. fact table 
    a. songplay_table
2. measure tables
    a. user_table
    b. song_table
    c. artist_table
    d. time_table

3. Staging Tables
    a. staging_events_table
    b. staging_songs_table

##### Transform and Load

Data is transformed to fit the table formatting and the data is copied or inserted into the relevant tables




