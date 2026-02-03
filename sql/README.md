# Snowflake Sales Analytics Pipeline (End-to-End Project)

##  Overview
This project demonstrates an **end-to-end Data Engineering pipeline on Snowflake** using industry practices.  
It ingests sales order data from **AWS S3 (CSV)** into Snowflake, applies **validation rules**, routes bad records into a **Quarantine table**, and builds an **Analytics layer (Star Schema)** with automated incremental processing.

This project is designed to showcase Snowflake skills for **Data Engineer roles**.

##  Tech Stack
- Snowflake (File format, Stage, Storage Integration)
- Snowpipe (Auto ingestion)
- Streams & Tasks (CDC + Automation)
- SQL (TRY_TO_NUMBER, TRY_TO_TIMESTAMP, MERGE)
- AWS S3 (source storage)
