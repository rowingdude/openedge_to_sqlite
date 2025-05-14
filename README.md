# Openedge to SQLite dump tool

1. Why did I do this when I can just use `proutil` to the same effect?

   In short, because I can and I'm working on mirroring this production database to another host to run analytics on it.

2. What can this program do?

   Not much other than pull data. The way it's designed (currently) is to parse the metadata from an Openedge table, create its SQLite twin, and then migrate the data using SELECT and INSERT statements. It's very simple. 

3. What is the goal of this project?

   This gets tricky here because our source database had a poor designer, so it lacks traditionally beneficial things like consistently unique indexes and primary keys. In fact, our source database has no reliable change detection mechanism, which is forcing us to use a non-trivial means to track updates. So in short, the goal is reliable 'delta sync' where we are up to date in near realtime for our model's accuracy.

4. What is your awesome user information like while it's running?

Glad you asked, again, very simple:

a. When initializing, we dump some information to the user:

   2025-05-14 15:34:25,117 - INFO - Starting data sync (full_sync=False)
   2025-05-14 15:34:25,717 - INFO - Connected to OpenEdge database
   2025-05-14 15:34:25,722 - INFO - Connected to SQLite database at analytics.db
   2025-05-14 15:34:25,728 - INFO - Ensured sync state table exists
   2025-05-14 15:34:25,935 - INFO - Found table Accounting_Table with 15 columns and PK: Accounting_Table_PK
   2025-05-14 15:34:25,970 - INFO - Found table Customer_Table with 2 columns and PK: Customer_Table_PK

b. In the case of an Authorization error, we report the JDBC error:

   WARNING - Error getting schema for some_progress_table: java.sql.SQLException: [DataDirect][OpenEdge JDBC Driver][OpenEdge] Access denied (Authorization failed) (7512)

c. Once we're running, the commentary is relatively pedestrian:

   2025-05-14 15:34:41,403 - INFO - Found 1,896 tables to sync
   2025-05-14 15:34:41,404 - INFO - Processing table 1/1,896: Account_Table
   2025-05-14 15:34:41,407 - INFO - Created table Account_Table
   2025-05-14 15:34:41,407 - INFO - Using full sync strategy for Account_Table
   2025-05-14 15:34:41,411 - INFO - Source table account has 256 rows
   2025-05-14 15:34:41,452 - INFO - Inserted 256 rows for Account_Table (total: 256 of 256 (100.0%))
   2025-05-14 15:34:41,456 - INFO - Updated sync state for Account_Table, key: 1031, rows: 256
   2025-05-14 15:34:41,456 - INFO - Completed full sync of Account_Table: 256 rows
   2025-05-14 15:34:41,458 - INFO - Processing table 2/1,896: Customer_Table
      ....

If there are an abundance of rows (more than 1,000), we show each fetch/insert:


   2025-05-14 16:13:51,453 - INFO - Inserted 1000 rows for Account_Line_Table (total: 9277000 of 9669794 (95.9%))


This is largely for psycological reasons so you know how long you're going to be sitting here watching this scroll!


If you have any comments, questions, or suggestions, I'm all ears. Cheers
