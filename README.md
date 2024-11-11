# Bitsochallenge
# Execution Order and Files explanation
## Prerequisites: 
Update the config.py file with the connection parameters for the database server with the source data and the connection parameters for the server that would host the DWH

## Execution Order:
### 1- Create_Tables.py: 
This file is run to create all the table structure needed for the DWH
### 2- ETL_DWH.py:
This file is responsible for making an initial data load to the DWH, as well as handling the logic required for subsequent loads.
### 3- ETL_Export_Data.py
This file has the logic necessary to export the data from each of the DWH tables into csv files for each of the tables.

# Other Files:
### Answers.sql: 
File with the necessary queries to answer each of the requested questions: 
● How many users were active on a given day (they made a deposit or withdrawal) 
● Identify users haven't made a deposit 
● Average deposited amount for Level 2 users in the mx jurisdiction 
● Latest user level for each user within each jurisdiction 
● Identify on a given day which users have made more than 5 deposits historically 
● When was the last time a user made a login 
● How many times a user has made a login between two dates 
● Number of unique currencies deposited on a given day 
● Number of unique currencies withdrew on a given day 
● Total amount deposited of a given currency on a given day

### CSV Files.zip
This file contains the CSV files corresponding to each of the DWH tables with the corresponding data.


# Data Modeling Documentation
## Overview
This project uses a dimensional modeling approach to structure the Data Warehouse. This approach organizes data into fact and dimension tables, facilitating business intelligence (BI) analysis and allowing for insights from multiple perspectives.

### 1. Modeling Techniques Used
   #### 1.1. Dimensional Modeling
Dimensional modeling is based on creating a central fact table containing transactional or event data, with dimension tables that provide additional context. This model is ideal for data analysis and reporting in a Data Warehouse environment, as it structures information in a way that is easy for business users to query and interpret.

  #### 1.2. Star Schema Structure
Within the dimensional model, a star schema structure was used, where:

The main fact table, Fact_Transactions, stores data related to user transactions (e.g., deposits and withdrawals).
Additional fact tables like Fact_Events and Fact_Activities capture other types of user events and activities.
Dimension tables (Dim_User, Dim_UserLevel, Dim_Jurisdiction, Dim_Date, Dim_Product) surround the fact table and connect to it through foreign keys, providing specific details about users, jurisdictions, products, user levels, and time.
This design facilitates data querying and allows information exploration from different perspectives, such as by date, jurisdiction, or transaction type.

### 2. Justification for Choosing Dimensional Modeling
The dimensional model and star schema structure were chosen for several reasons:

#### Ease of Understanding: 
The dimensional model is intuitive and easy for business users and analysts to understand. Organizing data into fact and dimension tables allows for straightforward and interpretable queries.
#### Efficiency in BI Queries: 
The star schema structure enables fast, optimized queries, ideal for analyzing large volumes of data in a data warehouse. Denormalizing dimensions avoids complex joins and reduces query response times.
#### Multidimensional Analysis: 
The dimensional model allows data to be analyzed from multiple perspectives (e.g., time, user, product, and jurisdiction), which is valuable for BI and decision-making.

#### Scalability for Growth: 
As a growing organization, Bitso can add new dimensions or modify existing ones without significantly altering the model, making this approach scalable and adaptable to future changes.


### 3. Potential Drawbacks of the Dimensional Model
While the dimensional model offers many advantages, it also presents certain drawbacks:

#### 3.1. Data Redundancy
Denormalization in dimension tables can lead to data redundancy, which implies greater storage usage and potential inconsistencies. For instance, if jurisdiction or product information changes, some records may need to be updated in multiple places.

#### 3.2. Complexity in Data Updates
Due to its denormalized structure, updating data can be complex. 

#### 3.3. Growth of the Fact Table
The Fact_Transactions table can grow rapidly as more transactions are added, which could impact query performance if not managed properly. To handle this growth, it’s important to consider partitioning and index optimization techniques.

#### 4. Conclusion
The dimensional model with a star schema structure is a suitable choice for this Data Warehouse, as it facilitates data analysis and optimizes queries for BI. However, maintenance and scalability strategies should be considered to mitigate potential drawbacks, especially concerning data redundancy and fact table growth. This approach enables Bitso to analyze its data effectively, while maintaining a comprehensible and adaptable structure aligned with business needs.
