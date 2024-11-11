# Bitsochallenge-- Data Modeling Documentation
## Overview
This project uses a dimensional modeling approach to structure the Data Warehouse. This approach organizes data into fact tables and dimension tables, facilitating business intelligence (BI) analysis and allowing for insights from multiple perspectives.

### 1. Modeling Techniques Used
   #### 1.1. Dimensional Modeling
Dimensional modeling is based on creating a central fact table containing transactional or event data, with dimension tables that provide additional context. This model is ideal for data analysis and reporting in a Data Warehouse environment, as it structures information in a way that is easy for business users to query and interpret.

  #### 1.2. Star Schema Structure
Within the dimensional model, a star schema structure was used, where:

The main fact table, Fact_Transactions, stores data related to user transactions (e.g., deposits and withdrawals).
Additional fact tables like Fact_Events and Fact_Activities capture other types of user events and activities.
Dimension tables (Dim_User, Dim_UserLevel, Dim_Jurisdiction, Dim_Date, Dim_Product) surround the fact table and connect to it through foreign keys, providing specific details about users, jurisdictions, products, user levels, and time.
This design facilitates data querying and allows exploration of information from different perspectives, such as by date, jurisdiction, or transaction type.

### 2. Justification for Choosing Dimensional Modeling
The dimensional model and star schema structure were chosen for several reasons:

#### Ease of Understanding: 
The dimensional model is intuitive and easy for business users and analysts to understand. Organizing data into fact and dimension tables allows for straightforward and interpretable queries.
#### Efficiency in BI Queries: 
The star schema structure enables fast, optimized queries, ideal for analyzing large volumes of data in a Data Warehouse. Denormalizing dimensions avoids complex joins and reduces query response times.
#### Multidimensional Analysis: 
The dimensional model allows data to be analyzed from multiple perspectives (e.g., time, user, product, and jurisdiction), which is valuable for BI and decision-making.
Scalability for Growth: As a growing organization, Bitso can add new dimensions or modify existing ones without significantly altering the model, making this approach scalable and adaptable to future changes.
### 3. Potential Drawbacks of the Dimensional Model
While the dimensional model offers many advantages, it also presents certain drawbacks:

#### 3.1. Data Redundancy
Denormalization in dimension tables can lead to data redundancy, which implies greater storage usage and potential inconsistencies. For instance, if jurisdiction or product information changes, some records may need to be updated in multiple places.

#### 3.2. Complexity in Data Updates
Due to its denormalized structure, updating data can be complex. For example:

User Level Changes: If a user changes levels, it’s necessary to ensure that the Dim_UserLevel dimension is updated correctly without affecting historical reports.
Jurisdiction and Product Updates: If new jurisdictions or products are added, dimensions must be adjusted without impacting historical data consistency.

#### 3.3. Growth of the Fact Table
The Fact_Transactions table can grow rapidly as more transactions are added, which could impact query performance if not managed properly. It’s important to consider partitioning and index optimization techniques to handle this growth.

#### 3.4. Scalability for New Products
The introduction of new products that do not fit the current model may require changes in the Data Warehouse design. Although the model is flexible, some structural adjustments may be necessary to support complex or product-specific data.

#### 4. Conclusion
The dimensional model with a star schema structure is a suitable choice for this Data Warehouse, as it facilitates data analysis and optimizes queries for BI. However, maintenance and scalability strategies should be considered to mitigate potential drawbacks, especially concerning data redundancy and fact table growth. This approach enables Bitso to analyze its data effectively, while maintaining a comprehensible and adaptable structure aligned with business needs.
