import psycopg2
from config import DWH_DB_HOST, DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB_SCHEMA

# Connection to DWH
conn = psycopg2.connect(
    host=DWH_DB_HOST,
    database=DWH_DB_NAME,
    user=DWH_DB_USER,
    password=DWH_DB_PASSWORD,
    options=f'-c search_path={DWH_DB_SCHEMA}'
)
cur = conn.cursor()

# SQL to create the tables
#For future implementations we can change to imported parameters from a file 
#to avoid modifying the file with the connection and execution when it is necessary 
# to modify the configuration of a table.


#The dim_product and fact_activities tables are created for possible products 
# that may be added in the future that are not strictly p2p transactions.
create_tables_sql = """
CREATE TABLE IF NOT EXISTS Dim_Jurisdiction (
    jurisdiction_id SERIAL PRIMARY KEY,
    country VARCHAR(10) NOT NULL,
    creation_date DATE
);

CREATE TABLE Dim_User (
    user_id VARCHAR(50) PRIMARY KEY,
    creation_date DATE
);

CREATE TABLE IF NOT EXISTS Dim_Date (
    date DATE PRIMARY KEY,
    year INT,
    month INT,
    quarter INT
);

CREATE TABLE IF NOT EXISTS Dim_UserLevel (
    user_level_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    jurisdiction_id INT,
    level INT,
    effective_date date,
    effective_timestamp timestamp without time zone, 
    FOREIGN KEY (user_id) REFERENCES Dim_User(user_id),
    FOREIGN KEY (effective_date) REFERENCES Dim_Date(date),
    FOREIGN KEY (jurisdiction_id) REFERENCES Dim_Jurisdiction(jurisdiction_id)
);

CREATE TABLE IF NOT EXISTS Dim_Product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    product_type VARCHAR(50),
    jurisdiction_availability VARCHAR(255)
);


CREATE TABLE Fact_Transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    amount NUMERIC,
    currency VARCHAR(10),
    transaction_type VARCHAR(50),
    transaction_date DATE,
    transaction_timestamp timestamp without time zone,
    tx_status VARCHAR(20),
    FOREIGN KEY (user_id) REFERENCES Dim_User(user_id),
    FOREIGN KEY (transaction_date) REFERENCES Dim_Date(date)
);



CREATE TABLE IF NOT EXISTS Fact_Events (
    event_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    event_type VARCHAR(50),
    event_date DATE,
    event_timestamp timestamp without time zone,
    FOREIGN KEY (user_id) REFERENCES Dim_User(user_id),
    FOREIGN KEY (event_date) REFERENCES Dim_Date(date)
);

CREATE TABLE IF NOT EXISTS public.dim_product
(
    product_id serial NOT NULL,
    product_name character varying(100),
    product_type character varying(50),
     creation_date DATE,
    jurisdiction_availability character varying(255),
    CONSTRAINT dim_product_pkey PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS public.fact_activities
(
    activity_id serial NOT NULL,
    user_id character varying(50),
    product_id integer,
    activity_type character varying(50),
    activity_date date,
    jurisdiction_id integer,
    CONSTRAINT fact_activities_pkey PRIMARY KEY (activity_id),
    FOREIGN KEY (user_id) REFERENCES dim_user (user_id),
    FOREIGN KEY (product_id) REFERENCES dim_product (product_id),
    FOREIGN KEY (jurisdiction_id) REFERENCES dim_jurisdiction (jurisdiction_id),
    FOREIGN KEY (activity_date) REFERENCES dim_date (date)
);

CREATE TABLE IF NOT EXISTS ETL_Metadata (
    table_name VARCHAR(50) PRIMARY KEY,
    last_updated TIMESTAMP
);

"""

# Execute table creation
cur.execute(create_tables_sql)
conn.commit()

# Close connection
cur.close()
conn.close()
print("Tables successfully created in DWH.")
