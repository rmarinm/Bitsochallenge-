import psycopg2
import pandas as pd
from datetime import datetime
from config import (
    SOURCE_DB_HOST, SOURCE_DB_NAME, SOURCE_DB_USER, SOURCE_DB_PASSWORD, SOURCE_DB_SCHEMA,
    DWH_DB_HOST, DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB_SCHEMA
)

# Connecting to the source database
source_conn = psycopg2.connect(
    host=SOURCE_DB_HOST,
    database=SOURCE_DB_NAME,
    user=SOURCE_DB_USER,
    password=SOURCE_DB_PASSWORD,
    options=f'-c search_path={SOURCE_DB_SCHEMA}'
)

# Connection to DWH
dwh_conn = psycopg2.connect(
    host=DWH_DB_HOST,
    database=DWH_DB_NAME,
    user=DWH_DB_USER,
    password=DWH_DB_PASSWORD,
    options=f'-c search_path={DWH_DB_SCHEMA}'
)

# Create a cursor for the DWH
dwh_cur = dwh_conn.cursor()


#Function to obtain the date of the last update of a specific table.
def get_last_updated(table_name):
    dwh_cur.execute("SELECT last_updated FROM ETL_Metadata WHERE table_name = %s", (table_name,))
    result = dwh_cur.fetchone()
    return result[0] if result else None

#function to update the date of the last update of a specific table.
def update_last_updated(table_name, last_updated):
    dwh_cur.execute("""
        INSERT INTO ETL_Metadata (table_name, last_updated)
        VALUES (%s, %s)
        ON CONFLICT (table_name)
        DO UPDATE SET last_updated = EXCLUDED.last_updated
    """, (table_name, last_updated))
    dwh_conn.commit()

# Function to insert unique data into Dim_Jurisdiction
def insert_dim_jurisdiction():
    # Extract all unique jurisdictions from user_level_sample 
    jurisdiction_df = pd.read_sql("SELECT DISTINCT jurisdiction AS country FROM user_level_sample", source_conn)
    
    # Extract jurisdictions that already exist in Dim_Jurisdiction
    existing_jurisdictions = pd.read_sql("SELECT country FROM Dim_Jurisdiction", dwh_conn)
    existing_jurisdictions_set = set(existing_jurisdictions['country'])

    # Filter jurisdictions that do not exist in Dim_Jurisdiction
    new_jurisdictions = jurisdiction_df[~jurisdiction_df['country'].isin(existing_jurisdictions_set)]

    # Insert the new jurisdictions with today's date as creation_date
    today_date = datetime.now().date()
    for _, row in new_jurisdictions.iterrows():
        dwh_cur.execute("""
            INSERT INTO Dim_Jurisdiction (country, creation_date)
            VALUES (%s, %s)
        """, (row['country'], today_date))
    #Updates the last update in the ETL Metadata table    
    update_last_updated("Dim_Jurisdiction",datetime.now())
    dwh_conn.commit()
    print("New jurisdictions inserted in Dim_Jurisdiction.")



# Function to insert data into Dim_User
def insert_dim_user():
    # Extract all unique users from user_id_sample
    user_df = pd.read_sql("SELECT DISTINCT user_id FROM user_id_sample", source_conn)
    
    # Extract users that already exist in Dim_User
    existing_users = pd.read_sql("SELECT user_id FROM Dim_User", dwh_conn)
    existing_users_set = set(existing_users['user_id'])

    # Filter users that do not exist in Dim_User
    new_users = user_df[~user_df['user_id'].isin(existing_users_set)]

    # Insert new users with today's date as creation_date
    today_date = datetime.now().date()
    for _, row in new_users.iterrows():
        dwh_cur.execute("""
            INSERT INTO Dim_User (user_id, creation_date)
            VALUES (%s, %s)
        """, (row['user_id'], today_date))

    #Updates the last update in the ETL Metadata table
    update_last_updated("Dim_User",datetime.now())
    dwh_conn.commit()
    print("New users inserted in Dim_User.")



# Function to insert data into Dim_UserLevel
def insert_dim_user_level():

    #Gets the last update of the Dim_UserLevel table
    last_updated = get_last_updated("Dim_UserLevel") or '1970-01-01'
    
    #Extract records from user_level that are newer than the last update of the Dim_UserLevel table
    level_df = pd.read_sql("""
        SELECT user_id, jurisdiction, level, event_timestamp AS effective_date,  event_timestamp AS effective_timestamp
        FROM user_level_sample
        WHERE event_timestamp > %s
    """, source_conn, params=(last_updated,))
    
    #Extracts the country and jurisdiction_id from the Dim_Jurisdiction table for later mapping
    jurisdiction_map = pd.read_sql("SELECT country, jurisdiction_id FROM Dim_Jurisdiction", dwh_conn)
    #Converts the jurisdiction_map DataFrame into a dictionary. Each country becomes a key and the corresponding jurisdiction_id becomes its value.
    jurisdiction_map = dict(zip(jurisdiction_map['country'], jurisdiction_map['jurisdiction_id']))
    #transforms jurisdiction names (country) to IDs(jurisdiction_id) in level_df, which makes it easier to work with foreign keys in the database.
    level_df['jurisdiction_id'] = level_df['jurisdiction'].map(jurisdiction_map)
    level_df = level_df.dropna(subset=['jurisdiction_id'])

    #inserts the prepared records into the Dim_UserLevel table
    for _, row in level_df.iterrows():
        dwh_cur.execute("""
            INSERT INTO Dim_UserLevel (user_id, jurisdiction_id, level, effective_date, effective_timestamp)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['user_id'], int(row['jurisdiction_id']), row['level'], row['effective_date'],row['effective_timestamp']))
    
    #Updates the last update in the ETL Metadata table
    max_effective_date = level_df['effective_date'].max()
    if pd.notna(max_effective_date):
        update_last_updated("Dim_UserLevel", max_effective_date)
    dwh_conn.commit()
    print("Datos insertados en Dim_UserLevel.")




# Function to insert data into Fact_Transactions
def insert_fact_transactions():
    #Gets the last update of the Dim_UserLevel table
    last_updated = get_last_updated("Fact_Transactions") or '1970-01-01'
    #Extracts all unique deposits from the corresponding table when the transaction date is newer than the last update of the ETL_METADATA table.
    deposit_df = pd.read_sql("SELECT distinct * FROM deposit_sample_data WHERE event_timestamp > %s", source_conn, params=(last_updated,))
    deposit_df['transaction_type'] = 'Deposit'
    #Extracts all unique Withdrawals from the corresponding table when the transaction date is newer than the last update of the ETL_METADATA table.
    withdrawal_df = pd.read_sql("SELECT distinct * FROM withdrawals_sample WHERE event_timestamp > %s", source_conn, params=(last_updated,))
    withdrawal_df['transaction_type'] = 'Withdrawal'
    #joins the two dataframes (deposits and Withdrawals) into a single dataframe
    df = pd.concat([deposit_df, withdrawal_df], ignore_index=True)

    #inserts the prepared records into the Fact_Transactions table
    for _, row in df.iterrows():
        dwh_cur.execute("""
            INSERT INTO Fact_Transactions (user_id, amount, currency, transaction_type, transaction_date, transaction_timestamp, tx_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row['user_id'], row['amount'], row['currency'], row['transaction_type'], row['event_timestamp'], row['event_timestamp'], row['tx_status']))
    
    #Updates the last update in the ETL Metadata table
    max_transaction_date = df['event_timestamp'].max()
    if pd.notna(max_transaction_date):
        update_last_updated("Fact_Transactions", max_transaction_date)
    dwh_conn.commit()
    print("Data inserted into Fact_Transactions.")




# Función para insertar datos en Fact_Events
def insert_fact_events():
    #Gets the last update of the Dim_UserLevel table
    last_updated = get_last_updated("Fact_Events") or '1970-01-01'
    #Extracts all events when the event date is newer than the last update of the ETL_METADATA table.
    df = pd.read_sql("SELECT * FROM event_sample WHERE event_timestamp > %s", source_conn, params=(last_updated,))
    
    #inserts the prepared records into the Fact_Events table
    for _, row in df.iterrows():
        dwh_cur.execute("INSERT INTO Fact_Events (user_id, event_type, event_date, event_timestamp) VALUES (%s, %s, %s, %s)", (row['user_id'], row['event_name'], row['event_timestamp'], row['event_timestamp']))
    
    #Updates the last update in the ETL Metadata table
    max_event_date = df['event_timestamp'].max()
    if pd.notna(max_event_date):
        update_last_updated("Fact_Events", max_event_date)
    dwh_conn.commit()
    print("Data inserted into Fact_Events.")


# Insert data into Dim_Date (Date Range)
def insert_dim_date():
    # Check if there is data in the Dim_Date table
    dwh_cur.execute("SELECT COUNT(*) FROM Dim_Date")
    count = dwh_cur.fetchone()[0]

    if count == 0:
        # If there are no records, perform a First Load: insert all dates from 01-01-2010 to the current date
        end_date = datetime.now().date()
        date_range = pd.date_range(start="2010-01-01", end=end_date)
        date_df = pd.DataFrame({
            'date': date_range,
            'year': date_range.year,
            'month': date_range.month,
            'quarter': date_range.quarter
        })
        for _, row in date_df.iterrows():
            dwh_cur.execute("""
                INSERT INTO Dim_Date (date, year, month, quarter)
                VALUES (%s, %s, %s, %s)
            """, (row['date'], row['year'], row['month'], row['quarter']))
        print("First upload completed on Dim_Date.")
    
    #In case it is not the first load
    else: 
        # Incremental loads: check if current date already exists
        today_date = datetime.now().date()
        dwh_cur.execute("SELECT 1 FROM Dim_Date WHERE date = %s", (today_date,))
        if dwh_cur.fetchone() is None:
            # Insert only current date if it doesn't exist
            year = today_date.year
            month = today_date.month
            quarter = (month - 1) // 3 + 1  # Calculate the quarter
            dwh_cur.execute("""
                INSERT INTO Dim_Date (date, year, month, quarter)
                VALUES (%s, %s, %s, %s)
            """, (today_date, year, month, quarter))
            print("Current date inserted into Dim_Date.")
        else:
            print("The current date already exists in Dim_Date. No insertion is required.")

    dwh_conn.commit()





# Call all insert functions in the corresponding order considering foreign keys
insert_dim_date()
insert_dim_jurisdiction()
insert_dim_user()
insert_dim_user_level()
insert_fact_transactions()
insert_fact_events()

# Close connections
dwh_cur.close()
source_conn.close()
dwh_conn.close()
print("ETL completed.")
