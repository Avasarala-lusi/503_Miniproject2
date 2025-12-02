import os
import sqlite3
import time
from psycopg2.extras import execute_values
import bcrypt
from dotenv import load_dotenv
import streamlit as st


STAGING_CREATE_SQL = """
-- Drop existing tables if they exist (in correct order due to foreign keys)
DROP TABLE IF EXISTS ProductCategory CASCADE;
DROP TABLE IF EXISTS OrderDetail CASCADE;
DROP TABLE IF EXISTS Product CASCADE;
DROP TABLE IF EXISTS Customer CASCADE;
DROP TABLE IF EXISTS Country CASCADE;
DROP TABLE IF EXISTS Region CASCADE;

CREATE TABLE IF NOT EXISTS Region(
        RegionID SERIAL NOT NULL PRIMARY KEY,
        Region TEXT UNIQUE NOT NULL
        );
CREATE TABLE IF NOT EXISTS Country(
        CountryID SERIAL NOT NULL PRIMARY KEY,
        Country TEXT UNIQUE NOT NULL,
        RegionID INTEGER NOT NULL,
        FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
        );
 CREATE TABLE IF NOT EXISTS Customer(
        CustomerID SERIAL NOT NULL PRIMARY KEY,
        FirstName TEXT NOT NULL,
        LastName TEXT NOT NULL,
        Address TEXT NOT NULL,
        City TEXT NOT NULL,
        CountryID INTEGER NOT NULL,
        FOREIGN KEY (CountryID) REFERENCES Country(CountryID)
        );
CREATE TABLE IF NOT EXISTS ProductCategory(
        ProductCategoryID SERIAL NOT NULL PRIMARY KEY,
        ProductCategory TEXT UNIQUE NOT NULL,
        ProductCategoryDescription TEXT NOT NULL
        );
 CREATE TABLE IF NOT EXISTS Product(
        ProductID SERIAL NOT NULL PRIMARY KEY,
        ProductName TEXT UNIQUE NOT NULL,
        ProductUnitPrice REAL NOT NULL,
        ProductCategoryID INTEGER NOT NULL,
        FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
        );
 CREATE TABLE IF NOT EXISTS OrderDetail(
        OrderID SERIAL NOT NULL PRIMARY KEY,
        CustomerID INTEGER NOT NULL,
        ProductID INTEGER NOT NULL,
        OrderDate TIMESTAMP NOT NULL,
        QuantityOrdered INTEGER NOT NULL,
        FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
        FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
        );
"""
def get_db_url():
    POSTGRES_USERNAME = st.secrets["POSTGRES_USERNAME"]
    POSTGRES_PASSWORD = st.secrets["POSTGRES_PASSWORD"]
    POSTGRES_SERVER = st.secrets["POSTGRES_SERVER"]
    POSTGRES_DATABASE = st.secrets["POSTGRES_DATABASE"]
    
    DATABASE_URL = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DATABASE}"

    return DATABASE_URL

def connect_databases(sqlite_db, postgres_db = DATABASE_URL):
    try:
        conn_s = sqlite3.connect(sqlite_db)
        conn_p = psycopg2.connect(postgres_db)
        return conn_s, conn_p
    except Exception as e:
        print(e)
        return None

def load_sqlite_to_postgres(conn_s, conn_p, batch_size=25000):
    
    cur_s = conn_s.cursor()
    cur_p = conn_p.cursor()
    start_total = time.time() # Start global timer
    try:
        cur_s.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [i[0] for i in cur_s.fetchall()]
        
        for table in tables:
            print(f"--- Starting Table: {table} ---")
            start_table = time.time()
            # load from sqlite3
            sql_query = f"SELECT * FROM {table}"
            cur_s.execute(sql_query)
            # Insert statement
            field_names = [description[0] for description in cur_s.description]
            col_names = ",".join(field_names)
            insert_query = f"INSERT INTO {table}({col_names}) VALUES %s ON CONFLICT DO NOTHING;"

            total_rows_table = 0
            while True:
                start_batch = time.time() # Start batch timer
                # prevents loading all rows into RAM at once
                rows = cur_s.fetchmany(batch_size)
                if not rows:
                    break
                # Fast Batch Insert
                execute_values(cur_p, insert_query, rows, page_size=batch_size)
                conn_p.commit()
                
                end_batch = time.time() # End batch timer
                total_rows_table += len(rows)
                duration = end_batch - start_batch
                print(f" Total  Batch of {total_rows_table} rows: {duration:.4f} sec")
            end_table = time.time() # End table timer
            duration_table = end_table - start_table
            print(f">>> Finished {table}: {total_rows_table} rows in {duration_table:.2f} sec")
    except Exception as e:
        print(f"Error; {e}")
        conn_p.rollback()
    finally:
        cur_s.close()
        cur_p.close()

        end_total = time.time()
        print(f"\nTotal Migration Time: {end_total - start_total:.2f} seconds")
 
# Main excution
if __name__ == "__main__":
    
    DATABASE_URL = get_db_url()
    # Create tables
    print("Creating tables...")
    conn_s, conn_p= connect_databases("project2normalized.db")
    conn_s.close()
    cur_p = conn_p.cursor()
    cur_p.execute(STAGING_CREATE_SQL)
    cur_p.commit()
    cur_p.close()
    conn_p.close()
    print("Tables created successfully\n")

    # Load staging data
    print("Loading data from sqlite3 to postgres...")
    conn_s, conn_p= connect_databases("project2normalized.db")
    load_sqlite_to_postgres(conn_s, conn_p)
    conn_s.close()
    conn_p.close()

