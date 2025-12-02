# Retail Data Migration & AI SQL Assistant
This project consists of two main components: a high-performance ETL pipeline to migrate data from SQLite to PostgreSQL, and a Streamlit-based AI application that allows users to query the database using natural language (Text-to-SQL) powered by OpenAI.

### Features
1. Data Migration (ETL)

* Source: SQLite 

* Destination: PostgreSQL

* Optimizations:

* Uses psycopg2.extras.execute_values for fast batch processing.

* Handles data type conversion automatically.

* Includes logic to handle schema discrepancies (e.g., column naming conflicts).

* Robust error handling with transactional commits.~

2. AI SQL Assistant (Streamlit App)

* Interface: Interactive chat interface built with Streamlit.

* Model: OpenAI (GPT-3.5/4) for generating SQL from natural language.

* Functionality:

* Accepts questions like "Who are the top 5 customers?".

* Generates valid PostgreSQL syntax based on the specific schema.

* Executes the query and displays results in a table.