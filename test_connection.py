import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to Snowflake
try:
    # Creating a Snowflake connection
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    # Create a cursor object to interact with Snowflake
    cursor = conn.cursor()

    # Execute a simple query to check the connection (e.g., current database)
    cursor.execute("SELECT current_database()")
    result = cursor.fetchone()
    
    print(f"Connected to Snowflake, current database: {result[0]}")

    # Close the cursor and connection
    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")
