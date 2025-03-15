# drop_emissions_table.py
import psycopg2
import os

# Get database configuration from environment variable (for Heroku)
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

try:
    print("Connecting to database...")
    connection = psycopg2.connect(DATABASE_URL)
    cursor = connection.cursor()
    
    print("Dropping emissions table...")
    cursor.execute("DROP TABLE IF EXISTS emissions CASCADE;")
    
    connection.commit()
    print("Emissions table successfully dropped")
    
    cursor.close()
    connection.close()
    print("Database connection closed")
except Exception as e:
    print(f"Error: {e}")