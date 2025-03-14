import psycopg2
import os

try:
    # Get database URL and fix postgres:// if needed (Heroku format)
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    # Connect using DATABASE_URL
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # Create block_data table
    print("Creating block_data table if it doesn't exist...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS block_data (
        current_block_number bigint PRIMARY KEY,
        current_block_timestamp bigint,
        previous_block_number bigint,
        previous_block_timestamp bigint,
        block_time_interval_seconds integer,
        network_hashrate numeric(18,2)
    );
    ''')

    # Create emissions table
    print("Creating emissions table if it doesn't exist...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS emissions (
        current_block_number bigint PRIMARY KEY,
        unix_timestamp bigint,
        date_time timestamp,
        money_supply numeric,
        block_reward numeric
    );
    ''')

    conn.commit()
    print('Tables created successfully')
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error setting up database: {e}")
