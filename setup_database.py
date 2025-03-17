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

    # Create block_data table with moving average column
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

    # Create market_data table
    print("Creating market_data table if it doesn't exist...")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS market_data (
        id SERIAL PRIMARY KEY,
        unix_timestamp bigint,
        date_time timestamp,
        price numeric,
        difficulty numeric
    );
    ''')

    # Check if moving_avg_100 column already exists in block_data, add it if not
    print("Checking if moving_avg_100 column exists in block_data...")
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'block_data' AND column_name = 'moving_avg_100';")
    if not cursor.fetchone():
        print("Adding moving_avg_100 column to block_data...")
        cursor.execute("ALTER TABLE block_data ADD COLUMN moving_avg_100 numeric(10,2);")
        print("Column moving_avg_100 added successfully!")
    else:
        print("Column moving_avg_100 already exists.")
        
    # Check if moving_avg_672 column already exists in block_data, add it if not
    print("Checking if moving_avg_672 column exists in block_data...")
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'block_data' AND column_name = 'moving_avg_672';")
    if not cursor.fetchone():
        print("Adding moving_avg_672 column to block_data...")
        cursor.execute("ALTER TABLE block_data ADD COLUMN moving_avg_672 numeric(10,2);")
        print("Column moving_avg_672 added successfully!")
    else:
        print("Column moving_avg_672 already exists.")

    conn.commit()
    print('Database setup completed successfully')
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error setting up database: {e}")
