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

    # Create tables if they don't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS block_data (
            current_block_number INTEGER PRIMARY KEY,
            current_block_timestamp NUMERIC,
            previous_block_number INTEGER,
            previous_block_timestamp NUMERIC,
            block_time_interval_seconds NUMERIC,
            moving_avg_100 NUMERIC(20,8),
            moving_avg_672 NUMERIC(20,8),
            network_hashrate NUMERIC
        );
    """)

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
    
    # Check if moving_avg columns have the right type and update if needed
    print("Checking if moving_avg columns have the correct precision...")
    
    # First check if the columns exist at all
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'block_data' AND column_name IN ('moving_avg_100', 'moving_avg_672');
    """)
    
    existing_columns = [row[0] for row in cursor.fetchall()]
    print(f"Found columns: {existing_columns}")
    
    # Check and add missing columns or update existing ones
    for column_name in ['moving_avg_100', 'moving_avg_672']:
        if column_name in existing_columns:
            # Column exists, check its precision
            cursor.execute(f"""
                SELECT data_type, numeric_precision, numeric_scale 
                FROM information_schema.columns 
                WHERE table_name = 'block_data' AND column_name = '{column_name}';
            """)
            
            data_type, precision, scale = cursor.fetchone()
            print(f"Column {column_name} exists with type {data_type}({precision},{scale})")
            
            # If precision or scale is too small, update the column type carefully to preserve data
            if data_type == 'numeric' and (precision < 20 or scale < 8):
                print(f"Updating {column_name} from NUMERIC({precision},{scale}) to NUMERIC(20,8)...")
                
                # Use a transaction to ensure data safety
                try:
                    # Create a backup of the data temporarily
                    cursor.execute(f"ALTER TABLE block_data ADD COLUMN {column_name}_backup NUMERIC(20,8)")
                    cursor.execute(f"UPDATE block_data SET {column_name}_backup = {column_name}")
                    
                    # Modify the column type
                    cursor.execute(f"ALTER TABLE block_data ALTER COLUMN {column_name} TYPE NUMERIC(20,8)")
                    
                    # Verify data consistency and use backup if needed
                    cursor.execute(f"UPDATE block_data SET {column_name} = {column_name}_backup WHERE {column_name} IS NULL")
                    
                    # Drop the temporary backup column
                    cursor.execute(f"ALTER TABLE block_data DROP COLUMN {column_name}_backup")
                    
                    conn.commit()
                    print(f"Successfully updated {column_name} to NUMERIC(20,8)")
                except Exception as e:
                    conn.rollback()
                    print(f"Error updating column {column_name}: {e}")
                    print("Attempting simpler column update as fallback...")
                    
                    # Fallback approach with direct type conversion
                    try:
                        cursor.execute(f"ALTER TABLE block_data ALTER COLUMN {column_name} TYPE NUMERIC(20,8)")
                        conn.commit()
                        print(f"Successfully updated {column_name} using fallback method")
                    except Exception as e2:
                        conn.rollback()
                        print(f"Failed to update column {column_name}: {e2}")
                        print("WARNING: Column type update failed, application may encounter numeric overflow errors.")
        else:
            # Column doesn't exist, add it
            print(f"Adding missing column {column_name}...")
            try:
                cursor.execute(f"ALTER TABLE block_data ADD COLUMN {column_name} NUMERIC(20,8)")
                conn.commit()
                print(f"Added column {column_name}")
            except Exception as e:
                conn.rollback()
                print(f"Error adding column {column_name}: {e}")
    
    # Add a function to recalculate missing moving averages
    print("Creating or replacing function to recalculate moving averages...")
    try:
        cursor.execute("""
            CREATE OR REPLACE FUNCTION recalculate_missing_averages() RETURNS void AS $$
            DECLARE
                block_record RECORD;
            BEGIN
                FOR block_record IN 
                    SELECT current_block_number 
                    FROM block_data 
                    WHERE moving_avg_100 IS NULL OR moving_avg_672 IS NULL
                    ORDER BY current_block_number
                LOOP
                    -- Calculate 100-block moving average
                    UPDATE block_data b
                    SET moving_avg_100 = (
                        SELECT AVG(block_time_interval_seconds)
                        FROM (
                            SELECT block_time_interval_seconds
                            FROM block_data
                            WHERE current_block_number <= block_record.current_block_number
                            ORDER BY current_block_number DESC
                            LIMIT 100
                        ) AS recent_blocks
                    )
                    WHERE b.current_block_number = block_record.current_block_number;
                    
                    -- Calculate 672-block moving average
                    UPDATE block_data b
                    SET moving_avg_672 = (
                        SELECT AVG(block_time_interval_seconds)
                        FROM (
                            SELECT block_time_interval_seconds
                            FROM block_data
                            WHERE current_block_number <= block_record.current_block_number
                            ORDER BY current_block_number DESC
                            LIMIT 672
                        ) AS recent_blocks
                    )
                    WHERE b.current_block_number = block_record.current_block_number;
                END LOOP;
            END;
            $$ LANGUAGE plpgsql;
        """)
        conn.commit()
        print("Created function to recalculate moving averages")
        
        # Check if we need to recalculate any missing averages
        cursor.execute("SELECT COUNT(*) FROM block_data WHERE moving_avg_100 IS NULL OR moving_avg_672 IS NULL")
        missing_count = cursor.fetchone()[0]
        
        if missing_count > 0:
            print(f"Found {missing_count} blocks with missing moving averages")
            # Don't automatically run this as it could take a while - just inform the user
            print("To recalculate missing averages, run: SELECT recalculate_missing_averages();")
    except Exception as e:
        conn.rollback()
        print(f"Error creating function to recalculate moving averages: {e}")

    conn.commit()
    print('Database setup completed successfully')
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error setting up database: {e}")
