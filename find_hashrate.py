import requestingFact
import psycopg2
import requests
import time
import importlib

# Fact0rn API base URL
BASE_URL = "https://explorer.fact0rn.io/api/"

# Database connection parameters
DB_PARAMS = {
    "dbname": "fact0rn_data",
    "user": "postgres",
    "password": "Haadimoto2005",  # Replace with your PostgreSQL password
    "host": "localhost",
    "port": "5432"
}

def fetch_api_data(endpoint):
    try:
        response = requests.get(BASE_URL + endpoint)
        response.raise_for_status()
        if "getblockhash" in endpoint:
            return response.text.strip()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching {endpoint}: {e}")
        return None

def fetch_network_hashrate():
    """Fetch the current network hashrate from the Fact0rn API."""
    try:
        # Use the getnetworkhashps endpoint to fetch the hashrate
        hashrate = fetch_api_data("getnetworkhashps")
        return hashrate
    except Exception as e:
        print(f"Error fetching network hashrate: {e}")
        return None

def save_hashrate_to_database(block_index, network_hashrate):
    """Save hashrate data to PostgreSQL database."""
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**DB_PARAMS)
        cursor = connection.cursor()
        
        # First, check if the hashrate column exists, if not add it
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='block_data' AND column_name='network_hashrate';
        """)
        
        if cursor.fetchone() is None:
            # Add the network_hashrate column if it doesn't exist
            print("Adding network_hashrate column to block_data table...")
            cursor.execute("""
                ALTER TABLE block_data 
                ADD COLUMN network_hashrate NUMERIC;
            """)
            connection.commit()
        
        # Update the block record with hashrate data
        cursor.execute("""
            UPDATE block_data 
            SET network_hashrate = %s
            WHERE current_block_number = %s;
        """, (network_hashrate, block_index))
        
        rows_updated = cursor.rowcount
        if rows_updated == 0:
            # If no rows were updated, the block might not exist yet in the database
            # We'll insert a minimal record with just the block index and hashrate
            cursor.execute("""
                INSERT INTO block_data 
                (current_block_number, network_hashrate)
                VALUES (%s, %s)
                ON CONFLICT (current_block_number) 
                DO UPDATE SET network_hashrate = EXCLUDED.network_hashrate;
            """, (block_index, network_hashrate))
        
        connection.commit()
        print(f"Hashrate data for block {block_index} saved to database.")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if connection:
            if cursor:
                cursor.close()
            connection.close()

def process_hashrate_data():
    # Get current block count
    current_block = fetch_api_data("getblockcount")
    if current_block is None:
        print("Failed to get current block count. Exiting.")
        return None
    
    print(f"\nCurrent block index: {current_block}")
    
    # Fetch current network hashrate
    network_hashrate = fetch_network_hashrate()
    if network_hashrate is not None:
        print(f"Current Network Hashrate: {network_hashrate} hashes per second")
        
        # Save hashrate to database
        save_hashrate_to_database(current_block, network_hashrate)
        return current_block
    else:
        print("Failed to fetch network hashrate")
        return None

def main():
    print("Starting continuous monitoring of Fact0rn network hashrate...")
    print("Waiting for new blocks... (Press Ctrl+C to stop)")
    
    # Store the last processed block to avoid processing the same block multiple times
    last_processed_block = None
    
    while True:
        try:
            # Get current block count
            current_block = fetch_api_data("getblockcount")
            
            # Only process if we have a new block or this is the first run
            if current_block is not None and current_block != last_processed_block:
                print(f"\n{'='*50}")
                print(f"New block detected: {current_block}")
                print(f"{'='*50}")
                
                process_hashrate_data()
                last_processed_block = current_block
            
            # Wait for 5 seconds before checking again
            time.sleep(5)
            
        except Exception as e:
            print(f"Error in main loop: {e}")
            print("Continuing in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    main()