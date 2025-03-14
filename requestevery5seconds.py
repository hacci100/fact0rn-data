import requests
import time
from datetime import datetime, timezone
import psycopg2
import os

BASE_URL = "https://explorer.fact0rn.io/api/"

# Get database configuration from environment variable (for Heroku)
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Database connection function
def get_db_connection():
    try:
        if DATABASE_URL:
            print("Using DATABASE_URL for connection")
            connection = psycopg2.connect(DATABASE_URL)
        else:
            # Fallback to local development configuration
            print("Using local database configuration")
            connection = psycopg2.connect(
                dbname="fact0rn_data",
                user="postgres",
                password="Haadimoto2005",  # Replace with your local password
                host="localhost",
                port="5432"
            )
        return connection
    except Exception as e:
        print(f"Database connection error: {e}")
        raise

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

def format_unix_time(unix_time):
    return datetime.fromtimestamp(unix_time, timezone.utc)

def save_to_database(block_index, block_hash, unix_timestamp, formatted_time, time_difference):
    """Save block data to PostgreSQL database."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Fetch current hashrate
        current_hashrate = fetch_current_hashrate()
        
        # Insert into block_data table with correct column names
        cursor.execute("""
            INSERT INTO block_data (
                current_block_number, 
                current_block_timestamp, 
                previous_block_number, 
                previous_block_timestamp, 
                block_time_interval_seconds,
                network_hashrate
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (current_block_number) DO NOTHING;
        """, (
            block_index, 
            unix_timestamp, 
            block_index - 1, 
            unix_timestamp - time_difference, 
            time_difference,
            current_hashrate
        ))
        connection.commit()
        print(f"Data for block {block_index} saved to database.")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def get_block_details(block_index):
    """Fetch hash and time for a given block index."""
    block_hash = fetch_api_data(f"getblockhash?index={block_index}")
    if block_hash is None:
        print(f"Failed to get block hash for index {block_index}.")
        return None, None
    
    block_info = fetch_api_data(f"getblock?hash={block_hash}")
    if block_info is None:
        print(f"Failed to get block info for index {block_index}.")
        return None, None
    
    block_time = block_info.get("time")
    return block_hash, block_time

def fetch_current_hashrate():
    """Fetch the current network hashrate from the Fact0rn API."""
    try:
        response = requests.get(BASE_URL + "getnetworkhashps")
        response.raise_for_status()
        hashrate = response.json()
        return hashrate
    except requests.RequestException as e:
        print(f"Error fetching hashrate: {e}")
        return None
    except ValueError as e:
        print(f"Error parsing hashrate response: {e}")
        return None

def main():
    # Initial block count
    last_block_count = fetch_api_data("getblockcount")
    if last_block_count is None:
        print("Failed to get initial block count. Exiting.")
        exit()

    print(f"Starting with block index: {last_block_count}")
    print("Monitoring for block index changes every 5 seconds...")

    while True:
        # Check current block count
        current_block_count = fetch_api_data("getblockcount")
        if current_block_count is None:
            print("Failed to get block count. Retrying in 5 seconds...")
            time.sleep(5)
            continue

        # Check if block count has changed
        if current_block_count != last_block_count:
            print(f"\nBlock index changed from {last_block_count} to {current_block_count}")
            
            # Fetch details for the new latest block
            latest_block_hash, latest_block_time = get_block_details(current_block_count)
            if latest_block_time is None:
                print("Failed to fetch latest block details. Continuing...")
                last_block_count = current_block_count
                time.sleep(5)
                continue

            # Fetch details for the previous block
            previous_block_index = current_block_count - 1
            previous_block_hash, previous_block_time = get_block_details(previous_block_index)
            if previous_block_time is None:
                print("Failed to fetch previous block details. Continuing...")
                last_block_count = current_block_count
                time.sleep(5)
                continue

            # Calculate time difference
            time_difference = latest_block_time - previous_block_time
            formatted_block_time = format_unix_time(latest_block_time)

            # Save the latest block data to the database
            save_to_database(
                current_block_count,
                latest_block_hash,
                latest_block_time,
                formatted_block_time,
                time_difference
            )

            # Fetch and display hashrate
            current_hashrate = fetch_current_hashrate()
            if current_hashrate is not None:
                print(f"Current Network Hashrate: {current_hashrate} hashes per second")

            # Print details
            print(f"Latest Block (Index {current_block_count}) Time (Unix): {latest_block_time}")
            print(f"Previous Block (Index {previous_block_index}) Time (Unix): {previous_block_time}")
            print(f"Hash of block {current_block_count}: {latest_block_hash}")
            print(f"Latest block completed: {formatted_block_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"Previous block completed: {format_unix_time(previous_block_time).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"Time difference between blocks: {time_difference} seconds")

            # Update last_block_count to the new current_block_count
            last_block_count = current_block_count

        # Wait 5 seconds before checking again
        time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")