# main.py
import requestingFact
import psycopg2
import requests
import time
import importlib
import subprocess
import os

# Fact0rn API base URL
BASE_URL = os.environ.get("API_BASE_URL", "https://explorer.fact0rn.io/api/")

def get_db_connection():
    try:
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

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
        # Convert to GH/s (gigahashes per second)
        if hashrate is not None:
            hashrate_gh = hashrate / 1_000_000_000  # Convert from H/s to GH/s
            # Round to 2 decimal places
            hashrate_gh = round(hashrate_gh, 2)
            return hashrate_gh
        return None
    except Exception as e:
        print(f"Error fetching network hashrate: {e}")
        return None

def save_to_database(block_index, block_time, second_previous_block_index, block_time_second, time_difference, network_hashrate=None):
    conn = get_db_connection()
    if not conn:
        return
        
    try:
        cursor = conn.cursor()
        # Insert data with hashrate
        if network_hashrate is not None:
            cursor.execute("""
                INSERT INTO block_data 
                (current_block_number, current_block_timestamp, previous_block_number, previous_block_timestamp, block_time_interval_seconds, network_hashrate)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (current_block_number) 
                DO UPDATE SET network_hashrate = EXCLUDED.network_hashrate;
            """, (block_index, block_time, second_previous_block_index, block_time_second, time_difference, network_hashrate))
        else:
            cursor.execute("""
                INSERT INTO block_data 
                (current_block_number, current_block_timestamp, previous_block_number, previous_block_timestamp, block_time_interval_seconds)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (current_block_number) DO NOTHING;
            """, (block_index, block_time, second_previous_block_index, block_time_second, time_difference))
        conn.commit()
        print(f"Data for block {block_index} saved to database.")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

def process_block_data():
    # Reload the requestingFact module to get fresh data
    importlib.reload(requestingFact)
    
    # Print block index and hash
    print(f"\nCurrent block index: {requestingFact.block_count}")
    block_hash = fetch_api_data(f"getblockhash?index={requestingFact.block_count}")
    if block_hash:
        print(f"Hash of block {requestingFact.block_count}: {block_hash}")

    # Calculate time difference
    time_difference = requestingFact.block_time - requestingFact.block_time_second
    
    # Fetch current network hashrate
    network_hashrate = fetch_network_hashrate()
    if network_hashrate is not None:
        print(f"Current Network Hashrate: {network_hashrate:.2f} GH/s")
    else:
        print("Failed to fetch network hashrate")

    # Save data to database
    save_to_database(
        requestingFact.block_count,
        requestingFact.block_time,
        requestingFact.second_previous_block_index,
        requestingFact.block_time_second,
        time_difference,
        network_hashrate
    )

    print("\nTracking emissions...")
    # Track emissions for this block
    try:
        import track_emissions
        importlib.reload(track_emissions)  # Reload to ensure we get fresh data
        track_emissions.track_emissions()
        print("✓ Emissions tracking completed successfully")
    except ImportError:
        print("✗ Error: track_emissions.py not found")
    except Exception as e:
        print(f"✗ Error tracking emissions: {str(e)}")

    # Print the data
    print(f"Index {requestingFact.block_count} Block Time (Unix): {requestingFact.block_time}")
    print(f"Index {requestingFact.second_previous_block_index} Block Time (Unix): {requestingFact.block_time_second}")
    print(f"First block completed: {requestingFact.format_unix_time(requestingFact.block_time)}")
    print(f"Second block completed: {requestingFact.format_unix_time(requestingFact.block_time_second)}")
    print(f"Time difference between blocks: {time_difference} seconds")

def main():
    print("Starting continuous monitoring of Fact0rn blockchain...")
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
                
                process_block_data()
                last_processed_block = current_block

            
            # Wait for 59 seconds before checking again
            time.sleep(59)
            
        except Exception as e:
            print(f"Error in main loop: {e}")
            print("Continuing in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    main()
