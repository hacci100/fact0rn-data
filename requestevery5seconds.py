import requests
import time
from datetime import datetime, timezone
import psycopg2
import os
from urllib3 import response

# Base URLs
BASE_URL = "https://explorer.fact0rn.io/api/"
EXT_URL = "https://explorer.fact0rn.io/ext/"

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

def fetch_api_data(endpoint, is_ext=False):
    """Fetch data from Fact0rn API or extension endpoints."""
    try:
        url = (EXT_URL if is_ext else BASE_URL) + endpoint
        response = requests.get(url)
        response.raise_for_status()
        
        # For text responses (like block hash, money supply, price)
        if any(x in endpoint for x in ["getblockhash", "getmoneysupply", "getcurrentprice", "getdifficulty"]):
            return response.text.strip()
        
        # For JSON responses
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching {endpoint}: {e}")
        return None

def format_unix_time(unix_time):
    return datetime.fromtimestamp(unix_time, timezone.utc)

def save_to_database(block_index, block_hash, unix_timestamp, formatted_time, time_difference):
    try:
        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Get current hashrate
        current_hashrate = fetch_current_hashrate()
        
        # Insert block data into the database
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
            ON CONFLICT (current_block_number) DO UPDATE SET
                current_block_timestamp = EXCLUDED.current_block_timestamp,
                previous_block_number = EXCLUDED.previous_block_number,
                previous_block_timestamp = EXCLUDED.previous_block_timestamp,
                block_time_interval_seconds = EXCLUDED.block_time_interval_seconds,
                network_hashrate = EXCLUDED.network_hashrate;
        """, (
            block_index, 
            unix_timestamp, 
            block_index - 1,
            unix_timestamp - time_difference,
            time_difference,
            current_hashrate
        ))
        
        connection.commit()
        
        # Update moving averages
        update_moving_averages(connection, cursor, block_index)
        
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"Error saving to database: {e}")
        return False

def update_moving_averages(connection, cursor, block_number):
    """Update moving averages for the given block number."""
    try:
        # Calculate 100-block moving average
        cursor.execute("""
            SELECT AVG(block_time_interval_seconds)
            FROM (
                SELECT block_time_interval_seconds
                FROM block_data
                WHERE current_block_number <= %s AND block_time_interval_seconds IS NOT NULL
                ORDER BY current_block_number DESC
                LIMIT 100
            ) AS recent_blocks
        """, (block_number,))
        
        result = cursor.fetchone()
        avg_100 = result[0] if result and result[0] is not None else None
        
        # Calculate 672-block moving average
        cursor.execute("""
            SELECT AVG(block_time_interval_seconds)
            FROM (
                SELECT block_time_interval_seconds
                FROM block_data
                WHERE current_block_number <= %s AND block_time_interval_seconds IS NOT NULL
                ORDER BY current_block_number DESC
                LIMIT 672
            ) AS recent_blocks
        """, (block_number,))
        
        result = cursor.fetchone()
        avg_672 = result[0] if result and result[0] is not None else None
        
        # Only update if we have valid averages
        if avg_100 is not None or avg_672 is not None:
            # Build the update query dynamically based on which averages we have
            update_parts = []
            params = []
            
            if avg_100 is not None:
                update_parts.append("moving_avg_100 = CAST(%s AS NUMERIC(20,8))")
                params.append(avg_100)
            
            if avg_672 is not None:
                update_parts.append("moving_avg_672 = CAST(%s AS NUMERIC(20,8))")
                params.append(avg_672)
            
            if update_parts:
                query = f"""
                    UPDATE block_data
                    SET {", ".join(update_parts)}
                    WHERE current_block_number = %s
                """
                params.append(block_number)
                
                cursor.execute(query, params)
                connection.commit()
                
                # Log the update
                avg_100_str = f"{avg_100:.2f}" if avg_100 is not None else "N/A"
                avg_672_str = f"{avg_672:.2f}" if avg_672 is not None else "N/A"
                print(f"Updated moving averages for block {block_number}: 100-block avg = {avg_100_str}, 672-block avg = {avg_672_str}")
        else:
            print(f"Not enough blocks to calculate moving averages for block {block_number}")
    except Exception as e:
        print(f"Error updating moving averages for block {block_number}: {e}")
        connection.rollback()

def check_and_fix_missing_averages(limit=100):
    """Check for blocks with missing moving averages and fix them."""
    try:
        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Find blocks with missing moving averages
        cursor.execute("""
            SELECT current_block_number 
            FROM block_data 
            WHERE moving_avg_100 IS NULL OR moving_avg_672 IS NULL
            ORDER BY current_block_number
            LIMIT %s
        """, (limit,))
        
        missing_blocks = [row[0] for row in cursor.fetchall()]
        
        if missing_blocks:
            print(f"Found {len(missing_blocks)} blocks with missing moving averages")
            
            # Process each block
            for block_number in missing_blocks:
                print(f"Fixing moving averages for block {block_number}...")
                update_moving_averages(connection, cursor, block_number)
        else:
            print("No blocks with missing moving averages found")
        
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"Error checking for missing averages: {e}")
        return False

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

def get_money_supply():
    """Get the current total money supply."""
    try:
        money_supply = fetch_api_data("getmoneysupply", is_ext=True)
        if money_supply:
            return float(money_supply)
        return None
    except Exception as e:
        print(f"Error getting money supply: {e}")
        return None

def get_current_price():
    """Get the current price of FACT0rn."""
    try:
        price_data = fetch_api_data("getcurrentprice", is_ext=True)
        if price_data:
            # Handle both JSON and plain text responses
            if isinstance(price_data, str) and price_data.startswith('{'):
                # It's a JSON string
                import json
                price_json = json.loads(price_data)
                if 'last_price_usd' in price_json:
                    return float(price_json['last_price_usd'])
                elif 'last_price_usdt' in price_json:
                    return float(price_json['last_price_usdt'])
            elif isinstance(price_data, dict):
                # It's already a JSON object
                if 'last_price_usd' in price_data:
                    return float(price_data['last_price_usd'])
                elif 'last_price_usdt' in price_data:
                    return float(price_data['last_price_usdt'])
            else:
                # Try to parse as float directly
                return float(price_data)
        return None
    except Exception as e:
        print(f"Error getting current price: {e}")
        return None

def get_difficulty():
    """Get the current network difficulty."""
    try:
        difficulty = fetch_api_data("getdifficulty")
        if difficulty:
            return float(difficulty)
        return None
    except Exception as e:
        print(f"Error getting difficulty: {e}")
        return None

def get_transaction_details(tx_hash):
    """Get transaction details using the transaction hash."""
    try:
        tx_details = fetch_api_data(f"getrawtransaction?txid={tx_hash}&decrypt=1")
        return tx_details
    except Exception as e:
        print(f"Error getting transaction details: {e}")
        return None

def get_block_reward(block_info):
    """Extract the block reward from the coinbase transaction."""
    if not block_info or 'tx' not in block_info or not block_info['tx']:
        return None
    
    # Get the first transaction in the block (coinbase transaction)
    tx_hash = block_info['tx'][0]
    
    tx_details = get_transaction_details(tx_hash)
    if not tx_details or 'vout' not in tx_details or not tx_details['vout']:
        return None
    
    # The block reward is in the first output of the coinbase transaction
    return tx_details['vout'][0]['value']

def save_emissions_data(block_number, unix_timestamp, formatted_time, block_reward=None):
    """Save emissions data to the database."""
    try:
        # Get money supply
        money_supply = get_money_supply()
        if money_supply is None:
            print("Failed to get money supply. Skipping emissions data.")
            return False

        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Check if we already have emissions data for this block
        cursor.execute("SELECT 1 FROM emissions WHERE current_block_number = %s", (block_number,))
        if cursor.fetchone():
            print(f"Emissions data for block {block_number} already exists.")
            cursor.close()
            connection.close()
            return False
        
        # Insert emissions data
        cursor.execute("""
            INSERT INTO emissions (current_block_number, unix_timestamp, date_time, money_supply, block_reward)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (current_block_number) DO NOTHING;
        """, (block_number, unix_timestamp, formatted_time, money_supply, block_reward))
        
        connection.commit()
        print(f"Emissions data for block {block_number} saved to database.")
        return True
    except Exception as e:
        print(f"Error saving emissions data: {e}")
        return False
    finally:
        if connection:
            cursor.close()
            connection.close()

def save_market_data():
    """Save market data (price and difficulty) to the database."""
    connection = None  # Initialize connection to avoid UnboundLocalError
    try:
        # Get current data
        current_time = datetime.now(timezone.utc)
        unix_timestamp = int(current_time.timestamp())
        price = get_current_price()
        difficulty = get_difficulty()
        
        if price is None or difficulty is None:
            print("Failed to get price or difficulty. Skipping market data.")
            return False
        
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Check the last entry's timestamp to avoid too frequent updates
        cursor.execute("SELECT MAX(unix_timestamp) FROM market_data")
        last_timestamp = cursor.fetchone()[0]
        
        # Only update if it's been at least 5 minutes (300 seconds) since the last update
        if last_timestamp and (unix_timestamp - last_timestamp) < 300:
            print("Skipping market data update (last update was less than 5 minutes ago)")
            cursor.close()
            connection.close()
            return False
        
        # Insert market data
        cursor.execute("""
            INSERT INTO market_data (unix_timestamp, date_time, price, difficulty)
            VALUES (%s, %s, %s, %s)
        """, (unix_timestamp, current_time, price, difficulty))
        
        connection.commit()
        print(f"Market data saved to database at {current_time}")
        return True
    except Exception as e:
        print(f"Error saving market data: {e}")
        return False
    finally:
        if connection:
            cursor.close()
            connection.close()

def ensure_blocks_table_exists():
    """Create blocks table if it doesn't exist."""
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Create blocks table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS block_data (
                current_block_number INTEGER PRIMARY KEY,
                current_block_timestamp NUMERIC,
                previous_block_number INTEGER,
                previous_block_timestamp NUMERIC,
                block_time_interval_seconds NUMERIC,
                network_hashrate NUMERIC
            );
        """)
        
        connection.commit()
        print("Blocks table created or already exists.")
    except psycopg2.Error as e:
        print(f"Database error creating blocks table: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def get_block_details(block_index):
    """Fetch hash and time for a given block index."""
    block_hash = fetch_api_data(f"getblockhash?index={block_index}")
    if block_hash is None:
        print(f"Failed to get block hash for index {block_index}.")
        return None, None, None
    
    block_info = fetch_api_data(f"getblock?hash={block_hash}")
    if block_info is None:
        print(f"Failed to get block info for index {block_index}.")
        return None, None, None
    
    block_time = block_info.get("time")
    return block_hash, block_time, block_info

def process_block(block_number):
    """Process a single block by fetching details and saving to database."""
    # Get block details
    block_hash, unix_timestamp, block_info = get_block_details(block_number)
    if unix_timestamp is None:
        raise Exception(f"Failed to get details for block {block_number}")
        
    # Get previous block for time difference calculation
    prev_block_hash, prev_unix_timestamp, _ = get_block_details(block_number - 1)
    if prev_unix_timestamp is None:
        raise Exception(f"Failed to get details for previous block {block_number - 1}")
    
    # Calculate time difference
    time_difference = unix_timestamp - prev_unix_timestamp
    formatted_time = format_unix_time(unix_timestamp)
    
    # Save to database
    success = save_to_database(block_number, block_hash, unix_timestamp, formatted_time, time_difference)
    
    if success:
        # Get block reward from coinbase transaction
        block_reward = get_block_reward(block_info)
        
        # Save emissions data
        save_emissions_data(block_number, unix_timestamp, formatted_time, block_reward)
    
    return success

def setup_database():
    ensure_blocks_table_exists()

if __name__ == "__main__":
    consecutive_failures = 0
    max_consecutive_failures = 5
    last_processed_block = None
    
    print(f"Starting Fact0rn block data collection...")
    try:
        # Set up database tables if needed
        setup_database()
    except Exception as e:
        print(f"Error setting up database: {e}")
    
    # Try to get the last processed block from the database
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        cursor.execute("SELECT MAX(current_block_number) FROM block_data")
        result = cursor.fetchone()
        if result and result[0]:
            last_processed_block = result[0]
            print(f"Last processed block: {last_processed_block}")
        
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error getting last processed block: {e}")
    
    # Main loop to check for new blocks every 5 seconds
    while True:
        try:
            # Get the latest block count
            response = requests.get('https://explorer.fact0rn.io/api/getblockcount')
            current_block_count = int(response.text.strip())
            
            # If this is our first run or we haven't processed a block yet
            if last_processed_block is None:
                last_processed_block = current_block_count - 1
            
            if current_block_count > last_processed_block:
                print(f"New block(s) detected! Current block: {current_block_count}, Last processed: {last_processed_block}")
                
                # Process all missing blocks
                for block_number in range(last_processed_block + 1, current_block_count + 1):
                    try:
                        print(f"Processing block {block_number}...")
                        process_block(block_number)
                        last_processed_block = block_number
                        consecutive_failures = 0  # Reset failure counter on success
                    except Exception as e:
                        print(f"Error processing block {block_number}: {e}")
                        consecutive_failures += 1
                        if consecutive_failures >= max_consecutive_failures:
                            print(f"Too many consecutive failures ({consecutive_failures}). Will try again next cycle.")
                            break
            else:
                print(f"No new blocks. Latest block: {current_block_count}")
                
        except Exception as e:
            print(f"Error in main loop: {e}")
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                print(f"Too many consecutive failures ({consecutive_failures}). Waiting longer before next attempt.")
                time.sleep(30)  # Wait longer after multiple failures
        
        # Sleep for 5 seconds before next check
        time.sleep(5)