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
        return None, None, None
    
    block_info = fetch_api_data(f"getblock?hash={block_hash}")
    if block_info is None:
        print(f"Failed to get block info for index {block_index}.")
        return None, None, None
    
    block_time = block_info.get("time")
    return block_hash, block_time, block_info

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

def main():
    # Initial block count
    last_block_count = fetch_api_data("getblockcount")
    if last_block_count is None:
        print("Failed to get initial block count. Exiting.")
        exit()

    print(f"Starting with block index: {last_block_count}")
    print("Monitoring for block index changes every 5 seconds...")
    
    # Initialize data update timers
    last_market_update = 0
    last_money_supply_update = 0
    last_money_supply_value = None

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
            latest_block_hash, latest_block_time, block_info = get_block_details(current_block_count)
            if latest_block_time is None:
                print("Failed to fetch latest block details. Continuing...")
                last_block_count = current_block_count
                time.sleep(5)
                continue

            # Fetch details for the previous block
            previous_block_index = current_block_count - 1
            previous_block_hash, previous_block_time, _ = get_block_details(previous_block_index)
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
            
            # Get block reward from coinbase transaction
            block_reward = get_block_reward(block_info)
            if block_reward is not None:
                print(f"Block Reward: {block_reward}")
            
            # Check if we should update money supply data (every 60 seconds)
            current_time = int(time.time())
            if current_time - last_money_supply_update >= 60:  # 60 seconds = 1 minute
                print("Updating money supply data...")
                money_supply = get_money_supply()
                if money_supply is not None:
                    last_money_supply_value = money_supply
                    last_money_supply_update = current_time
                    print(f"Money Supply: {money_supply}")
            
            # Save emissions data with the latest money supply value
            try:
                connection = get_db_connection()
                cursor = connection.cursor()
                
                # Check if we already have emissions data for this block
                cursor.execute("SELECT 1 FROM emissions WHERE current_block_number = %s", (current_block_count,))
                if not cursor.fetchone():
                    # Insert emissions data
                    cursor.execute("""
                        INSERT INTO emissions (current_block_number, unix_timestamp, date_time, money_supply, block_reward)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (current_block_number) DO NOTHING;
                    """, (current_block_count, latest_block_time, formatted_block_time, last_money_supply_value, block_reward))
                    
                    connection.commit()
                    print(f"Emissions data for block {current_block_count} saved to database.")
                else:
                    print(f"Emissions data for block {current_block_count} already exists.")
            except Exception as e:
                print(f"Error saving emissions data: {e}")
            finally:
                if connection:
                    cursor.close()
                    connection.close()

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
        
        # Check if we should update market data (every 5 minutes)
        current_time = int(time.time())
        if current_time - last_market_update >= 300:  # 300 seconds = 5 minutes
            print("Updating market data...")
            if save_market_data():
                last_market_update = current_time
                
        # Check if we should update money supply outside of block updates (every 60 seconds)
        if current_time - last_money_supply_update >= 60:
            print("Updating money supply data (outside block update)...")
            money_supply = get_money_supply()
            if money_supply is not None:
                last_money_supply_value = money_supply
                last_money_supply_update = current_time
                print(f"Money Supply (outside block update): {money_supply}")

        # Wait 5 seconds before checking again
        time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")