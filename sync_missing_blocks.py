import requests
import time
from datetime import datetime, timezone
import psycopg2
import os
import sys

# Import functions from requestevery5seconds.py
from requestevery5seconds import (
    get_db_connection,
    fetch_api_data,
    get_block_details,
    format_unix_time,
    fetch_current_hashrate,
    update_moving_averages,
    get_block_reward,
    save_emissions_data
)

def sync_missing_blocks(start_block, end_block):
    """
    Sync missing blocks from start_block to end_block (inclusive).
    """
    print(f"Starting sync of blocks from {start_block} to {end_block}...")
    
    # Connect to the database
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check which blocks are already in the database
        cursor.execute("SELECT current_block_number FROM block_data WHERE current_block_number BETWEEN %s AND %s", 
                      (start_block, end_block))
        existing_blocks = set(row[0] for row in cursor.fetchall())
        
        # Calculate blocks to sync
        blocks_to_sync = [block for block in range(start_block, end_block + 1) if block not in existing_blocks]
        
        if not blocks_to_sync:
            print("No missing blocks to sync!")
            return
            
        print(f"Found {len(blocks_to_sync)} blocks to sync: {blocks_to_sync}")
        
        # Sync each missing block
        for block_index in blocks_to_sync:
            print(f"Processing block {block_index}...")
            
            # Get block details
            block_hash, block_time, block_info = get_block_details(block_index)
            if block_time is None:
                print(f"Failed to fetch details for block {block_index}. Skipping...")
                continue
            
            # Get previous block details
            prev_block_index = block_index - 1
            prev_block_hash, prev_block_time, _ = get_block_details(prev_block_index)
            if prev_block_time is None:
                print(f"Failed to fetch previous block details for block {block_index}. Skipping...")
                continue
            
            # Calculate time difference
            time_difference = block_time - prev_block_time
            formatted_block_time = format_unix_time(block_time)
            
            # Get current hashrate
            current_hashrate = fetch_current_hashrate()
            
            # Insert into block_data table
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
                block_time, 
                prev_block_index,
                prev_block_time,
                time_difference,
                current_hashrate
            ))
            
            # Update moving averages
            update_moving_averages(conn, cursor, block_index)
            
            # Get block reward from coinbase transaction
            block_reward = get_block_reward(block_info)
            
            # Save emissions data for the block
            save_emissions_data(block_index, block_time, formatted_block_time, block_reward)
            
            print(f"Successfully synced block {block_index}")
            
        conn.commit()
        print(f"Successfully synced all missing blocks from {start_block} to {end_block}")
    except Exception as e:
        print(f"Error syncing blocks: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    # Get start and end blocks from command line arguments
    if len(sys.argv) < 3:
        print("Usage: python sync_missing_blocks.py <start_block> <end_block>")
        sys.exit(1)
    
    start_block = int(sys.argv[1])
    end_block = int(sys.argv[2])
    
    sync_missing_blocks(start_block, end_block)
