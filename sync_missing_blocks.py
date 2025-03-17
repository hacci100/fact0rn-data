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
    get_block_reward,
    save_emissions_data
)

# Define moving averages periods
MOVING_AVERAGES = [100, 672]  # Only using MA-100 and MA-672

# Custom update_moving_averages function that uses the correct column names
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
            
            # Update moving averages using our custom function
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
