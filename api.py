from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import os
import logging
import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection function (reused from requestevery5seconds.py)
def get_db_connection():
    try:
        # Get database URL and fix postgres:// if needed (Heroku format)
        DATABASE_URL = os.environ.get('DATABASE_URL')
        if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
        if DATABASE_URL:
            logger.info("Using DATABASE_URL for connection")
            connection = psycopg2.connect(DATABASE_URL)
        else:
            # Fallback to local development configuration
            logger.info("Using local database configuration")
            connection = psycopg2.connect(
                dbname="fact0rn_data",
                user="postgres",
                password="Haadimoto2005",  # Consider using environment variables for security
                host="localhost",
                port="5432"
            )
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

# API Routes

@app.route('/')
def index():
    return jsonify({
        "message": "Welcome to the Fact0rn Blockchain API",
        "endpoints": {
            "GET /api/blocks": "Get recent blocks (with optional limit parameter)",
            "GET /api/blocks/<block_number>": "Get details for a specific block",
            "GET /api/stats": "Get blockchain statistics",
            "GET /api/all-data": "Get all blockchain data (use with caution)",
            "GET /api/emissions/daily": "Get emissions data grouped by UTC day"
        }
    })

@app.route('/api/blocks', methods=['GET'])
def get_blocks():
    try:
        # Get parameters from request
        limit = request.args.get('limit', 50, type=int)
        start_block = request.args.get('start_block', type=int)
        end_block = request.args.get('end_block', type=int)
        
        # Ensure reasonable limits for performance
        if limit > 10000:  # Cap at 10000 for performance
            limit = 10000
        
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First check if moving_avg_100 column exists
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'block_data' AND column_name = 'moving_avg_100';")
        has_moving_avg = cursor.fetchone() is not None
        
        # Construct the query to get block data with moving average from block_data table
        if has_moving_avg:
            query = """
                SELECT 
                    current_block_number, 
                    block_time_interval_seconds,
                    current_block_timestamp,
                    moving_avg_100
                FROM block_data
            """
        else:
            query = """
                SELECT 
                    current_block_number, 
                    block_time_interval_seconds,
                    current_block_timestamp
                FROM block_data
            """
        
        conditions = []
        
        # Add conditions based on parameters
        if start_block:
            conditions.append(f"current_block_number >= {start_block}")
        if end_block:
            conditions.append(f"current_block_number <= {end_block}")
        
        # Add WHERE clause if there are conditions
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Add ORDER BY and LIMIT clauses
        query += " ORDER BY current_block_number DESC"
        if limit:
            query += f" LIMIT {limit}"
        
        # Execute the query
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        blocks = []
        for row in rows:
            block_data = {
                'block_number': row[0],
                'block_time_seconds': row[1],
                'timestamp': row[2],
                'datetime': datetime.datetime.fromtimestamp(row[2]).strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Add moving average if it exists and the column exists
            if has_moving_avg and len(row) > 3 and row[3] is not None:
                block_data['moving_avg_100'] = float(row[3])
                
            blocks.append(block_data)
        
        # Sort blocks by block_number (ascending)
        blocks.sort(key=lambda x: x['block_number'])
        
        # Return the result as JSON
        return jsonify(blocks)
    
    except Exception as e:
        # Log the error and return an error response
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/blocks/<int:block_number>', methods=['GET'])
def get_block(block_number):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get block data
        cursor.execute("""
            SELECT current_block_number, current_block_timestamp, 
                   previous_block_number, previous_block_timestamp,
                   block_time_interval_seconds, network_hashrate
            FROM block_data
            WHERE current_block_number = %s
        """, (block_number,))
        block = cursor.fetchone()
        
        if not block:
            cursor.close()
            conn.close()
            return jsonify({'error': f'Block {block_number} not found'}), 404
        
        # Format block data
        block_time = block[1]  # Unix timestamp
        formatted_time = datetime.datetime.fromtimestamp(block_time).strftime('%Y-%m-%d %H:%M:%S')
        
        result = {
            'block_number': block[0],
            'timestamp': block_time,
            'datetime': formatted_time,
            'previous_block': block[2],
            'previous_timestamp': block[3],
            'block_time_seconds': block[4],
            'network_hashrate': float(block[5]) if block[5] is not None else None
        }
        
        # Check if emissions data exists for this block
        cursor.execute("""
            SELECT money_supply, block_reward
            FROM emissions
            WHERE current_block_number = %s
        """, (block_number,))
        emissions = cursor.fetchone()
        
        if emissions:
            result['money_supply'] = float(emissions[0]) if emissions[0] is not None else None
            result['block_reward'] = float(emissions[1]) if emissions[1] is not None else None
        
        cursor.close()
        conn.close()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_block: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get latest block
        cursor.execute("""
            SELECT MAX(current_block_number) FROM block_data
        """)
        latest_block = cursor.fetchone()[0]
        
        # Get average block time (last 100 blocks)
        cursor.execute("""
            SELECT AVG(block_time_interval_seconds)
            FROM (
                SELECT block_time_interval_seconds 
                FROM block_data 
                ORDER BY current_block_number DESC 
                LIMIT 100
            ) as recent_blocks
        """)
        avg_block_time = cursor.fetchone()[0]
        
        # Get average hashrate (last 100 blocks)
        cursor.execute("""
            SELECT AVG(network_hashrate)
            FROM (
                SELECT network_hashrate 
                FROM block_data 
                WHERE network_hashrate IS NOT NULL
                ORDER BY current_block_number DESC 
                LIMIT 100
            ) as recent_blocks
        """)
        avg_hashrate = cursor.fetchone()[0]
        
        # Get total blocks count
        cursor.execute("""
            SELECT COUNT(*) FROM block_data
        """)
        total_blocks = cursor.fetchone()[0]
        
        # Get latest money supply if available
        money_supply = None
        cursor.execute("""
            SELECT money_supply
            FROM emissions
            ORDER BY current_block_number DESC
            LIMIT 1
        """)
        supply_result = cursor.fetchone()
        if supply_result:
            money_supply = float(supply_result[0]) if supply_result[0] is not None else None
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'latest_block': latest_block,
            'total_blocks_recorded': total_blocks,
            'average_block_time': float(avg_block_time) if avg_block_time is not None else None,
            'average_hashrate': float(avg_hashrate) if avg_hashrate is not None else None,
            'money_supply': money_supply
        })
    except Exception as e:
        logger.error(f"Error in get_stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/all-data', methods=['GET'])
def get_all_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all block data
        cursor.execute("""
            SELECT current_block_number, current_block_timestamp, 
                   previous_block_number, previous_block_timestamp,
                   block_time_interval_seconds, network_hashrate
            FROM block_data
            ORDER BY current_block_number DESC
        """)
        blocks = cursor.fetchall()
        
        result = []
        for block in blocks:
            block_time = block[1]  # Unix timestamp
            formatted_time = datetime.datetime.fromtimestamp(block_time).strftime('%Y-%m-%d %H:%M:%S')
            
            block_data = {
                'block_number': block[0],
                'timestamp': block_time,
                'datetime': formatted_time,
                'previous_block': block[2],
                'previous_timestamp': block[3],
                'block_time_seconds': block[4],
                'network_hashrate': float(block[5]) if block[5] is not None else None
            }
            
            # Get emissions data for this block if available
            cursor.execute("""
                SELECT money_supply, block_reward
                FROM emissions
                WHERE current_block_number = %s
            """, (block[0],))
            emissions = cursor.fetchone()
            
            if emissions:
                block_data['money_supply'] = float(emissions[0]) if emissions[0] is not None else None
                block_data['block_reward'] = float(emissions[1]) if emissions[1] is not None else None
            
            result.append(block_data)
        
        cursor.close()
        conn.close()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_all_data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/emissions/daily', methods=['GET'])
def get_daily_emissions():
    try:
        # Get parameters from request
        days = request.args.get('days', 7, type=int)  # Default to last 7 days
        
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if emissions table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'emissions')")
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            return jsonify({"error": "Emissions data is not available"}), 404
        
        # Query to get emissions data grouped by day (in UTC time)
        query = """
            SELECT 
                DATE_TRUNC('day', date_time) as day,
                MAX(money_supply) - MIN(money_supply) as daily_emission,
                MIN(money_supply) as start_supply,
                MAX(money_supply) as end_supply,
                COUNT(*) as block_count,
                MIN(current_block_number) as first_block,
                MAX(current_block_number) as last_block
            FROM emissions
            WHERE date_time >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY DATE_TRUNC('day', date_time)
            ORDER BY day DESC
            LIMIT %s
        """
        
        cursor.execute(query, (days, days))
        rows = cursor.fetchall()
        
        if not rows:
            return jsonify({"error": "No emissions data found for the specified period"}), 404
        
        # Format the results
        emissions_data = []
        for row in rows:
            day, daily_emission, start_supply, end_supply, block_count, first_block, last_block = row
            
            emissions_data.append({
                "date": day.strftime("%Y-%m-%d"),
                "daily_emission": float(daily_emission) if daily_emission is not None else None,
                "start_supply": float(start_supply) if start_supply is not None else None,
                "end_supply": float(end_supply) if end_supply is not None else None,
                "block_count": block_count,
                "first_block": first_block,
                "last_block": last_block
            })
        
        cursor.close()
        conn.close()
        
        return jsonify(emissions_data)
    
    except Exception as e:
        logger.error(f"Error in get_daily_emissions: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Run the Flask app
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)