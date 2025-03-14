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
            "GET /api/all-data": "Get all blockchain data (use with caution)"
        }
    })

@app.route('/api/blocks', methods=['GET'])
def get_blocks():
    try:
        # Get limit parameter (default 100)
        limit = request.args.get('limit', default=100, type=int)
        if limit > 1000:  # Cap at 1000 for performance
            limit = 1000
            
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT current_block_number, current_block_timestamp, 
                   previous_block_number, previous_block_timestamp,
                   block_time_interval_seconds, network_hashrate
            FROM block_data
            ORDER BY current_block_number DESC
            LIMIT %s
        """, (limit,))
        blocks = cursor.fetchall()
        
        result = []
        for block in blocks:
            block_time = block[1]  # Unix timestamp
            formatted_time = datetime.datetime.fromtimestamp(block_time).strftime('%Y-%m-%d %H:%M:%S')
            
            result.append({
                'block_number': block[0],
                'timestamp': block_time,
                'datetime': formatted_time,
                'previous_block': block[2],
                'previous_timestamp': block[3],
                'block_time_seconds': block[4],
                'network_hashrate': float(block[5]) if block[5] is not None else None
            })
        
        cursor.close()
        conn.close()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in get_blocks: {e}")
        return jsonify({'error': str(e)}), 500

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

if __name__ == '__main__':
    # Run the Flask app
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)