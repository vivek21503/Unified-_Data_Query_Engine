from pymongo import MongoClient
import mysql.connector
import json
from jellyfish import jaro_winkler_similarity
import spacy
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load spaCy model
try:
    nlp = spacy.load("en_core_web_sm")
except Exception as e:
    logging.error(f"Error loading spaCy model: {e}")
    exit(1)

def get_mongodb_connection():
    """Get MongoDB connection for Amazon data"""
    try:
        logging.info("Connecting to MongoDB...")
        client = MongoClient("mongodb://192.168.49.246:27017/")
        db = client["iia_amazon"]
        logging.info("Successfully connected to MongoDB")
        return client, db
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        return None, None

def get_mysql_connection():
    """Get MySQL connection for Flipkart data"""
    try:
        logging.info("Connecting to MySQL...")
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="admin",
            database="flipkart"
        )
        logging.info("Successfully connected to MySQL")
        return connection
    except mysql.connector.Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

def fetch_amazon_data():
    """Fetch data from Amazon MongoDB"""
    client, db = get_mongodb_connection()
    if not client:
        return []
    
    try:
        collection = db["combined_amazon_data"]
        results = list(collection.find({}))
        # Convert ObjectId to string
        for result in results:
            result['_id'] = str(result['_id'])
        logging.info(f"Fetched {len(results)} records from Amazon")
        return results
    except Exception as e:
        logging.error(f"Error fetching Amazon data: {e}")
        return []
    finally:
        client.close()

def fetch_flipkart_data():
    """Fetch data from Flipkart MySQL"""
    connection = get_mysql_connection()
    if not connection:
        return []
    
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT 
                category, name, ratings, discount_price, 
                actual_price, description, highlights, image_links
            FROM global_flipkart_products_view
        """)
        results = cursor.fetchall()
        logging.info(f"Fetched {len(results)} records from Flipkart")
        return results
    except mysql.connector.Error as e:
        logging.error(f"Error fetching Flipkart data: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def create_hlv_table():
    """Create HLV table in Flipkart database"""
    connection = get_mysql_connection()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        # Drop table if exists
        cursor.execute("DROP TABLE IF EXISTS hlv")
        logging.info("Dropped existing HLV table")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS hlv (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(500),
            category VARCHAR(100),
            ratings FLOAT,
            discount_price DECIMAL(10,2),
            actual_price DECIMAL(10,2),
            source VARCHAR(50)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        logging.info("Created new HLV table")
        return True
    except mysql.connector.Error as e:
        logging.error(f"Error creating HLV table: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def extract_entities(text):
    """Extract entities from text using spaCy"""
    doc = nlp(text)
    return [ent.text.lower() for ent in doc.ents]

def calculate_similarity(amazon_item, flipkart_item):
    """Calculate similarity between Amazon and Flipkart items"""
    try:
        # Calculate name similarity
        name_similarity = jaro_winkler_similarity(
            str(amazon_item['nameitem']).lower(),
            str(flipkart_item['name']).lower()
        )
        
        # Calculate price similarity
        amazon_price = float(amazon_item['reducedprice'])
        flipkart_price = float(flipkart_item['discount_price'])
        price_diff = abs(amazon_price - flipkart_price)
        max_price = max(amazon_price, flipkart_price)
        price_similarity = 1 - (price_diff / max_price if max_price > 0 else 0)
        
        # Weighted average (giving more weight to name similarity)
        similarity = (0.7 * name_similarity + 0.3 * price_similarity)
        
        return similarity
    except Exception as e:
        # logging.error(f"Error calculating similarity: {e}")
        return 0

def insert_into_hlv(matched_items):
    """Insert matched items into HLV table"""
    connection = get_mysql_connection()
    if not connection:
        return
    
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO hlv (
            name, category, ratings, discount_price, actual_price, source
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, matched_items)
        connection.commit()
        logging.info(f"Inserted to HLV")
    except mysql.connector.Error as e:
        logging.error(f"Error inserting into HLV: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def main():
    logging.info("Starting entity matching process...")
    
    # Create HLV table
    if not create_hlv_table():
        return
    
    # Fetch data
    amazon_data = fetch_amazon_data()
    flipkart_data = fetch_flipkart_data()
    
    if not amazon_data or not flipkart_data:
        logging.error("No data to process")
        return
    
    matched_items = []
    similarity_threshold = 0.7
    
    logging.info("Starting matching process...")
    # Match items
    for i, amazon_item in enumerate(amazon_data):
        for flipkart_item in flipkart_data:
            try:
                similarity = calculate_similarity(amazon_item, flipkart_item)
                
                if similarity >= similarity_threshold:
                    # Add Amazon item
                    matched_items.append((
                        amazon_item['nameitem'],
                        amazon_item.get('sub_category', ''),
                        float(amazon_item.get('user_ratings', 0)),
                        float(amazon_item['reducedprice']),
                        float(amazon_item['actualprice']),
                        'amazon'
                    ))
                    
                    # Add Flipkart item
                    matched_items.append((
                        flipkart_item['name'],
                        flipkart_item.get('category', ''),
                        float(flipkart_item.get('ratings', 0)),
                        float(flipkart_item['discount_price']),
                        float(flipkart_item['actual_price']),
                        'flipkart'
                    ))
            except Exception as e:
                logging.error(f"Error processing items: {e}")
                logging.error(f"Amazon item: {amazon_item}")
                logging.error(f"Flipkart item: {flipkart_item}")
                continue
        
        if i % 100 == 0:
            # logging.info(f"Processed {i} Amazon items...")
            pass
    
    # Insert matched items into HLV
    if matched_items:
        
        data= set(matched_items)

        finaldata=list(data)
        
        insert_into_hlv(finaldata)
        logging.info(f"Successfully matched and stored")
    else:
        logging.warning("No matches found")

if __name__ == "__main__":
    main() 