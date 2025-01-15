# import json
# import os
# import mysql.connector

# # Function to fetch all table and view names from a database
# def fetch_table_names(database):
#     # Connect to the MySQL database
#     connection = mysql.connector.connect(
#         host="localhost", 
#         user="root", 
#         password="admin", 
#         database=database
#     )
#     cursor = connection.cursor()

#     # Query to get the list of tables and views
#     cursor.execute("SHOW TABLES")
#     tables = [table[0] for table in cursor.fetchall()]

#     # Close the connection
#     cursor.close()
#     connection.close()

#     return tables

# # Function to fetch column names from a database view or table
# def fetch_column_names(database, table):
#     # Connect to the MySQL database
#     connection = mysql.connector.connect(
#         host="localhost", 
#         user="root", 
#         password="admin", 
#         database=database
#     )
#     cursor = connection.cursor()

#     # Use backticks around table names to handle special characters (spaces, etc.)
#     query = f"DESCRIBE `{table}`"
#     cursor.execute(query)
#     columns = [column[0] for column in cursor.fetchall()]

#     # Close the connection
#     cursor.close()
#     connection.close()

#     return columns

# # Function to save metadata in a JSON file
# def save_metadata(database_name, metadata_folder="metadata"):
#     # Ensure the metadata folder exists
#     if not os.path.exists(metadata_folder):
#         os.makedirs(metadata_folder)
    
#     # Fetch all table names in the database
#     table_names = fetch_table_names(database_name)
    
#     metadata = {}
    
#     # Fetch column names for each table/view
#     for table in table_names:
#         columns = fetch_column_names(database_name, table)
#         metadata[table] = columns
    
#     # Save the metadata to a JSON file
#     with open(f"{metadata_folder}/{database_name}_metadata.json", "w") as json_file:
#         json.dump(metadata, json_file, indent=4)

# # Example usage: Automatically fetch metadata for Amazon and Flipkart databases
# # save_metadata("amazon")
# save_metadata("flipkart")







# import json
# import os
# import mysql.connector
# from pymongo import MongoClient
# import socket

# # MySQL-related Functions
# def fetch_table_names_mysql(database):
#     try:
#         # Connect to the MySQL database
#         connection = mysql.connector.connect(
#             host="localhost", 
#             user="root", 
#             password="admin", 
#             database=database
#         )
#         cursor = connection.cursor()

#         # Query to get the list of tables
#         cursor.execute("SHOW TABLES")
#         tables = [table[0] for table in cursor.fetchall()]

#         # Close the connection
#         cursor.close()
#         connection.close()

#         return tables
#     except mysql.connector.Error as err:
#         print(f"Error connecting to MySQL: {err}")
#         return []

# def fetch_column_names_mysql(database, table):
#     try:
#         # Connect to the MySQL database
#         connection = mysql.connector.connect(
#             host="localhost", 
#             user="root", 
#             password="admin", 
#             database=database
#         )
#         cursor = connection.cursor()

#         # Query to get column names from a table
#         query = f"DESCRIBE `{table}`"
#         cursor.execute(query)
#         columns = [column[0] for column in cursor.fetchall()]

#         # Close the connection
#         cursor.close()
#         connection.close()

#         return columns
#     except mysql.connector.Error as err:
#         print(f"Error fetching columns from MySQL table {table}: {err}")
#         return []

# # MongoDB-related Functions
# def fetch_collection_names_mongo(database):
#     try:
#         # Connect to MongoDB
#         client = MongoClient("mongodb://localhost:27017/")  # Assuming MongoDB is on localhost
#         db = client[database]

#         # Fetch all collections
#         collections = db.list_collection_names()
#         return collections
#     except Exception as e:
#         print(f"Error connecting to MongoDB: {e}")
#         return []

# def fetch_field_names_mongo(database, collection):
#     try:
#         # Connect to MongoDB
#         client = MongoClient("mongodb://localhost:27017/")  # Assuming MongoDB is on localhost
#         db = client[database]
#         coll = db[collection]

#         # Fetch a sample document to extract fields (this method assumes at least one document exists)
#         sample_document = coll.find_one()
#         if sample_document:
#             fields = list(sample_document.keys())
#         else:
#             fields = []
#         return fields
#     except Exception as e:
#         print(f"Error fetching field names from MongoDB collection {collection}: {e}")
#         return []

# # Socket-related Function for Vivek's Amazon Database
# def send_query_to_vevek_amazon(query):
#     host = '192.168.49.246'  # Replace with the IP address of Vivek's machine
#     port = 12345  # Ensure this matches the server's port

#     # Create socket connection to the server
#     client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     client_socket.connect((host, port))

#     # Send the SQL query to the server
#     client_socket.send(query.encode('utf-8'))

#     # Receive the response from the server
#     response = client_socket.recv(100000).decode('utf-8')

#     # If the response is JSON, decode it
#     try:
#         # Convert any bytes objects in the response to string (str)
#         result = json.loads(response)

#         # Convert any byte values in the result to strings
#         for i, table in enumerate(result):
#             result[i] = [col.decode('utf-8') if isinstance(col, bytes) else col for col in table]

#         return result
#     except json.JSONDecodeError:
#         print("Error:", response)
#         return None
#     finally:
#         client_socket.close()


# # Function to save metadata for MySQL, MongoDB, and Amazon
# def save_metadata(database_name, metadata_folder="metadata", is_mongo=False, is_amazon=False):
#     # Ensure the metadata folder exists
#     if not os.path.exists(metadata_folder):
#         os.makedirs(metadata_folder)

#     metadata = {}

#     if is_mongo:
#         # Fetch MongoDB collections and fields
#         collection_names = fetch_collection_names_mongo(database_name)
#         if collection_names:
#             for collection in collection_names:
#                 fields = fetch_field_names_mongo(database_name, collection)
#                 metadata[collection] = fields
#         else:
#             print(f"No collections found in MongoDB database {database_name}.")
#     elif is_amazon:
#         # For Amazon (Vivek's machine), we need to send a query to fetch metadata
#         # First fetch list of tables
#         query = "SHOW TABLES;"  # Query to fetch list of tables
#         tables_result = send_query_to_vevek_amazon(query)

#         if tables_result:
#             # For each table, send a DESCRIBE query to fetch columns and data types
#             for table in tables_result:
#                 # Since tables_result is a list, we can directly access each table
#                 table_name = table[0]  # The table name is at index 0 of each list item
#                 if table_name:
#                     describe_query = f"DESCRIBE `{table_name}`;"  # Query to describe table columns
#                     columns_result = send_query_to_vevek_amazon(describe_query)
#                     if columns_result:
#                         metadata[table_name] = {col['Field']: col['Type'] for col in columns_result}
#         else:
#             print(f"No tables found in Amazon database on Vivek's machine.")
#     else:
#         # Fetch MySQL tables and columns along with data types
#         table_names = fetch_table_names_mysql(database_name)
#         if table_names:
#             for table in table_names:
#                 columns = fetch_column_names_mysql(database_name, table)
#                 metadata[table] = columns
#         else:
#             print(f"No tables found in MySQL database {database_name}.")

#     # Save the metadata to a JSON file
#     if metadata:
#         with open(f"{metadata_folder}/{database_name}_metadata.json", "w") as json_file:
#             json.dump(metadata, json_file, indent=4)
#         print(f"Metadata for {database_name} saved successfully.")
#     else:
#         print(f"No metadata available for {database_name}.")


# # Example usage: Fetch metadata for Instagram (MongoDB), Flipkart (MySQL), and Amazon (via Socket)
# save_metadata("instagram", is_mongo=True)  # For MongoDB (ensure your database is named 'instagram')
# save_metadata("flipkart", is_mongo=False)  # For MySQL (ensure your database is named 'flipkart')
# save_metadata("amazon", is_amazon=True)    # For Amazon (Vivek's database)


# import json
# import os
# import mysql.connector
# from pymongo import MongoClient
# import socket

# # MySQL-related Functions
# def fetch_table_names_mysql(database):
#     try:
#         connection = mysql.connector.connect(
#             host="localhost", 
#             user="root", 
#             password="admin", 
#             database=database
#         )
#         cursor = connection.cursor()
#         cursor.execute("SHOW TABLES")
#         tables = [table[0] for table in cursor.fetchall()]
#         cursor.close()
#         connection.close()
#         return tables
#     except mysql.connector.Error as err:
#         print(f"Error connecting to MySQL: {err}")
#         return []

# def fetch_column_names_mysql(database, table):
#     try:
#         connection = mysql.connector.connect(
#             host="localhost", 
#             user="root", 
#             password="admin", 
#             database=database
#         )
#         cursor = connection.cursor()
#         query = f"DESCRIBE {table}"
#         cursor.execute(query)
#         columns = [column[0] for column in cursor.fetchall()]
#         cursor.close()
#         connection.close()
#         return columns
#     except mysql.connector.Error as err:
#         print(f"Error fetching columns from MySQL table {table}: {err}")
#         return []

# # MongoDB-related Functions
# def fetch_collection_names_mongo(database):
#     try:
#         client = MongoClient("mongodb://localhost:27017/")
#         db = client[database]
#         collections = db.list_collection_names()
#         return collections
#     except Exception as e:
#         print(f"Error connecting to MongoDB: {e}")
#         return []

# def fetch_field_names_mongo(database, collection):
#     try:
#         client = MongoClient("mongodb://localhost:27017/")
#         db = client[database]
#         coll = db[collection]
#         sample_document = coll.find_one()
#         fields = list(sample_document.keys()) if sample_document else []
#         return fields
#     except Exception as e:
#         print(f"Error fetching field names from MongoDB collection {collection}: {e}")
#         return []

# # Socket-related Function for Vivek's Amazon Database
# def send_query_to_vevek_amazon(query):
#     host = '192.168.49.246'  # Replace with the IP address of Vivek's machine
#     port = 12345  # Ensure this matches the server's port

#     client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     try:
#         client_socket.connect((host, port))
#         client_socket.send(query.encode('utf-8'))
#         response = client_socket.recv(100000).decode('utf-8')

#         # Decode JSON response
#         try:
#             result = json.loads(response)
#             return result
#         except json.JSONDecodeError as e:
#             print(f"Failed to decode JSON response: {e}")
#             return None
#     except Exception as e:
#         print(f"Error during socket communication: {e}")
#         return None
#     finally:
#         client_socket.close()

# def save_metadata(database_name, metadata_folder="metadata", is_mongo=False, is_amazon=False):
#     # Ensure the metadata folder exists
#     if not os.path.exists(metadata_folder):
#         os.makedirs(metadata_folder)

#     metadata = {}

#     if is_amazon:
#         query = "SHOW TABLES;"
#         tables_result = send_query_to_vevek_amazon(query)

#         if tables_result:
#             for table in tables_result:
#                 table_name = table[0]
#                 if table_name:
#                     describe_query = f"DESCRIBE {table_name};"
#                     columns_result = send_query_to_vevek_amazon(describe_query)
#                     if columns_result:
#                         metadata[table_name] = {
#                             col[0]: col[1] for col in columns_result
#                         }
#         else:
#             print(f"No tables found in Amazon database on Vivek's machine.")
#     elif is_mongo:
#         # Fetch MongoDB collections and fields
#         collection_names = fetch_collection_names_mongo(database_name)
#         if collection_names:
#             for collection in collection_names:
#                 fields = fetch_field_names_mongo(database_name, collection)
#                 metadata[collection] = fields
#             # print(f"Fetched metadata for MongoDB database {database_name}: {metadata}")
#         else:
#             print(f"No collections found in MongoDB database {database_name}.")
#     elif not is_amazon:
#         # MySQL part
#         table_names = fetch_table_names_mysql(database_name)
#         if table_names:
#             for table in table_names:
#                 columns = fetch_column_names_mysql(database_name, table)
#                 metadata[table] = columns
#             # print(f"Fetched metadata for MySQL database {database_name}: {metadata}")
#         else:
#             print(f"No tables found in MySQL database {database_name}.")

#     if metadata:
#         with open(f"{metadata_folder}/{database_name}_metadata.json", "w") as json_file:
#             json.dump(metadata, json_file, indent=4)
#         print(f"Metadata for {database_name} saved successfully.")
#     else:
#         print(f"No metadata available for {database_name}.")

# # Example usage
# save_metadata("instagram", is_mongo=True)  # MongoDB
# save_metadata("flipkart", is_mongo=False)  # MySQL
# save_metadata("amazon", is_amazon=True)    # Vivek's database












###############################################################################################################################\
    
    
    
import json
import os
import mysql.connector
from pymongo import MongoClient

# MySQL-related Functions
def fetch_table_names_mysql(database):
    try:
        connection = mysql.connector.connect(
            host="localhost", 
            user="root", 
            password="admin", 
            database=database
        )
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        connection.close()
        return tables
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return []

def fetch_column_names_mysql(database, table):
    try:
        connection = mysql.connector.connect(
            host="localhost", 
            user="root", 
            password="admin", 
            database=database
        )
        cursor = connection.cursor()
        query = f"DESCRIBE {table}"
        cursor.execute(query)
        columns = [column[0] for column in cursor.fetchall()]
        cursor.close()
        connection.close()
        return columns
    except mysql.connector.Error as err:
        print(f"Error fetching columns from MySQL table {table}: {err}")
        return []

# MongoDB-related Functions
def fetch_collection_names_mongo(database, mongo_uri="mongodb://192.168.47.237:27017/"):
    
    if database == "iia_amazon":
        try:
            client = MongoClient("mongodb://192.168.49.246:27017/")
            db = client[database]
            collections = db.list_collection_names()
            return collections
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            return []
    else:
        try:
            client = MongoClient("mongodb://192.168.47.237:27017/")
            db = client[database]
            collections = db.list_collection_names()
            return collections
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            return []

def fetch_field_names_mongo(database, collection, mongo_uri="mongodb://localhost:27017/"):
    if database == "iia_amazon":
        try:
            client = MongoClient("mongodb://192.168.49.246:27017/")
            db = client[database]
            coll = db[collection]
            sample_document = coll.find_one()
            fields = list(sample_document.keys()) if sample_document else []
            return fields
        except Exception as e:
            print(f"Error fetching field names from MongoDB collection {collection}: {e}")
            return []
    else:
        try:
            client = MongoClient("mongodb://192.168.47.237:27017/")
            db = client[database]
            coll = db[collection]
            sample_document = coll.find_one()
            fields = list(sample_document.keys()) if sample_document else []
            return fields
        except Exception as e:
            print(f"Error fetching field names from MongoDB collection {collection}: {e}")
            return []

def save_metadata(database_name, metadata_folder="metadata", is_mongo=False, is_amazon=False, mongo_uri="mongodb://localhost:27017/"):
    # Ensure the metadata folder exists
    if not os.path.exists(metadata_folder):
        os.makedirs(metadata_folder)

    metadata = {}

    if is_amazon:
        # Fetch MongoDB collections and fields for Vivek's Amazon database
        collection_names = fetch_collection_names_mongo(database_name, mongo_uri=mongo_uri)
        if collection_names:
            for collection in collection_names:
                fields = fetch_field_names_mongo(database_name, collection, mongo_uri=mongo_uri)
                metadata[collection] = fields
        else:
            print(f"No collections found in Amazon MongoDB database {database_name}.")
    elif is_mongo:
        # Fetch MongoDB collections and fields
        collection_names = fetch_collection_names_mongo(database_name, mongo_uri=mongo_uri)
        if collection_names:
            for collection in collection_names:
                fields = fetch_field_names_mongo(database_name, collection, mongo_uri=mongo_uri)
                metadata[collection] = fields
        else:
            print(f"No collections found in MongoDB database {database_name}.")
    elif not is_amazon:
        # MySQL part
        table_names = fetch_table_names_mysql(database_name)
        if table_names:
            for table in table_names:
                columns = fetch_column_names_mysql(database_name, table)
                metadata[table] = columns
        else:
            print(f"No tables found in MySQL database {database_name}.")

    if metadata:
        with open(f"{metadata_folder}/{database_name}_metadata.json", "w") as json_file:
            json.dump(metadata, json_file, indent=4)
        print(f"Metadata for {database_name} saved successfully at {metadata_folder}/{database_name}_metadata.json.")
    else:
        print(f"No metadata available for {database_name}.")
        return False

    return True

# Example usage
if save_metadata("instagram", is_mongo=True):
    print("Instagram metadata saved successfully.")

if save_metadata("flipkart", is_mongo=False):
    print("Flipkart metadata saved successfully.")

if save_metadata("iia_amazon", is_amazon=True, mongo_uri="mongodb://192.168.49.246:27017/"):
    print("Amazon metadata saved successfully.")
