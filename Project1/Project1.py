

# import subprocess
# import json
# import os
# import re
# import mysql.connector
# from tkinter import Tk, Label, Text, Button, Scrollbar, messagebox, ttk


# # Helper function to connect to the database
# def create_connection(database_name=None):
#     try:
#         return mysql.connector.connect(
#             host="localhost",
#             user="root",
#             password="admin",
#             database=database_name if database_name else None
#         )
#     except mysql.connector.Error as e:
#         print(f"Error connecting to MySQL database: {e}")
#         raise


# # Run the metadata generation script (meta.py)
# def generate_metadata():
#     try:
#         subprocess.run(["python", "meta.py"], check=True)
#         print("Metadata generated successfully.")
#     except subprocess.CalledProcessError as e:
#         print(f"Error running meta.py: {e}")


# # Load metadata from JSON files
# def load_metadata(metadata_folder="metadata"):
#     metadata = {}
#     if not os.path.exists(metadata_folder):
#         print(f"Metadata folder '{metadata_folder}' does not exist.")
#         return metadata

#     for file_name in os.listdir(metadata_folder):
#         if file_name.endswith("_metadata.json"):
#             database_name = file_name.replace("_metadata.json", "")
#             try:
#                 with open(os.path.join(metadata_folder, file_name), "r") as json_file:
#                     metadata[database_name] = json.load(json_file)
#             except json.JSONDecodeError as e:
#                 print(f"Error parsing {file_name}: {e}")

#     return metadata

# # # Validate query components against metadata
# def validate_query_with_metadata(parsed_queries, metadata):
#     for parsed_query in parsed_queries:
#         for table in parsed_query["tables"]:
#             db_name, table_name = table.split(".")
#             if db_name not in metadata or table_name not in metadata[db_name]:
#                 raise ValueError(f"Table '{table}' not found in metadata.")
            
#             valid_columns = metadata[db_name][table_name]

#             # Validate selected columns
#             for column in parsed_query["columns"]:
#                 match = re.match(r"(AVG|SUM|MIN|MAX|COUNT)\((.*)\)", column.strip(), re.IGNORECASE)
#                 if match:
#                     col = match.groups()[1].strip()
#                     if col not in valid_columns:
#                         raise ValueError(f"Column '{col}' not found in table '{table}' for aggregate function.")
#                 elif column != "*" and column not in valid_columns:
#                     raise ValueError(f"Column '{column}' not found in table '{table}'.")

#             # Validate GROUP BY columns
#             if parsed_query["groupby"]:
#                 for col in parsed_query["groupby"].split(","):
#                     if col.strip() not in valid_columns:
#                         raise ValueError(f"Column '{col.strip()}' in GROUP BY not found in table '{table}'.")

#             # Validate HAVING conditions
#             if parsed_query["having"]:
#                 # Extract column names and aggregate functions used in HAVING
#                 having_columns = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", parsed_query["having"])
#                 aggregate_functions = re.findall(r"(AVG|SUM|MIN|MAX|COUNT)\([^\)]+\)", parsed_query["having"], re.IGNORECASE)

#                 # Check if HAVING references valid columns or aggregate functions
#                 for col in having_columns:
#                     if col not in valid_columns and col not in parsed_query["columns"] and col not in aggregate_functions:
#                         raise ValueError(f"Invalid column or function '{col}' in HAVING clause.")



# # Parse SQL query to extract columns, tables, operations, GROUP BY, HAVING, and LIKE
# def parse_sql_query(query):
#     query = query.strip()
#     union_parts = query.split("UNION ALL")
#     parsed_queries = []

#     for part in union_parts:
#         columns_pattern = r"SELECT\s+(DISTINCT\s+)?(.+?)\s+FROM"
#         table_pattern = r"FROM ([\w.]+)"
#         groupby_pattern = r"GROUP BY (.+?)(?: HAVING|$)"
#         having_pattern = r"HAVING (.+?)(?: ORDER BY|$)"
#         orderby_pattern = r"ORDER BY (.+?)(?: LIMIT|$)"
#         limit_pattern = r"LIMIT (\d+)"
#         like_pattern = r"WHERE\s+(.+?)\s+LIKE\s+'([^']+)'"


#         columns_match = re.search(columns_pattern, part, re.IGNORECASE)
#         tables = re.findall(table_pattern, part, re.IGNORECASE)
#         groupby = re.findall(groupby_pattern, part, re.IGNORECASE)
#         having = re.findall(having_pattern, part, re.IGNORECASE)
#         orderby = re.findall(orderby_pattern, part, re.IGNORECASE)
#         limit = re.findall(limit_pattern, part, re.IGNORECASE)
#         like = re.findall(like_pattern, part, re.IGNORECASE)

#         is_distinct = bool(columns_match and columns_match.group(1))
#         columns = [col.strip() for col in columns_match.group(2).split(",")] if columns_match else []

#         # Parse aggregate functions and regular columns
#         parsed_columns = []
#         for column in columns:
#             match = re.match(r"(AVG|SUM|MIN|MAX|COUNT)\((.*)\)", column.strip(), re.IGNORECASE)
#             if match:
#                 func, col = match.groups()
#                 parsed_columns.append(f"{func.upper()}({col.strip()})")
#             else:
#                 parsed_columns.append(column.strip())

#         parsed_query = {
#             "distinct": is_distinct,
#             "columns": parsed_columns,
#             "tables": [table.strip() for table in tables],
#             "groupby": groupby[0].strip() if groupby else None,
#             "having": having[0].strip() if having else None,
#             "orderby": orderby[0].strip() if orderby else None,
#             "limit": int(limit[0]) if limit else None,
#             "like": like[0] if like else None  # Includes both column and pattern
#         }
#         parsed_queries.append(parsed_query)

#     return parsed_queries


# # Execute SQL query with enhanced functionality
# def execute_query(parsed_queries, metadata):
#     combined_data = []
#     columns = []

#     for parsed_query in parsed_queries:
#         for table in parsed_query["tables"]:
#             db_name, table_name = table.split(".")
#             connection = create_connection(db_name)
#             cursor = connection.cursor(dictionary=True)

#             distinct_clause = "DISTINCT " if parsed_query["distinct"] else ""
#             columns_str = ", ".join(parsed_query["columns"]) if parsed_query["columns"] else "*"
#             query = f"SELECT {distinct_clause}{columns_str} FROM `{table_name}`"

#             if parsed_query.get("like"):
#                 column, pattern = parsed_query["like"]
#                 if "%" not in pattern:
#                     pattern = f"%{pattern}%"  # Ensure wildcard pattern
#                 query += f" WHERE LOWER({column}) LIKE LOWER('{pattern}')"

#             if parsed_query["groupby"]:
#                 query += f" GROUP BY {parsed_query['groupby']}"

#             if parsed_query["having"]:
#                 query += f" HAVING {parsed_query['having']}"

#             if parsed_query["orderby"]:
#                 query += f" ORDER BY {parsed_query['orderby']}"
#             if parsed_query["limit"]:
#                 query += f" LIMIT {parsed_query['limit']}"

#             try:
#                 cursor.execute(query)
#                 result = cursor.fetchall()
#                 combined_data.extend(result)
#                 if result:
#                     columns = list(result[0].keys())  # Extract columns dynamically
#             except mysql.connector.Error as e:
#                 print(f"Error executing query on table '{table}': {e}")
#             finally:
#                 cursor.close()
#                 connection.close()

#     return combined_data, columns


# # GUI Functions
# def execute_query_gui():
#     query = query_text.get("1.0", "end-1c").strip()
#     if not query:
#         messagebox.showwarning("Input Error", "Please enter a SQL query.")
#         return

#     try:
#         parsed_queries = parse_sql_query(query)
#         validate_query_with_metadata(parsed_queries, metadata)
#         result, columns = execute_query(parsed_queries, metadata)
#         display_results(result, columns)
#     except Exception as e:
#         messagebox.showerror("Error", str(e))


# def display_results(data, columns):
#     # Clear existing data in the treeview
#     for row in result_tree.get_children():
#         result_tree.delete(row)

#     if not data:
#         messagebox.showinfo("Query Results", "No results found.")
#         return

#     # Add new column headings
#     result_tree["columns"] = columns
#     result_tree["show"] = "headings"

#     for col in columns:
#         result_tree.heading(col, text=col)
#         result_tree.column(col, anchor="w")

#     # Insert rows
#     for row in data:
#         result_tree.insert("", "end", values=[row.get(col, "NULL") for col in columns])


# # GUI Setup
# root = Tk()
# root.title("SQL Query Executor")

# # Query input
# Label(root, text="Enter SQL Query:").pack(anchor="w", padx=10, pady=5)
# query_text = Text(root, height=10, width=80)
# query_text.pack(padx=10, pady=5)

# # Execute button
# Button(root, text="Execute Query", command=execute_query_gui).pack(pady=5)

# # Result display
# result_frame = ttk.Frame(root)
# result_frame.pack(fill="both", expand=True, padx=10, pady=5)

# scrollbar = Scrollbar(result_frame, orient="vertical")
# scrollbar.pack(side="right", fill="y")

# result_tree = ttk.Treeview(result_frame, yscrollcommand=scrollbar.set)
# result_tree.pack(fill="both", expand=True)

# scrollbar.config(command=result_tree.yview)

# # Load metadata
# generate_metadata()
# metadata = load_metadata()

# # Run the GUI
# root.mainloop()














import subprocess
import json
import os
import re
import mysql.connector
from tkinter import Tk, Label, Text, Button, Scrollbar, messagebox, ttk
from jellyfish import jaro_winkler_similarity
from pymongo import MongoClient
from fuzzywuzzy import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
import traceback

# Helper function to connect to the database
def create_connection(database_name=None):
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="admin",
            database=database_name if database_name else None
        )
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL database: {e}")
        raise


# Run the metadata generation script (meta.py)
def generate_amazonbusiness():
    try:
        subprocess.run(["python", "amazonbusiness.py"], check=True)
        print("amazonbusiness generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running amazonbusiness.py: {e}")

# Run the metadata generation script (meta.py)
def generate_metadata():
    try:
        subprocess.run(["python", "meta.py"], check=True)
        print("Metadata generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running meta.py: {e}")

def generate_entity_matcher():
    try:
        subprocess.run(["python", "entity_matcher.py"], check=True)
        print("Entity Matcher generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running entity_matcher.py: {e}")

def generate_instagram_matching_and_mapping():
    try:
        subprocess.run(["python", "instagrammapping.py"], check=True)
        print("Instagram Mapping/matching generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running matcher: {e}")

# Load metadata from JSON files
def load_metadata(metadata_folder="metadata"):
    metadata = {}
    if not os.path.exists(metadata_folder):
        print(f"Metadata folder '{metadata_folder}' does not exist.")
        return metadata

    for file_name in os.listdir(metadata_folder):
        if file_name.endswith("_metadata.json"):
            database_name = file_name.replace("_metadata.json", "")
            try:
                with open(os.path.join(metadata_folder, file_name), "r") as json_file:
                    metadata[database_name] = json.load(json_file)
            except json.JSONDecodeError as e:
                print(f"Error parsing {file_name}: {e}")

    return metadata

def load_instagram_metadata():
    try:
        metadata_path = "metadata/instagram_metadata.json"
        print(f"\nTrying to load Instagram metadata from: {metadata_path}")
        
        if not os.path.exists(metadata_path):
            # If metadata file doesn't exist, create it by fetching from MongoDB
            client = MongoClient("mongodb://192.168.47.237:27017/")
            db = client["instagram"]
            
            # Get all collection names
            collections = db.list_collection_names()
            metadata = {}
            
            # For each collection, get its structure
            for collection in collections:
                # Get sample document to determine structure
                sample = db[collection].find_one()
                if sample:
                    # Extract fields from sample document
                    fields = list(sample.keys())
                    metadata[collection] = fields
            
            # Save metadata to file
            os.makedirs("metadata", exist_ok=True)
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=4)
            
            print(f"Created Instagram metadata file at {metadata_path}")
            return metadata
            
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
            print("\nLoaded Instagram metadata successfully")
            print("Available collections:", list(metadata.keys()))
            return metadata
    except Exception as e:
        print(f"Error handling Instagram metadata: {e}")
        return {}


# Validate query components against metadata
def validate_query_with_metadata(parsed_queries, metadata):
    for parsed_query in parsed_queries:
        for table in parsed_query["tables"]:
            db_name, table_name = table.split(".")
            if db_name not in metadata or table_name not in metadata[db_name]:
                raise ValueError(f"Table '{table}' not found in metadata.")
            
            valid_columns = metadata[db_name][table_name]

            # Validate selected columns
            for column in parsed_query["columns"]:
                match = re.match(r"(AVG|SUM|MIN|MAX|COUNT)\((.*)\)", column.strip(), re.IGNORECASE)
                if match:
                    col = match.groups()[1].strip()
                    if col not in valid_columns:
                        raise ValueError(f"Column '{col}' not found in table '{table}' for aggregate function.")
                elif column != "*" and column not in valid_columns:
                    raise ValueError(f"Column '{column}' not found in table '{table}'.")

            # Validate GROUP BY columns
            if parsed_query["groupby"]:
                for col in parsed_query["groupby"].split(","):
                    if col.strip() not in valid_columns:
                        raise ValueError(f"Column '{col.strip()}' in GROUP BY not found in table '{table}'.")

            # Validate HAVING conditions
            if parsed_query["having"]:
                # Extract column names and aggregate functions used in HAVING
                having_columns = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", parsed_query["having"])
                aggregate_functions = re.findall(r"(AVG|SUM|MIN|MAX|COUNT)\([^\)]+\)", parsed_query["having"], re.IGNORECASE)

                # Check if HAVING references valid columns or aggregate functions
                for col in having_columns:
                    if col not in valid_columns and col not in parsed_query["columns"] and col not in aggregate_functions:
                        raise ValueError(f"Invalid column or function '{col}' in HAVING clause.")

def process_results(results, aliases):
    """Process results and apply column aliases"""
    processed_results = []
    for row in results:
        processed_row = {}
        for key, value in row.items():
            # Use alias if exists, otherwise use original column name
            alias = aliases.get(key, key)
            processed_row[alias] = value
        processed_results.append(processed_row)
    return processed_results

# Parse SQL query to extract columns, tables, operations, GROUP BY, HAVING, LIKE, and INSERT
def parse_sql_query(query):
    query = query.strip()
    
    union_parts = query.split("UNION ALL")
    parsed_queries = []

    for part in union_parts:
        # Check if the query is an INSERT statement
        is_insert = part.strip().upper().startswith("INSERT")
        if is_insert:
            # Directly store the insert flag and skip parsing
            parsed_queries.append({"query_type": "INSERT", "insert": True})
            continue

        # Existing SELECT parsing logic
        columns_pattern = r"SELECT\s+(DISTINCT\s+)?(.+?)\s+FROM"
        table_pattern = r"FROM ([\w.]+)"
        groupby_pattern = r"GROUP BY (.+?)(?: HAVING|$)"
        having_pattern = r"HAVING (.+?)(?: ORDER BY|$)"
        orderby_pattern = r"ORDER BY (.+?)(?: LIMIT|$)"
        limit_pattern = r"LIMIT (\d+)"
        like_pattern = r"WHERE\s+(.+?)\s+LIKE\s+'([^']+)'"

        columns_match = re.search(columns_pattern, part, re.IGNORECASE)
        tables = re.findall(table_pattern, part, re.IGNORECASE)
        groupby = re.findall(groupby_pattern, part, re.IGNORECASE)
        having = re.findall(having_pattern, part, re.IGNORECASE)
        orderby = re.findall(orderby_pattern, part, re.IGNORECASE)
        limit = re.findall(limit_pattern, part, re.IGNORECASE)
        like = re.findall(like_pattern, part, re.IGNORECASE)

        is_distinct = bool(columns_match and columns_match.group(1))
        
        # Handle column aliases
        columns = []
        aliases = {}
        if columns_match:
            column_parts = columns_match.group(2).split(",")
            for col_part in column_parts:
                col_part = col_part.strip()
                # Check for AS keyword
                as_match = re.match(r"(.+?)\s+[aA][sS]\s+(\w+)$", col_part)
                if as_match:
                    original_col = as_match.group(1).strip()
                    alias = as_match.group(2).strip()
                    columns.append(original_col)
                    aliases[original_col] = alias
                else:
                    columns.append(col_part)

        # Parse aggregate functions and regular columns
        parsed_columns = []
        for column in columns:
            match = re.match(r"(AVG|SUM|MIN|MAX|COUNT)\((.*)\)", column.strip(), re.IGNORECASE)
            if match:
                func, col = match.groups()
                parsed_columns.append(f"{func.upper()}({col.strip()})")
            else:
                parsed_columns.append(column.strip())

        parsed_query = {
            "distinct": is_distinct,
            "columns": parsed_columns,
            "aliases": aliases,  # Add aliases to parsed query
            "tables": [table.strip() for table in tables],
            "groupby": groupby[0].strip() if groupby else None,
            "having": having[0].strip() if having else None,
            "orderby": orderby[0].strip() if orderby else None,
            "limit": int(limit[0]) if limit else None,
            "like": like[0] if like else None
        }
        parsed_queries.append(parsed_query)
        # print(parsed_queries)
    return parsed_queries

def find_best_matching_column(source_column, target_columns, threshold=0.85):
    """
    Find the best matching column name using Jaro-Winkler similarity
    Returns the best match if similarity is above threshold, otherwise None
    """
    best_match = None
    best_score = 0
    
    for target_column in target_columns:
        score = jaro_winkler_similarity(source_column.lower(), target_column.lower())
        if score > best_score and score >= threshold:
            best_score = score
            best_match = target_column
            
    return best_match, best_score

# Modify the execute_mongo_query function
def execute_mongo_query(collection_name, query):
    """Executes a query on a MongoDB collection and returns the results."""
    try:
        client = MongoClient("mongodb://192.168.47.237:27017/")
        db = client["instagram"]  # Use the appropriate database name
        collection = db[collection_name]
        print(f"Connected to Instagram MongoDB database, querying {collection_name}")
        
        # Convert query to MongoDB format
        if query.get("username"):
            # Handle LIKE pattern
            pattern = query["username"]["$regex"]
            # Remove % signs from the pattern
            pattern = pattern.replace('%', '')
            query["username"] = {"$regex": pattern, "$options": "i"}
        
        print(f"Executing MongoDB query: {query}")
        results = collection.find(query)
        results_list = list(results)
        print(f"Found {len(results_list)} results from Instagram")
        
        # Convert MongoDB _id to string to make it JSON serializable
        for result in results_list:
            if '_id' in result:
                result['_id'] = str(result['_id'])
                
        return results_list
    except Exception as e:
        print(f"Error executing MongoDB query: {e}")
        return []
    finally:
        client.close()

# Add this function to load Amazon metadata
def load_amazon_metadata():
    try:
        metadata_path = "metadata/iia_amazon_metadata.json"
        print(f"\nTrying to load Amazon metadata from: {metadata_path}")
        
        if not os.path.exists(metadata_path):
            print(f"ERROR: Metadata file not found at {metadata_path}")
            return {}
            
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
            print("\nLoaded Amazon metadata successfully")
            print("Available collections:", list(metadata.keys()))  # Debugging output
            return metadata
    except Exception as e:
        print(f"Error loading Amazon metadata: {e}")
        return {}

# Function to calculate Cosine Similarity or Jaro-Winkler similarity
def get_best_matching_collection(table_name, available_collections):
    """
    This function finds the best matching collection name based on the similarity to the input table_name.
    Cosine similarity (via TF-IDF) and Jaro-Winkler similarity are used.
    """
    # Option 1: Cosine Similarity using TF-IDF (for better matching of table names)
    vectorizer = TfidfVectorizer().fit_transform([table_name] + available_collections)
    cosine_similarities = (vectorizer * vectorizer.T).A[0, 1:]

    # Option 2: Use Jaro-Winkler Similarity as an alternative
    jaro_similarities = [fuzz.jaro_winkler(table_name, collection) for collection in available_collections]

    # Combine both similarity scores and choose the maximum
    combined_scores = [(available_collections[i], max(cosine_similarities[i], jaro_similarities[i]))
                       for i in range(len(available_collections))]

    # Sort by the highest similarity score
    best_match = sorted(combined_scores, key=lambda x: x[1], reverse=True)[0]
    return best_match[0]

# Modify the execute_query function
def execute_query(parsed_queries, metadata):
    combined_data = []
    columns = []
    # print(parsed_queries)
    
    for parsed_query in parsed_queries:
        query_results = []
        for table in parsed_query["tables"]:
            db_name, table_name = table.split(".")
            print(f"\nProcessing table: {db_name}.{table_name}")
            
            # Execute SQL query with enhanced functionality
            # Execute SQL query with enhanced functionality
            if "iia_amazon" in db_name.lower():
                print("\nAmazon MongoDB Processing:")
                try:
                    # Load Amazon metadata
                    amazon_metadata = load_amazon_metadata()
                    
                    if not amazon_metadata:
                        print("Error: No Amazon metadata available")
                        return [], []
                    
                    # Find the correct collection case-insensitively
                    actual_collection_name = None
                    for collection in amazon_metadata.keys():
                        if collection.lower() == table_name.lower():
                            actual_collection_name = collection
                            break
                    
                    if not actual_collection_name:
                        print(f"Error: Collection '{table_name}' not found")
                        return [], []
                    
                    # Connect to MongoDB
                    client = MongoClient("mongodb://192.168.49.246:27017/")
                    db = client["iia_amazon"]
                    collection = db[actual_collection_name]
                    
                    # Build MongoDB pipeline
                    pipeline = []
                    
                    # Handle WHERE/LIKE clause
                    if parsed_query.get("like"):
                        column, pattern = parsed_query["like"]
                        pattern = pattern.replace('%', '')
                        pipeline.append({
                            "$match": {
                                column: {"$regex": pattern, "$options": "i"}
                            }
                        })
                    
                    # Handle column selection
                    projection = {"_id": 0}  # Always include _id
                    if "*" not in parsed_query["columns"]:
                        for col in parsed_query["columns"]:
                            if col in amazon_metadata[actual_collection_name]:
                                projection[col] = 1
                    
                    if projection:
                        pipeline.append({"$project": projection})
                    
                    # Handle GROUP BY
                    if parsed_query["groupby"]:
                        group_fields = {field.strip(): f"${field.strip()}" 
                                    for field in parsed_query["groupby"].split(",")}
                        group_stage = {
                            "$group": {
                                "_id": group_fields
                            }
                        }
                        
                        # Handle aggregations
                        for col in parsed_query["columns"]:
                            col = col.strip().upper()
                            if "COUNT" in col:
                                group_stage["$group"]["count"] = {"$sum": 1}
                            elif "AVG" in col:
                                field = col[4:-1]  # Remove AVG() wrapper
                                group_stage["$group"][f"avg_{field}"] = {"$avg": f"${field}"}
                            elif "SUM" in col:
                                field = col[4:-1]  # Remove SUM() wrapper
                                group_stage["$group"][f"sum_{field}"] = {"$sum": f"${field}"}
                            elif "MIN" in col:
                                field = col[4:-1]  # Remove MIN() wrapper
                                group_stage["$group"][f"min_{field}"] = {"$min": f"${field}"}
                            elif "MAX" in col:
                                field = col[4:-1]  # Remove MAX() wrapper
                                group_stage["$group"][f"max_{field}"] = {"$max": f"${field}"}
                        
                        pipeline.append(group_stage)
                    
                    # Handle ORDER BY
                    if parsed_query["orderby"]:
                        sort_parts = parsed_query["orderby"].split()
                        field = sort_parts[0]
                        direction = -1 if len(sort_parts) > 1 and sort_parts[1].upper() == "DESC" else 1
                        pipeline.append({"$sort": {field: direction}})
                    
                    # Handle LIMIT
                    if parsed_query["limit"]:
                        pipeline.append({"$limit": parsed_query["limit"]})
                    
                    # Execute pipeline
                    print(f"Executing pipeline: {pipeline}")
                    if pipeline:
                        results = list(collection.aggregate(pipeline))
                    else:
                        results = list(collection.find({}, projection))
                    
                    print(f"Found {len(results)} results from Amazon")
                    
                    # Process results with aliases
                    processed_results = process_results(results, parsed_query.get("aliases", {}))
                    query_results.extend(processed_results)
                    
                except Exception as e:
                    print(f"Error processing Amazon query: {e}")
                    traceback.print_exc()
                finally:
                    client.close()
            elif "instagram" in db_name.lower():
                print("Processing Instagram query...")
                try:
                    # Load Instagram metadata
                    instagram_metadata = load_instagram_metadata()
                    
                    if not instagram_metadata:
                        print("Error: No Instagram metadata available")
                        return [], []
                    
                    # Connect to MongoDB
                    client = MongoClient("mongodb://192.168.47.237:27017/")
                    db = client["instagram"]
                    
                    # Find the correct collection
                    actual_collection = None
                    for collection in instagram_metadata.keys():
                        if collection.lower() == table_name.lower():
                            actual_collection = collection
                            break
                    
                    if not actual_collection:
                        print(f"Error: Collection '{table_name}' not found")
                        return [], []
                    
                    # Build MongoDB query
                    mongo_query = {}
                    projection = {"_id": 0}  # Exclude MongoDB _id by default
                    
                    # Handle column selection
                    if "*" not in parsed_query["columns"]:
                        for col in parsed_query["columns"]:
                            if col in instagram_metadata[actual_collection]:
                                projection[col] = 1
                    
                    # Handle WHERE/LIKE clause
                    if parsed_query.get("like"):
                        column, pattern = parsed_query["like"]
                        pattern = pattern.replace('%', '')
                        mongo_query[column] = {"$regex": pattern, "$options": "i"}
                    
                    # Execute query
                    collection = db[actual_collection]
                    pipeline = []
                    
                    # Add match stage if we have conditions
                    if mongo_query:
                        pipeline.append({"$match": mongo_query})
                    
                    # Add project stage for column selection
                    if projection:
                        pipeline.append({"$project": projection})
                    
                    # Handle GROUP BY
                    if parsed_query["groupby"]:
                        group_fields = {field: f"${field}" for field in parsed_query["groupby"].split(",")}
                        group_stage = {
                            "$group": {
                                "_id": group_fields
                            }
                        }
                        
                        # Handle aggregations
                        for col in parsed_query["columns"]:
                            if "COUNT" in col.upper():
                                group_stage["$group"]["count"] = {"$sum": 1}
                            elif "AVG" in col.upper():
                                field = col[4:-1]  # Remove AVG() wrapper
                                group_stage["$group"][f"avg_{field}"] = {"$avg": f"${field}"}
                            elif "SUM" in col.upper():
                                field = col[4:-1]  # Remove SUM() wrapper
                                group_stage["$group"][f"sum_{field}"] = {"$sum": f"${field}"}
                        
                        pipeline.append(group_stage)
                    
                    # Handle ORDER BY
                    if parsed_query["orderby"]:
                        field, direction = parsed_query["orderby"].split()
                        sort_direction = -1 if direction.upper() == "DESC" else 1
                        pipeline.append({"$sort": {field: sort_direction}})
                    
                    # Handle LIMIT
                    if parsed_query["limit"]:
                        pipeline.append({"$limit": parsed_query["limit"]})
                    
                    # Execute pipeline
                    if pipeline:
                        results = list(collection.aggregate(pipeline))
                    else:
                        results = list(collection.find(mongo_query, projection))
                    
                    # Process results
                    processed_results = process_results(results, parsed_query.get("aliases", {}))
                    query_results.extend(processed_results)
                    
                    print(f"Found {len(processed_results)} results from Instagram")
                    
                except Exception as e:
                    print(f"Error processing Instagram query: {e}")
                    traceback.print_exc()
                finally:
                    client.close()
            else:
                
                connection = create_connection(db_name)
                cursor = connection.cursor(dictionary=True)

                try:
                    # Handle INSERT queries for Flipkart
                    if parsed_query.get("query_type") == "INSERT":
                        columns = parsed_query.get("columns", [])
                        values = parsed_query.get("values", [])
                        
                        if not columns or not values:
                            raise ValueError("Missing columns or values for INSERT query")
                        
                        # Validate columns against metadata
                        if db_name not in metadata or table_name not in metadata[db_name]:
                            raise ValueError(f"Table '{table_name}' not found in metadata")
                        
                        valid_columns = set(metadata[db_name][table_name])
                        insert_columns = set(columns)
                        
                        # Check if all required columns from metadata are present
                        missing_columns = valid_columns - insert_columns
                        if missing_columns:
                            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
                        
                        # Check if there are any invalid columns
                        invalid_columns = insert_columns - valid_columns
                        if invalid_columns:
                            raise ValueError(f"Invalid columns found: {', '.join(invalid_columns)}")
                        
                        # If validation passes, proceed with INSERT
                        columns_str = ", ".join(f"`{col}`" for col in columns)
                        placeholders = ", ".join(["%s"] * len(values))
                        query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
                        
                        print(f"Executing INSERT: {query}")
                        cursor.execute(query, values)
                        connection.commit()
                        
                        print(f"Successfully inserted {cursor.rowcount} row(s) into Flipkart")
                        query_results.append({"message": f"Inserted {cursor.rowcount} row(s)"})
                    else:
                        # Existing SELECT query logic remains unchanged
                        distinct_clause = "DISTINCT " if parsed_query["distinct"] else ""
                        columns_str = ", ".join(parsed_query["columns"]) if parsed_query["columns"] else "*"
                        query = f"SELECT {distinct_clause}{columns_str} FROM `{table_name}`"

                        if parsed_query.get("like"):
                            column, pattern = parsed_query["like"]
                            if "%" not in pattern:
                                pattern = f"%{pattern}%"
                            query += f" WHERE LOWER({column}) LIKE LOWER('{pattern}')"

                        if parsed_query["groupby"]:
                            query += f" GROUP BY {parsed_query['groupby']}"

                        if parsed_query["having"]:
                            query += f" HAVING {parsed_query['having']}"

                        if parsed_query["orderby"]:
                            query += f" ORDER BY {parsed_query['orderby']}"
                        if parsed_query["limit"]:
                            query += f" LIMIT {parsed_query['limit']}"

                        cursor.execute(query)
                        results = cursor.fetchall()
                        # Process results with aliases
                        processed_results = process_results(results, parsed_query.get("aliases", {}))
                        query_results.extend(processed_results)
                except mysql.connector.Error as e:
                    print(f"Error executing query on table '{table_name}': {e}")
                finally:
                    cursor.close()
                    connection.close()
        
        combined_data.extend(query_results)
        print(parsed_query)
    print(f"\nTotal results before ordering/limiting: {len(combined_data)}")
    
    # Apply global ORDER BY to combined results
    if parsed_queries[0]["orderby"]:
        order_col = parsed_queries[0]["orderby"].split()[0]
        reverse = "DESC" in parsed_queries[0]["orderby"].upper()
        print(f"Ordering combined results by {order_col} {'DESC' if reverse else 'ASC'}")
        
        # Sort using the aliased column name if it exists
        combined_data.sort(
            key=lambda x: float(str(x.get(order_col, 0)).replace('₹', '').replace(',', '')) 
            if x.get(order_col) is not None else 0,
            reverse=reverse
        )
    
    # Apply global LIMIT to sorted results
    if parsed_queries[0]["limit"]:
        limit = parsed_queries[0]["limit"]
        print(f"Applying limit {limit} to combined results")
        combined_data = combined_data[:limit]
    
    print(f"Final result count: {len(combined_data)}")
    
    # Get columns from the first result
    if combined_data:
        columns = list(combined_data[0].keys())
    
    return combined_data, columns

# GUI Functions
def execute_query_gui():
    query = query_text.get("1.0", "end-1c").strip()
    if not query:
        messagebox.showwarning("Input Error", "Please enter a SQL query.")
        return

    try:
        parsed_queries = parse_sql_query(query)
        print(f"Parsed Queries: {parsed_queries}")

        # Check if the query is an INSERT query
        if parsed_queries and parsed_queries[0].get("query_type") == "INSERT":
            original_query = query  # Use the original query directly
            print(f"Executing INSERT query directly: {original_query}")
            connection = create_connection("flipkart")  # Specify the database name
            cursor = connection.cursor()
            try:
                cursor.execute(original_query)
                connection.commit()
                print("Successfully executed INSERT query.")
                messagebox.showinfo("Success", "INSERT query executed successfully.")
            except mysql.connector.Error as e:
                print(f"Error executing INSERT query: {e}")
                messagebox.showerror("Error", f"Error executing INSERT query: {e}")
            finally:
                cursor.close()
                connection.close()
        else:
            # For non-INSERT queries, proceed with the usual execution flow
            validate_query_with_metadata(parsed_queries, metadata)
            result, columns = execute_query(parsed_queries, metadata)
            display_results(result, columns)
    except Exception as e:
        messagebox.showerror("Error", str(e))

def display_results(data, columns):
    # Clear existing data in the treeview
    for row in result_tree.get_children():
        result_tree.delete(row)

    if not data:
        messagebox.showinfo("Query Results", "No results found.")
        return

    # Add new column headings
    result_tree["columns"] = columns
    result_tree["show"] = "headings"

    for col in columns:
        result_tree.heading(col, text=col)
        result_tree.column(col, anchor="w")

    # Insert non-null rows
    valid_rows = 0
    for row in data:
        # Check if any value in the row is NULL or None
        row_values = [row.get(col) for col in columns]
        if not any(val is None or val == "NULL" for val in row_values):
            result_tree.insert("", "end", values=row_values)
            valid_rows += 1

    if valid_rows == 0:
        messagebox.showinfo("Query Results", "No non-null results found.")

# GUI Setup
root = Tk()
root.title("SQL Query Executor")

# Query input
Label(root, text="Enter SQL Query:").pack(anchor="w", padx=10, pady=5)
query_text = Text(root, height=10, width=80)
query_text.pack(padx=10, pady=5)

# Execute button
Button(root, text="Execute Query", command=execute_query_gui).pack(pady=5)

# Result display
result_frame = ttk.Frame(root)
result_frame.pack(fill="both", expand=True, padx=10, pady=5)

scrollbar = Scrollbar(result_frame, orient="vertical")
scrollbar.pack(side="right", fill="y")

result_tree = ttk.Treeview(result_frame, yscrollcommand=scrollbar.set)
result_tree.pack(fill="both", expand=True)

scrollbar.config(command=result_tree.yview)

generate_amazonbusiness()
generate_instagram_matching_and_mapping()
generate_metadata()
metadata = load_metadata()

generate_entity_matcher()
generate_metadata()

# Run the GUI
root.mainloop()