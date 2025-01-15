

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
def generate_metadata():
    try:
        subprocess.run(["python", "meta.py"], check=True)
        print("Metadata generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running meta.py: {e}")


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

# # Validate query components against metadata
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



# Parse SQL query to extract columns, tables, operations, GROUP BY, HAVING, and LIKE
def parse_sql_query(query):
    query = query.strip()
    union_parts = query.split("UNION ALL")
    parsed_queries = []

    for part in union_parts:
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
        columns = [col.strip() for col in columns_match.group(2).split(",")] if columns_match else []

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
            "tables": [table.strip() for table in tables],
            "groupby": groupby[0].strip() if groupby else None,
            "having": having[0].strip() if having else None,
            "orderby": orderby[0].strip() if orderby else None,
            "limit": int(limit[0]) if limit else None,
            "like": like[0] if like else None  # Includes both column and pattern
        }
        parsed_queries.append(parsed_query)

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
        client = MongoClient("mongodb://localhost:27017/")
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
            print("Available collections:", list(metadata.keys()))
            return metadata
    except Exception as e:
        print(f"Error loading Amazon metadata: {e}")
        return {}
# Modify the execute_query function
def execute_query(parsed_queries, metadata):
    combined_data = []
    columns = []
    
    for parsed_query in parsed_queries:
        query_results = []
        for table in parsed_query["tables"]:
            db_name, table_name = table.split(".")
            print(f"\nProcessing table: {db_name}.{table_name}")
            
            # Execute SQL query with enhanced functionality
            if "amazon" in db_name.lower():
                    print("\nAmazon MongoDB Processing:")
                    # Load Amazon metadata
                    amazon_metadata = load_amazon_metadata()
                    # Initialize mongo_query before using it
                    mongo_query = {}
                    
                    # Try to find the collection name case-insensitively
                    actual_collection_name = None
                    for collection in amazon_metadata.keys():
                        if collection.lower() == table_name.lower():
                            actual_collection_name = collection
                            print(f"Found matching collection: {actual_collection_name}")
                            break
                    
                    if not actual_collection_name:
                        print(f"ERROR: Collection '{table_name}' not found in metadata!")
                        print("Available collections:", list(amazon_metadata.keys()))
                        return [], []
                    
                    collection_metadata = amazon_metadata.get(actual_collection_name, [])  # Get list of fields
                    table_name = actual_collection_name  # Use the correct case for the collection name
                    
                    # Handle column selection
                    selected_columns = parsed_query["columns"]
                    projection = {"_id": 1}  # Always include _id
                    
                    # Map the column names exactly as they appear in metadata
                    if "*" not in selected_columns:
                        for col in selected_columns:
                            if col in collection_metadata:
                                projection[col] = 1
                            # Check for exact match in metadata fields
                            elif col in amazon_metadata[actual_collection_name]:
                                projection[col] = 1
                    
                    print(f"Projection: {projection}")
                    print(f"Selected columns: {selected_columns}")
                    
                    # Connect to MongoDB for Amazon
                    try:
                        client = MongoClient("mongodb://192.168.49.246:27017/")
                        db = client["iia_amazon"]
                        collection = db[table_name]
                        print(f"Connected to Amazon MongoDB, querying {table_name}")
                        
                        # Execute the query
                        results = collection.find(mongo_query, projection)
                        results_list = list(results)
                        print(f"Found {len(results_list)} results from Amazon")
                        
                        # Process results
                        mapped_results = []
                        for result in results_list:
                            if '_id' in result:
                                result['_id'] = str(result['_id'])
                            mapped_results.append(result)
                        
                        # Handle sorting
                        if parsed_query["orderby"]:
                            order_col = parsed_query["orderby"].split()[0]
                            reverse = "DESC" in parsed_query["orderby"].upper()
                            mapped_results.sort(
                                key=lambda x: float(str(x.get(order_col, 0)).replace('₹', '').replace(',', '')) 
                                if x.get(order_col) is not None else 0,
                                reverse=reverse
                            )
                        
                        # Handle LIMIT
                        if parsed_query["limit"]:
                            mapped_results = mapped_results[:parsed_query["limit"]]
                        
                        query_results.extend(mapped_results)
                        print(f"Added {len(mapped_results)} results from Amazon to combined data")
                        
                    except Exception as e:
                        print(f"Error executing Amazon MongoDB query: {e}")
                        print(f"Query: {mongo_query}")
                        print(f"Projection: {projection}")
                    finally:
                        client.close()    
            elif "instagram" in db_name.lower():
                print("Processing Instagram query...")
                # MongoDB logic for Instagram
                mongo_query = {}
                
                # Handle column selection
                selected_columns = parsed_query["columns"]
                projection = {col: 1 for col in selected_columns} if "*" not in selected_columns else {}
                
                # Handle LIKE clause
                if parsed_query.get("like"):
                    column, pattern = parsed_query["like"]
                    if "%" not in pattern:
                        pattern = f"%{pattern}%"
                    mongo_query[column] = {"$regex": pattern.replace('%', ''), "$options": "i"}
                
                # Handle ORDER BY
                sort_order = None
                if parsed_query["orderby"]:
                    order_col = parsed_query["orderby"].split()[0]
                    direction = -1 if "DESC" in parsed_query["orderby"].upper() else 1
                    sort_order = [(order_col, direction)]
                
                # Execute MongoDB query
                results = execute_mongo_query(table_name, mongo_query)
                
                # Handle sorting if specified
                if sort_order:
                    results.sort(key=lambda x: x.get(order_col, ''), reverse=(direction == -1))
                
                # Handle LIMIT
                if parsed_query["limit"]:
                    results = results[:parsed_query["limit"]]
                    
                query_results.extend(results)
                print(f"Added {len(results)} results from Instagram to combined data")
                
            else:
                # Existing Flipkart logic remains unchanged
                connection = create_connection(db_name)
                cursor = connection.cursor(dictionary=True)

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

                try:
                    print(f"Executing")
                    cursor.execute(query)
                    results = cursor.fetchall()
                    query_results.extend(results)
                except mysql.connector.Error as e:
                    print(f"Error executing Flipkart query: {e}")
                finally:
                    cursor.close()
                    connection.close()
        
        combined_data.extend(query_results)
    
    # Global ORDER BY and LIMIT handling for UNION ALL queries
    if parsed_queries[0]["orderby"]:
        order_col = parsed_queries[0]["orderby"].split()[0]
        reverse = "DESC" in parsed_queries[0]["orderby"].upper()
        
        # Sort the combined results
        combined_data.sort(
            key=lambda x: float(str(x.get(order_col, 0)).replace('₹', '').replace(',', '')) 
            if x.get(order_col) is not None else 0,
            reverse=reverse
        )
    
    # Apply the LIMIT to the final sorted results
    if parsed_queries[0]["limit"]:
        combined_data = combined_data[:parsed_queries[0]["limit"]]
    
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
        # validate_query_with_metadata(parsed_queries, metadata)
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

# Load metadata
generate_metadata()
metadata = load_metadata()

# Run the GUI
root.mainloop()










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
def generate_metadata():
    try:
        subprocess.run(["python", "meta.py"], check=True)
        print("Metadata generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running meta.py: {e}")


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

# # Validate query components against metadata
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

# Parse SQL query to extract columns, tables, operations, GROUP BY, HAVING, and LIKE
def parse_sql_query(query):
    query = query.strip()
    union_parts = query.split("UNION ALL")
    parsed_queries = []

    for part in union_parts:
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
        client = MongoClient("mongodb://localhost:27017/")
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
            print("Available collections:", list(metadata.keys()))
            return metadata
    except Exception as e:
        print(f"Error loading Amazon metadata: {e}")
        return {}
# Modify the execute_query function
def execute_query(parsed_queries, metadata):
    combined_data = []
    columns = []
    
    for parsed_query in parsed_queries:
        query_results = []
        for table in parsed_query["tables"]:
            db_name, table_name = table.split(".")
            print(f"\nProcessing table: {db_name}.{table_name}")
            
            # Execute SQL query with enhanced functionality
            if "amazon" in db_name.lower():
                    print("\nAmazon MongoDB Processing:")
                    # Load Amazon metadata
                    amazon_metadata = load_amazon_metadata()
                    # Initialize mongo_query before using it
                    mongo_query = {}
                    
                    # Try to find the collection name case-insensitively
                    actual_collection_name = None
                    for collection in amazon_metadata.keys():
                        if collection.lower() == table_name.lower():
                            actual_collection_name = collection
                            print(f"Found matching collection: {actual_collection_name}")
                            break
                    
                    if not actual_collection_name:
                        print(f"ERROR: Collection '{table_name}' not found in metadata!")
                        print("Available collections:", list(amazon_metadata.keys()))
                        return [], []
                    
                    collection_metadata = amazon_metadata.get(actual_collection_name, [])  # Get list of fields
                    table_name = actual_collection_name  # Use the correct case for the collection name
                    
                    # Handle column selection
                    selected_columns = parsed_query["columns"]
                    projection = {"_id": 1}  # Always include _id
                    
                    # Map the column names exactly as they appear in metadata
                    if "*" not in selected_columns:
                        for col in selected_columns:
                            if col in collection_metadata:
                                projection[col] = 1
                            # Check for exact match in metadata fields
                            elif col in amazon_metadata[actual_collection_name]:
                                projection[col] = 1
                    
                    print(f"Projection: {projection}")
                    print(f"Selected columns: {selected_columns}")
                    
                    # Connect to MongoDB for Amazon
                    try:
                        client = MongoClient("mongodb://192.168.49.246:27017/")
                        db = client["iia_amazon"]
                        collection = db[table_name]
                        print(f"Connected to Amazon MongoDB, querying {table_name}")
                        
                        # Execute the query
                        results = collection.find(mongo_query, projection)
                        results_list = list(results)
                        print(f"Found {len(results_list)} results from Amazon")
                        
                        # Process results with aliases
                        mapped_results = process_results(results_list, parsed_query.get("aliases", {}))
                        query_results.extend(mapped_results)
                        print(f"Added {len(mapped_results)} results from Amazon to combined data")
                        
                    except Exception as e:
                        print(f"Error executing Amazon MongoDB query: {e}")
                        print(f"Query: {mongo_query}")
                        print(f"Projection: {projection}")
                    finally:
                        client.close()    
            elif "instagram" in db_name.lower():
                print("Processing Instagram query...")
                # MongoDB logic for Instagram
                mongo_query = {}
                
                # Handle column selection
                selected_columns = parsed_query["columns"]
                projection = {col: 1 for col in selected_columns} if "*" not in selected_columns else {}
                
                # Handle LIKE clause
                if parsed_query.get("like"):
                    column, pattern = parsed_query["like"]
                    if "%" not in pattern:
                        pattern = f"%{pattern}%"
                    mongo_query[column] = {"$regex": pattern.replace('%', ''), "$options": "i"}
                
                # Handle ORDER BY
                sort_order = None
                if parsed_query["orderby"]:
                    order_col = parsed_query["orderby"].split()[0]
                    direction = -1 if "DESC" in parsed_query["orderby"].upper() else 1
                    sort_order = [(order_col, direction)]
                
                # Execute MongoDB query
                results = execute_mongo_query(table_name, mongo_query)
                
                # Handle sorting if specified
                if sort_order:
                    results.sort(key=lambda x: x.get(order_col, ''), reverse=(direction == -1))
                
                # Handle LIMIT
                if parsed_query["limit"]:
                    results = results[:parsed_query["limit"]]
                    
                query_results.extend(results)
                print(f"Added {len(results)} results from Instagram to combined data")
                
            else:
                # Existing Flipkart logic remains unchanged
                connection = create_connection(db_name)
                cursor = connection.cursor(dictionary=True)

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

                try:
                    print(f"Executing")
                    cursor.execute(query)
                    results = cursor.fetchall()
                    # Process results with aliases
                    processed_results = process_results(results, parsed_query.get("aliases", {}))
                    query_results.extend(processed_results)
                except mysql.connector.Error as e:
                    print(f"Error executing Flipkart query: {e}")
                finally:
                    cursor.close()
                    connection.close()
        
        combined_data.extend(query_results)
    
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
        # validate_query_with_metadata(parsed_queries, metadata)
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

# Load metadata
generate_metadata()
metadata = load_metadata()

# Run the GUI
root.mainloop()

