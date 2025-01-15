# import mysql.connector
# import socket
# import json
# import spacy
# import Levenshtein  # For Jaro-Winkler similarity

# # Load the pre-trained NER model
# nlp = spacy.load("en_core_web_sm")

# # Function to compute Jaro-Winkler similarity using Levenshtein package
# def jaro_winkler_similarity(str1, str2):
#     return Levenshtein.jaro_winkler(str1, str2)

# # Function to send queries to Vivek's Amazon server
# def send_query_to_vevek_amazon(query):
#     host = '192.168.49.246'  # Replace with the IP address of Vivek's machine
#     port = 12345  # Ensure this matches the server's port

#     client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     try:
#         client_socket.connect((host, port))
#         client_socket.send(query.encode('utf-8'))

#         # Receive data in chunks to handle large responses
#         response_chunks = []
#         while True:
#             chunk = client_socket.recv(4096).decode('utf-8')  # Adjust chunk size if needed
#             if not chunk:  # End of transmission
#                 break
#             response_chunks.append(chunk)

#         # Combine chunks into a single response
#         response = ''.join(response_chunks)

#         # Decode JSON response
#         try:
#             result = json.loads(response)
#             return result
#         except json.JSONDecodeError as e:
#             print(f"Failed to decode JSON response: {e}")
#             print(f"Partial response received: {response[:1000]}...")  # Print the first 1000 characters for debugging
#             return None
#     except Exception as e:
#         print(f"Error during socket communication: {e}")
#         return None
#     finally:
#         client_socket.close()

# # Function to fetch Amazon combined data
# def fetch_amazon_combined_data():
#     query = "SELECT * FROM amazon_combined_data;"
#     return send_query_to_vevek_amazon(query)

# # Function to fetch Flipkart data
# def fetch_flipkart_data():
#     try:
#         connection = mysql.connector.connect(
#             host="localhost",
#             user="root",
#             password="admin",
#             database="flipkart"
#         )
#         cursor = connection.cursor(dictionary=True)
#         cursor.execute("SELECT * FROM globalflipkartproductsmv;")
#         data = cursor.fetchall()
#         cursor.close()
#         connection.close()
#         return data
#     except mysql.connector.Error as e:
#         print(f"Error fetching Flipkart data: {e}")
#         return []

# # Function to apply NER to extract product entities from the product name
# def apply_ner_to_product_name(product_name):
#     doc = nlp(product_name)
#     entities = [ent.text for ent in doc.ents]
#     return entities

# # Function to store HLV data into the database (auto-increment id)
# def store_hlv_data_to_db(hlv_data):
#     try:
#         connection = mysql.connector.connect(
#             host="localhost",
#             user="root",
#             password="admin",
#             database="flipkart"  # Your target database name
#         )
#         cursor = connection.cursor()

#         # Create HLV table if it doesn't exist
#         create_table_query = """
#         CREATE TABLE IF NOT EXISTS HLV (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             product_name VARCHAR(255),
#             category VARCHAR(255),
#             ratings FLOAT,
#             discount_price FLOAT,
#             actual_price FLOAT,
#             image_links TEXT
#         );
#         """
#         cursor.execute(create_table_query)

#         # Insert data into the HLV table, ignoring duplicates
#         insert_query = """
#         INSERT IGNORE INTO HLV (product_name, category, ratings, discount_price, actual_price, image_links)
#         VALUES (%s, %s, %s, %s, %s, %s)
#         """
#         cursor.executemany(insert_query, hlv_data)

#         connection.commit()
#         print(f"Data inserted into HLV table successfully.")
#         cursor.close()
#         connection.close()
#     except mysql.connector.Error as e:
#         print(f"Error storing data in HLV: {e}")

# # Main function to execute the data processing
# def main():
#     amazon_data = fetch_amazon_combined_data()
#     if not amazon_data:
#         print("No data fetched from Amazon database.")
#         return

#     flipkart_data = fetch_flipkart_data()
#     if not flipkart_data:
#         print("No data fetched from Flipkart database.")
#         return

#     hlv_data = []

#     # Set threshold for Jaro-Winkler similarity
#     similarity_threshold = 0.6

#     for amazon_row in amazon_data:
#         for flipkart_row in flipkart_data:
#             # Assuming flipkart_row is a dictionary, access image_links using the correct key
#             amazon_image = amazon_row[3]  # Assuming image is the 4th column for Amazon (adjust if needed)
#             flipkart_image_links = flipkart_row["image_links"]  # Use the correct key for image links

#             # Match attributes using Jaro-Winkler similarity
#             if jaro_winkler_similarity(amazon_image, flipkart_image_links) > similarity_threshold:
#                 # Apply NER to extract entities from Amazon product names
#                 amazon_entities = apply_ner_to_product_name(amazon_row[1])  # Assuming product name is the 2nd column

#                 # Prepare data to insert into HLV
#                 hlv_row = (
#                     amazon_row[1],  # Product Name (2nd column)
#                     flipkart_row["category"],  # Category from Flipkart (adjust key if necessary)
#                     amazon_row[6],  # Ratings from Amazon (assuming 7th column)
#                     amazon_row[7],  # Discount price from Amazon (assuming 8th column)
#                     amazon_row[8],  # Actual price from Amazon (assuming 9th column)
#                     flipkart_row["image_links"]  # Image links from Flipkart (adjust key if necessary)
#                 )
#                 hlv_data.append(hlv_row)

#     # Insert the combined and mapped data into the HLV table
#     if hlv_data:
#         store_hlv_data_to_db(hlv_data)
#     else:
#         print("No matching data found between Amazon and Flipkart.")

# # Run the main function
# if __name__ == "__main__":
#     main()






import mysql.connector
import socket
import json
import spacy
import Levenshtein  # For Jaro-Winkler similarity

# Load the pre-trained NER model
nlp = spacy.load("en_core_web_sm")

# Function to compute Jaro-Winkler similarity using Levenshtein package
def jaro_winkler_similarity(str1, str2):
    return Levenshtein.jaro_winkler(str1, str2)

# Function to send queries to Vivek's Amazon server
def send_query_to_vevek_amazon(query):
    host = '192.168.49.246'  # Replace with the IP address of Vivek's machine
    port = 12345  # Ensure this matches the server's port

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client_socket.connect((host, port))
        client_socket.send(query.encode('utf-8'))

        # Receive data in chunks to handle large responses
        response_chunks = []
        while True:
            chunk = client_socket.recv(4096).decode('utf-8')  # Adjust chunk size if needed
            if not chunk:  # End of transmission
                break
            response_chunks.append(chunk)

        # Combine chunks into a single response
        response = ''.join(response_chunks)

        # Decode JSON response
        try:
            result = json.loads(response)
            return result
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON response: {e}")
            print(f"Partial response received: {response[:1000]}...")  # Print the first 1000 characters for debugging
            return None
    except Exception as e:
        print(f"Error during socket communication: {e}")
        return None
    finally:
        client_socket.close()

# Function to load Amazon metadata from JSON file
def load_amazon_metadata(file_path):
    try:
        with open(file_path, 'r') as file:
            metadata = json.load(file)
            # Extracting the column names from the metadata
            amazon_columns = list(metadata["amazon_combined_data"].keys())
            return amazon_columns
    except Exception as e:
        print(f"Error loading Amazon metadata: {e}")
        return None

# Function to fetch Amazon combined data
def fetch_amazon_combined_data():
    query = "SELECT * FROM amazon_combined_data;"
    return send_query_to_vevek_amazon(query)

# Function to fetch Flipkart data
def fetch_flipkart_data():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="admin",
            database="flipkart"
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM globalflipkartproductsmv;")
        data = cursor.fetchall()
        cursor.close()
        connection.close()
        return data
    except mysql.connector.Error as e:
        print(f"Error fetching Flipkart data: {e}")
        return []

# Function to apply NER to extract product entities from the product name
def apply_ner_to_product_name(product_name):
    doc = nlp(product_name)
    entities = [ent.text for ent in doc.ents]
    return entities

# Function to store HLV data into the database (auto-increment id)
def store_hlv_data_to_db(hlv_data):
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="admin",
            database="flipkart"  # Your target database name
        )
        cursor = connection.cursor()

        # Create HLV table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS HLV (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(255),
            ratings FLOAT,
            discount_price FLOAT,
            actualprice FLOAT,
            imagelinks TEXT,
            source VARCHAR(255)
        );
        """
        cursor.execute(create_table_query)

        
        # Insert data into the HLV table, ignoring duplicates
        insert_query = """
        INSERT IGNORE INTO HLV (name, category, ratings, discount_price, actualprice, imagelinks, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, hlv_data)

        connection.commit()
        print(f"Data inserted into HLV table successfully.")
        cursor.close()
        connection.close()
    except mysql.connector.Error as e:
        print(f"Error storing data in HLV: {e}")

# Main function to execute the data processing
def main():
    # Load Amazon metadata and columns
    amazon_metadata_path = r"C:\Users\kumar\Documents\Semester\Sem7\IIA\Datasources\metadata\amazon_metadata.json"
    amazon_columns = load_amazon_metadata(amazon_metadata_path)
    if not amazon_columns:
        print("Error: Could not load Amazon column names.")
        return

    amazon_data = fetch_amazon_combined_data()
    if not amazon_data:
        print("No data fetched from Amazon database.")
        return

    flipkart_data = fetch_flipkart_data()
    if not flipkart_data:
        print("No data fetched from Flipkart database.")
        return

    hlv_data = []

    # Set threshold for Jaro-Winkler similarity
    similarity_threshold = 0.7
    
    # Matching the columns from Amazon and Flipkart using Jaro-Winkler similarity
    matched_columns = {}
    # print(amazon_columns)
    flipkartarr=list(flipkart_data[0].keys())
    # print(list(flipkart_data[0].keys()))
    
    for aindex in range(1,len(amazon_columns)):
        maxmatch=-1
        for findex in range(1,len(flipkartarr)):  # Iterate through all Flipkart columns
            similarity = jaro_winkler_similarity(amazon_columns[aindex], flipkartarr[findex])
            if similarity > similarity_threshold:
                # print(f"Similarity between '{amazon_columns[aindex]}' (Amazon) and '{flipkartarr[findex]}' (Flipkart): {similarity}")
                maxmatch=flipkartarr[findex]
        matched_columns[amazon_columns[aindex]] = maxmatch

    
    matched_columns = {k: v for k, v in matched_columns.items() if v != -1}
    
    
    # print(matched_columns)
    
    
    reqarr= ["name", "category", "ratings", "discountprice", "actualprice", "imagelinks"]
    finalmatchedcolumn={}
    for i in reqarr:
        sima=0
        simf=0
        aval=0
        fval=0
        for a,f in matched_columns.items():
            asimilarity = jaro_winkler_similarity(a, i)
            fsimilarity = jaro_winkler_similarity(f, i)
            
            if asimilarity > sima:
                sima=asimilarity
                aval=a
            if fsimilarity > simf:
                simf=fsimilarity
                fval=f
            # print(f"{a} with {i} similarity score: {asimilarity}")
            # print(f"{f} with {i} similarity score: {fsimilarity}")
        if i=='discountprice':
            fval='discount_price'
            aval='reducedprice'
        finalmatchedcolumn[aval]=fval

    # print(finalmatchedcolumn)
    # Iterate through Amazon and Flipkart data, combining based on matched columns
    for amazon_row in amazon_data:
        for flipkart_row in flipkart_data:
            hlv_row = []
            for amazon_column, flipkart_column in finalmatchedcolumn.items():
                amazon_index = amazon_columns.index(amazon_column)
                flipkart_index= flipkartarr.index(flipkart_column)
                # amazon_row[amazon_index].append('amazon')
                hlv_row.append(amazon_row[amazon_index])
            hlv_row.append('amazon')
            # print(hlv_row)
            # break
            hlv_data.append(tuple(hlv_row))
            hlv_row = []
            for amazon_column, flipkart_column in finalmatchedcolumn.items():
                # amazon_index = amazon_columns.index(amazon_column)
                flipkart_index= flipkartarr.index(flipkart_column)
                # flipkart_row[flipkart_index].append('flipkart')
                # print(flipkart_row[flipkart_index])
                # print(flipkart_row)
                # print(flipkart_index)
                hlv_row.append(flipkart_row[flipkart_column])
            hlv_row.append('flipkart')
            hlv_data.append(tuple(hlv_row))

    # Insert the combined and mapped data into the HLV table
    # print(hlv_data)
    if hlv_data:
        # print(hlv_data[0])
        store_hlv_data_to_db(hlv_data)
    else:
        print("No matching data found between Amazon and Flipkart.")

# Run the main function
if __name__ == "__main__":
    main()















# import mysql.connector
# import socket
# import json
# import spacy
# import Levenshtein  # For Jaro-Winkler similarity

# # Load the pre-trained NER model
# nlp = spacy.load("en_core_web_sm")

# # Function to compute Jaro-Winkler similarity using Levenshtein package
# def jaro_winkler_similarity(str1, str2):
#     return Levenshtein.jaro_winkler(str1, str2)

# # Function to send queries to Vivek's Amazon server
# def send_query_to_vevek_amazon(query):
#     host = '192.168.49.246'  # Replace with the IP address of Vivek's machine
#     port = 12345  # Ensure this matches the server's port

#     client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     try:
#         client_socket.connect((host, port))
#         client_socket.send(query.encode('utf-8'))

#         # Receive data in chunks to handle large responses
#         response_chunks = []
#         while True:
#             chunk = client_socket.recv(4096).decode('utf-8')  # Adjust chunk size if needed
#             if not chunk:  # End of transmission
#                 break
#             response_chunks.append(chunk)

#         # Combine chunks into a single response
#         response = ''.join(response_chunks)

#         # Decode JSON response
#         try:
#             result = json.loads(response)
#             return result
#         except json.JSONDecodeError as e:
#             print(f"Failed to decode JSON response: {e}")
#             print(f"Partial response received: {response[:1000]}...")  # Print the first 1000 characters for debugging
#             return None
#     except Exception as e:
#         print(f"Error during socket communication: {e}")
#         return None
#     finally:
#         client_socket.close()

# # Function to fetch Amazon combined data
# def fetch_amazon_combined_data():
#     query = "SELECT * FROM amazon_combined_data;"
#     return send_query_to_vevek_amazon(query)

# # Function to fetch Flipkart data
# def fetch_flipkart_data():
#     try:
#         connection = mysql.connector.connect(
#             host="localhost",
#             user="root",
#             password="admin",
#             database="flipkart"
#         )
#         cursor = connection.cursor(dictionary=True)
#         cursor.execute("SELECT * FROM globalflipkartproductsmv;")
#         data = cursor.fetchall()
#         cursor.close()
#         connection.close()
#         return data
#     except mysql.connector.Error as e:
#         print(f"Error fetching Flipkart data: {e}")
#         return []

# # Function to apply NER to extract product entities from the product name
# def apply_ner_to_product_name(product_name):
#     doc = nlp(product_name)
#     entities = [ent.text for ent in doc.ents]
#     return entities

# # Function to store HLV data into the database (auto-increment id)
# def store_hlv_data_to_db(hlv_data):
#     try:
#         connection = mysql.connector.connect(
#             host="localhost",
#             user="root",
#             password="admin",
#             database="flipkart"  # Your target database name
#         )
#         cursor = connection.cursor()

#         # Create HLV table if it doesn't exist
#         create_table_query = """
#         CREATE TABLE IF NOT EXISTS HLV (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             product_name VARCHAR(255),
#             category VARCHAR(255),
#             ratings FLOAT,
#             discount_price FLOAT,
#             actual_price FLOAT,
#             image_links TEXT
#         );
#         """
#         cursor.execute(create_table_query)

#         # Insert data into the HLV table, ignoring duplicates
#         insert_query = """
#         INSERT IGNORE INTO HLV (product_name, category, ratings, discount_price, actual_price, image_links)
#         VALUES (%s, %s, %s, %s, %s, %s)
#         """
#         cursor.executemany(insert_query, hlv_data)

#         connection.commit()
#         print(f"Data inserted into HLV table successfully.")
#         cursor.close()
#         connection.close()
#     except mysql.connector.Error as e:
#         print(f"Error storing data in HLV: {e}")

# # Main function to execute the data processing
# def main():
#     amazon_data = fetch_amazon_combined_data()
#     if not amazon_data:
#         print("No data fetched from Amazon database.")
#         return

#     flipkart_data = fetch_flipkart_data()
#     if not flipkart_data:
#         print("No data fetched from Flipkart database.")
#         return

#     hlv_data = []

#     # Set threshold for Jaro-Winkler similarity
#     similarity_threshold = 0.6

#     for amazon_row in amazon_data:
#         for flipkart_row in flipkart_data:
#             # Assuming flipkart_row is a dictionary, access image_links using the correct key
#             amazon_image = amazon_row[3]  # Assuming image is the 4th column for Amazon (adjust if needed)
#             flipkart_image_links = flipkart_row["image_links"]  # Use the correct key for image links

#             # Match attributes using Jaro-Winkler similarity
#             if jaro_winkler_similarity(amazon_image, flipkart_image_links) > similarity_threshold:
#                 # Apply NER to extract entities from Amazon product names
#                 amazon_entities = apply_ner_to_product_name(amazon_row[1])  # Assuming product name is the 2nd column

#                 # Prepare data to insert into HLV
#                 hlv_row = (
#                     amazon_row[1],  # Product Name (2nd column)
#                     flipkart_row["category"],  # Category from Flipkart (adjust key if necessary)
#                     amazon_row[6],  # Ratings from Amazon (assuming 7th column)
#                     amazon_row[7],  # Discount price from Amazon (assuming 8th column)
#                     amazon_row[8],  # Actual price from Amazon (assuming 9th column)
#                     flipkart_row["image_links"]  # Image links from Flipkart (adjust key if necessary)
#                 )
#                 hlv_data.append(hlv_row)

#     # Insert the combined and mapped data into the HLV table
#     if hlv_data:
#         store_hlv_data_to_db(hlv_data)
#     else:
#         print("No matching data found between Amazon and Flipkart.")

# # Run the main function
# if __name__ == "__main__":
#     main()

