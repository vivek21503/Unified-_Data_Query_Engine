

# from pymongo import MongoClient

# # MongoDB configuration
# MAC_IP = "192.168.47.237"  # Replace with your macOS IP
# PORT = 27017            # Default MongoDB port
# DATABASE_NAME = "social_media_db"
# COLLECTION_NAME = "business_profiles"

# def fetch_data_from_mongodb():
#     try:
#         # Connect to MongoDB on macOS
#         client = MongoClient(f"mongodb://{MAC_IP}:{PORT}/")
#         db = client[DATABASE_NAME]
#         collection = db[COLLECTION_NAME]

#         # Fetch all documents
#         data = list(collection.find({}, {"_id": 0}))  # Exclude MongoDB's _id field
#         return data
#     except Exception as e:
#         print(f"Error connecting to MongoDB: {e}")
#         return []

# if __name__ == "__main__":
#     data = fetch_data_from_mongodb()
#     if data:
#         print(f"Fetched {len(data)} records from MongoDB:")
#         for record in data:
#             print(record)
#     else:
#         print("No data fetched or connection failed.")









from pymongo import MongoClient

# MongoDB configurations
IP = "192.168.49.246"  # Replace with your macOS IP    # Localhost IP for your PC
PORT = 27017               # Default MongoDB port
DATABASE_NAME = "iia_amazon"
COLLECTION_NAME = "tshirts"

def fetch_data_from_mongodb(use_local=False):
    try:
        # Determine which IP to connect to
        ip =  IP
        
        # Connect to MongoDB
        client = MongoClient(f"mongodb://{ip}:{PORT}/")
        print(client)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Fetch all documents
        data = list(collection.find({}, {"_id": 0}))  # Exclude MongoDB's _id field
        return data
    except Exception as e:
        print(f"Error connecting to MongoDB ({'local' if use_local else 'macOS'}): {e}")
        return []

if __name__ == "__main__":
    # Switch between local and macOS connections
    use_local = True  # Change to False to connect to macOS MongoDB
    data = fetch_data_from_mongodb(use_local=use_local)
    print(data)
    if data:
        print(f"Fetched {len(data)} records from {'local' if use_local else 'macOS'} MongoDB:")
        for record in data:
            print(record)
    else:
        print("No data fetched or connection failed.")


