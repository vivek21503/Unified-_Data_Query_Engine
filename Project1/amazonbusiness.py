from pymongo import MongoClient
from rapidfuzz import fuzz

# Connect to MongoDB
instagram_client = MongoClient("mongodb://192.168.47.237:27017/")  # Local connection for Instagram DB
amazon_client = MongoClient("mongodb://192.168.49.246:27017/")  # Remote connection for IIA Amazon DB

# Databases
insta_db = instagram_client['instagram']
iia_amazon_db = amazon_client['iia_amazon']

# Collections
sales_collection = insta_db['sales_partnership_data']
amazon_collection = iia_amazon_db['combined_amazon_data']
output_collection = iia_amazon_db['amazon_sales_partership_view']

# Step 1: Retrieve column names dynamically
sales_columns = sales_collection.find_one().keys()
amazon_columns = amazon_collection.find_one().keys()

# Step 2: Extract the 'business_category' column values
sales_data = list(sales_collection.find({}))  # Retrieve all rows
business_categories = [str(entry.get('business_category', "")) for entry in sales_data]

# Step 3: Compute similarity between 'business_category' and amazon column data
max_similarity_column = None
max_similarity_score = 0

# Iterate over dynamically retrieved amazon columns
for amazon_column in amazon_columns:
    if amazon_column in ["_id", "nameitem"]:  # Skip non-relevant columns
        continue

    amazon_data = list(amazon_collection.find({}, {amazon_column: 1, "_id": 0}))
    amazon_values = [str(entry.get(amazon_column, "")) for entry in amazon_data if entry.get(amazon_column)]

    if not amazon_values:  # Skip if the column is empty
        continue

    # Compute fuzzy matching score for each amazon value with business_category
    for amazon_value in amazon_values:
        for business_category in business_categories:
            similarity_score = fuzz.ratio(business_category, amazon_value) / 100.0  # Normalizing to 0-1 scale

            # Find the maximum similarity score
            if similarity_score > max_similarity_score:
                max_similarity_score = similarity_score
                max_similarity_column = amazon_column

# Step 4: Match rows based on the identified column and similarity threshold
# Iterate through the matched rows and insert combined data into the collection
for sales_row in sales_data:
    business_category = str(sales_row.get('business_category', ""))
    company_name = str(sales_row.get('company_name', "")).lower()

    # Retrieve amazon rows for the column with the highest similarity
    amazon_rows = list(amazon_collection.find({
        max_similarity_column: {"$regex": business_category, "$options": "i"}
    }))

    for amazon_row in amazon_rows:
        nameitem = str(amazon_row.get("nameitem", "")).lower()

        # Check if company_name is a subset of nameitem
        if company_name in nameitem:
            # Combine all data from both sales_row and amazon_row
            combined_data = {**sales_row, **amazon_row}
            combined_data['matched_column'] = max_similarity_column
            # combined_data['similarity_score'] = similarity_score

            # Remove '_id' if it exists, to avoid duplicate key error
            if '_id' in combined_data:
                del combined_data['_id']

            # Insert into the new collection
            output_collection.insert_one(combined_data)

print("Data matching and insertion completed successfully.")