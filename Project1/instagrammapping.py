from pymongo import MongoClient
import pprint

# Connect to MongoDB
client = MongoClient("mongodb://192.168.47.237:27017/")
db = client["instagram"]

# Collections
business_profiles = db["business_profiles_data"]
sales_partnership = db["sales_partnership_data"]

# Dynamic Schema Analysis and Creation
def analyze_and_create_collections():
    """
    Analyze similarities and differences between collections and create new schema-based collections.
    """
    # Collections for results
    common_profiles = db["common_profiles"]
    exclusive_business_profiles = db["exclusive_business_profiles"]
    exclusive_sales_partnership = db["exclusive_sales_partnership"]
    analysis_results = db["analysis_results"]

    # Clear existing data in the results collections
    common_profiles.delete_many({})
    exclusive_business_profiles.delete_many({})
    exclusive_sales_partnership.delete_many({})
    analysis_results.delete_many({})

    # Analyze common and exclusive fields
    pipeline = [
        {
            "$lookup": {
                "from": "sales_partnership_data",
                "localField": "email",
                "foreignField": "email",
                "as": "matched_sales"
            }
        },
        {
            "$project": {
                "_id": 1,
                "user_name": 1,
                "email": 1,
                "followers": 1,
                "business_type": 1,
                "matched_sales": 1
            }
        }
    ]

    business_results = list(business_profiles.aggregate(pipeline))

    # Process results
    common_data = []
    unique_business_data = []
    unique_sales_data = []

    for result in business_results:
        if result["matched_sales"]:
            # Common profiles
            for sales in result["matched_sales"]:
                common_data.append({
                    "user_name": result["user_name"],
                    "email": result["email"],
                    "followers": result.get("followers"),
                    "business_type": result.get("business_type"),
                    "company_name": sales.get("company_name"),
                    "product": sales.get("product"),
                    "profit_in_amazon_percent": sales.get("profit_in_amazon_percent"),
                    "profit_in_flipkart_percent": sales.get("profit_in_flipkart_percent")
                })
        else:
            # Exclusive business profile
            unique_business_data.append({
                "user_name": result["user_name"],
                "email": result["email"],
                "followers": result.get("followers"),
                "business_type": result.get("business_type")
            })

    # Get exclusive sales data
    sales_emails = sales_partnership.distinct("email")
    business_emails = business_profiles.distinct("email")
    exclusive_sales = sales_partnership.find({"email": {"$nin": business_emails}})
    for sales in exclusive_sales:
        unique_sales_data.append(sales)

    # Insert into collections
    if common_data:
        common_profiles.insert_many(common_data)
    if unique_business_data:
        exclusive_business_profiles.insert_many(unique_business_data)
    if unique_sales_data:
        exclusive_sales_partnership.insert_many(unique_sales_data)

    # Generate Analysis Results
    analysis = [
        {"type": "most_followed", "data": business_profiles.find_one(sort=[("followers", -1)])},
        {"type": "top_amazon_profit", "data": sales_partnership.find_one(sort=[("profit_in_amazon_percent", -1)])},
        {"type": "top_flipkart_profit", "data": sales_partnership.find_one(sort=[("profit_in_flipkart_percent", -1)])}
    ]
    analysis_results.insert_many(analysis)

# Querying the New Collections
def query_collections():
    """
    Query and display the newly created collections for validation.
    """
    for profile in db["common_profiles"].find():
        pprint.pprint(profile)

    for profile in db["exclusive_business_profiles"].find():
        pprint.pprint(profile)

    for partnership in db["exclusive_sales_partnership"].find():
        pprint.pprint(partnership)

    for result in db["analysis_results"].find():
        pprint.pprint(result)

# Example Usage
if __name__ == "__main__":
    # Perform analysis and create collections
    analyze_and_create_collections()

    # Query the newly created collections
    query_collections()
