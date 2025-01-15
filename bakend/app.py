from flask import Flask, render_template, request, jsonify
import mysql.connector
from flask_cors import CORS
import os
import json
from openai import OpenAI
import subprocess

# Flask app setup
app = Flask(__name__)
CORS(app)

# Configure NVIDIA LLaMA API
NVIDIA_API_KEY = "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"
client = OpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key=NVIDIA_API_KEY
)

# Database connections
mysql_conn_flipkart = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="flipkart"
)

mysql_cursor_flipkart = mysql_conn_flipkart.cursor(dictionary=True)

# Load metadata for query context
def load_metadata():
    metadata = {}
    metadata_folder = "metadata"
    for file_name in os.listdir(metadata_folder):
        if file_name.endswith("_metadata.json"):
            with open(os.path.join(metadata_folder, file_name), "r") as f:
                metadata[file_name] = json.load(f)
    return metadata
def generate_metadata():
    try:
        subprocess.run(["python", "meta.py"], check=True)
        print("Metadata generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running meta.py: {e}")
# Generate SQL Query
def generate_sql_query(user_query, metadata):
    try:
        print("Sending request to LLaMA API...")
        completion = client.chat.completions.create(
            model="nvidia/llama-3.1-nemotron-70b-instruct",
            messages=[
                {
                    "role": "system",
                    "content": "You are a SQL query generator. Respond ONLY with the SQL query, no explanations or markdown."
                },
                {
                    "role": "user",
                    "content": f"Generate a SQL SELECT query for: {user_query}. Using table flipkart.hlv with columns: name, category, ratings, discount_price, actual_price, source."
                }
            ],
            temperature=0.1,
            max_tokens=512
        )

        # Extract and validate SQL query
        raw_response = completion.choices[0].message.content.strip()
        print("Raw response from LLaMA:", raw_response)
        if '```sql' in raw_response:
            sql_query = raw_response.split('```sql')[1].split('```')[0].strip()
        elif '```' in raw_response:
            sql_query = raw_response.split('```')[1].strip()
        else:
            start_pos = raw_response.upper().find('SELECT')
            if start_pos >= 0:
                sql_query = raw_response[start_pos:].strip()
            else:
                sql_query = raw_response.strip()

        if not sql_query.upper().startswith('SELECT'):
            raise ValueError("Generated query must be a SELECT statement")
        print("Generated SQL Query:", sql_query)
        return sql_query
    except Exception as e:
        print(f"Error in generate_sql_query: {e}")
        raise Exception(f"Failed to generate SQL query: {e}")

def genertate_entity_matcher():
    try:
        subprocess.run(["python", "entity_matcher.py"], check=True)
        print("Entity Matcher generated successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running entity_matcher.py: {e}")


# Execute SQL Query
def execute_sql_query(query):
    try:
        print("Executing SQL query:", query)
        mysql_cursor_flipkart.execute(query)
        results = mysql_cursor_flipkart.fetchall()
        print("Query Results:", results)
        return results
    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
        raise Exception(f"Error executing SQL query: {e}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    try:
        # genertate_entity_matcher()
        # generate_metadata()
        print("Received search request")
        request_data = request.get_json()
        print("Request data:", request_data)
        user_query = request_data.get('query', '').strip()
        if not user_query:
            return jsonify({"error": "Query cannot be empty."}), 400

        # Load metadata for context
        metadata = load_metadata()
        print("Loaded metadata:", metadata)

        # Generate SQL query
        try:
            sql_query = generate_sql_query(user_query, metadata)
        except Exception as e:
            return jsonify({"error": f"Error generating SQL query: {e}"}), 500

        # Execute SQL query
        results = execute_sql_query(sql_query)
        if not results:
            return jsonify({"query": sql_query, "results": [], "count": 0})

        formatted_results = [
            {key: str(value) if value is not None else None for key, value in row.items()}
            for row in results
        ]

        print("Formatted Results:", formatted_results)

        return jsonify({
            "query": sql_query,
            "results": formatted_results,
            "count": len(formatted_results)
        })
    except Exception as e:
        print(f"General error in search endpoint: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
