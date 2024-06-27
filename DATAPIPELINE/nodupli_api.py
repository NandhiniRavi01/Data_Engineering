import requests
import psycopg2
from psycopg2 import sql

# Fetch data from API
def fetch_data_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data from the API")
        return None

# Parse JSON data
def parse_json_data(json_data):
    if isinstance(json_data, dict):
        return [json_data]
    elif isinstance(json_data, list):
        return json_data
    else:
        print("JSON data is not in the expected format")
        return None

# Connect to PostgreSQL
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            dbname = 'sample1' ,
            user = 'postgres' ,
            password = 'nandhu01',
            host = 'localhost',
            port = '5432'
        )
        return conn
    except Exception as e:
        print("Error connecting to PostgreSQL database:", e)
        return None

# Insert data into PostgreSQL with upsert
def insert_data_with_upsert(conn, data):
    cursor = conn.cursor()
    for item in data:
        cursor.execute(
            sql.SQL("""
                INSERT INTO api_data (userId, id, title, completed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """),
            (item['userId'], item['id'], item['title'], item['completed'])
        )
    conn.commit()
    cursor.close()
    print("Data inserted/updated successfully")

# Main function
def main():
    api_url = "https://jsonplaceholder.typicode.com/todos"  # Replace with actual API URL
    json_data = fetch_data_from_api(api_url)
    if json_data:
        parsed_data = parse_json_data(json_data)
        conn = connect_to_postgres()
        if conn:
            insert_data_with_upsert(conn, parsed_data)
            conn.close()

if __name__ == "__main__":
    main()


'''CREATE TABLE IF NOT EXISTS api_data (
                UserId INT,
            id INT PRIMARY KEY ,
            title VARCHAR(255),
                   completed VARCHAR(255)
            
        );
'''