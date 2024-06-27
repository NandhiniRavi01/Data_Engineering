import csv
import psycopg2
from psycopg2 import sql

# Read data from CSV file
def read_data_from_csv(file_path):
    data = []
    try:
        with open(file_path, mode='r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                data.append(row)
    except Exception as e:
        print("Error reading CSV file:", e)
    return data

# Connect to PostgreSQL
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            dbname = 'samples',
            user = 'postgres',
            password = 'nandhu01',
            host = 'localhost',
            port = '5432'
        )
        return conn
    except Exception as e:
        print("Error connecting to PostgreSQL database:", e)
        return None

# Insert data into PostgreSQL with conflict handling
def insert_data_with_conflict_handling(conn, table_name, data):
    cursor = conn.cursor()
    for row in data:
        # Prepare SQL query
        sql_query = sql.SQL("INSERT INTO {} VALUES ({}) ON CONFLICT DO NOTHING").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Literal, row))  # Using sql.Literal to escape values
        )
        # Execute query
        cursor.execute(sql_query)
    conn.commit()
    cursor.close()
    print("Data inserted into PostgreSQL with conflict handling")

# Main function
def main():
    # Read data from CSV file
    csv_file_path = "file.csv"  # Replace with your CSV file path
    data_to_insert = read_data_from_csv(csv_file_path)
   
    # Connect to PostgreSQL
    conn = connect_to_postgres()
    if conn:
        # Insert data with conflict handling
        insert_data_with_conflict_handling(conn, "system_data", data_to_insert)
        conn.close()

if __name__ == "__main__":
    main()

