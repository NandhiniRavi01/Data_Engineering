import psycopg2

def fetch_data_from_source_db():
    # Connect to the source database
    source_conn = psycopg2.connect(
        user="postgres",
            password="nandhu01",
            host="127.0.0.1",
            port="5432",
            database="project1"
    )

    # Fetch data from source database (example query)
    source_cursor = source_conn.cursor()
    source_cursor.execute("SELECT * FROM users")
    data = source_cursor.fetchall()

    # Close cursor and connection
    source_cursor.close()
    source_conn.close()

    return data

def insert_data_into_destination_db(data):
    # Connect to the destination database
    dest_conn = psycopg2.connect(
            user="postgres",
            password="nandhu01",
            host="127.0.0.1",
            port="5432",
            database="project"
    )

    # Insert data into destination database with upsert (example query)
    dest_cursor = dest_conn.cursor()
    for row in data:
        dest_cursor.execute("""
            INSERT INTO users (id, name, email)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, row)

    # Commit changes and close cursor and connection
    dest_conn.commit()
    dest_cursor.close()
    dest_conn.close()

def main():
    # Fetch data from source database
    data = fetch_data_from_source_db()

    # Insert data into destination database
    insert_data_into_destination_db(data)


if __name__ == "__main__":
    main()
