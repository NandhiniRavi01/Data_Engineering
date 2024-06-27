import psycopg2
from hdfs import InsecureClient
import pandas as pd
import io

# HDFS Configuration
hdfs_url = "http://localhost:9870"
client = InsecureClient(hdfs_url, user='nandhumidhun')

# PostgreSQL Configuration
pg_config = {
    'dbname': 'project1',
    'user': 'postgres',
    'password': 'nandhu01',
    'host': '127.0.0.1',
    'port': '5432'
}

def extract_data(source_file):
    # Example: Read CSV file
    data = pd.read_csv(source_file)
    return data

def transform_data(data):
    # Example transformation: Convert column names to lowercase
    data.columns = [col.lower() for col in data.columns]
    return data
def load_to_hdfs(data, hdfs_path):
    with client.write(hdfs_path) as writer:
        output = io.BytesIO()
        data.to_csv(output, index=False, header=True)
        output.seek(0)
        writer.write(output.getvalue())

def load_to_postgres(data, table_name):
    # Example: Loading data to PostgreSQL
    try:
        conn = psycopg2.connect(**pg_config)
        cur = conn.cursor()

        # Create table if not exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join([f'{col} TEXT' for col in data.columns])}
        );
        """
        cur.execute(create_table_query)
        conn.commit()

        # Insert data into PostgreSQL
        output = io.StringIO()
        data.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        cur.copy_from(output, table_name, null="")
        conn.commit()

        print("Data successfully loaded to PostgreSQL")
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def etl_process(source_file, hdfs_path, table_name):
    # Extract
    data = extract_data(source_file)

    # Transform
    data = transform_data(data)

    # Load to HDFS
    load_to_hdfs(data, hdfs_path)

    # Load to PostgreSQL
    load_to_postgres(data, table_name)
print("Successfull")

if __name__ == "__main__":
    source_file ="/home/nandhumidhun/file.csv"
    hdfs_path = "/dockerhadoop/file.csv"
    table_name = "sample"

    etl_process(source_file, hdfs_path, table_name)
