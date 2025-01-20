import pandas as pd
from pathlib import Path
import pyodbc
 
class HiveIngestion:
    def __init__(self, conn_str):
        self.conn_str = conn_str
 
    def process_columns(self, df):
        """
        In Hive, there are special characters - need to remove them and prepare for Hive format.
        Args:
            df (DataFrame): Input DataFrame.
        Returns:
            List[str]: Cleaned column names.
        """
        return [col.replace(" ", "_").replace("-", "_").strip().lower() for col in df.columns]
 
    def create_table_query(self, table_name, columns, file_type):
        """
        CREATE TABLE query dynamically based on file type.
        Args:
            table_name (str): Hive table name.
            columns (List[str]): List of column names.
            file_type (str): File type ('csv' or 'parquet').
        Returns:
            str: Generated CREATE TABLE query.
        """
        base_query = f"CREATE TABLE IF NOT EXISTS sales_db_object.{table_name} (\n"
        base_query += ",\n".join([f"    {col} STRING" for col in columns])
        base_query += "\n)"
 
        if file_type == 'csv':
            row_format = "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
            storage_format = "STORED AS TEXTFILE TBLPROPERTIES ('skip.header.line.count'='1')"
        elif file_type == 'parquet':
            row_format = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
            storage_format = "STORED AS PARQUET TBLPROPERTIES ('serialization.null.format'='\\N', 'parquet.compression'='SNAPPY')"
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
 
        return f"{base_query}\n{row_format}\n{storage_format};"
 
    def load_data_query(self, table_name, input_file):
        """
        Generate the LOAD DATA query for Hive.
        Args:
            table_name (str): Hive table name.
            input_file (str): Input file name.
        Returns:
            str: Generated LOAD DATA query.
        """
        return f"LOAD DATA INPATH '/path/{input_file}' " \
               f"OVERWRITE INTO TABLE sales_db_object.{table_name};"
 
    def execute_query(self, query, verbose=False):
        """
        Args:
            query (str): SQL query to execute.
            verbose (bool): If True, prints the query instead of executing.
        """
        if verbose:
            print("Query to execute:")
            print(query)
        else:
            try:
                conn = pyodbc.connect(self.conn_str, autocommit=True)
                cursor = conn.cursor()
                cursor.execute(query)
                print("Query executed successfully.")
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
 
    def ingest_data(self, table_name, input_file, file_type, columns, verbose=False):
        """
        Args:
            table_name (str): Hive table name.
            input_file (str): Input file name.
            file_type (str): File type ('csv' or 'parquet').
            columns (List[str]): List of column names.
            verbose (bool): If True, prints the queries instead of executing.
        """
 
        create_table_sql = self.create_table_query(table_name, columns, file_type)
        load_data_sql = self.load_data_query(table_name, input_file)
 
        print(f"Ingesting data for table: {table_name}")
        self.execute_query(create_table_sql, verbose=verbose)
        self.execute_query(load_data_sql, verbose=verbose)
 
 
if __name__ == "__main__":
 
    path = Path("mock_ingestion_data.parquet")
 
     
    if path.suffix == '.csv':
        df = pd.read_csv(path)
        file_type = 'csv'
    elif path.suffix == '.parquet':
        df = pd.read_parquet(path)
        file_type = 'parquet'
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")
 
    conn_str = "DSN=hive"
    hive_ingestion = HiveIngestion(conn_str)
 
    columns = hive_ingestion.process_columns(df)
    table_name = "transaction_mock_data"
    input_file = path.name
 
    hive_ingestion.ingest_data(table_name, input_file, file_type, columns, verbose=False)  # Set verbose=True for debugging
