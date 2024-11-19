"""
DB connection class. 
This is a generatic class to connect to the database 
currently supported by mysql and postgresql databases 
"""

import mysql.connector as mysql
import psycopg2


class DBConnection:
    def __init__(self, db_type, db_config):
        self.db_type = db_type
        self.db_config = db_config
        self.connection = None
        self.cursor = None

    def init_conn(self):
        try:
            if self.db_type == "mysql":
                self.connection = mysql.connect(**self.db_config)
            elif self.db_type == "postgresql":
                self.connection = psycopg2.connect(**self.db_config)
            else:
                raise ValueError("Unsupported database type")
            print(f"Connection established for {self.db_type}")
            return self.connection
        except Exception as e:
            print(f"Error occurred while connecting to {self.db_type} database: {e}")
            self.connection = None
            return self.connection

    def close_conn(self):
        try:
            if self.db_type == "mysql":
                if self.connection.is_connected():
                    self.cursor.close()
                    self.connection.close()
                    print("MySQL database connection closed")
            elif self.db_type == "postgresql":
                if self.connection:
                    self.cursor.close()
                    self.connection.close()
                    print("PostgreSQL database connection closed")
        except (mysql.Error, psycopg2.Error) as e:
            print(f"Error occurred while closing database connection: {e}")

    def get_cursor(self):
        try:
            if self.connection is None:
                print("No active database connection")
                return None

            if self.db_type == "mysql":
                if not self.connection.is_connected():
                    self.connection.reconnect()
            elif self.db_type == "postgresql":
                if self.connection.closed:
                    self.connection = psycopg2.connect(**self.db_config)

            self.cursor = self.connection.cursor()
            return self.cursor
        except (mysql.Error, psycopg2.Error) as e:
            print(f"Error occurred while getting cursor: {e}")
            return None


"""
File Process class
Generic class used to open and process a file
currently supporting csv and text files
"""
import csv
import os
import pandas as pd

class FileReader:
    def __init__(self, file_config):
        self.file_name = file_config.get("file_name")
        self.file_path = file_config.get("file_path")
        self.file_type = file_config.get("file_type")
        self.delimiter = file_config.get("delimiter")
        self.columns = file_config.get("columns")

        self.file_path = self.file_path if self.file_path else os.getcwd()
        # Construct full file path
        self.full_path = os.path.join(self.file_path, self.file_name)
        self.content = None

    def read_file(self):
        try:
            if not os.path.exists(self.full_path):
                raise FileNotFoundError(f"File not found: {self.full_path}")
            
            if self.file_type == 'text':
                # Handle text files; check if tab-delimited
                self.content =  self._read_text_tab_delimited() if self.delimiter == '\t' else self._read_text()
                return self.content
            elif self.file_type == 'csv':
                self.content =  self._read_csv()
                return self.content
            else:
                raise ValueError(f"Unsupported file type: {self.file_type}")
        except Exception as e:
            raise RuntimeError(f"An error occurred while reading the file: {e}")
    
    def _read_text(self):
        try:
            with open(self.full_path, 'r', encoding='utf-8') as file:
                return file.read()
        except Exception as e:
            raise IOError(f"Error reading text file: {e}")
    
    def _read_text_tab_delimited(self):
        try:
            with open(self.full_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file, delimiter='\t')
                return [row for row in reader]
        except Exception as e:
            raise IOError(f"Error reading tab-delimited text file: {e}")
    
    def _read_csv(self):
        try:
            # csv-reader implementation
            # with open(self.full_path, 'r', encoding='utf-8') as file:
            #     reader = csv.reader(file, delimiter=self.delimiter)
            #     return [row for row in reader]

            # pandas-read_csv implementation
            df = pd.read_csv(self.full_path, delimiter=self.delimiter)
            if len(self.columns) > 0:
                df.columns = self.columns
            return df
        except Exception as e:
            raise IOError(f"Error reading CSV file: {e}")




# ============================ ETL PIPELINE START============================
# necessary imports
import json


print("============================ ETL PIPELINE START============================")
# read the etl resource json file
etl_info ={}
etl_info_json = "etl_info.json"
with open(etl_info_json) as f:
    etl_info =json.load(f)
print("\n============================ Etl_info json read =========================")

# variable declarations
etl_source_details = etl_info["source"]
etl_target_details = etl_info["target"]
pipelines = etl_info["pipelines"]

# loop the pipeline section
for pipeline in pipelines:
    print(f"\n============================ Pipeline: {pipeline["pipeline_name"]} ===========================")
    pipeline_src_type = pipeline["extract"]["source_type"]
    pipeline_src_id = pipeline["extract"]["source_id"]
    extracted_data = None
    print("Extracting...")
    print("Source type: ",pipeline_src_type)
    print("Source id: ", pipeline_src_id)

    # Extract:  extract source data to transform
    # check the source type
    if pipeline_src_type == "file":
        # find and get the file configratuion form etl_source_details
        for file_config in etl_source_details["type"][pipeline_src_type]:
            if pipeline_src_id in file_config["file_name"]:
                print(f"File found for extracting. File name: [{file_config["file_name"]}] and file type [{file_config["file_type"]}] ")
                file_reader_obj = FileReader(file_config)
                extracted_data = file_reader_obj.read_file()
                break
    elif pipeline_src_type == "database":
        pass
    elif pipeline_src_type == "api":
        pass

    print(extracted_data.head())
    # Transform : perform operation on the data to be transformed
    # Condition : get the product with rating of 3 and above, for those product change the timestamp
    transformation_steps = pipeline["transform"]["steps"]  #list of steps to be transformed
    for idx, step in enumerate(transformation_steps, start=1):
        print(f"Step {idx}: [{step["step_name"]}]")
        

    # Load: load the transformed data into the target