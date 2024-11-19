# Purpose: Contains functions for interacting with SQLite databases, such as retrieving table names and converting SQLite tables to pandas DataFrames.
# Key Functions: get_table_names, sqlite_to_dataframe.

import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, Any, Union
import os
import json
from .data_ops import byte_preprocessing

def get_table_names(sqlite_db_path):
    conn = sqlite3.connect(sqlite_db_path)
    #cursor to allow SQL commands to be executed
    cursor = conn.cursor()
    #run a Query to get all the tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    #return the tables to the "tables" variable and return all the table names(the name is in the 0 position)
    tables = cursor.fetchall()
    conn.close()
    return [table[0] for table in tables]


def sqlite_to_dataframe(sqlite_db_path, table_name):
    """
    Transforms data from a specified SQLite table into a pandas DataFrame.

    Parameters:
    - sqlite_db_path (str): Path to the SQLite database file.
    - table_name (str): Name of the table to read data from.

    Returns:
    - pd.DataFrame: DataFrame containing the data from the specified SQLite table.
    """
    # Create a connection to the SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    
    # Query to select all data from the specified table
    query = f'SELECT * FROM "{table_name}"'
    
    # Read the data into a DataFrame
    df = pd.read_sql_query(query, conn)
    
    # Close the database connection
    conn.close()
    # Convert byte columns to string or appropriate numerical formats
    for column in df.columns:
        # Check if the column dtype is object (which includes bytes)
        if df[column].dtype == 'object':
            # Attempt to decode bytes and handle any conversion issues
            df[column] = df[column].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
            
            # Convert string representations of numerical arrays to actual numpy arrays
            try:
                df[column] = df[column].apply(lambda x: np.fromstring(x.strip('[]'), sep=' ') if '[' in x else x)
            except Exception as e:
                print(f"Error converting column '{column}': {e}")
    return df


def save_to_sqlite(arrays: Dict[str, np.ndarray], sqlite_db_path: str) -> None:
    '''
    DOCUMENATION: The code converts an array of byte strings into a list of NumPy arrays of float64.
    '''
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    table_name = os.path.basename(sqlite_db_path).split('.db')[0]
    # base_name = os.path.splitext(os.path.basename(root_file_path))[0]
    sanitized_table_name = f'"{table_name.replace(".", "_")}"'
    columns = []
    for key, array in arrays.items():
        if array.dtype == "O":
            columns.append(f'"{key}" TEXT')
        else:
            columns.append(f'"{key}" REAL')
    # cursor.execute(f"CREATE TABLE {sanitized_table_name} ({', '.join(arrays.keys())})")
    cursor.execute(f"CREATE TABLE {sanitized_table_name} ({', '.join(columns)})")
    data = []
    for key, array in arrays.items():
        if array.dtype == object:
            try:
                array = np.array([np.array(item, dtype=np.float64) for item in array]) # [item_.astype('float32') for item_ in array.tolist()][0]
            except ValueError:
                list_of_arrays = []
                print(f'Can not convert {key} to float64. Converting to byte strings and storing as variable-length sequence')
                np.set_printoptions(threshold=np.inf)
                array_with_byte_data = np.array([str(item.astype(float)) for item in array], dtype='S')
                for item in array_with_byte_data:
                    if len(str(item)) > 1:                            
                        list_of_arrays.append(byte_preprocessing(item))
            array = np.array(list_of_arrays, dtype=object)  # Use dtype=object to handle variable-length sequences
        else:
            array = array.astype(array.dtype)
        data.append(array)

    placeholders = ", ".join(["?"] * len(arrays.keys()))
    # data = np.column_stack(list(arrays.values()))
    # max_length = max(len(arr) for arr in data)
    # data = [np.pad(arr, (0, max_length - len(arr)), 'constant', constant_values=np.nan) for arr in data]
    
    data = np.column_stack(data)
    # data = [[item.tobytes() if isinstance(item, np.ndarray) else item for item in row] for row in data]
    data = [[json.dumps(item.tolist()) if isinstance(item, np.ndarray) else item for item in row] for row in data]
    
    cursor.executemany(f"INSERT INTO {sanitized_table_name} VALUES ({placeholders})", data)
    conn.commit()
    conn.close()
    print(f'Data has been successfully written to {sqlite_db_path}')
    print(5 * '-----------------------------------')
    return None

def read_sqlite_to_df(sqlite_db_path, table_name):
    conn = sqlite3.connect(sqlite_db_path)
    query = f'SELECT * FROM "{table_name}"'
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert JSON strings back to arrays
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(lambda x: np.array(json.loads(x)) if isinstance(x, str) else x)
    
    return df

