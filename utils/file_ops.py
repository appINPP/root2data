#    Copyright 2024 appINPP

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
   
# Purpose: Contains utility functions for file operations, such as listing HDF5 and SQLite files in a directory.


import os
from typing import List

def list_root_files(root_dir: str) -> List[str]:
    """
    Lists all HDF5 files in the given directory.

    Parameters:
        - h5_dir (str): Directory to search for HDF5 files.

    Returns:
        - List[str]: List of HDF5 file paths.
    
    Example:
    >>> list_root_files("data/root")
    ['file_1.root', 'file_2.root']
    """
    root_files = [f for f in os.listdir(root_dir) if f.endswith('.root')]
    return root_files    
    
def list_h5_files(h5_dir: str) -> List[str]:
    """
    Lists all HDF5 files in the given directory.

    Parameters:
        - h5_dir (str): Directory to search for HDF5 files.

    Returns:
        - List[str]: List of HDF5 file paths.
    
    Example:
    >>> list_h5_files("data/h5")
    ['file_1.h5', 'file_2.h5']
    """
    h5_files = [f for f in os.listdir(h5_dir) if f.endswith('.h5')]
    return h5_files

def list_sqlite_files(sqlite_dir: str) -> List[str]:
    """
    Lists all SQLite files in the given directory.

    Parameters:
        - sqlite_dir (str): Directory to search for SQLite files.

    Returns:
        - List[str]: List of SQLite file paths.
    
    Example:
    >>> list_sqlite_files("data/sqlite")
    ['sqlitedile_1.db', 'sqlitedile_2.db']
    """
    return [file for file in os.listdir(sqlite_dir) if file.endswith(".db")]

def list_parquet_files(parquet_dir: str) -> List[str]:
    """
    Lists all Parquet files in the given directory.

    Parameters:
     - parquet_dir (str): Directory to search for Parquet files.

    Returns:
     - List[str]: List of Parquet file paths.
    
    Example:
    >>> list_parquet_files("data/parquet")
    ['file_1.parquet', 'file_2.parquet']
    """
    parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith('.parquet')]
    return parquet_files