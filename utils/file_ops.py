# Purpose: Contains utility functions for file operations, such as listing HDF5 and SQLite files in a directory.
# Key Functions: list_h5_files, list_sqlite_files.
 

import os
from typing import List


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
    ['db1.sqlite3', 'db2.sqlite3']
    """
    return [file for file in os.listdir(sqlite_dir) if file.endswith(".sqlite3")]