# Purpose: Provides functions to convert ROOT files to HDF5 and SQLite formats.
# Key Functions: root2h5, convert_branches_to_sqlite.


from typing import List
import os
import uproot
import numpy as np
import sqlite3
from .data_ops import root_to_dict_of_arrays, root_to_awkward_arrays
from .sqlite_ops import save_to_sqlite
from .hdf5_ops import save_to_h5


def root2h5(features: List[str], root_files: List[str], h5_dir:str) -> None:
    """
    Converts a list of root files to HDF5 format and saves them in the specified directory.
    - The function converts each root file in the `root_files` list to HDF5 format.
    - The converted HDF5 files are saved in the `h5_dir` directory.
    - The function uses the `root_to_arrays()` function to convert the root files to arrays.
    - The converted arrays are then saved to HDF5 files using the `save_to_h5()` function.

    Parameters:
     - root_files (List[str]): A list of root file paths to be converted.
     - h5_dir (str): The directory where the converted HDF5 files will be saved.

    Returns:
        None

    Examples:
        >>> root_files = ['/path/to/file1.root', '/path/to/file2.root']
        >>> h5_dir = '/path/to/h5_files'
        >>> convert_new_root_files(root_files, h5_dir)
    """
    for file in root_files:
        # columns_to_find = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        h5_file_path = os.path.join(h5_dir, os.path.basename(file).replace('.root', '.h5'))
        array_data_dict = root_to_dict_of_arrays(file, features)
        awkward_array = root_to_awkward_arrays(file, features) #NOTE: awkward array is not used 
        save_to_h5(array_data_dict, awkward_array, h5_file_path) 
    return None


def convert_branches_to_sqlite(root_file_path, tree_name, branch_names, sqlite_db_path):
    with uproot.open(root_file_path) as file:
        tree = file[tree_name]

        conn = sqlite3.connect(sqlite_db_path)
        cursor = conn.cursor()

        base_name = os.path.splitext(os.path.basename(root_file_path))[0]
        sanitized_table_name = f'"{base_name.replace(".", "_")}"'

        columns = ", ".join([f'"{branch_name}" REAL' for branch_name in branch_names])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {sanitized_table_name} ({columns})")

        # Initialize an empty list to store the branch data.
        data = []  

        # Iterate over all the branch names in the list 'branch_names'.
        for branch_name in branch_names:
            # Check if the current branch name exists in the tree's keys.
            if branch_name not in tree.keys():
                print(f"Branch '{branch_name}' not found in the tree '{tree_name}'")
                continue  # Skip to the next branch if the current one is not found.

            # Extract the branch data as a NumPy array
            branch_data = tree[branch_name].array(library="np")

            # Check if the data type of the branch is byte strings.
            if branch_data.dtype.type is np.bytes_:
                # Convert byte strings to regular strings (UTF-8 decoding).
                branch_data = np.char.decode(branch_data, 'utf-8')
            
            # Check if the branch data type is 'object', which may indicate mixed types or non-numeric data.
            elif branch_data.dtype == np.dtype('O'):
                try:
                    # Attempt to convert the branch data to 'float64' type (standard numeric type).
                    branch_data = branch_data.astype(np.float64)
                except ValueError:
                    """
                    The following line is needed, because otherwise large arrays appear truncated, i.e.: [0, 1, 2, ...., n-1, n]
                    Therefore, the arrays when converted to byte strings, are stored truncated and the conversion is not successful.

                    WARNING: Currently it is commented out, because a large amount of memory is needed for the conversion to be completed successfully. 
                    As a result, for the large ROOT files that are created from the KM3NeT PreAnalysis, the code crashes and it is never executed. 
                    Most probably, it has to be run in a cluster, like in Lyon.
                    """
                    #np.set_printoptions(threshold = np.inf)
                    """
                    For each item, we have to specifically state that it is of 'float' type.
                    Otherwise, boolean values are not passed as 0 or 1, but as True and False. 
                    """
                    # Handle the case where some values are not convertible to float64.
                    # np.set_printoptions(threshold=np.inf)

                    # Convert each item in the branch data to a string after converting to float.
                    # This ensures all data is treated as floats, including boolean values.
                    # Boolean values should be converted to 0 or 1 rather than 'True' or 'False'.
                    branch_data = np.array([str(item.astype(float)) for item in branch_data], dtype='S')

            # Append the processed branch data to the 'data' list.
            data.append(branch_data)

        # Combine the list of branch data into a 2D NumPy array where each branch becomes a column.
        data = np.column_stack(data)


        placeholders = ", ".join(["?"] * len(branch_names))
        cursor.executemany(f"INSERT INTO {sanitized_table_name} VALUES ({placeholders})", data)

        conn.commit()
        conn.close()
        print(f"Data from '{root_file_path}' has been successfully written to {sqlite_db_path}")


def root2sqlite(features: List[str], root_files: List[str], sqlite_dir:str):
    for file_ in root_files:
        # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        sqlite_file_path = os.path.join(sqlite_dir, os.path.basename(file_).replace('.root', '.db'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        save_to_sqlite(array_data_dict, sqlite_file_path)
        # save_to_h5(array_data_dict, sqlite_file_path) 
    return None
    pass
