import sqlite3
import uproot
import h5py
import argparse
import os
import re
import json
import numpy as np
import pandas as pd
import awkward as ak #pip install akward-pandas 
from typing import List, Dict


# INCOMPLETE
# def save_to_sqlite(arrays: Dict[str, np.ndarray], awkward_arrays, h5_file_path: str) -> None:
def save_to_sqlite(db_file_path, arrays: Dict[str, np.ndarray]):
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    
    for key, array in arrays.items():
        # Create a table for each key
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {key} (id INTEGER PRIMARY KEY, data BLOB)")
        
        if array.dtype == object:
            try:
                array = np.array([np.array(item, dtype=np.float64) for item in array])
            except ValueError:
                list_of_arrays = []
                print('Cannot convert to float64. Converting to byte strings')
                array_with_byte_data = np.array([str(item) for item in array], dtype='S')
                for item in array_with_byte_data:
                    if len(str(item)) > 1:
                        string_data = item.decode('utf-8').replace('[', '').replace(']', '').replace('\n', '').strip()
                        string_data = re.sub(r'\s+', ' ', string_data).split(' ')
                        float_data = [float(x) for x in string_data]
                        list_of_arrays.append(np.array(float_data, dtype=np.float64))
                array = list_of_arrays
        else:
            array = array.astype(array.dtype)
        
        # Insert data into the table
        for idx, item in enumerate(array):
            cursor.execute(f"INSERT INTO {key} (id, data) VALUES (?, ?)", (idx, item.tobytes()))
    
    conn.commit()
    conn.close()
    print(f'Data has been successfully written to {db_file_path}')
    return None

# INCOMPLETE
def show_group_content(path: str) -> None:
    structure = {} 
    with h5py.File(path, 'r') as h5_file:
        print(f"Contents of {path}:")
        if isinstance(h5_file, h5py.Group):
            structure[os.path.basename(path)] = list(h5_file.keys())
            for key, item in h5_file.items():
                if isinstance(item, h5py.Group):
                    print(key, " ->  ", sep="", end="")
                elif isinstance(item, h5py.Dataset):
                    print(key, ": ", sep="")
                    structure[os.path.basename(path)][key] = {'len':len(item), 'shape':item.shape, 'data':item[:]}
                    # print("\t", item.name, ", with shape: ", item.shape, "and columns: ", item.dtype.names)
        elif isinstance(h5_file, h5py.Dataset):
            print("\t", h5_file.name, ", with shape: ", h5_file.shape, "and columns: ", h5_file.dtype.names)
    return structure

# INCOMPLETE
def read_h5_file(h5_file_path: str) -> None:
    with h5py.File(h5_file_path, 'r') as h5_file:
        print(f"Contents of {h5_file_path}:")
        for column in h5_file.keys():
            if isinstance(h5_file[column], h5py.Group):
                print(f"Group: {column}")
                for subkey in h5_file[column].keys():
                    data = h5_file[column][subkey][:]
                    print(f"\nDataset_subkey: {subkey}")
                    print(data)
            else:
                data = h5_file[column][:]
                print(f"\nDataset: {column}")
                print(data)

# INCOMPLETE
def root_to_h5(root_files_path: str, cols_to_find: List[str], h5_file: str) -> np.ndarray:   

    # Open the ROOT file
    root_file = uproot.open(root_files_path)
 

    for tree_name in root_file.keys():
        print(f"Processing tree: {tree_name}")
        tree = root_file[tree_name]
        
        # Get all branches
        branches = tree.keys()
        
        # Create a group for each tree
        tree_group = h5_file.create_group(tree_name)
        
        # Read all data from the tree
        data = tree.arrays()
        
        # Iterate through branches
        for branch in branches:
            branch_data = data[branch]
            
            # Convert to numpy array if possible
            try:
                np_data = ak.to_numpy(branch_data)
                # If successful, store as a dataset
                tree_group.create_dataset(branch, data=np_data)
            except ValueError:
                # If conversion to numpy is not possible, store as awkward array
                tree_group.create_dataset(branch, data=ak.to_buffers(branch_data))
                
            print(f"Processed branch: {branch}")
    h5_files_path = 'XXX'
    print(f"Conversion completed. HDF5 file created: {h5_files_path}")


# INCOMPLETE
def create_dataframe_from_hdf5_scenario_2(h5_file_path: str) -> None:
    df_rows = []
    with h5py.File(h5_file_path, 'r') as h5_file:
        print(f"Creating a dataframe for: {h5_file_path}")
        for column in h5_file.keys():
            if isinstance(h5_file[column], h5py.Group):
                print(f"Column: {column} is a group and contains {len(h5_file[column])} datasets:") 
                for subkey in h5_file[column].keys():
                    data = h5_file[column][subkey][:]
                    # row = {h5_file[column][subkey][:]}
                    # print(f"Dataset_subkey: {subkey}")
                    row = {f'{column}': data} ##BUG : IS ORDERING NOT PRESERVED ?
                    df_rows.append(row)
                    # print(data)


            else:
                print(f"Column: {column} is NOT a group and contains {len(h5_file[column])} datasets:")
                data = h5_file[column][:]
                print(f"Dataset: {column}")
                # print(data)
                df_rows.append(data)

    return df_rows


# RECHECK
def hdf5_hierarchy(h5_file_path: str) -> None:
    """
    Create a json-like representation of the HDF5 file structure.
    """
    with h5py.File(h5_file_path, 'r') as h5_file:
        print(f"Contents of {h5_file_path}:")
        for column in h5_file.keys():
            if isinstance(h5_file[column], h5py.Group):
                print(f"Group: {column}")
                for subkey in h5_file[column].keys():
                    data = h5_file[column][subkey][:]
                    print(f"\nDataset_subkey: {subkey}")
                    print(data)
            else:
                data = h5_file[column][:]
                print(f"\nDataset: {column}")
                print(data)
            print(data)


# Your existing function to scan for ROOT files
def scan_for_new_root_files(root_dir: str, h5_dir: str = None, sqlite_dir: str = None) -> List[str]:
    root_files = os.listdir(root_dir)
    h5_files = [x.split('.h5')[0] for x in os.listdir(h5_dir)]
    sqlite_files = [x.split('.sqlite3')[0] for x in os.listdir(sqlite_dir)]

    new_root_for_h5 = [x for x in root_files if x.endswith('.root') and x.split('.root')[0] not in h5_files]
    new_root_for_sqlite = [x for x in root_files if x.endswith('.root') and x.split('.root')[0] not in sqlite_files]

    new_root_files_h5, new_root_files_sqlite = [], []

    print("\nDo you want to convert to:")
    print("1. HDF5")
    print("2. SQLite")
    print("3. Both")
    conversion_choice = input("Enter your choice (1/2/3): ").strip()

    if conversion_choice == '1':  # Convert to HDF5
        if not new_root_for_h5:
            print('No new root files found for HDF5.')
        else:
            new_root_files_h5 = user_file_selection(new_root_for_h5)

    elif conversion_choice == '2':  # Convert to SQLite
        if not new_root_for_sqlite:
            print('No new root files found for SQLite.')
        else:
            new_root_files_sqlite = user_file_selection(new_root_for_sqlite)

    elif conversion_choice == '3':  # Convert to both HDF5 and SQLite
        if not new_root_for_h5:
            print('No new root files found for HDF5.')
        else:
            new_root_files_h5 = user_file_selection(new_root_for_h5)

        if not new_root_for_sqlite:
            print('No new root files found for SQLite.')
        else:
            new_root_files_sqlite = user_file_selection(new_root_for_sqlite)

    else:
        print("Invalid option. No files will be converted.")

    new_root_files_h5 = [os.path.join(root_dir, x) for x in new_root_files_h5]
    new_root_files_sqlite = [os.path.join(root_dir, x) for x in new_root_files_sqlite]

    return new_root_files_h5, new_root_files_sqlite



def list_h5_files(h5_dir: str) -> List[str]:
    """
    Lists all HDF5 files in the given directory.

    Parameters:
     - h5_dir (str): Directory to search for HDF5 files.

    Returns:
     - List[str]: List of HDF5 file paths.
    """
    h5_files = [f for f in os.listdir(h5_dir) if f.endswith('.h5')]
    return h5_files


def root2h5(root_files: List[str], h5_dir:str) -> None:
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
        columns_to_find = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        h5_file_path = os.path.join(h5_dir, os.path.basename(file).replace('.root', '.h5'))
        array_data_dict = root_to_dict_of_arrays(file, columns_to_find)
        awkward_array = root_to_awkward_arrays(file, columns_to_find) #NOTE: awkward array is not used 
        save_to_h5(array_data_dict, awkward_array, h5_file_path) 
    return None


def get_tree_branches(root_file, columns_to_find) -> Dict[str, List[str]]: 
    """
    Extracts and returns the columns of interest from a ROOT file. It identifies the tree containing the branches(columns_to_find)
    and returns a dictionary mapping tree names to the list of these columns.

    Parameters:
     - root_file (str): The file path to the ROOT file.
     - cols_to_find (List[str]): A list of columns to extract from the ROOT file.

    Returns:
     - tree_dict (Dict): A dictionary where the keys are tree names and the values are lists of columns found in those trees.
    
    Notes:
    - A modification is used because the trees created in the PreAnalysis script are <ins>**larger**</ins> than the buffer size
    and therefore ROOT creates multiple namecycles (i.e. there are two trees created, named "ProcessedEvents;8" and 
    "ProcessedEvents;7"). We want to extract the branches from the more recent tree, i.e. the tree with the greater cycle number. 
    The following set is used to store the basenames of the trees we have already processed.
    """
    tree_basenames = set() 
    tree_dict = {}
    for tree_name in root_file.keys():
        tree_name = tree_name.split(';')[0]
        if tree_name in tree_basenames: 
            continue
        tree = root_file[tree_name]
        if (tree.classname != 'TTree'):
            continue
        if all(col in tree.keys() for col in columns_to_find): # check if all requested columns are in the tree
            tree_list = [col for col in columns_to_find if col in tree.keys()] # get a list of the columns that are in the tree: eg. phaseIITriggerTree
            tree_dict[tree_name] = tree_list
            print(f"Found columns: {tree_list} in tree: {tree_name} for file: {os.path.basename(root_file.file_path)}")
        else:
            print(f"Columns not found in tree: {tree_name}")
         # add tree basename to the set, after we have extracted the branches
        tree_basenames.add(tree_name)
    return tree_dict


def root_to_dict_of_arrays(root_files_path: str, columns_to_find: List[str]) -> Dict[str, np.ndarray]:
    """
    Extract data from a ROOT file and return them as a dictionary of NumPy ndarrays.

    Parameters:
     - root_files_path (str): The path to the ROOT file from which data is to be extracted.
     - columns_to_find (List[str]): A list of column names to be extracted from the ROOT file.

    Returns:
     - dict_of_arrays (Dict[str, np.ndarray]): A dictionary where keys are the column names and values are NumPy arrays containing data.

    Examples:
    >>> root_files_path = "data/annie_data.root"
    >>> columns_to_find = ["eventNumber", "digitX", "digitY", "digitT"]
    >>> dict_of_arrays = root_to_dict_of_arrays(root_files_path, columns_to_find)
    >>> print(dict_of_arrays)
    {'eventNumber': array([    44,     72,     84, ..., 390861, 391175, 391191], dtype=int32),
    'digitX': array([array([ -15.23916721,  -47.80203247,  -67.9304657 ,  -68.50760651,
                15.04926872, -112.59906006,  -98.98416901,  -98.98416901,
               -93.81991577,  -59.74597549,  -58.7647934 ,  -27.30049896,
               -59.25538254,  -27.30049896,   27.20409203, -105.66539001,
              -105.66539001,  -93.81991577,  -27.06107521,  -54.94448471,
              ..... ..... ...... ),
    'digitY': array([array([149.68470648, 150.18379095, 150.25854376, 149.82546881,
              149.82546881,  60.22801474,  60.22801474, -26.48331949,
               15.47083214, -71.6477215 , -26.75432321,  60.20386007,
               60.20386007, -26.4035618 ,  60.2419956 , 100.74185827,
               15.47083214, 100.74185827,  15.43998197, 100.59675482,
              100.71101263,  15.48869291,  15.43998197]),
              ....... ....... .......),
    'digitT': array([array([ 7.11189461,  7.33561611,  7.88873792, 15.27171946,  9.84215736,
               5.76198912,  5.88614655,  6.79743958,  5.8970437 ,  9.83736324,
               7.01077414,  6.06781387,  5.08803034,  7.9968996 ,  7.1203146 ,
               6.65937638,  6.39350533,  6.07382917,  6.04535913,  5.72223711,
               6.41045713,  6.88058758,  5.54067039]), dtype=object)}
    """
    root_file = uproot.open(root_files_path)
    tree_and_branches = get_tree_branches(root_file, columns_to_find)
    dict_of_arrays = {}
    for tree_name, cols in tree_and_branches.items():
        # root_file[tree_name].arrays(cols, library='np') #NOTE: another way to extract data not used here
        dict_of_arrays.update(root_file[tree_name].arrays(cols, library="np")) #IMPORTANT
    return dict_of_arrays


def root_to_awkward_arrays(root_files_path: str, columns_to_find: List[str]) -> np.ndarray:
    """
    Extract data from a ROOT file and return it as an Awkward Array.

    Parameters:
     - root_files_path (str): The path to the ROOT file from which data is to be extracted.
     - columns_to_find (List[str]): A list of column names to be extracted from the ROOT file.

    Returns:
     - awkward_data (ak.Array): An Awkward Array containing the extracted data.

    Examples:
    >>> root_files_path = "data/annie_data.root"
    >>> columns_to_find = ["eventNumber", "digitX", "digitY", "digitZ", "digitT"]
    >>> awkward_data = root_to_awkward_arrays(root_files_path, columns_to_find)
    >>> print(awkward_data)    
        <Array [{eventNumber: 44, ...}, ..., {...}] type='2108 * {eventNumber: int3...'>
    >>> print(awkward_data['eventNumber'] )
        <Array [0, 1, 2, ...] type='2108 * int32'>
    >>> print(awkward_data['digitX'] )
        <Array [[-15.2, -47.8, ..., 26.9, -54.9], ...] type='2108 * var * float64'>
    """
    root_file = uproot.open(root_files_path)
    tree_and_branches = get_tree_branches(root_file, columns_to_find)
    for tree_name, cols in tree_and_branches.items():
        awkward_data = root_file[tree_name].arrays(cols) # returns <class 'awkward.highlevel.Array'>
    return awkward_data


def save_to_h5(arrays: Dict[str, np.ndarray], awkward_arrays, h5_file_path: str) -> None:
    '''
    The code converts an array of byte strings into a list of NumPy arrays of float64.
    It handles variable-length sequences by using h5py.special_dtype(vlen=np.dtype('float64')).

    * A special dtype dt is created for variable-length sequences of float64 using h5py.special_dtype.
    * The list_of_arrays is converted to a NumPy array with the special dtype dt.
    '''
    with h5py.File(h5_file_path, 'w') as h5_file: #os.path.basename(h5_file_path).split('.h5')[0]
        group_name = os.path.basename(h5_file_path).split('.h5')[0]
        group = h5_file.create_group(group_name)
        for key, array in arrays.items():
            if array.dtype == 'O':
                try:
                    array = np.array([np.array(item, dtype=np.float64) for item in array])
                except ValueError:
                    list_of_arrays = []
                    print(f'Can not convert {key} to float64. Converting to byte strings and storing as variable-length sequence')
                    np.set_printoptions(threshold=np.inf) #NOTE: possible solution error for larger datasets
                    array_with_byte_data = np.array([str(item.astype(float)) for item in array], dtype='S') #NOTE: item.astype(float) useful when item is boolean
                    for item in array_with_byte_data:
                        if len(str(item)) > 1:                            
                            list_of_arrays.append(preprocessing(item)) #COMMENT: convert byte string data to list of arrays
                    dt = h5py.special_dtype(vlen=np.dtype('float64'))# store the list of arrays as a variable-length sequence <!>
                    array = np.array(list_of_arrays, dtype=dt)
            else:
                # array = array.astype(np.float64) 
                array = array.astype(array.dtype) #COMMENT: convert array to its original dtype
            group.create_dataset(key, data=array)
    print(f'Data has been successfully written to {h5_file_path}')
    print(5*'-----------------------------------')
    return None


def preprocessing(item: str) -> np.ndarray:
    """
    Preprocesses the data in the byte string format and converts it to a NumPy array of float64.

    Parameters:
        - item (str): A byte string containing the data.

    Returns:
        - np.array(float_data, dtype=np.float64): A NumPy array containing the data.

    """
    string_data = item.decode('utf-8').strip('[]').replace('\n', '').strip()
    string_data = re.sub(r'\s+', ' ', string_data).split(' ')
    float_data = [float(x) for x in string_data]
    return np.array(float_data, dtype=np.float64)


def h5_to_json(h5_file_path: str) -> str:
    """
    Convert the structure of an H5 file to a JSON-like format.

    Parameters:
        - h5_file_path (str): The path to the H5 file.

    Returns:
        - str: A JSON-like string representing the structure of the H5 file.
          The structure includes groups and datasets with their respective
          types, shapes, and data types.

    Example:
    >>> h5_file_path = 'path_to_your_file.h5'
    >>> print(h5_to_json(h5_file_path))
    {
        "/": {
            "type": "Group",
            "children": {
                "/group1": {
                    "type": "Group",
                    "children": {
                        "/group1/dataset1": {
                            "type": "Dataset",
                            "shape": [10, 10],
                            "dtype": "float64"
                        }
                    }
                }
            }
        }
    }
    """
    def visit(name, node):
        if isinstance(node, h5py.Group):
            structure[name] = {"type": "Group", "children": {}}
            for key, item in node.items():
                visit(f"{name}/{key}", item)
        elif isinstance(node, h5py.Dataset):
            structure[name] = {"type": "Dataset", "shape": node.shape, "dtype": str(node.dtype)}

    structure = {}
    with h5py.File(h5_file_path, 'r') as h5_file:
        visit('/', h5_file['/'])

    return json.dumps(structure, indent=4)


def create_dataframe_and_show_structure(h5_file_path: str) -> pd.DataFrame:
    """
    Create a pandas DataFrame from an HDF5 file and show the structure of the file.
    This function reads an HDF5 file and extracts datasets from it to create a pandas DataFrame.
    It prints the structure of the HDF5 file, including groups and datasets, along with their types and lengths.
    
    Parameters:
        - h5_file_path (str): The path to the HDF5 file.
    
    Returns:
        - pd.DataFrame: A DataFrame containing the data from the HDF5 file.
    """
    print(5*'-----------------------------------')
    total_data = {}

    # Open the H5 file
    with h5py.File(h5_file_path, 'r') as h5_file:
        keys = list(h5_file.keys())
        print("Keys in the H5 file:", keys)
        for key in keys:
            if isinstance(h5_file[key], h5py.Group):
                print(f"Key: {key} is a group containing:")
                for subkey in h5_file[key].keys():
                    item = h5_file[key][subkey]
                    if isinstance(item, h5py.Dataset):
                        print(f"  |-> Dataset: {subkey} has length: {len(item[:])} | type: {type(item[:])} | dtype: {item.dtype}")
                    else:
                        print(f"  Subkey: {subkey} is a group")
                        for subsubkey in item.keys():
                            subsubitem = item[subsubkey]
                            if isinstance(subsubitem, h5py.Dataset):
                                print(f"    Subsubkey: {subsubkey}, with values of length: {len(subsubitem[:])} | type: {type(subsubitem[:])} | dtype: {subsubitem.dtype}")
                            else:
                                print(f"    Subsubkey: {subsubkey} is of unknown type")
            elif isinstance(h5_file[key], h5py.Dataset):
                item = h5_file[key]
                print(f"Key: {key}, with values of length: {len(item[:])} and type: {item[:]}")
            else:
                print(f"Key: {key} is of unknown type")

        for key in keys:
            for dataset in h5_file[key].keys():
                data = h5_file[key][dataset][:]
                total_data[dataset] = data
            # Print the data
            # print(f"Data in the '{key}' dataset:", data)
    print(f'\nData for {h5_file_path}:')
    return pd.DataFrame(total_data)


def traverse_hdf5(h5_file_path: str) -> Dict:
    """
    Traverse all groups and datasets in an HDF5 file and retrieve their contents.

    Parameters:
        - h5_file_path (str): The path to the HDF5 file.

    Returns:
        - dict: A dictionary containing information about all groups and datasets in the file.
    """
    def collect_info(name, node):
        if isinstance(node, h5py.Group):
            info[name] = {
                "type": "Group",
                "children": list(node.keys()),
                "num_children": len(node.keys())
            }
        elif isinstance(node, h5py.Dataset):
            dataset_info = {
                "type": "Dataset",
                "shape": node.shape,
                "dtype": str(node.dtype),
                "data": node[:].tolist(),  # Convert data to list for easier JSON serialization
                "attrs": {key: val.tolist() if isinstance(val, (np.ndarray, h5py.Dataset)) else val for key, val in node.attrs.items()}
            }
            # Add compression info if available
            if node.compression:
                dataset_info["compression"] = node.compression
                dataset_info["compression_opts"] = node.compression_opts
            info[name] = dataset_info

    info = {}
    with h5py.File(h5_file_path, 'r') as h5_file:
        h5_file.visititems(collect_info)

    return info

# Function to select files as implemented before
def user_file_selection(files_list):
    """
    Displays the available files and lets the user choose specific ones.
    
    Parameters:
    - files_list (list): A list of available files.

    Returns:
    - selected_files (list): A list of selected files based on user input.
    """
    print("\nThe following new ROOT files were found:")
    for idx, file in enumerate(files_list):
        print(f"{idx + 1}. {file}")

    choice = input("\nDo you want to convert all files? (y/n): ").strip().lower()
    if choice == 'y':
        return files_list
    else:
        selected_files = []
        print("\nEnter the numbers of the files you want to convert, separated by commas:")
        selection = input().split(",")
        for idx in selection:
            try:
                selected_files.append(files_list[int(idx) - 1])
            except (ValueError, IndexError):
                print(f"Invalid input: {idx}. Skipping this file.")
        return selected_files
    
# Function to convert branches to SQLite
def convert_branches_to_sqlite(root_file_path, tree_name, branch_names, sqlite_db_path):
    with uproot.open(root_file_path) as file:
        tree = file[tree_name]

        conn = sqlite3.connect(sqlite_db_path)
        cursor = conn.cursor()

        base_name = os.path.splitext(os.path.basename(root_file_path))[0]
        sanitized_table_name = f'"{base_name.replace(".", "_")}"'

        columns = ", ".join([f'"{branch_name}" REAL' for branch_name in branch_names])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {sanitized_table_name} ({columns})")

        data = []
        for branch_name in branch_names:
            if branch_name not in tree.keys():
                print(f"Branch '{branch_name}' not found in the tree '{tree_name}'")
                continue

            branch_data = tree[branch_name].array(library="np")

            if branch_data.dtype.type is np.bytes_:
                branch_data = np.char.decode(branch_data, 'utf-8')
            elif branch_data.dtype == np.dtype('O'):
                try:
                    branch_data = branch_data.astype(np.float64)
                except ValueError:
                    branch_data = np.array([str(item) for item in branch_data], dtype='S')

            data.append(branch_data)

        data = np.column_stack(data)

        placeholders = ", ".join(["?"] * len(branch_names))
        cursor.executemany(f"INSERT INTO {sanitized_table_name} VALUES ({placeholders})", data)

        conn.commit()
        conn.close()
        print(f"Data from '{root_file_path}' has been successfully written to {sqlite_db_path}")


def read_sqlite_table(sqlite_db_path, table_name):
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    query = f'SELECT * FROM "{table_name}"'
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    
    print(f"Table: {table_name}")
    print("-" * (len(column_names) * 15))
    print(" | ".join(column_names))
    print("-" * (len(column_names) * 15))
    
    for row in rows:
        print(" | ".join(map(str, row)))
    
    print("\n")
    conn.close()
    
def get_table_names(sqlite_db_path):
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    return [table[0] for table in tables]


# Function to list SQLite files
def list_sqlite_files(sqlite_dir):
    return [file for file in os.listdir(sqlite_dir) if file.endswith(".sqlite3")]

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

# Main function to include SQLite and HDF5 conversions
def main():
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    root_dir = os.path.join(parent_directory, "data", "root")
    h5_dir = os.path.join(parent_directory, "data", "h5")
    sqlite_dir = os.path.join(parent_directory, "data", "sqlite")

    columns_to_find = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']

    os.makedirs(h5_dir, exist_ok=True)
    os.makedirs(sqlite_dir, exist_ok=True)
    os.makedirs(root_dir, exist_ok=True)

    while True:
        print("\nPlease choose an option:")
        print("1. Read an HDF5 or SQLite file")
        print("2. Convert ROOT files to HDF5 or SQLite")
        print("3. Exit")
        choice = input("Enter your choice (1/2/3): ")

        if choice == '1':
            print("Select the file type you want to read:")
            print("1. HDF5")
            print("2. SQLite")
            choice = input("Enter 1 or 2: ")
            if choice == '1':
                h5_files = list_h5_files(h5_dir)
                if not h5_files:
                    print(f"No HDF5 files found in {h5_dir}.")
                else:
                    print("Available HDF5 files:")
                    for idx, h5_file in enumerate(h5_files):
                        print(f"{idx + 1}. {h5_file}")
                    file_choice = int(input(f"Select a file to read (1-{len(h5_files)}): "))
                    selected_h5_file = os.path.join(h5_dir, h5_files[file_choice - 1])
                    df = create_dataframe_and_show_structure(selected_h5_file)
                    print(df)
            elif choice == '2':
                sqlite_files = list_sqlite_files(sqlite_dir)
                if not sqlite_files:
                    print(f"No SQLite files found in {sqlite_dir}.")
                else:
                    print("Available SQLite files:")
                    for idx, sqlite_file in enumerate(sqlite_files):
                        print(f"{idx + 1}. {sqlite_file}")
                    file_choice = int(input(f"Select a file to read (1-{len(sqlite_files)}): "))
                    selected_sqlite_file = os.path.join(sqlite_dir, sqlite_files[file_choice - 1])
                    
                    # Get the table names from the selected SQLite file
                    table_names = get_table_names(selected_sqlite_file)
                    if not table_names:
                        print(f"No tables found in {selected_sqlite_file}.")
                    else:
                        print("Available tables:")
                        for idx, table_name in enumerate(table_names):
                            print(f"{idx + 1}. {table_name}")
                        table_choice = int(input(f"Select a table to read (1-{len(table_names)}): "))
                        selected_table = table_names[table_choice - 1]
                        df = sqlite_to_dataframe(selected_sqlite_file, selected_table)
                        # Read and display the selected table
                        print(df)
            else:
                print("Invalid choice! Please enter either 1 or 2.")
                

        elif choice == '2':
            print(f"Scanning {root_dir} for new ROOT files to convert...")
            new_root_files_h5, new_root_files_sqlite = scan_for_new_root_files(root_dir, h5_dir, sqlite_dir)

            if new_root_files_h5:
                print(f"Converting {len(new_root_files_h5)} ROOT files to HDF5 format...")
                root2h5(new_root_files_h5, h5_dir)
                print(f"Conversion completed. HDF5 files saved to {h5_dir}")

            if new_root_files_sqlite:
                print(f"Converting {len(new_root_files_sqlite)} ROOT files to SQLite format...")
                for root_file in new_root_files_sqlite:
                    convert_branches_to_sqlite(root_file, "phaseIITriggerTree;1", columns_to_find, os.path.join(sqlite_dir, os.path.basename(root_file).replace('.root', '.sqlite3')))
                print(f"Conversion completed. SQLite files saved to {sqlite_dir}")

        elif choice == '3':
            print("Exiting...")
            break

        else:
            print("Invalid option, please choose again.")

if __name__ == "__main__":
    try:
        main()
    except Exception as error_main:
        print('[main error]:', error_main)