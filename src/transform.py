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
    """
    Convert a ROOT file to an HDF5 file.

    * Parameters:
        root_file_path : str
        h5_file_path : str

    * Returns:
        None
    """
    # Open the ROOT file
    root_file = uproot.open(root_files_path)
    # tree_dict = get_tree_branches(root_file, columns_to_find) # get a dictionary with the tree and columns(branches) to extract
    # for tree_name, cols in tree_dict.items():
    #     arrays = root_file[tree_name].arrays(cols, library="np")
    # return arrays

    # for tree_name in tree_dict.keys():
    #     root_file[tree_name].arrays(library="np")['eventNumber'] # method 1
    #     root_file[tree_name].arrays(columns_to_find)['eventNumber']# method 2
        # print(f"\nProcessing tree: {tree_name}")


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
    # case 2
    # with h5py.File(h5_file_path, 'r') as h5_file:
    #     print(f"Contents of {h5_file_path}:")
    #     for column in h5_file.keys():
    #         if isinstance(h5_file[column], h5py.Group):
    #             print(f"Group: {column}")
    #             for subkey in h5_file[column].keys():
    #                 data = h5_file[column][subkey][:]
    #                 print(f"\nDataset_subkey: {subkey}")
    #                 print(data)
    #         else:
    #             data = h5_file[column][:]
    #             print(f"\nDataset: {column}")
    #             print(data)


def scan_for_new_root_files(root_dir: str, h5_dir:str=None, sqlite_dir:str=None) -> List[str]:
    """
    Scans the root directory for new files that have not been converted to h5 AND sqlite files.

    Parameters:
     - root_dir (str): The directory containing the ROOT files.
     - h5_dir (str): The directory containing the HDF5 files.
     - sqlite_dir (str): The directory containing the SQLite files.
    
    Returns:
     - new_root_files (list): A list of file paths to the new ROOT files.
    
    """
    root_files = os.listdir(root_dir)
    h5_files = [x.split('.h5')[0] for x in os.listdir(h5_dir)]
    sqlite_files = [x.split('.sqlite3')[0] for x in os.listdir(sqlite_dir)]
    # h5
    new_root_for_h5 = [x for x in root_files if x.endswith('.root') and x.split('.root')[0] not in h5_files]
    if not new_root_for_h5:
        print('No new root files found for h5')
    else:
        print(f'New root files: {new_root_for_h5} for h5')
    # sqlite
    new_root_for_sqlite = [x for x in root_files if x.endswith('.root') and x.split('.root')[0] not in sqlite_files]
    if not new_root_for_sqlite:
        print('No new root files found for sqlite')
    else:
        print(f'New root files: {new_root_for_sqlite} for sqlite')

    new_root_files_h5 = [os.path.join(os.getcwd(), 'data', 'root', x) for x in new_root_for_h5]
    new_root_files_sqlite = [os.path.join(os.getcwd(), 'data', 'root', x) for x in new_root_for_sqlite]

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
        if all(col in tree.keys() for col in columns_to_find): # check if all requested columns are in the tree
            tree_list = [col for col in columns_to_find if col in tree.keys()] # get a list of the columns that are in the tree: eg. phaseIITriggerTree
            tree_dict[tree_name] = tree_list
            print(f"Found columns: {tree_list} in tree: {tree_name} for file: {os.path.basename(root_file.file_path)}")
        else:
            print(f"Columns not found in tree: {tree_name}")
        #Add tree basename to the set, after we have extracted the branches
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
##########################################################################################
#      SCENARIO 1 : padding arrays with different lengths to the same length
##########################################################################################
    """
    * Handling digitX: Since digitX contains arrays of different lengths, we pad each array to the length of the longest array using np.pad. This ensures that all arrays have the same length and can be stored in a 2D dataset in the HDF5 file.
    * Padded Data: The padded arrays are stored in the HDF5 file, with np.nan used to fill the padding.

    When storing data from a ROOT file into an HDF5 file, padding is necessary for columns like digitX and digitY that contain arrays of arrays (i.e., nested arrays) with varying lengths. Here's why padding is important:
    Reasons for Padding:
    1.Uniform Shape Requirement:
    DF5 datasets require that all elements have the same shape. If digitX and digitY contain arrays of different lengths, they cannot be directly stored in a single HDF5 dataset without making their lengths uniform.
    2.Data Integrity:
    Padding ensures that the structure of the data is preserved when converting from ROOT to HDF5. Without padding, you might lose information about the original structure of the nested arrays.
    3.Ease of Access:
    By padding the arrays to the same length, you can easily access and manipulate the data in a consistent manner. This uniformity simplifies downstream data processing and analysis.

    <!> Check for Nested Arrays: The code checks if the column's data type is object ('O'), which indicates nested arrays.
    """
    # with h5py.File(h5_file_path, 'w') as hdf5_file:
    #         # Step 4: Store the fetched data
    #         for key_, values_ in arrays.items():
    #             if arrays[key_].dtype == 'O':
    #                 # Handle the array of arrays case
    #                 max_len = max(len(arr) for arr in values_)
    #                 padded_data = np.array([np.pad(arr, (0, max_len - len(arr)), 'constant', constant_values=np.nan) for arr in values_])
    #                 hdf5_file.create_dataset(key_, data=padded_data)
    #             else:
    #                 hdf5_file.create_dataset(key_, data=values_)

##########################################################################################
#      SCENARIO 2 : store each subarray as a separate dataset within the group
##########################################################################################
    # if arrays is an instance of ak.Array
    # if isinstance(awkward_arrays, ak.Array): #isinstance(ak.type(awkward_arrays), ak.types.ListType)
    #     with h5py.File(h5_file_path, 'w') as h5_file: 
    #         # Example: awkward_arrays['digitX']
    #         # ArrayType(ListType(NumpyType('float64')), 2108, None)
    #         # <Array [[-15.2, -47.8, ..., 26.9, -54.9], ...] type='2108 * var * float64'>
    #         for field in awkward_arrays.fields:
    #             array_type = ak.type(awkward_arrays[field])
    #             if isinstance(array_type, ak.types.ArrayType) and \
    #             isinstance(array_type.content, ak.types.ListType) and \
    #             isinstance(array_type.content.content, ak.types.NumpyType):  
    #         #    ak.type(awkward_arrays[field]).type.type.dtype == 'float64' and \
    #         #    ak.type(awkward_arrays[field]).parameters == 2108:
    #             # if 
    #             # group for each field
    #                 group = h5_file.create_group(field)
    #                 # each subarray as a separate dataset within the group
    #                 for i, subarray in enumerate(awkward_arrays[field]):
    #                     group.create_dataset(str(i), data=ak.to_numpy(subarray))
    #             else:
    #                 h5_file.create_dataset(field, data=ak.to_numpy(arrays[field]))
    # return None

##########################################################################################
#      SCENARIO 3 - convert byte string data to list of arrays and store as variable-length sequence       
##########################################################################################
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
                        print(f"  |-> Dataset: {subkey} of type: {item.dtype}, has length: {len(item[:])} | type: {type(item[:])} | dtype: {item.dtype}")
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


def main():
    root_dir = os.getcwd() + "/data/root"  # Directory containing ROOT files
    h5_dir = os.getcwd() + "/data/h5"  # Directory to save HDF5 files
    # sqlite_dir = f"{os.getcwd()}/data/sqlite/"
    columns_to_find =['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']

    if not os.path.exists(h5_dir):
        os.makedirs(h5_dir)

    while True:
        print("Please choose an option:")
        print("1. Read an HDF5 file")
        print("2. Convert ROOT files to HDF5 and save them")
        print("3. Exit")
        choice = input("Enter your choice (1/2/3): ")

        if choice == '1':
            # List all available HDF5 files
            h5_files = list_h5_files(h5_dir)

            if not h5_files:
                print(f"No HDF5 files found in {h5_dir}.")
            else:
                print("Available HDF5 files:")
                for idx, h5_file in enumerate(h5_files):
                    print(f"{idx + 1}. {h5_file}")

                file_choice = int(input(f"Select a file to read (1-{len(h5_files)}): "))
                selected_h5_file = os.path.join(h5_dir, h5_files[file_choice - 1])

                # read_h5_file(selected_h5_file)
                df = create_dataframe_and_show_structure(selected_h5_file)
                print(df)
                print(5*'-----------------------------------')
        
        elif choice == '2':
            print(f"Scanning {root_dir} for new ROOT files to convert...")
            new_root_files_h5, new_root_files_sqlite = scan_for_new_root_files(root_dir, h5_dir, sqlite_dir=None)

            if not new_root_files_h5:
                print("No new ROOT files found for conversion.")
            else:
                print(f"Converting {len(new_root_files_h5)} ROOT files to HDF5 format...")
                root2h5(new_root_files_h5, h5_dir)
                print(f"Conversion completed. HDF5 files saved to {h5_dir}")

        elif choice == '3':
            print("Exiting...")
            break

        else:
            print("Invalid option, please choose again.")

if __name__ == "__main__":
    #TODO: these columns could be added as arguments when executing transform.py from the command line => python3 transform.py --columns eventNumber,digitX, digitY, digitZ, digitT
    # parser = argparse.ArgumentParser(description="Process ROOT files and save data to HDF5 and SQLite format.")
    # parser.add_argument('--columns_to_find', nargs='+', default=['column1', 'column2'], help='List of columns to find in the ROOT file.')
    # parser.add_argument('--root_dir', default='path_to_your_root_file.root', help='Path to the ROOT file.')
    # parser.add_argument('--h5_dir', default='path_to_your_hdf5_file.h5', help='Path to the HDF5 file.')
    # parser.add_argument('--sqlite_dir', default='path_to_your_sqlite_file.db', help='Path to the SQLite file.')
    # args = parser.parse_args()
    
    # columns_to_find = args.columns_to_find
    # root_dir = args.root_dir
    # h5_dir = args.h5_dir
    # sqlite_dir = args.sqlite_dir

    # try:
    # # STEP 1: convert ROOT to H5
    #     root_dir = f"{os.getcwd()}/data/root/"
    #     h5_dir = f"{os.getcwd()}/data/h5/"
    #     sqlite_dir = f"{os.getcwd()}/data/sqlite/"

    #     columns_to_find =['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT']

    #     files_path = scan_for_new_root_files(root_dir, h5_dir, sqlite_dir)
    #     # dhf5 conversion
    #     try:
    #         root2h5(files_path[0], h5_dir) #files_path[0] for h5
    #     except Exception as error_hdf5:
    #         print('[hdf5 error]: ',error_hdf5)

    # # STEP 2: create a dataframe from H5 file  
    #     h5_output_files = os.listdir(h5_dir)
    #     for h5_file in h5_output_files:
    #         if h5_file.endswith('.h5'):
    #             # file_contents = traverse_hdf5(h5_dir+h5_file)
    #             # print(file_contents)

    #             data = create_dataframe_and_show_structure(f"{h5_dir}/{h5_file}")
    #             print(pd.DataFrame(data))
    #             print(5*'-----------------------------------')

    # except Exception as error_main:
    #     print('[main error]: ',error_main)

    try:
        main()
        # root_files_path = '/home/nikosili/projects/annie_gnn/data/root/after_phase_0.0.root'
        # root_file = uproot.open(root_files_path)
        # get_tree_branches(root_file,['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy'])
    except Exception as error_main:
        print('[main error]: ',error_main)