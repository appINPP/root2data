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


def scan_for_new_root_files(root_dir: str, h5_dir:str=None, sqlite_dir:str=None) -> List[str]:

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

    # Go one directory up and then search for data/root
    new_root_files_h5 = [os.path.join(os.path.dirname(os.getcwd()), 'data', 'root', x) for x in new_root_for_h5]
    new_root_files_sqlite = [os.path.join(os.path.dirname(os.getcwd()), 'data', 'root', x) for x in new_root_for_sqlite]

    return new_root_files_h5, new_root_files_sqlite


def list_h5_files(h5_dir: str) -> List[str]:

    h5_files = [f for f in os.listdir(h5_dir) if f.endswith('.h5')]
    return h5_files


def root2h5(root_files: List[str], h5_dir:str) -> None:

    for file in root_files:
        columns_to_find = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        h5_file_path = os.path.join(h5_dir, os.path.basename(file).replace('.root', '.h5'))
        array_data_dict = root_to_dict_of_arrays(file, columns_to_find)
        awkward_array = root_to_awkward_arrays(file, columns_to_find) #NOTE: awkward array is not used 
        save_to_h5(array_data_dict, awkward_array, h5_file_path) 
    return None


def get_tree_branches(root_file, columns_to_find) -> Dict[str, List[str]]: 

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
    root_file = uproot.open(root_files_path)
    tree_and_branches = get_tree_branches(root_file, columns_to_find)
    dict_of_arrays = {}
    for tree_name, cols in tree_and_branches.items():
        # root_file[tree_name].arrays(cols, library='np') #NOTE: another way to extract data not used here
        dict_of_arrays.update(root_file[tree_name].arrays(cols, library="np")) #IMPORTANT
    return dict_of_arrays


def root_to_awkward_arrays(root_files_path: str, columns_to_find: List[str]) -> np.ndarray:

    root_file = uproot.open(root_files_path)
    tree_and_branches = get_tree_branches(root_file, columns_to_find)
    for tree_name, cols in tree_and_branches.items():
        awkward_data = root_file[tree_name].arrays(cols) # returns <class 'awkward.highlevel.Array'>
    return awkward_data


def save_to_h5(arrays: Dict[str, np.ndarray], awkward_arrays, h5_file_path: str) -> None:


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
    current_directory = os.getcwd()

    # Construct the paths
    
    parent_directory = os.path.dirname(current_directory)
    root_dir = os.path.join(parent_directory, "data", "root")  # Directory containing ROOT files
    h5_dir = os.path.join(parent_directory, "data", "h5") 

    # sqlite_dir = f"{os.getcwd()}/data/sqlite/"
    columns_to_find =['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']

    if not os.path.exists(h5_dir):
        os.makedirs(h5_dir)
        
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)

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

    try:
        main()
        # root_files_path = '/home/nikosili/projects/annie_gnn/data/root/after_phase_0.0.root'
        # root_file = uproot.open(root_files_path)
        # get_tree_branches(root_file,['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy'])
    except Exception as error_main:
        print('[main error]: ',error_main)