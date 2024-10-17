# Purpose: Handles the processing & extraction of data from ROOT files, converting them into dictionaries of arrays or awkward arrays.
# Key Functions: byte_preprocessing,get_tree_branches, root_to_dict_of_arrays, root_to_awkward_arrays.

from typing import List, Dict
import uproot
import h5py
import os, re
import numpy as np
import pandas as pd

def byte_preprocessing(item: str) -> np.ndarray:
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