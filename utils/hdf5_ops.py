# Purpose: Manages operations related to HDF5 files, including saving data to HDF5 format.
# Key Functions: save_to_h5


import h5py
import numpy as np
import os
from typing import Dict
from .data_ops import byte_preprocessing

def save_to_h5(arrays: Dict[str, np.ndarray], awkward_arrays, h5_file_path: str) -> None:
    '''
    The code converts an array of byte strings into a list of NumPy arrays of float64.
    It handles variable-length sequences by using h5py.special_dtype(vlen=np.dtype('float64')).

    * A special dtype dt is created for variable-length sequences of float64 using h5py.special_dtype.
    * The list_of_arrays is converted to a NumPy array with the special dtype dt.
    '''
    with h5py.File(h5_file_path, 'w') as h5_file:
        group_name = os.path.basename(h5_file_path).split('.h5')[0]
        group = h5_file.create_group(group_name)
        for key, array in arrays.items():
            # If the array is of object dtype (contains mixed data types)
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
                            list_of_arrays.append(byte_preprocessing(item)) #COMMENT: convert byte string data to list of arrays
                    # Create a special dtype for variable-length float sequences
                    dt = h5py.special_dtype(vlen=np.dtype('float64'))
                    array = np.array(list_of_arrays, dtype=dt)
            else:
                # If array is already a numeric type, ensure it is saved in the original dtype
                array = array.astype(array.dtype) #COMMENT: convert array to its original dtype
            # Save the dataset in the HDF5 group
            group.create_dataset(key, data=array)
    
    print(f'Data has been successfully written to {h5_file_path}')
    print(5 * '-----------------------------------')
    return None

