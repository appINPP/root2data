import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from typing import Dict
import numpy as np
# from .data_ops import root_to_dict_of_arrays

def save_to_parquet(arrays: Dict[str, np.ndarray], parquet_path: str) -> None:
    """
    Saves dictionary of arrays to a Parquet file with optimized storage and compression.
    
    Parameters:
        - arrays (Dict[str, np.ndarray]): Dictionary mapping column names to numpy arrays
        - parquet_path (str): Output path for the Parquet file
        
    Features:
        - Automatic schema detection
        - Snappy compression by default
        - Efficient handling of nested arrays
        - Column-based storage optimization
    """
    # Convert arrays to PyArrow arrays with proper schema
    pa_arrays = {}
    for key, array in arrays.items():
        if array.dtype == "O":
            # Handle nested arrays using PyArrow list type
            pa_arrays[key] = pa.array(array.tolist())
        else:
            pa_arrays[key] = pa.array(array)
    
    # Create table and write to Parquet
    table = pa.Table.from_pydict(pa_arrays)
    pq.write_table(
        table,
        parquet_path,
        compression='snappy',
        use_dictionary=True,
        write_statistics=True
    )
    
    print(f'Successfully wrote data to {parquet_path}')
    print(5 * '-----------------------------------')

def parquet_to_dataframe(parquet_path: str) -> pd.DataFrame:
    """
    Reads a Parquet file into a pandas DataFrame with optimized performance.
    
    Parameters:
        parquet_path (str): Path to the Parquet file
        
    Returns:
        pd.DataFrame: DataFrame containing the data
    """
    return pd.read_parquet(parquet_path)


# if __name__ == "__main__":
    # import os 
    # Example usage
    # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']
    # root_files = ['/home/nikosili/projects/annie_gnn/data/root/after_phase_0.0.root']
    # parquet_dir = '/home/nikosili/projects/annie_gnn/data/parquet'

    # for file_ in root_files:
    #     # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
    #     parquet_file_path = os.path.join(parquet_dir, os.path.basename(file_).replace('.root', '.parquet'))
    #     array_data_dict = root_to_dict_of_arrays(file_, features)
    #     # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
    #     save_to_parquet(array_data_dict, parquet_file_path)
    #     # save_to_h5(array_data_dict, sqlite_file_path) 

    # df = parquet_to_dataframe(parquet_path='data/parquet/after_phase_0.0.parquet')
    # print(df.head())
    # print(df.info())

    # print('\n+++++ Done +++++')