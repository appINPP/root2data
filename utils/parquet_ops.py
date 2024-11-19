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

# Purpose: Operations for  Parquet format.

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

    pa_arrays = {}
    for key, array in arrays.items():
        if array.dtype == "O":
            # nested arrays using PyArrow list type
            pa_arrays[key] = pa.array(array.tolist())
        else:
            pa_arrays[key] = pa.array(array)
    
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
        - parquet_path (str): Path to the Parquet file
        
    Returns:
        pd.DataFrame: DataFrame containing the data
    """
    return pd.read_parquet(parquet_path)

