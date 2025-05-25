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
import pandas as pd
import os
from .file_ops import list_parquet_files
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
    
    print(f'Data has been successfully written to {parquet_path}')
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

def process_parquet_format(features: list) -> None:
    """
    """
    parquet_data_path = os.path.join(os.getcwd(), 'data', 'parquet')
    output_dir = os.path.join(os.getcwd(), 'data', 'processed_parquet')
    parquet_files = list_parquet_files(parquet_data_path)
    # files = [f for f in all_items if f.endswith('.parquet')]
    dirs = [d for d in parquet_files if os.path.isdir(os.path.join(f'parquet/{d}'))]

    def expand_parquet_file(df): 
        expanded = []

        for evt_id, row in df.iterrows(): 

            length =  len(row['digitX']) #len(row['hit_pos_x'])

            # for i in range(length): 

            #     expanded.append(
            #             {
            #                 'evt_id': evt_id,
            #                 'hit_pos_x': row['hit_pos_x'][i],
            #                 'hit_pos_y': row['hit_pos_y'][i],
            #                 'hit_pos_z': row['hit_pos_z'][i],
            #                 'hit_dir_x': row['hit_dir_x'][i],
            #                 'hit_dir_y': row['hit_dir_y'][i],
            #                 'hit_dir_z': row['hit_dir_z'][i],
            #                 'hit_time': row['hit_time'][i],
            #                 'hit_tot': row['hit_tot'][i],
            #                 #'first_hit_tot_per_pmt': row['first_hit_tot_per_pmt'][i],
            #                 'is_triggered': row['is_triggered'][i],
            #                 'is_cherenkov': row['is_cherenkov'][i],
            #             }
            #     )

            # Determine the length of the nested arrays by checking the first column
            # first_col = df.columns[0]
            # length = len(row[first_col])

            for i in range(length):
                entry = {'evt_id': evt_id}
                for col in df.columns:
                    try:
                        entry[col] = row[col][i]
                    except Exception:
                        entry[col] = row[col]  # fallback if not a sequence
                expanded.append(entry)

        expanded_df = pd.DataFrame(expanded)
        return expanded_df

    for filename in parquet_files:
        input_file = os.path.join(parquet_data_path, filename)
        print(filename)

        if filename.split('.parquet')[0] in dirs: 
            continue

        # if not (os.path.isfile(f"parquet/{filename}")): 
        #     continue
        if not os.path.isfile(input_file):
            print(f"Input file not found: {input_file}")
            continue

        table_name = filename.split(".parquet")[0]
        print(table_name)
        base_dir = os.path.join(output_dir, table_name)
        # base_dir = os.path.join("parquet", table_name)
        print(base_dir)
        os.makedirs(base_dir, exist_ok = True)

        # features_dir = os.path.join(base_dir, "features")
        # os.makedirs(features_dir, exist_ok = True)
        # truth_dir = os.path.join(base_dir, "truth")
        # os.makedirs(truth_dir, exist_ok = True)
        for subdir in ['features', 'truth', 'weights']:
            os.makedirs(os.path.join(base_dir, subdir), exist_ok=True)

        # features = ['hit_pos_x', 'hit_pos_y', 'hit_pos_z', 'hit_dir_x', 'hit_dir_y',
        # 'hit_dir_z', 'hit_time', 'hit_tot', 'first_hit_tot_per_pmt',
        # 'is_triggered', 'is_cherenkov']

        truth = ['logE_visible', 'logE_nu', 'logE_mu',
        'pseudo_runid', 'pseudo_livetime', 'evt_num',
        'num_triggered_pmts']

        # df = pd.read_parquet(f'parquet/{filename}')
        df = pd.read_parquet(input_file)

        features_df = df[features].set_index(df['eventNumber']) #FIXME: this is a hack to get the index to work because Spyros used "evt_id" 
        expanded_df = expand_parquet_file(features_df)
        print(expanded_df.keys(), expanded_df.index)

        final_df = expanded_df.set_index('evt_id')
        print(final_df.keys(), final_df.index)

        truth_df = df[truth].set_index(df['evt_id']) #FIXME: I do not understand what should be decided as truth

        feat_file = os.path.join(features_dir, "features_0.parquet")
        truth_file = os.path.join(truth_dir, "truth_0.parquet")

        final_df.to_parquet(feat_file)
        truth_df.to_parquet(truth_file)
        pass