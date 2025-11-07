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

# Purpose: Provides functions to convert ROOT files to HDF5 and SQLite formats.


from typing import List
import os
import uproot
import numpy as np
import sqlite3
from .data_ops import root_to_dict_of_arrays, root_to_awkward_arrays
from .sqlite_ops import save_to_sqlite
from .hdf5_ops import save_to_h5
from .parquet_ops import save_to_parquet
# import time

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
    # start_time = time.time()
    for file_ in root_files:
        # columns_to_find = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        # features = ['pseudo_runid', 'pseudo_subRunid', 'pseudo_livetime', 'evt_id', 'evt_num', 'evt_overlays', 'reco_type', 'jbeta0', 'jbeta0_deg', 'D', 'D3d', 'cos_zen', 'fi', 'R', 'mean_tres_it', 'zenith', 'logEreco', 'Ereco', 'E_mu_max', 'E_mu_depos', 'E_bundle', 'E_visible', 'logE_visible', 'E_nu', 'multiplicity_muon', 'multiplicity_neutrino', 'logEreco2', 'logbeta0', 'jlik', 'GNhit', 'Snpe', 'SnpeT', 'Slen', 'Elen', 'best_trk_pos_x', 'best_trk_pos_y', 'best_trk_pos_z', 'best_trk_dir_x', 'best_trk_dir_y', 'best_trk_dir_z', 'Q1', 'Q1new', 'diffangle', 'delta_zenith', 'mu_pos_x', 'mu_pos_y', 'mu_pos_z', 'mu_dir_x', 'mu_dir_y', 'mu_dir_z', 'nu_pos_x', 'nu_pos_y', 'nu_pos_z', 'nu_dir_x', 'nu_dir_y', 'nu_dir_z', 'cos_zen_mu', 'zenith_mu', 'logE_mu', 'logE_nu', 'logEbundle', 'logEdepos', 'bjorken_y', 'cos_zen_nu', 'zenith_nu', 'w1', 'w2', 'w3atm', 'w3iceHESE', 'w3loi', 'w3random', 'w3ice', 'w3antares', 'w3new', 'neutrino_ngen', 'neutrino_type', 'neutrino_interaction', 'Ntrack_all', 'Nallbehind', 'Nallbehind2', 'Nallbehind3', 'NtrackIT10', 'NtrackIT30', 'NtrackIT', 'NtrackEarly', 'NtrackLate', 'TrLengthIT', 'TrLengthIT_2', 'TrLengthIT_3', 'ratio130', 'ratio430', 'ratio330', 'ratio110', 'ratio410', 'ratio310', 'ratio1', 'ratio2', 'ratio3', 'ratio4', 'ratio5', 'ratio6', 'ratiol', 'ratiol_trig', 'ratio4_trig', 'myratio50_muon', 'myratio30_muon', 'myratio50_cascmuon', 'myratio30_cascmuon', 'myratio50_casc', 'myratio30_casc', 'myratio50_cascmuon_over_mu', 'myratio30_cascmuon_over_mu', 'myratio50_casc_over_mu', 'myratio30_casc_over_mu', 'redToT_cascmuon_over_mu', 'redToT_casc_over_mu', 'ratio_closehits_cascmuon_over_mu', 'ratio_closehits_casc_over_mu', 'diff_theta', 'ratio_closehits_muon', 'ratio_closehits_cascmuon', 'ratio_closehits_casc', 'redToT_muon', 'redToT_cascmuon', 'redToT_casc', 'diff_dist_mu', 'diff_dist_casc_mu', 'diff_dist_casc', 'max_lik_down', 'max_lik_up', 'costheta_min', 'costheta_max', 'num_of_good_sol', 'downsol', 'upsol', 'nhits_casc', 'nhits_casc_100', 'sum_ToT_casc', 'min_dist_casc', 'max_dist_casc', 'nhits_mu', 'nhits_mu_100', 'sum_ToT_mu', 'min_dist_mu', 'max_dist_mu', 'nhits_casc_mu', 'nhits_casc_mu_100', 'sum_ToT_casc_mu', 'min_dist_casc_mu', 'max_dist_casc_mu', 'min_diff_sollik', 'max_diff_sollik', 'min_diff_sol', 'max_diff_sol', 'min_zen_sol', 'max_zen_sol', 'diffangle_shower_nu', 'diffangle_shower_mu', 'delta_zenith_shower_mu', 'delta_zenith_shower_nu', 'zenith_shower', 'beta0_shower', 'beta0_shower_deg', 'lik_shower', 'Nhit_shower', 'best_trk_pos_shower_x', 'best_trk_pos_shower_y', 'best_trk_pos_shower_z', 'best_trk_dir_shower_x', 'best_trk_dir_shower_y', 'best_trk_dir_shower_z', 'Ereco_shower', 'Ereco_shower_corrected', 'logEreco_shower', 'dlik', 'normdlik', 'itoverlen', 'delta_zenith_track_shower', 'diffangle_track_shower', 'flag_muon_3D', 'flag_shower_3D', 'flag_shower_MX', 'ToT_border_mu', 'ToT_border_casc', 'ToT_border_cascmu', 'ToT_trig', 'max_ToT_trig', 'ToT_IT', 'ToT_allIT', 'Nborder_hits', 'Nborder_cherenkov_hits', 'Nborder_dtres_hits', 'Nborder_DOMs', 'Nhits_upper', 'Nhits_lower', 'Nhits_border_upper', 'Nhits_border_lower', 'Nhits_cherenkov_upper', 'Nhits_cherenkov_lower', 'Nhits_border_cherenkov_upper', 'Nhits_border_cherenkov_lower', 'NtrackIT50', 'NtrackIT50_2', 'NtrackIT30_2', 'NtrackIT10_2', 'NtrackIT50_3', 'NtrackIT30_3', 'NtrackIT10_3', 'num_triggered_hits', 'num_triggered_lines', 'num_triggered_doms', 'num_triggered_pmts', 'num_cherenkov_hits', 'num_cherenkov_lines', 'num_cherenkov_doms', 'num_cherenkov_pmts', 'num_cascade_hits', 'num_cascade_lines', 'num_cascade_doms', 'num_cascade_pmts', 'ratio_cherenkov_hits', 'ratio_cherenkov_lines', 'ratio_cherenkov_doms', 'ratio_cherenkov_pmts', 'ratio_cascade_hits', 'ratio_cascade_lines', 'ratio_cascade_doms', 'ratio_cascade_pmts', 'associated_n_hits', 'reco_tres_vector', 'reco_d_photon_vector', 'reco_d_closest_vector', 'reco_cos_angle_vector', 'diff_cos_angle', 'hit_pos_x', 'hit_pos_y', 'hit_pos_z', 'hit_dir_x', 'hit_dir_y', 'hit_dir_z', 'hit_time', 'hit_tot', 'first_hit_tot_per_pmt', 'hit_a', 'is_triggered', 'is_cherenkov']
        h5_file_path = os.path.join(h5_dir, os.path.basename(file_).replace('.root', '.h5'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        awkward_array = None
        save_to_h5(array_data_dict, awkward_array, h5_file_path) 
    # end_time = time.time()
    # print(f"HDF5 conversion completed in {end_time - start_time:.2f} seconds")
    return None


def convert_branches_to_sqlite(root_file_path, tree_name, branch_names, sqlite_db_path):
    with uproot.open(root_file_path) as file_:
        tree = file_[tree_name]

        if not branch_names:
            print(f"❌ No columns found for table creation in file {root_file_path}, tree {tree_name}. Skipping.")
            return  # Skip if no columns found

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
    """
    Converts a list of root files to SQLite format and saves them in the specified directory.
    - The function converts each root file in the `root_files` list to SQLite format.
    - The converted SQLite files are saved in the `sqlite_dir` directory.
    - The function uses the `root_to_arrays()` function to convert the root files to arrays.
    - The converted arrays are then saved to Parquet files using the `save_to_sqlite()` function.

    Parameters:
        - root_files (List[str]): A list of root file paths to be converted.
        - sqlite_dir (str): The directory where the converted SQLite files will be saved.

    Returns:
        None

    Examples:
        >>> root_files = ['/path/to/file1.root', '/path/to/file2.root']
        >>> sqlite_dir = '/path/to/sqlite_files'
        >>> convert_new_root_files(root_files, sqlite_dir)
    """
    for file_ in root_files:
        # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        sqlite_file_path = os.path.join(sqlite_dir, os.path.basename(file_).replace('.root', '.db'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        save_to_sqlite(array_data_dict, sqlite_file_path)
        # save_to_h5(array_data_dict, sqlite_file_path) 
    return None
    pass


def root2parquet(features: List[str], root_files: List[str], parquet_dir:str):
    """
    Converts a list of root files to Parquet format and saves them in the specified directory.
    - The function converts each root file in the `root_files` list to HDF5 format.
    - The converted Parquet files are saved in the `parquet_dir` directory.
    - The function uses the `root_to_arrays()` function to convert the root files to arrays.
    - The converted arrays are then saved to Parquet files using the `save_to_parquet()` function.

    Parameters:
        - root_files (List[str]): A list of root file paths to be converted.
        - parquet_dir (str): The directory where the converted Parquet files will be saved.

    Returns:
        None

    Examples:
        >>> root_files = ['/path/to/file1.root', '/path/to/file2.root']
        >>> parquet_dir = '/path/to/parquet_files'
        >>> convert_new_root_files(root_files, parquet_dir)
    """
    for file_ in root_files:
        # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        parquet_file_path = os.path.join(parquet_dir, os.path.basename(file_).replace('.root', '.parquet'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        save_to_parquet(array_data_dict, parquet_file_path)
    return None

def convert_branches_to_sqlite(root_file_path, tree_name, branch_names, sqlite_db_path):
    with uproot.open(root_file_path) as file_:
        tree = file_[tree_name]

        if not branch_names:
            print(f"❌ No columns found for table creation in file {root_file_path}, tree {tree_name}. Skipping.")
            return  # Skip if no columns found

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
    """
    Converts a list of root files to SQLite format and saves them in the specified directory.
    - The function converts each root file in the `root_files` list to SQLite format.
    - The converted SQLite files are saved in the `sqlite_dir` directory.
    - The function uses the `root_to_arrays()` function to convert the root files to arrays.
    - The converted arrays are then saved to Parquet files using the `save_to_sqlite()` function.

    Parameters:
        - root_files (List[str]): A list of root file paths to be converted.
        - sqlite_dir (str): The directory where the converted SQLite files will be saved.

    Returns:
        None

    Examples:
        >>> root_files = ['/path/to/file1.root', '/path/to/file2.root']
        >>> sqlite_dir = '/path/to/sqlite_files'
        >>> convert_new_root_files(root_files, sqlite_dir)
    """
    for file_ in root_files:
        # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        sqlite_file_path = os.path.join(sqlite_dir, os.path.basename(file_).replace('.root', '.db'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        save_to_sqlite(array_data_dict, sqlite_file_path)
        # save_to_h5(array_data_dict, sqlite_file_path) 
    return None
    pass


def root2parquet(features: List[str], root_files: List[str], parquet_dir:str):
    """
    Converts a list of root files to Parquet format and saves them in the specified directory.
    - The function converts each root file in the `root_files` list to HDF5 format.
    - The converted Parquet files are saved in the `parquet_dir` directory.
    - The function uses the `root_to_arrays()` function to convert the root files to arrays.
    - The converted arrays are then saved to Parquet files using the `save_to_parquet()` function.

    Parameters:
        - root_files (List[str]): A list of root file paths to be converted.
        - parquet_dir (str): The directory where the converted Parquet files will be saved.

    Returns:
        None

    Examples:
        >>> root_files = ['/path/to/file1.root', '/path/to/file2.root']
        >>> parquet_dir = '/path/to/parquet_files'
        >>> convert_new_root_files(root_files, parquet_dir)
    """
    for file_ in root_files:
        # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        parquet_file_path = os.path.join(parquet_dir, os.path.basename(file_).replace('.root', '.parquet'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        save_to_parquet(array_data_dict, parquet_file_path)
    return None

def convert_branches_to_sqlite(root_file_path, tree_name, branch_names, sqlite_db_path):
    with uproot.open(root_file_path) as file_:
        tree = file_[tree_name]

        if not branch_names:
            print(f"❌ No columns found for table creation in file {root_file_path}, tree {tree_name}. Skipping.")
            return  # Skip if no columns found

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
    """
    Converts a list of root files to SQLite format and saves them in the specified directory.
    - The function converts each root file in the `root_files` list to SQLite format.
    - The converted SQLite files are saved in the `sqlite_dir` directory.
    - The function uses the `root_to_arrays()` function to convert the root files to arrays.
    - The converted arrays are then saved to Parquet files using the `save_to_sqlite()` function.

    Parameters:
        - root_files (List[str]): A list of root file paths to be converted.
        - sqlite_dir (str): The directory where the converted SQLite files will be saved.

    Returns:
        None

    Examples:
        >>> root_files = ['/path/to/file1.root', '/path/to/file2.root']
        >>> sqlite_dir = '/path/to/sqlite_files'
        >>> convert_new_root_files(root_files, sqlite_dir)
    """
    for file_ in root_files:
        # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        sqlite_file_path = os.path.join(sqlite_dir, os.path.basename(file_).replace('.root', '.db'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        save_to_sqlite(array_data_dict, sqlite_file_path)
        # save_to_h5(array_data_dict, sqlite_file_path) 
    return None
    pass


def root2parquet(features: List[str], root_files: List[str], parquet_dir:str):
    """
    Converts a list of root files to Parquet format and saves them in the specified directory.
    - The function converts each root file in the `root_files` list to HDF5 format.
    - The converted Parquet files are saved in the `parquet_dir` directory.
    - The function uses the `root_to_arrays()` function to convert the root files to arrays.
    - The converted arrays are then saved to Parquet files using the `save_to_parquet()` function.

    Parameters:
        - root_files (List[str]): A list of root file paths to be converted.
        - parquet_dir (str): The directory where the converted Parquet files will be saved.

    Returns:
        None

    Examples:
        >>> root_files = ['/path/to/file1.root', '/path/to/file2.root']
        >>> parquet_dir = '/path/to/parquet_files'
        >>> convert_new_root_files(root_files, parquet_dir)
    """
    for file_ in root_files:
        # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        parquet_file_path = os.path.join(parquet_dir, os.path.basename(file_).replace('.root', '.parquet'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        save_to_parquet(array_data_dict, parquet_file_path)
    return None

def convert_branches_to_sqlite(root_file_path, tree_name, branch_names, sqlite_db_path):
    with uproot.open(root_file_path) as file_:
        tree = file_[tree_name]

        if not branch_names:
            print(f"❌ No columns found for table creation in file {root_file_path}, tree {tree_name}. Skipping.")
            return  # Skip if no columns found

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
    """
    Converts a list of root files to SQLite format and saves them in the specified directory.
    - The function converts each root file in the `root_files` list to SQLite format.
    - The converted SQLite files are saved in the `sqlite_dir` directory.
    - The function uses the `root_to_arrays()` function to convert the root files to arrays.
    - The converted arrays are then saved to Parquet files using the `save_to_sqlite()` function.

    Parameters:
        - root_files (List[str]): A list of root file paths to be converted.
        - sqlite_dir (str): The directory where the converted SQLite files will be saved.

    Returns:
        None

    Examples:
        >>> root_files = ['/path/to/file1.root', '/path/to/file2.root']
        >>> sqlite_dir = '/path/to/sqlite_files'
        >>> convert_new_root_files(root_files, sqlite_dir)
    """
    for file_ in root_files:
        # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        sqlite_file_path = os.path.join(sqlite_dir, os.path.basename(file_).replace('.root', '.db'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        save_to_sqlite(array_data_dict, sqlite_file_path)
        # save_to_h5(array_data_dict, sqlite_file_path) 
    return None
    pass


def root2parquet(features: List[str], root_files: List[str], parquet_dir:str):
    """
    Converts a list of root files to Parquet format and saves them in the specified directory.
    - The function converts each root file in the `root_files` list to HDF5 format.
    - The converted Parquet files are saved in the `parquet_dir` directory.
    - The function uses the `root_to_arrays()` function to convert the root files to arrays.
    - The converted arrays are then saved to Parquet files using the `save_to_parquet()` function.

    Parameters:
        - root_files (List[str]): A list of root file paths to be converted.
        - parquet_dir (str): The directory where the converted Parquet files will be saved.

    Returns:
        None

    Examples:
        >>> root_files = ['/path/to/file1.root', '/path/to/file2.root']
        >>> parquet_dir = '/path/to/parquet_files'
        >>> convert_new_root_files(root_files, parquet_dir)
    """
    for file_ in root_files:
        # features = ['eventNumber', 'digitX', 'digitY', 'digitZ', 'digitT', 'trueNeutrinoEnergy', 'trueMuonEnergy']#IMPORTANT: columns to extract from the ROOT file should be dynamic
        parquet_file_path = os.path.join(parquet_dir, os.path.basename(file_).replace('.root', '.parquet'))
        array_data_dict = root_to_dict_of_arrays(file_, features)
        print(f"Converting file {file_} to Parquet format...")
        print(f"dict of arrays keys: {list(array_data_dict.keys())}")
        # awkward_array = root_to_awkward_arrays(file_, features) #NOTE: awkward array is not used 
        save_to_parquet(array_data_dict, parquet_file_path)
    return None
