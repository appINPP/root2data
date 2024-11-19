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

# Purpose: Facilitates user interaction for file selection and other input-related tasks.

import os
from typing import List, Tuple


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
    # choice = 'y' #ATTENTION: only for testing, DELETE this line when in production!

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
    

def scan_for_new_root_files(root_dir: str, h5_dir: str = None, sqlite_dir: str = None, parquet_dir:str = None) -> List[str]:
    '''
    Scans the specified root directory for new .root files that do not have corresponding files in the specified HDF5, SQLite, or Parquet directories.
    
    Parameters:
        - root_dir (str): The directory containing the .root files.
        - h5_dir (str, optional): The directory containing the .h5 files. Defaults to None.
        - sqlite_dir (str, optional): The directory containing the .db files. Defaults to None.
        - parquet_dir (str, optional): The directory containing the .parquet files. Defaults to None.
    
    Returns:
        - List[str]: A tuple containing three lists:
        - List of new .root files to be converted to HDF5.
        - List of new .root files to be converted to SQLite.
        - List of new .root files to be converted to Parquet.
    '''
    root_files = os.listdir(root_dir)
    h5_files = [x.split('.h5')[0] for x in os.listdir(h5_dir)]
    sqlite_files = [x.split('.db')[0] for x in os.listdir(sqlite_dir)]
    parquet_files = [x.split('.parquet')[0] for x in os.listdir(parquet_dir)]

    new_root_for_h5 = [x for x in root_files if x.endswith('.root') and x.split('.root')[0] not in h5_files]
    new_root_for_sqlite = [x for x in root_files if x.endswith('.root') and x.split('.root')[0] not in sqlite_files]
    new_root_for_parquet = [x for x in root_files if x.endswith('.root') and x.split('.root')[0] not in parquet_files]

    new_root_files_h5, new_root_files_sqlite, new_root_files_parquet = [], [], []

    print("\nDo you want to convert to:")
    print("1. HDF5")
    print("2. SQLite")
    # print("3. Both")
    print("3. Parquet")
    conversion_choice = input("Enter your choice (1/2/3): ").strip()
    # conversion_choice = '2' #ATTENTION: only for testing, DELETE this line when in production!

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

    # elif conversion_choice == '3':  # Convert to both HDF5 and SQLite
    #     if not new_root_for_h5:
    #         print('No new root files found for HDF5.')
    #     else:
    #         new_root_files_h5 = user_file_selection(new_root_for_h5)

    #     if not new_root_for_sqlite:
            # print('No new root files found for SQLite.')
        # else:
        #     new_root_files_sqlite = user_file_selection(new_root_for_sqlite)

    elif conversion_choice == '3':
        # Convert to Parquet
        if not new_root_for_parquet:
            print('No new root files found for Parquet.')
        else:
            new_root_files_parquet = user_file_selection(new_root_for_parquet)

    else:
        print("Invalid option. No files will be converted.")

    new_root_files_h5 = [os.path.join(root_dir, x) for x in new_root_files_h5]
    new_root_files_sqlite = [os.path.join(root_dir, x) for x in new_root_files_sqlite]
    new_root_files_parquet = [os.path.join(root_dir, x) for x in new_root_files_parquet]

    return new_root_files_h5, new_root_files_sqlite, new_root_files_parquet