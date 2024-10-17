# Purpose: Facilitates user interaction for file selection and other input-related tasks.
# Key Functions: user_file_selection.
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

    # choice = input("\nDo you want to convert all files? (y/n): ").strip().lower()
    choice = 'y' #ATTENTION: only for testing, DELETE this line when in production!

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
    # conversion_choice = input("Enter your choice (1/2/3): ").strip()
    conversion_choice = '2' #ATTENTION: only for testing, DELETE this line when in production!

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