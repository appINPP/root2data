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
# 
import os
import argparse
from utils.file_ops import list_h5_files, list_sqlite_files
from utils.conversion import root2h5, root2sqlite, root2parquet, convert_branches_to_sqlite
from utils.data_ops import create_dataframe_and_show_structure
from utils.sqlite_ops import get_table_names, sqlite_to_dataframe, read_sqlite_to_df
from utils.parquet_ops import parquet_to_dataframe
from utils.file_ops import list_h5_files, list_sqlite_files, list_parquet_files
from utils.ui_ops import user_file_selection, scan_for_new_root_files



def parse_arguments():
    current_directory = os.getcwd()
    # parent_directory = os.path.dirname(current_directory)
    default_root_dir = os.path.join(current_directory, "data", "root")
    default_h5_dir = os.path.join(current_directory, "data", "h5")
    default_sqlite_dir = os.path.join(current_directory, "data", "sqlite")
    default_parquet_dir = os.path.join(current_directory, "data", "parquet")

    parser = argparse.ArgumentParser(description="Process ROOT files and save data to HDF5/SQLite format.")
    parser.add_argument('--features', nargs='+', default=' ', help='List of columns to find in the ROOT file.')
    # parser.add_argument('--features', type=str, help='Comma-separated list of columns to find in the ROOT file.')
    parser.add_argument('--root_dir', default=default_root_dir, help='Path to the ROOT file directory.')
    parser.add_argument('--h5_dir', default=default_h5_dir, help='Path to the HDF5 file directory.')
    parser.add_argument('--sqlite_dir', default=default_sqlite_dir, help='Path to the SQLite file directory.') 
    parser.add_argument('--parquet_dir', default=default_parquet_dir, help='Path to the Parquet file directory.')
    # If we want comma-separated features into a list then uncomment the following lines
    # args = parser.parse_args()
    # if args.features:
    #     args.features = args.features.split(',')
    # return args
        
    return parser.parse_args()

# Main function to include SQLite and HDF5 conversions
def main():
    args = parse_arguments()

    root_dir = args.root_dir
    h5_dir = args.h5_dir
    sqlite_dir = args.sqlite_dir
    parquet_dir = args.parquet_dir
    features = args.features

    os.makedirs(h5_dir, exist_ok=True)
    os.makedirs(sqlite_dir, exist_ok=True)
    os.makedirs(root_dir, exist_ok=True)
    os.makedirs(parquet_dir, exist_ok=True)

    while True:
        print("\nPlease choose an option:")
        print("1. Read a HDF5 | SQLite | Parquet file")
        print("2. Convert ROOT files to HDF5 | SQLite | Parquet")
        print("3. Exit")

        choice = input("Enter your choice (1/2/3): ")
        # choice = '2' #ATTENTION: only for testing, DELETE this line when in production!

        if choice == '1':
            print("Select the file type you want to read:")
            print("1. HDF5")
            print("2. SQLite")
            print("3. Parquet")
            choice = input("Enter 1 or 2 or 3: ")
            # choice = '2' #ATTENTION: only for testing, DELETE this line when in production!
            if choice == '1':
                h5_files = list_h5_files(h5_dir)
                if not h5_files:
                    print(f"No HDF5 files found in {h5_dir}.")
                else:
                    print("Available HDF5 files:")
                    for idx, h5_file in enumerate(h5_files):
                        print(f"{idx + 1}. {h5_file}")
                    file_choice = int(input(f"Select a file to read (1-{len(h5_files)}): "))
                    selected_h5_file = os.path.join(h5_dir, h5_files[file_choice - 1])
                    df = create_dataframe_and_show_structure(selected_h5_file)
                    print(df)
            elif choice == '2':
                sqlite_files = list_sqlite_files(sqlite_dir)
                if not sqlite_files:
                    print(f"No SQLite files found in {sqlite_dir}.")
                else:
                    print("Available SQLite files:")
                    for idx, sqlite_file in enumerate(sqlite_files):
                        print(f"{idx + 1}. {sqlite_file}")
                    file_choice = int(input(f"Select a file to read (1-{len(sqlite_files)}): "))
                    selected_sqlite_file = os.path.join(sqlite_dir, sqlite_files[file_choice - 1])
                    
                    # Get the table names from the selected SQLite file
                    table_names = get_table_names(selected_sqlite_file)
                    if not table_names:
                        print(f"No tables found in {selected_sqlite_file}.")
                    else:
                        print("Available tables:")
                        for idx, table_name in enumerate(table_names):
                            print(f"{idx + 1}. {table_name}")
                        table_choice = int(input(f"Select a table to read (1-{len(table_names)}): "))
                        selected_table = table_names[table_choice - 1]
                        # df = read_sqlite_to_df(sqlite_db_path, table_name)
                        df = sqlite_to_dataframe(selected_sqlite_file, selected_table)
                        # Read and display the selected table
                        print(df)
            elif choice == '3':
                parquet_files = list_parquet_files(parquet_dir)
                if not parquet_files:
                    print(f"No Parquet files found in {h5_dir}.")
                else:
                    print("Available Parquet files:")
                    for idx, parquet_file in enumerate(parquet_files):
                        print(f"{idx + 1}. {parquet_file}")
                    file_choice = int(input(f"Select a file to read (1-{len(parquet_files)}): "))
                    selected_h5_file = os.path.join(parquet_dir, parquet_files[file_choice - 1])
                    df = parquet_to_dataframe(selected_h5_file)
                    print(df)
            else:
                print("Invalid choice! Please enter either 1 or 2.")
                
        elif choice == '2':
            if '' in features: 
                    print("No features provided! Please provide columns to find in the ROOT file, seperated by space.")
                    print("Example: python3 transform.py --features groupID EnergyX EnergyY ")
                    continue   # Return to the main menu
            
            print(f"Scanning {root_dir} for new ROOT files to convert...")
            new_root_files_h5, new_root_files_sqlite, new_root_files_parquet = scan_for_new_root_files(root_dir, h5_dir, sqlite_dir, parquet_dir)

            if new_root_files_h5:
                print(f"Converting {len(new_root_files_h5)} ROOT files to HDF5 format...")
                root2h5(features, new_root_files_h5, h5_dir)
                print(f"Conversion completed. HDF5 files saved to {h5_dir}")

            if new_root_files_sqlite:
                print(f"Converting {len(new_root_files_sqlite)} ROOT files to SQLite format...")
                root2sqlite(features, new_root_files_sqlite, sqlite_dir)
                # for root_file in new_root_files_sqlite:
                #     convert_branches_to_sqlite(root_file, "phaseIITriggerTree;1", features, os.path.join(sqlite_dir, os.path.basename(root_file).replace('.root', '.sqlite3')))
                print(f"Conversion completed. SQLite files saved to {sqlite_dir}")

            if new_root_files_parquet:
                print(f"Converting {len(new_root_files_h5)} ROOT files to Parquet format...")
                root2parquet(features, new_root_files_parquet, parquet_dir)
                print(f"Conversion completed. Parquet files saved to {parquet_dir}")

        elif choice == '3':
            print("Exiting...")
            break

        else:
            print("Invalid option, please choose again.")

if __name__ == "__main__":
    try:
        main()
    except Exception as error_main:
        print('[main error]:', error_main)
