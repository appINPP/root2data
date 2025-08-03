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
from html import parser
import os
import argparse
from utils.file_ops import list_h5_files, list_sqlite_files
from utils.conversion import root2h5, root2sqlite, root2parquet, convert_branches_to_sqlite
from utils.data_ops import create_dataframe_and_show_structure
from utils.sqlite_ops import get_table_names, sqlite_to_dataframe, read_sqlite_to_df
from utils.parquet_ops_v2 import parquet_to_dataframe, process_parquet_format
from utils.file_ops import list_h5_files, list_sqlite_files, list_parquet_files, list_root_files
from utils.ui_ops import user_file_selection, scan_for_new_root_files



def parse_arguments():
    # current_directory = os.getcwd()
    parser = argparse.ArgumentParser(description="Process ROOT files and save data to HDF5/SQLite format.")
    # parser.add_argument('--features', nargs='+', default=' ', help='List of columns to find in the ROOT file.')
    parser.add_argument('--features', type=str, help='Comma-separated list of columns to find in the ROOT file.') # passing features as a string with comma-separated values
    parser.add_argument('--truth', type=str, help='Comma-separated list of truth columns to find in the ROOT file.') 
    parser.add_argument('--index_col', type=str, default='eventNumber', help='Column name to be used as index. Default is "event".')    
    parser.add_argument('--root_dir', help='Path to the ROOT file directory.')
    parser.add_argument('--output', help='The final output files converted.')
    parser.add_argument('--mode',  help='The transformation mode to apply to the data.')
    parser.add_argument('--format', help='The format structure to use for processing parquet files.')
    # If we want comma-separated features into a list then uncomment the following lines
    args = parser.parse_args()
    if args.features:
        args.features = args.features.split(',')
    if args.truth:
        args.truth = args.truth.split(',')
    return args
    # return parser.parse_args()

def main():
    args = parse_arguments()
    mode = args.mode
    output = args.output
    features = args.features if args.features else []
    truth = args.truth if args.truth else []
    root_dir = args.root_dir
    h5_dir = os.path.dirname(root_dir)+"/h5"
    sqlite_dir = os.path.dirname(root_dir)+"/sqlite"
    parquet_dir = os.path.dirname(root_dir)+"/parquet"

    os.makedirs(h5_dir, exist_ok=True)
    os.makedirs(sqlite_dir, exist_ok=True)
    # os.makedirs(root_dir, exist_ok=True)
    os.makedirs(parquet_dir, exist_ok=True)
    
    root_files = list_root_files(root_dir)
    # root_file_paths = [os.path.join(root_dir,x) for x in list_root_files(root_dir)]
    root_file_paths = [os.path.join(root_dir, x) for x in root_files]

    if mode == 'convert' and output != None:
        print(f"\n 👀 Scanning {root_dir=} for ROOT files to convert.")
        print(f"Found {len(root_files)} ROOT files in root_dir")
        if output == 'h5':
            print(f" 🛠️  Converting ROOT files to HDF5 format..")
            root2h5(features, root_file_paths, h5_dir)
            print(f" ✅ Conversion completed. HDF5 files saved to {h5_dir}")
        elif output == 'sqlite':
            print(f" 🛠️ Converting ROOT files to SQLite format..")
            root2sqlite(features, root_file_paths, sqlite_dir)
            print(f" ✅ Conversion completed. SQLite files saved to {sqlite_dir}")
        elif output == 'parquet':
            print(f" 🛠️ Converting ROOT files to Parquet format..")
            all_columns = list(set(features + truth))
            root2parquet(all_columns, root_file_paths, parquet_dir)
            # root2parquet(features, root_file_paths, parquet_dir)
            print(f" ✅ Conversion completed. Parquet files saved to {parquet_dir}")
        else:
            raise ValueError("❌ Invalid output format. Please choose one of the following: 'h5', 'sqlite', 'parquet'.")
        
    elif mode == 'read' and output != None:
        if output == 'h5':
            print(f" ⌛ Reading HDF5 files..\n")
            h5_files = [os.path.join(h5_dir,x) for x in list_h5_files(h5_dir)]
            for file_ in h5_files:  
                df = create_dataframe_and_show_structure(file_)
                print(df)
            print(f" ✅ HDF5 files from  {h5_dir} read successfully!")
        elif output == 'sqlite':
            print(f" ⌛ Reading SQLite files..\n")
            sqlite_files = [os.path.join(sqlite_dir,x) for x in list_sqlite_files(sqlite_dir)]
            for file_ in sqlite_files: 
                table_names = get_table_names(file_) 
                if not table_names:
                    print(f"No tables found in {file_}.")
                else:
                    print(f" ⚙️ table: {file_}")
                    df = sqlite_to_dataframe(file_, table_names[0])
                    print(df)
                    print(5* '-----------------------------------')
            print(f" ✅ SQLite files from  {sqlite_dir} read successfully!")
        elif output == 'parquet':
            print(f" ⌛ Reading Parquet files..")
            if args.format == 'graphnet':
                for i in range(10):
                    base_dir = os.path.join(os.path.dirname(root_dir),"processed_parquet", f"phase2tree.{i}")
                    subdirs = ["features", "truth"]
                    for sub in subdirs:
                        parquet_read_dir = os.path.join(base_dir, sub)
                        if not os.path.exists(parquet_read_dir):
                            print(f"Directory not found: {parquet_read_dir}")
                            continue
                        parquet_files = [os.path.join(parquet_read_dir, x) for x in list_parquet_files(parquet_read_dir)]
                        print(f"Available Parquet files in {parquet_read_dir}: {list_parquet_files(parquet_read_dir)}\n")
                        for file_ in parquet_files:
                            print(f" ⚙️ file: {file_}")
                            df = parquet_to_dataframe(file_)
                            print(df)
                            print(5* '-----------------------------------')
                    print(f" ✅ Parquet files from processed_parquet/features and truth read successfully!")
            else:
                parquet_read_dir = parquet_dir
                parquet_files = [os.path.join(parquet_read_dir, x) for x in list_parquet_files(parquet_read_dir)]
                print(f"Available Parquet files: {list_parquet_files(parquet_read_dir)}\n")
                for file_ in parquet_files:
                    print(f" ⚙️ file: {file_}")
                    df = parquet_to_dataframe(file_)
                    print(df)
                    print(5* '-----------------------------------')
                print(f" ✅ Parquet files from  {parquet_read_dir} read successfully!")
        else:
            raise ValueError("❌ Invalid output format. Please choose one of the following: 'h5', 'sqlite', 'parquet'.")

if __name__ == "__main__":
    try:
        main()
        args = parse_arguments()
        if args.output == 'parquet' and  args.format == 'graphnet':
            process_parquet_format(args.features, args.truth, args.index_col)  
            print(f'✅ Parquet files processed -> please check data/processed_parquet folder')
    except Exception as error_main:
        print('[main error]:', error_main)
