import os
import uproot
import h5py
import numpy as np
import sqlite3
import pandas as pd

# Function to list all trees in a ROOT file
def list_trees(root_file_path):
    with uproot.open(root_file_path) as file:
        return file.keys()

# Function to list all branches in a TTree
def list_branches(root_file_path, tree_name):
    with uproot.open(root_file_path) as file:
        tree = file[tree_name]
        branches = tree.keys()
        return branches

# Function to check for required branches
def check_required_branches(branches, required_branches):
    missing_branches = [branch for branch in required_branches if branch not in branches]
    if missing_branches:
        print(f"Error: The following required branches are missing: {', '.join(missing_branches)}")
        return False
    return True

def convert_branches_to_h5(root_file_path, tree_name, branch_names, h5_file_path):
    with uproot.open(root_file_path) as file:
        tree = file[tree_name]

        with h5py.File(h5_file_path, 'w') as h5_file:
            for branch_name in branch_names:
                if branch_name not in tree.keys():
                    print(f"Branch '{branch_name}' not found in the tree '{tree_name}'")
                    continue

                branch_data = tree[branch_name].array(library="np")

                # Handle byte-encoded strings
                if branch_data.dtype.type is np.bytes_:
                    branch_data = np.array([x.decode('utf-8') for x in branch_data])  # Decode bytes to strings
                elif branch_data.dtype == np.dtype('O'):
                    # Handle objects that cannot be directly converted to floats
                    try:
                        branch_data = branch_data.astype(np.float64)
                    except ValueError:
                        branch_data = np.array([str(item) for item in branch_data], dtype='S')

                h5_file.create_dataset(branch_name, data=branch_data)
                print(f"Branch '{branch_name}' has been successfully written to {h5_file_path}")



# Function to convert specific branches of a ROOT file to an SQLite database
def convert_branches_to_sqlite(root_file_path, tree_name, branch_names, sqlite_db_path):
    with uproot.open(root_file_path) as file:
        tree = file[tree_name]

        conn = sqlite3.connect(sqlite_db_path)
        cursor = conn.cursor()
        
        base_name = os.path.splitext(os.path.basename(root_file_path))[0]
        sanitized_table_name = f'"{base_name.replace(".", "_")}"'
        
        columns = ", ".join([f'"{branch_name}" REAL' for branch_name in branch_names])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {sanitized_table_name} ({columns})")
        
        data = []
        for branch_name in branch_names:
            if branch_name not in tree.keys():
                print(f"Branch '{branch_name}' not found in the tree '{tree_name}'")
                continue

            branch_data = tree[branch_name].array(library="np")

            # Handle byte-encoded strings
            if branch_data.dtype.type is np.bytes_:
                branch_data = np.char.decode(branch_data, 'utf-8')  # Decode byte strings to regular strings
            elif branch_data.dtype == np.dtype('O'):
                try:
                    branch_data = branch_data.astype(np.float64)
                except ValueError:
                    branch_data = np.array([str(item) for item in branch_data], dtype='S')

            data.append(branch_data)

        data = np.column_stack(data)
        
        placeholders = ", ".join(["?"] * len(branch_names))
        cursor.executemany(f"INSERT INTO {sanitized_table_name} VALUES ({placeholders})", data)
        
        conn.commit()
        conn.close()
        print(f"Data from '{root_file_path}' has been successfully written to {sqlite_db_path}")


def process_directory(root_dir, tree_name, branch_names, output_dir, conversion_func):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for filename in os.listdir(root_dir):
        if filename.endswith(".root"):
            root_file_path = os.path.join(root_dir, filename)
            base_name = os.path.splitext(filename)[0]
            output_file_path = os.path.join(output_dir, f"{base_name}.{conversion_func.__name__.split('_')[-1]}")

            print(f"Processing {root_file_path}...")
            conversion_func(root_file_path, tree_name, branch_names, output_file_path)

def get_table_names(sqlite_db_path):
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    return [table[0] for table in tables]

def read_sqlite_table(sqlite_db_path, table_name):
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    query = f'SELECT * FROM "{table_name}"'
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    
    print(f"Table: {table_name}")
    print("-" * (len(column_names) * 15))
    print(" | ".join(column_names))
    print("-" * (len(column_names) * 15))
    
    for row in rows:
        print(" | ".join(map(str, row)))
    
    print("\n")
    conn.close()
    
# Function to read data from HDF5 file
def read_h5_file(h5_file_path, branch_names):
    with h5py.File(h5_file_path, 'r') as h5_file:
        data = {}
        for branch in branch_names:
            if branch in h5_file:
                dataset = h5_file[branch][:]
                # Handle byte-encoded strings
                if dataset.dtype.type is np.bytes_:
                    data[branch] = np.array([x.decode('utf-8') for x in dataset])  # Decode bytes to strings
                else:
                    data[branch] = dataset
            else:
                print(f"Warning: Branch '{branch}' does not exist in the HDF5 file.")
        
        df = pd.DataFrame(data)
        return df

    
    
# Function to convert specific branches of a ROOT file into a DataFrame
def convert_branches_to_dataframe(root_file_path, tree_name, branch_names):
    with uproot.open(root_file_path) as file:
        tree = file[tree_name]
        data = {}
        
        for branch_name in branch_names:
            if branch_name not in tree.keys():
                print(f"Branch '{branch_name}' not found in the tree '{tree_name}'")
                continue

            branch_data = tree[branch_name].array(library="np")

            # Handle byte-encoded strings
            if branch_data.dtype.type is np.bytes_:
                branch_data = np.array([x.decode('utf-8') for x in branch_data])  # Decode bytes to strings
            data[branch_name] = branch_data
        
        df = pd.DataFrame(data)
        return df

def main():
    # Predefined array of branch names (modify this array as needed)
    branch_names = ["eventNumber", "digitX", "digitY", "digitZ"]

    print("Choose an option:")
    print("1: Convert from ROOT to HDF5")
    print("2: Read HDF5 file")
    
    choice = int(input("Enter choice (1 or 2): ").strip())
    
    while choice not in [1, 2]:
        print("Invalid choice. Please choose either 1 or 2.")
        choice = int(input("Enter choice (1 or 2): ").strip())
    
    if choice == 1:
        # Conversion from ROOT to HDF5
        root_dir = os.getcwd() + "/annie-temp/root_transformer/Data/Data_forEnergy"  # Directory containing ROOT files
        output_dir = os.getcwd() + "/annie-temp/root_transformer/Data_forEnergy_h5"  # Directory to save HDF5 files
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Process all ROOT files in the directory
        for filename in os.listdir(root_dir):
            if filename.endswith(".root"):
                root_file_path = os.path.join(root_dir, filename)
                print(f"Processing {root_file_path}...")

                # List trees and select the first one
                all_trees = list_trees(root_file_path)
                if not all_trees:
                    print(f"No trees found in {root_file_path}. Skipping...")
                    continue
                
                tree_name = all_trees[0]  # Selecting the first tree for conversion
                print(f"Using tree: {tree_name}")
                
                # Convert selected branches to HDF5
                output_file = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.h5")
                convert_branches_to_h5(root_file_path, tree_name, branch_names, output_file)

                print(f"Data from {root_file_path} has been written to {output_file}")

    elif choice == 2:
        # Reading from HDF5
        h5_dir = os.getcwd() + "/annie-temp/root_transformer/Data_forEnergy_h5"  # Directory containing HDF5 files
        h5_files = [f for f in os.listdir(h5_dir) if f.endswith('.h5')]
        
        if not h5_files:
            print("No HDF5 files found. Exiting.")
            return
        
        print("Available HDF5 files:")
        for idx, h5_file in enumerate(h5_files):
            print(f"{idx + 1}: {h5_file}")
        
        h5_choice = int(input("Choose an HDF5 file by number: ").strip()) - 1
        if h5_choice < 0 or h5_choice >= len(h5_files):
            print("Invalid choice. Exiting.")
            return
        
        h5_file_path = os.path.join(h5_dir, h5_files[h5_choice])
        
        print(f"Reading file: {h5_file_path}")
        df = read_h5_file(h5_file_path, branch_names)
        
        # Sort and display DataFrame
        if 'eventNumber' in df.columns:
            df = df.sort_values(by='eventNumber').reset_index(drop=True)
        print(df)

if __name__ == "__main__":
    main()