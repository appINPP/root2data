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

def main():
    root_dir = os.getcwd() + "/root_transformer/Data/Data_forEnergy"  # Directory containing ROOT files

    example_root_file = os.path.join(root_dir, os.listdir(root_dir)[0])
    
    print("Listing all available trees...")
    all_trees = list_trees(example_root_file)
    print("Available trees:")
    for idx, tree in enumerate(all_trees):
        print(f"{idx + 1}: {tree}")
    
    tree_choice = int(input("Choose a tree by number: ")) - 1
    if tree_choice < 0 or tree_choice >= len(all_trees):
        print("Invalid choice. Exiting.")
        return
    
    tree_name = all_trees[tree_choice]
    print(f"Selected tree: {tree_name}")
    
    all_branches = list_branches(example_root_file, tree_name)
    print(f"All branches in the tree '{tree_name}':")
    for idx, branch in enumerate(all_branches):
        print(f"{idx + 1}: {branch}")

    required_branches = ["eventNumber", "digitX", "digitY", "digitZ"]
    if not check_required_branches(all_branches, required_branches):
        print("Required branches are missing. Exiting.")
        return

    branch_choices = input("Enter branch numbers separated by commas: ")
    branch_indices = [int(x) - 1 for x in branch_choices.split(',')]
    branch_names = [all_branches[idx] for idx in branch_indices if 0 <= idx < len(all_branches)]
    
    if not branch_names:
        print("No valid branches selected. Exiting.")
        return

    print("Choose the output format:\n1: Convert to H5\n2: Convert to SQLite")
    choice = int(input())

    while choice not in [1, 2]:
        print("Please choose either 1 or 2\n")
        choice = int(input())

    if choice == 1:
        output_dir = os.getcwd() + "/root_transformer/Data_forEnergy_h5"  # Directory to save the H5 files
        process_directory(root_dir, tree_name, branch_names, output_dir, convert_branches_to_h5)

    elif choice == 2:
        output_dir = os.getcwd() + "/root_transformer/Data_forEnergy_sqlite"  # Directory to save the SQLite files
        process_directory(root_dir, tree_name, branch_names, output_dir, convert_branches_to_sqlite)

    print("Would you like to view the file? [Y,n]:")
    view = str(input()).strip().lower()

    while view not in ["y", "n", ""]:
        print("Choose either [Y,n]:")
        view = str(input()).strip().lower()

    if view in ["y", ""]:
        if choice == 1:
            h5_files = [f for f in os.listdir(output_dir) if f.endswith('.h5')]
            if h5_files:
                h5_file_path = os.path.join(output_dir, h5_files[0])

                with h5py.File(h5_file_path, 'r') as h5_file:
                    data = {}
                    for branch in branch_names:
                        dataset = h5_file[branch][:]
                        # Handle byte-encoded strings
                        if dataset.dtype.type is np.bytes_:
                            data[branch] = np.array([x.decode('utf-8') for x in dataset])  # Decode bytes to strings
                        else:
                            data[branch] = dataset
                    df = pd.DataFrame(data)
                    df = df.sort_values(by='eventNumber').reset_index(drop=True)
                    print(df)

            else:
                print("No H5 files found in the output directory.")
                
        elif choice == 2:
            sqlite_files = [f for f in os.listdir(output_dir) if f.endswith('.sqlite')]
            if sqlite_files:
                sqlite_db_path = os.path.join(output_dir, sqlite_files[0])
                table_names = get_table_names(sqlite_db_path)

                for table_name in table_names:
                    read_sqlite_table(sqlite_db_path, table_name)
            else:
                print("No SQLite files found in the output directory.")

if __name__ == "__main__":
    main()
