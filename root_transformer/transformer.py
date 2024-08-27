import os
import uproot
import h5py
import numpy as np
import sqlite3
import pandas as pd

# Function to list all branches in the TTree
def list_branches(root_file_path, tree_name):
    with uproot.open(root_file_path) as file:
        tree = file[tree_name]
        branches = tree.keys()
        return branches

# Function to convert specific branches of a ROOT file to an H5 file
def convert_branches_to_h5(root_file_path, tree_name, branch_names, h5_file_path):
    # Open the ROOT file
    with uproot.open(root_file_path) as file:
        # Access the TTree
        tree = file[tree_name]

        # Create a new H5 file
        with h5py.File(h5_file_path, 'w') as h5_file:
            # Iterate over the list of branch names
            for branch_name in branch_names:
                # Check if the branch exists
                if branch_name not in tree.keys():
                    print(f"Branch '{branch_name}' not found in the tree '{tree_name}'")
                    continue

                # Read the specific branch
                branch_data = tree[branch_name].array(library="np")

                # Convert object dtype to a compatible type if necessary
                if branch_data.dtype == np.dtype('O'):
                    # Attempt to convert to numeric type if possible
                    try:
                        branch_data = branch_data.astype(np.float64)
                    except ValueError:
                        # Handle conversion to string if numeric conversion fails
                        branch_data = np.array([str(item) for item in branch_data], dtype='S')

                # Write the branch data to the H5 file
                h5_file.create_dataset(branch_name, data=branch_data)
                print(f"Branch '{branch_name}' has been successfully written to {h5_file_path}")
                
                
# Function to convert specific branches of a ROOT file to an SQLite database
def convert_branches_to_sqlite(root_file_path, tree_name, branch_names, sqlite_db_path):
    # Open the ROOT file
    with uproot.open(root_file_path) as file:
        # Access the TTree
        tree = file[tree_name]

        # Create a new SQLite database or connect to an existing one
        conn = sqlite3.connect(sqlite_db_path)
        cursor = conn.cursor()
        
        # Create a sanitized table name
        base_name = os.path.splitext(os.path.basename(root_file_path))[0]
        sanitized_table_name = f'"{base_name.replace(".", "_")}"'
        
        # Create columns for the table
        columns = ", ".join([f'"{branch_name}" REAL' for branch_name in branch_names])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {sanitized_table_name} ({columns})")
        
        # Prepare data for insertion
        data = []
        for branch_name in branch_names:
            # Check if the branch exists
            if branch_name not in tree.keys():
                print(f"Branch '{branch_name}' not found in the tree '{tree_name}'")
                continue

            # Read the specific branch
            branch_data = tree[branch_name].array(library="np")

            # Convert object dtype to a compatible type if necessary
            if branch_data.dtype == np.dtype('O'):
                # Attempt to convert to numeric type if possible
                try:
                    branch_data = branch_data.astype(np.float64)
                except ValueError:
                    # Handle conversion to string if numeric conversion fails
                    branch_data = np.array([str(item) for item in branch_data], dtype='S')

            data.append(branch_data)

        # Transpose the data to match the table structure
        data = np.column_stack(data)
        
        # Insert data into the SQLite database
        placeholders = ", ".join(["?"] * len(branch_names))
        cursor.executemany(f"INSERT INTO {sanitized_table_name} VALUES ({placeholders})", data)
        
        # Commit and close the database connection
        conn.commit()
        conn.close()
        print(f"Data from '{root_file_path}' has been successfully written to {sqlite_db_path}")



# Function to process all ROOT files in a directory
def process_directory(root_dir, tree_name, branch_names, output_dir):
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Iterate over all files in the directory
    for filename in os.listdir(root_dir):
        if filename.endswith(".root"):
            root_file_path = os.path.join(root_dir, filename)
            # Create a corresponding H5 file name
            base_name = os.path.splitext(filename)[0]
            h5_file_path = os.path.join(output_dir, f"{base_name}.h5")
            
            # Convert the branches and save to H5
            print(f"Processing {root_file_path}...")
            convert_branches_to_h5(root_file_path, tree_name, branch_names, h5_file_path)
            
def get_table_names(sqlite_db_path):
    # Connect to the SQLite file
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    
    # Query to retrieve the names of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    # Close the database connection
    conn.close()
    
    # Return the table names
    return [table[0] for table in tables]

def read_sqlite_table(sqlite_db_path, table_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    
    # Query to retrieve all data from the specified table
    query = f'SELECT * FROM "{table_name}"'
    cursor.execute(query)
    
    # Fetch all rows from the executed query
    rows = cursor.fetchall()
    
    # Fetch the column names
    column_names = [description[0] for description in cursor.description]
    
    # Print the table name
    print(f"Table: {table_name}")
    print("-" * (len(column_names) * 15))
    
    # Print the column names
    print(" | ".join(column_names))
    print("-" * (len(column_names) * 15))
    
    # Print each row
    for row in rows:
        print(" | ".join(map(str, row)))
    
    print("\n")
    
    # Close the database connection
    conn.close()


def main():
    root_dir = os.getcwd()+"/Data/Data_forEnergy"  # Directory containing ROOT files
    tree_name = "phaseIITriggerTree;1"  # Name of the TTree
    branch_names = ["digitX", "digitY", "digitZ", "eventNumber"]  # List of branch names to be converted

    print("Welcome to the file transformer script! \nChoose 1 for root -> h5\nChoose 2 for root -> sqlite")
    choice = int(input())

    while choice not in [1, 2]:
        print("Please choose either 1 or 2\n")
        choice = int(input())

    if choice == 1:
        output_dir = os.getcwd()+"/Data_forEnergy_h5"  # Directory to save the H5 files
        # List all branches in the first ROOT file
        example_root_file = os.path.join(root_dir, os.listdir(root_dir)[0])
        all_branches = list_branches(example_root_file, tree_name)
        print(f"All branches in the tree '{tree_name}':")
        for branch in all_branches:
            print(branch)

        # Process the directory
        process_directory(root_dir, tree_name, branch_names, output_dir)

    elif choice == 2:
        output_dir = os.getcwd()+"/Data/Data_forEnergy_sqlite"  # Directory to save the SQLite files
        # Ensure the output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Iterate over all files in the directory
        for filename in os.listdir(root_dir):
            if filename.endswith(".root"):
                root_file_path = os.path.join(root_dir, filename)
                # Create a corresponding SQLite database file name
                base_name = os.path.splitext(filename)[0]
                sqlite_db_path = os.path.join(output_dir, f"{base_name}.sqlite")

                # Convert the branches and save to SQLite
                print(f"Processing {root_file_path}...")
                convert_branches_to_sqlite(root_file_path, tree_name, branch_names, sqlite_db_path)

    print("Would you like to view the file? [Y,n]:")
    view = str(input()).strip().lower()

    while view not in ["y", "n", ""]:
        print("Choose either [Y,n]:")
        view = str(input()).strip().lower()

    if view in ["y", ""]:
        if choice == 1:
            h5_files = [f for f in os.listdir(output_dir) if f.endswith('.h5')]
            if h5_files:
                # Choose the first .h5 file from the list
                h5_file_path = os.path.join(output_dir, h5_files[0])

                with h5py.File(h5_file_path, 'r') as h5_file:
                    # Read the necessary datasets and convert each entry to a list of floats
                    digitX = [np.fromstring(x.decode('utf-8').strip('[]'), sep=' ') for x in h5_file['digitX'][:]]
                    digitY = [np.fromstring(y.decode('utf-8').strip('[]'), sep=' ') for y in h5_file['digitY'][:]]
                    digitZ = [np.fromstring(z.decode('utf-8').strip('[]'), sep=' ') for z in h5_file['digitZ'][:]]
                    eventNumber = h5_file['eventNumber'][:]

                    # Create a DataFrame directly from the lists
                    df = pd.DataFrame({
                        'digitX': digitX,
                        'digitY': digitY,
                        'digitZ': digitZ,
                        'eventNumber': eventNumber
                    })

                    # Sort the DataFrame by 'eventNumber'
                    df = df.sort_values(by='eventNumber').reset_index(drop=True)

                    print(df)  # Prints the DataFrame in a table format

            else:
                print("No H5 files found in the output directory.")
                
        elif choice == 2:
            # Get the first SQLite file in the output directory
            sqlite_files = [f for f in os.listdir(output_dir) if f.endswith('.sqlite')]
            if sqlite_files:
                sqlite_db_path = os.path.join(output_dir, sqlite_files[0])
                # Get all table names
                table_names = get_table_names(sqlite_db_path)

                # Print all tables
                for table_name in table_names:
                    read_sqlite_table(sqlite_db_path, table_name)
            else:
                print("No SQLite files found in the output directory.")


if __name__ == "__main__":
    main()
