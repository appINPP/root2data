<img width="243" alt="root2data_logo" src="https://github.com/user-attachments/assets/4b9d8739-b41d-4d2d-b84a-ee4724f9e00c"> 

# Convert .root files to other data formats

# ROOT to HDF5 Converter

## Abstract

This repository provides a Python toolset for converting ROOT files to another format. 
It includes functionalities for reading data from ROOT files, saving them as HDF5 files, and optionally storing the data in SQLite databases. 
Additionally, it offers tools to explore the structure of HDF5 files as a dataframe.

**Prerequisites**

```
pip install -r requirements.txt
```

## Usage

1) Directory Structure: Ensure you have the following directories set up in your working directory:
    - data/root: Directory containing ROOT files.
    - data/h5: Directory where HDF5 files will be saved.
    
2) User Interface: Upon running the script, you'll be prompted to choose one of the following options:
    - Read an existing HDF5 file.
    - Convert ROOT files to HDF5 format.
    - Exit the program.

## Functions:

- create_dataframe_and_show_structure : Creates a dataframe of the root file so that it is easy to view and perform data analysis tasks.
- read_h5_file : Reads and prints the content of a HDF5 file
- root2h5 : Converts root -> HDF5, with arguments:
    - The path to the root file.
    - List of column names to extract.
    - the path to save the h5 file.
- scan_for_new_root_files : Searches a given directory for root files that can be converted.
- list_h5_files : Display all h5 files so that the user can pick one to view.

### Example

![demo](https://github.com/user-attachments/assets/d49a68ae-dff3-4ec4-b0f3-1aab664b7dbf)











## License

This project is licensed under the Apache License. See the [LICENSE](https://github.com/appINPP/root2data/blob/main/LICENSE) for details.

