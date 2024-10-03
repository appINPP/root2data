<img width="256" alt="ROOT2Data_1" src="https://github.com/user-attachments/assets/5a104cd6-f1b6-4096-adde-716d1459ffcf"> 

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


## Walkthrough

### Clone the repository
![Screenshot from 2024-10-03 12-44-40](https://github.com/user-attachments/assets/985c0d09-75a7-4035-9125-296ebd91a448)
### Go the executable python code at root2data/src
![Screenshot from 2024-10-03 12-45-44](https://github.com/user-attachments/assets/7c8ed717-6cb3-4205-acb6-ec2934f9247a)
### You are promted to pick an action (I pick 2)
![Screenshot from 2024-10-03 12-46-14](https://github.com/user-attachments/assets/ca964fe4-158c-4eb5-af62-093a4f6e8a66)
### Add root files to the root2data/data/root directory
![Screenshot from 2024-10-03 12-51-56](https://github.com/user-attachments/assets/8d16d686-bba9-4cdd-a266-da5c3d5812ac)
### Your files are now converted to HDF5 format
![Screenshot from 2024-10-03 12-52-56](https://github.com/user-attachments/assets/22cf6911-0bc2-45ae-8b28-b28524348221)
### To change the features you want to extract from the root file to the h5 go to line 203 and add them to the array
![Screenshot from 2024-10-03 12-54-31](https://github.com/user-attachments/assets/d15c771e-27ed-46b9-a9d7-01472b88b8ac)
### After conversion i can now read the h5 file that i created by choosing (1)
![Screenshot from 2024-10-03 12-55-26](https://github.com/user-attachments/assets/d5a3097b-0a68-429c-9de0-24b63249bbc8)
### The HDF5 file is shown as a dataframe
![Screenshot from 2024-10-03 12-56-09](https://github.com/user-attachments/assets/2bb7dba9-176f-4fbb-8413-a5d5a31a637f)




## License

This project is licensed under the Apache License. See the [LICENSE](https://github.com/appINPP/root2data/blob/main/LICENSE) for details.

