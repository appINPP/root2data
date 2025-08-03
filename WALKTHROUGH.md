## Walkthrough

1. #### Clone the repository.
```
git clone https://github.com/appINPP/root2data.git
```
![Screenshot from 2024-10-03 12-44-40](https://github.com/user-attachments/assets/985c0d09-75a7-4035-9125-296ebd91a448)

2. #### Create the virtual environment, as discusssed above in the prerequisites section.

3. #### Execute the main.py using the following command(**seperate the features with comma while inside quotation marks**).
```
python3 main3.py --features "variable_1.1,variable_1.2,variable_1.3" --root_dir /path/to/root/directory --output parquet/h5/sqlite --mode convert/read --index_col variable_1.1

```

**What it does:**

- **--features**: A comma-separated list of feature columns to extract from the ROOT files (e.g., `feature1,feature2,feature3`).
- **--root_dir**: The path to the directory containing your input ROOT files.
- **--output**: The desired output format. Choose one of `parquet`, `h5`, or `sqlite` to specify whether you want the converted files in Parquet, HDF5, or SQLite format.
- **--mode**: The operation mode. Use `convert` to convert ROOT files to the selected output format, or `read` to read and explore already converted files.
- **--index_col**: The name of the column to be used as the index in the output data (e.g., 'event', 'entry', etc.). This index column is used to uniquely identify and align rows across features and truth tables, and it is essential for proper merging or referencing of related data entries.
Default is "eventNumber".

  **Example usage:**
   
   ![alt text](/images/sc1_1.png)

5. #### When you run the command, the script will display the following message:
   
   ![alt text](/images/sc1_2.png)

   This means the tool is searching the specified directory for all available ROOT files. It will then list how many ROOT files were found and proceed with the conversion or reading process based on your selected options.
   

6.  #### The conversion pipeline is initiated and the files are converted. Our process indicates if the declared features are found and in which root file tree.
   
   ![alt text](/images/sc1_3.png)

7.  #### If you want to read the processed file, you can run the following command:
```
python3 main3.py --features "feature1,feature2,feature3" --root_dir /path/to/root/directory --output parquet --mode read

```
**Example usage:**

   
  ![alt text](/images/sc1_4.png)

  

8. #### When you run the command, the script will display the following message showing all the available parquet files:

![alt text](/images/sc1_5.png)
  
9. #### For the parquet file, you can also read the data structure and print it as a dataframe.
   
  ![alt text](/images/sc1_6.png)

  This means the tool is searching the specified directory for all available Parquet files. It will then list the files found, allowing you to select and explore the data as a DataFrame or perform further analysis.
  
10. #### Usage of format argument while also adding truth argument
      Execute the main.py using the following command(seperate the features and truths with comma while inside quotation marks).

      ```
    python3 main3.py --features "variable_1.1,variable_1.2,variable_1.3" --truth "variable_1.4,variable_1.4,variable_1.6" --root_dir /path/to/root/directory --output parquet --mode read --format graphnet

      ```
      **What it does (with `--truth` and `--format graphnet`):**

- **--features**: A comma-separated list of feature columns to extract from the ROOT files (e.g., `digitQ,digitT`).
- **--truth**: A comma-separated list of "truth" (target/label) columns to extract from the ROOT files (e.g., `trueVtxX,trueVtxY,trueVtxZ`). 
- **--root_dir**: The path to the directory containing your input ROOT files.
- **--output**: The desired output format. Use `parquet` to save the converted files in Parquet format.
- **--mode**: The operation mode. Use `convert` to convert ROOT files to the selected output format.
- **--format graphnet**: After conversion, this option triggers additional post-processing to split the resulting Parquet files into separate "features" and "truth" datasets. This is especially useful for machine learning workflows (such as [GraphNet](https://github.com/graphnet-team/graphnet)
), where features and labels need to be organized separately for training and evaluation.

**Example usage:**
```
python3 main3.py --features "digitQ,digitT" --truth "trueVtxX,trueVtxY,trueVtxZ" --root_dir /home/mike/root2data/data/root --output parquet --mode convert --format graphnet
```
This command will:
- Convert all ROOT files in `/path/to/root/directory` to Parquet format,
- Extract the specified features and truth columns,
- Organize the output so that features and truth values are saved in separate datasets.

![alt text](/images/sc1_7.png)

To read the .parquet files that are created from using the `--format graphnet`, use the following command:

```
python3 main.py --output parquet --mode read --format graphnet --root_dir /path/to/root/directory

```

#### Processed Parquet Directory Structure

After running the conversion with `--format graphnet`, your processed Parquet files will be organized as follows:

```
data/processed_parquet/
├── file1/
│   ├── features/
│   │   ├── Tree;1.parquet
│   │   ├── Tree;2.parquet
│   │   └── ...
│   └── truth/
│       ├── Tree;1.parquet
│       ├── Tree;2.parquet
│       └── ...
```

- **features/**: Contains Parquet files with only the selected feature columns for each input ROOT file.
- **truth/**: Contains Parquet files with only the selected truth (target/label) columns for each input ROOT file.