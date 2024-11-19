#!/bin/bash
python3 -m venv root2data

. ./root2data/bin/activate # dot (.) is a synonym for source in bash.

pip install --upgrade pip

pip install pandas numpy awkward uproot h5py psycopg2-binary pyarrow

# execute the script by running the following command in the terminal: bash create_env.sh source create_env.sh