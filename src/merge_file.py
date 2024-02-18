import pandas as pd
import os
from utils.common_functions import setup_logging

logger = setup_logging()

def main(year):
    """
    Merge all output file into one file within yearly folder
    Args:
        year (str): Year to be merged
    Returns:
        Yearly merged file in 
    """
    # Path to the directory containing CSV files
    parent_directory = os.path.dirname(os.path.dirname(__file__))
    output_directory = os.path.join(parent_directory, f'output\{year}')

    # Initialize an empty list to store DataFrames
    dfs = []

    # Iterate over each file in the directory
    for filename in os.listdir(output_directory):
        if filename.startswith("outputMergedFile-SG"):
            logger.info(f"Merging {filename}")
            # Read the CSV file into a DataFrame
            df = pd.read_csv(os.path.join(output_directory, filename))
            # Append the DataFrame to the list
            dfs.append(df)

    # Concatenate all DataFrames into one
    merged_df = pd.concat(dfs, ignore_index=True)

    # Path to save the merged CSV file
    merged_csv_path = f'{output_directory}/merged_outputMergedFile-SG-jt-{year}.csv'

    # Save the merged DataFrame to a CSV file
    merged_df.to_csv(merged_csv_path, index=False)

    logger.info(f"Merged CSV file: merged_outputMergedFile-SG-jt-{year}.csv successfully!")

if __name__ == "__main__":
    year = input("Year to process: ")
    main(year)