# Jobtech Data Ingestion (Main Pipeline)

## _Overview_
For transforming raw data sourced from Jobtech into stage 1 data formatted as CSV files.

## _Prerequisites_
Before initiating the transformation, ensure the following conditions are met:

A yearly folder within the data directory containing the following files:
1. jt-jobs-{year}m-{month}.json
2. jt-jobs-cwkft-{year}m{month}.csv
3. jt-jobs-sfw-{year}m{month}.csv
4. jt-jobs-ssec-fos-{year}m{month}.csv
5. jt-jobs-tsc-{year}m{month}.csv
6. Recruitment Companies in JobTech Data.csv (outside yearly folder)

## _Instructions_
1. Open the terminal and execute the command **python src/main.py** to initiate the transformation process.

2. Input the following parameters:

    i. year: The year of the data to be processed
    ii. cycle_type: Specify either 'quarter' or 'month'
    iii. period: Enter a value between 1 and 12 for months or 1 and 4 for quarters
    iv. source_file: Provide the path to the JSON file containing job data

## _Output_
1. outputMergedFile-SG-jt-{year}m{month}.csv in output folder on yearly basis

# Jobtech Data Ingestion (Merge Files)

## _Overview_
This section describes the process of merging all output CSV files that start with 'outputMergedFile-SG' within the yearly folder.

## _Prerequisites_
Before commencing the merge operation, ensure the following condition is met:

A yearly folder within output directory containing a file named as follows:

1. outputMergedFile-SG-jt-{year}m{month}.csv

## _Instructions_
1. Open the terminal and execute the command **python src/merge_file.py** to initiate the merging process.

2. Input the following parameter:

    i. year: Specify the year of the data to be processed

## _Output_
1. merged_outputMergedFile-SG-jt-{year}.csv in output folder on yearly basis