import os
import pandas as pd
from pandas import json_normalize
import re
from utils.common_functions import setup_logging, create_directory

class JobTechDataIngestion:
    def __init__(self, year, cycle_type, period, source_file):
        """
        Initialize the JobTechDataIngestion class.

        Args:
            year (str): The year for data processing.
            cycle_type (str): The cycle type for data processing.
            period (str): The period for data processing.
            source_file (str): The name of the source file for data processing.
        """
        self.logger = setup_logging()
        self.year = year
        self.period = period
        self.countryCode = 'SG'
        self.vendor_code ='jt'
        self.parent_directory = os.path.dirname(os.path.dirname(__file__))
        self.data_directory = os.path.join(self.parent_directory, f'data\{year}')
        self.output_directory = os.path.join(self.parent_directory, f'output\{year}')
        self.recruitment_agencies_filepath = os.path.join(os.path.join(self.parent_directory, "data"), 'Recruitment Companies in JobTech Data.csv')
        self.source_path = os.path.join(self.data_directory, source_file)
        if cycle_type == "quarter":
            self.output_path = os.path.join(self.output_directory, f"outputMergedFile-{self.countryCode}-{self.vendor_code}-{self.year}q-{self.period}.csv")
            self.statics_path = os.path.join(self.output_directory, f"statics-{self.countryCode}-{self.vendor_code}-{self.year}q-{self.period}.csv")
        elif cycle_type == "month":
            # self.quarter = self.get_quarter_from_month(self.period)
            # self.output_path = os.path.join(self.output_directory, f"outputMergedFile-{self.countryCode}-{self.vendor_code}-{self.year}q-{elf.quarter}.csv")
            self.output_path = os.path.join(self.output_directory, f"outputMergedFile-{self.countryCode}-{self.vendor_code}-{self.year}m-{self.period}.csv")
            self.statics_path = os.path.join(self.output_directory, f"statics-{self.countryCode}-{self.vendor_code}-{self.year}m-{self.period}.csv")

    # def get_quarter_from_month(self, month):
    #     return (int(month)-1) // 3 + 1


    def normalize_result_data(self, file_path):
        """
        Load JSON data from a file and normalize the data.

        Args:
            file_path (str): The path to the JSON file.

        Returns:
            DataFrame: The normalized DataFrame.
        """
        df_jobs = pd.read_json(file_path)
        result_data = df_jobs["results"]
        df_result = json_normalize(result_data)
        return df_result

    def output_file(self, df_output, file_path):
        """
        Output a DataFrame to a CSV file.

        Args:
            df_output (DataFrame): The DataFrame to be saved.
            file_path (str): The path to the output CSV file.
        """
        if not df_output.empty:
            try:
                self.logger.info("Please wait while the data gets saved to an excel file...")
                df_output.to_csv(file_path, index=False, encoding="utf-8")
                self.logger.info("Your data has been saved to an excel file!")
                self.logger.info(f"The output DF contains {len(df_output)} rows of data.")
            except Exception as error:
                raise Exception(f"An output error occurred: {error}")

    def clean_job_description(self, df):
        """
        Clean job descriptions by removing HTML tags and special characters.

        Args:
            df (DataFrame): The DataFrame containing job descriptions.

        Returns:
           DataFrame: The cleaned DataFrame.
        """
        # Define a regular expression to remove HTML tags and special characters
        clean_regex = re.compile(r'<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});|nbsp;')

        # Define a function to apply all cleaning steps
        def clean_text(text):
            # Replace &lt; with <
            text = re.sub(r'&lt;', '<', text)
            # Replace &gt; with >
            text = re.sub(r'&gt;', '>', text)
            # Remove HTML tags and special characters
            text = re.sub(clean_regex, ' ', text)
            # Replace extra white spacing
            text = re.sub(r"\s{2,}", ' ', text).strip()
            # Remove break lines
            text = re.sub(r"<|>", '', text)
            # Replace star
            text = re.sub(r'<.*?>', ' ', text)
            return text

        # Apply the cleaning function to the jobDescription column
        df['jobDescription'] = df['jobDescription'].apply(clean_text)
        return df

    def clean_code(self, code):
        """
        Clean SSIC or SSOC codes.

        Args:
            code (str): The code to be cleaned.

        Returns:
            str: The cleaned code.
        """
        return '0' if code in ('None', '', None) or pd.isna(code) else code

    def populate_job_id(self, df):
        """
        Populate the 'jobDedupId' column based on 'jobId' and 'jobDedupId'.

        Args:
            df (DataFrame): The DataFrame to be processed.

        Returns:
            DataFrame: The DataFrame with populated 'jobDedupId' column.
        """
        df['jobDedupId'] = df[['jobId', 'jobDedupId']].apply(
            lambda x: x['jobId'] if x['jobDedupId'] == 'None' else x['jobDedupId'], axis=1
        )
        return df

    def drop_duplicates(self, df):
        """
        Drop duplicate records based on 'jobDedupId'.

        Args:
            df (DataFrame): The DataFrame to be processed.

        Returns:
            DataFrame: The DataFrame with duplicates removed.
        """
        # df.drop_duplicates(subset=['jobDedupId'], inplace=True)
        return df

    def data_cleaning_final(self, df):
        """
        Perform final data cleaning steps.

        Args:
            df (pandas.DataFrame): The DataFrame to be cleaned.

        Returns:
            pandas.DataFrame: The cleaned DataFrame.
        """
        df = self.populate_job_id(df)
        df = self.drop_duplicates(df)
        df = self.clean_job_description(df)
        df['SSOC5dCode'] = df['SSOC5dCode'].apply(self.clean_code)
        df['SSIC5dCode'] = df['SSIC5dCode'].apply(self.clean_code)
        print(df.shape[0])
        print("*"*120)
        return df

    def insert_sfw_job_role(self, df, df_sfw):
        """
        Insert SFW job role, sector, and job role ID into the main DataFrame.

        Args:
            df (DataFrame): Original DataFrame of job posting data.
            df_sfw (DataFrame): DataFrame of jobID to SFW data.

        Returns:
            DataFrame: The final DataFrame with SFW job role information added.
        """
        df_final = pd.merge(df, df_sfw, how='left', left_on='jobId', right_on='job_id')
        column_rename = {
            'jobrole_title': 'sfwJobRole',
            'ssg_sector': 'sfwSector',
            'jobrole_id': 'sfwJobRoleId'
        }
        df_final = df_final.rename(columns=column_rename)
        df_final = df_final.drop(columns=['ssg_track', 'job_id'])
        return df_final

    def insert_recruitment_company(self, df, recruitment_list):
        """
        Insert a column indicating whether a company is a recruitment firm.

        Args:
            df (DataFrame): Original DataFrame of data.
            recruitment_list (list): List of recruitment firms.

        Returns:
            DataFrame: The final DataFrame with an 'isRecruitmentAgency' column.
        """
        df['isRecruitmentAgency'] = df['companyName'].apply(
            lambda x: 1 if x in recruitment_list else 0
        )
        return df

    def statics_data(self, final_df):
        row_count = final_df.shape[0]
        missing_ssoc_count = final_df['SSOC5dCode'].isna().sum()
        missing_ssic_count = final_df['SSIC5dCode'].isna().sum()
        zero_ssoc_count = (final_df['SSOC5dCode'] == "0").sum()
        zero_ssic_count = (final_df['SSIC5dCode'] == "0").sum()

        data = {
            "row_count": [row_count],
            "missing_ssoc_count": [missing_ssoc_count],
            "missing_ssic_count": [missing_ssic_count],
            "zero_ssoc_count": [zero_ssoc_count],
            "zero_ssic_count": [zero_ssic_count]
        }

        statics_df = pd.DataFrame(data)

        return statics_df


    def process_data(self):
        try:
            # recruitment firm
            df_recruitment = pd.read_csv(self.recruitment_agencies_filepath)
            recruitment_list = list(df_recruitment['RecruitmentCompanies'])
            self.logger.info("Loaded recruitment firm data.")
            # job posting file
            df_normalized = self.normalize_result_data(self.source_path)
            # print(df_normalized["SSOC5dCode"].value_counts())
            # print(df_normalized.shape[0])
            self.logger.info("Loaded and normalized job posting data.")
            # sfw data
            df_sfw = pd.read_csv(os.path.join(self.data_directory, f'{self.vendor_code}-jobs-sfw-{self.year}m{self.period}.csv'))
            # df_sfw[df_sfw["job_id"] == '6a4fdcf987b42489c61021594d0b9fb0d5a4c664a72c2f63c6fc66eb4791d065'].to_csv("output/testing.csv")
            self.logger.info("Loaded skills future data.")
            # data transformation
            df = self.insert_sfw_job_role(df_normalized, df_sfw)
            # print("*"*120)
            # print(df.shape[0])
            # print(df["jobId"].value_counts())
            df = self.insert_recruitment_company(df, recruitment_list)
            # print("*"*120)
            # print(df.shape[0])
            final_df = self.data_cleaning_final(df)
            print("*"*120)
            print(final_df["SSOC5dCode"].value_counts())
            # print(final_df["jobId"].value_counts())
            # print(final_df.shape[0])
            statics_df = self.statics_data(final_df)
            # print(statics_df)

            # # write to destination
            create_directory(self.output_directory)
            
            self.output_file(final_df, os.path.join(self.output_directory, self.output_path))
            self.output_file(statics_df, os.path.join(self.output_directory, self.statics_path))
            self.logger.info("Data processing and writing completed.")
        except Exception as error:
            raise Exception(f"An error occurred: {error}")

def main():
    year = input("Year to process: ")
    cycle_type = input("quarter or month: ")
    period = input("Period to process: ")
    source_file = input("Source file to process: ")

    if not year or not cycle_type or not source_file:
        raise ValueError("Year, period and source file must be provided.")
    elif cycle_type == "quarter":
        if not (1 <= int(period) <= 4):
            raise ValueError("The period must between 1 and 4 for quarter.")
    elif cycle_type == "month":
        if not (1 <= int(period) <= 12):
            raise ValueError("The period must between 1 and 12 for month.")

    ingestion = JobTechDataIngestion(year, cycle_type, period, source_file)
    ingestion.process_data()

    # year = "2021"
    # cycle_type = "month"
    # for month in range(1, 13):
    #     # Format month with leading zero if less than 10
    #     source_file = f"jt-jobs-{year}m-{month}.json"
    #     period = f"{month:02d}"
    #     ingestion = JobTechDataIngestion(year, cycle_type, period, source_file)
    #     ingestion.process_data()


if __name__ == "__main__":
    main()
