import csv
from collections import defaultdict
import pandas as pd
import numpy as np
import ast
import logging


class DataProfiler:
    def __init__(self, profile_data, header_row_data_type, data_frame):
        self.profile_data = profile_data
        self.input_file = None
        self.output_file = None
        self.header_row_data_type = header_row_data_type
        self.data_frame = data_frame
        self.logger = logging.getLogger(self.__class__.__name__)

    def create_output_file(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.write_header_row(output_file)
        self.write_index_column(input_file)

    def write_header_row(self, output_file):
        header_fields = ("Field_Name", *self.profile_data)

        with open(self.output_file, "w") as output_file:
            writer = csv.writer(output_file, delimiter=",")
            # self.logger.info([fields for fields in header_fields if fields not in (None, self.input_file, self.output_file)])
            # writer.writerow(
            #     [fields for fields in header_fields if fields not in (None, self.input_file, self.output_file)])
            writer.writerow(header_fields)

    def write_index_column(self, input_file):
        with open(self.output_file, "a+") as output_file, open(self.input_file, "r+") as input_file:
            # reader = csv.reader(input_file, delimiter=",")
            writer = csv.writer(output_file)
            line = input_file.readline().strip()
            for fields in line.split(","):
                writer.writerow([fields])

    def flush_output(self, column_name, value, operation):
        # with open(self.output_file, 'a') as output_file:
        #     writer = csv.DictWriter(output_file, )
        self.logger.info(f'column_name={column_name}, value={value}, operation={operation}')
        if value in (None, ''):
            value = np.nan
        df = pd.read_csv(self.output_file, index_col="Field_Name")
        df.at[column_name, operation] = value
        df.to_csv(self.output_file)

    def calculate_stats(self, input_file, output_file):
        self.create_output_file(input_file, output_file)

        #for each_columns in input_file. accumulate the df in a dictionary
        # columns = defaultdict(list)
        # with open(self.input_file) as input_file:
        #     dict_reader = csv.DictReader(input_file)
        #     for row in dict_reader:
        #         for column_name, value in row.items():
        #             columns[column_name].append(value)

        for column_name, value_type in self.header_row_data_type.items():
            #df = pd.DataFrame(value_list)
            df = self.data_frame[column_name]
            if "count" in self.profile_data:
                result = self.calculate_count(df)
                self.flush_output(column_name, result, operation="count")
            if "null_count" in self.profile_data:
                result = self.calculate_null_count(df)
                self.flush_output(column_name, result, operation="null_count")
            if "non_null_count" in self.profile_data:
                result = self.calculate_non_null_count(df)
                self.flush_output(column_name, result, operation="non_null_count")
            if "percent_null" in self.profile_data:
                result = self.calculate_percent_null(df)
                self.flush_output(column_name, result, operation="percent_null")
            if "unique_count" in self.profile_data:
                result = self.calculate_unique_count(df)
                self.flush_output(column_name, result, operation="unique_count")
            if "primary_key_candidate" in self.profile_data:
                result = self.calculate_primary_key_candidate(df)
                self.flush_output(column_name, result, operation="primary_key_candidate")
            if "data_type" in self.profile_data:
                result = self.calculate_data_type(column_name)
                self.flush_output(column_name, result, operation="data_type")
            if "minimum_length" in self.profile_data:
                result = self.calculate_minimum_length(df)
                self.flush_output(column_name, result, operation="minimum_length")
            if "maximum_length" in self.profile_data:
                result = self.calculate_maximum_length(df)
                self.flush_output(column_name, result, operation="maximum_length")
            if "minimum_value" in self.profile_data:
                result = self.calculate_minimum_value(df, column_name)
                self.flush_output(column_name, result, operation="minimum_value")
            if "maximum_value" in self.profile_data:
                result = self.calculate_maximum_value(df, column_name)
                self.flush_output(column_name, result, operation="maximum_value")
            if "mean" in self.profile_data:
                result = self.calculate_mean(df, column_name)
                self.flush_output(column_name, result, operation="mean")
            if "median" in self.profile_data:
                result = self.calculate_median(df, column_name)
                self.flush_output(column_name, result, operation="median")
            if "mode" in self.profile_data:
                result = self.calculate_mode(df, column_name)
                self.flush_output(column_name, result, operation="mode")
            if "standard_deviation" in self.profile_data:
                result = self.calculate_standard_deviation(df, column_name)
                self.flush_output(column_name, result, operation="standard_deviation")

    def calculate_count(self, df):
        return len(df)

    def calculate_null_count(self, df):
        return df.isnull().sum()

    def calculate_non_null_count(self, df):
        return df.count()

    def calculate_percent_null(self, df):
        return df.isnull().sum() * 100 / len(df)

    def calculate_unique_count(self, df):
        df.dropna(inplace=True)
        return len(pd.unique(df))

    def calculate_primary_key_candidate(self, df):
        df.dropna(inplace=True)
        return 1 if len(pd.unique(df)) == len(df) and len(df) > 0 else 0

    def calculate_data_type(self, column_name):
        #return header_row_data_type.get(column_name, object)
        if column_name in self.header_row_data_type.keys():
            return self.header_row_data_type[column_name]
        else:
            self.logger.warning("Invalid type")
            return object

    def calculate_minimum_length(self, df):
        #table = df.apply(lambda x: x if x not in (np.nan, 'NULL', '', 0) else np.nan).to_frame()
        df.dropna(inplace=True)
        return df.astype('str').map(len).min()

    def calculate_maximum_length(self, df):
        #table = df.iloc[:, 0].apply(lambda x: x if x not in (np.nan, 'NULL', '', 0) else np.nan).to_frame()
        df.dropna(inplace=True)
        return df.astype('str').map(len).max()

    def calculate_minimum_value(self, df, column_name):
        #df = df[0].replace('', np.nan).dropna().astype(self.header_row_data_type[column_name]).min(skipna=True)
        df.dropna(inplace=True)
        return df.min() if self.header_row_data_type[column_name] in (int, float) else np.nan

    def calculate_maximum_value(self, df, column_name):
        #return df.astype(self.header_row_data_type[column_name]).max(skipna=True)
        df.dropna(inplace=True)
        return df.max() if self.header_row_data_type[column_name] in (int, float) else np.nan

    def calculate_mean(self, df, column_name):
        #return df.astype(self.header_row_data_type[column_name]).mean(skipna=True)
        df.dropna(inplace=True)
        return df.mean() if self.header_row_data_type[column_name] in (int, float) else np.nan

    def calculate_median(self, df, column_name):
        #return df.astype(self.header_row_data_type[column_name]).median(skipna=True)
        df.dropna(inplace=True)
        return df.median() if self.header_row_data_type[column_name] in (int, float) else np.nan

    def calculate_mode(self, df, column_name):
        #return df.astype(self.header_row_data_type[column_name]).mode(dropna=True).min()
        df.dropna(inplace=True)
        return df.mode().min() if self.header_row_data_type[column_name] in (int, float) else np.nan

    def calculate_standard_deviation(self, df, column_name):
        #return df.astype(self.header_row_data_type[column_name]).std(skipna=True)
        df.dropna(inplace=True)
        return df.std() if self.header_row_data_type[column_name] in (int, float) else np.nan
