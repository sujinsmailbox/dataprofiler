import pandas as pd
import numpy as np
import os
from collections import defaultdict
import csv
import ast
import logging


class DataCleaner:
    def __init__(self, config):
        self.null_values = config.null_value if hasattr(config, 'null_value') else None
        self.replace_values = config.replace_values if hasattr(config, 'replace_values') else None
        self.upper_ranges = config.upper_range if hasattr(config, 'upper_range') else None
        self.lower_ranges = config.lower_range if hasattr(config, 'lower_range') else None
        self.trim_data = config.lower_range if hasattr(config, 'lower_range') else None
        self.input_file = None
        self.output_file = None
        self.sample_file = None
        self.data_frame = None
        self.header_row_data_type = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def clean_data(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
        self.generate_sample()

        columns = defaultdict(list)
        with open(self.sample_file) as input_file:
            dict_reader = csv.DictReader(input_file)
            for row in dict_reader:
                for header, value in row.items():
                    columns[header].append(value)

        for header, value_list in columns.items():
            value_type = self.determine_type(value_list)
            self.logger.info(f"Type of {header} is {value_type}")
            self.header_row_data_type[header] = value_type

        self.data_frame = pd.read_csv(self.input_file)

        if self.null_values is not None:
            self.process_null_values(self.null_values)

        if self.replace_values is not None:
            self.process_replace_values(self.replace_values)

        self.data_frame.to_csv(self.output_file, index=False)

    def generate_sample(self):
        df = pd.read_csv(self.input_file, skiprows=range(1, 100), nrows=500)
        df.replace(r'^\s+$', np.nan, regex=True, inplace=True)
        df.replace("NULL", np.nan, inplace=True)
        df.replace("0", np.nan, inplace=True)
        df.fillna(method="backfill", inplace=True)
        self.sample_file = os.path.splitext(self.input_file)[0] + '_sample.csv'
        df.to_csv(self.sample_file, index=False)

    def determine_type(self, value_list):
        value_type_and_count = {}
        for value in value_list:
            try:
                value_type = type(ast.literal_eval(str(value)))
            except (ValueError):
                value_type = type(value)
            except Exception:
                continue
            #self.logger.info(f"After conversion, type = {type(value_type)} , value = {value}")
            if value_type not in value_type_and_count:
                value_type_and_count[value_type] = 1
            else:
                value_type_and_count[value_type] += 1

        if len(value_type_and_count.keys()) > 1:
            self.logger.info(" More than one type of value exists. Treating as an object")
        if len(value_type_and_count.keys()) == 0:
            return object
        return max(value_type_and_count, key=value_type_and_count.get)

    def process_null_values(self, null_values):
        if hasattr(null_values, 'type'):
            if hasattr(null_values.type, "string"):
                for column_name, column_type in self.header_row_data_type.items():
                    if column_type == str:
                        for replace_value in null_values.type.string:
                            self.logger.info(f"string: Replacing nan for {replace_value} for column = {column_name}")
                            self.data_frame[column_name].replace(replace_value, np.nan, inplace=True)
            if hasattr(null_values.type, "int"):
                for column_name, column_type in self.header_row_data_type.items():
                    if column_type == int:
                        for replace_value in null_values.type.int:
                            self.logger.info(f"int: Replacing nan for {replace_value} for column = {column_name}")
                            self.data_frame[column_name].replace(replace_value, np.nan, inplace=True)
            if hasattr(null_values.type, "float"):
                for column_name, column_type in self.header_row_data_type.items():
                    if column_type == float:
                        for replace_value in null_values.type.string:
                            self.logger.info(f"float: Replacing nan for {replace_value} for column = {column_name}")
                            self.data_frame[column_name].replace(replace_value, np.nan, inplace=True)

        if hasattr(null_values, 'column'):
            # process null values by name
            pass

    def process_replace_values(self, replace_values):
        pass