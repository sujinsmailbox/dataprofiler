from DataProfiler import DataProfiler
from DataCleaner import DataCleaner
from Config import Config
import logger_config


def run(config_file_path):
    config_obj = Config(config_file_path)
    config_obj.reload()
    parsed_data = config_obj.parse_all_data()

    logger = logger_config.get_logger(parsed_data.logging)
    logger.info(parsed_data.data_cleaning)

    if parsed_data.data_cleaning.clean_data == "True":
        cleaner = DataCleaner(parsed_data.data_cleaning.clean_data_config)
        cleaner.clean_data(parsed_data.input_file, parsed_data.cleaned_file)

    profiler = DataProfiler(parsed_data.profile_data, cleaner.header_row_data_type, cleaner.data_frame)
    if parsed_data.data_cleaning.clean_data == "True":
        profiler.calculate_stats(parsed_data.cleaned_file, parsed_data.output_file)
    else:
        profiler.calculate_stats(parsed_data.input_file, parsed_data.output_file)

if __name__ == '__main__':
    config = {
        "input_file": "/Users/a843219yara.com/code/DataProfiler/whole_with_clusters copy.csv",
        "output_file": "/Users/a843219yara.com/code/DataProfiler/output.csv",
        "generate_stat_list": ["count", "null_count",
                               "percent_null", "blank_count", "minimum_value", "maximum_value", "mode", "pattern_count",
                               "unique_count", "uniqueness", "primary_key_candidate", "data_type", "data_length",
                               "actual_type", "minimum_length", "maximum_length", "mean", "median", "non_null_count",
                               "standard_deviation", "input_file"],
        "mobile_number": ["column1", "column2"],
        "clean_data_config": {
            "clean_data": "True",
            "clean_data_config":
            {"null_value":
                 {"type":{"string": ["NULL", "", "nan", "0"], "int": ["0", "nan", ""], "float": []},
                "name": {"column1":[], "column2" :[]}},
            "replace_values":
                {"types": {"string": [], "int": []}, "name": {"column1": [], "column2": []},
                 "name": {"column1":[], "column2": []}},
                "upper_range": {"types": {"int": 10000, "string": 256}, "names": {"column1": 2, "column2": 3}}
             }
        }
    }

    run("/Users/a843219yara.com/code/DataProfiler/config.txt")
