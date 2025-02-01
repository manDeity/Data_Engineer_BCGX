import yaml

def load_data_from_csv(spark_session, csv_location):
    """
    Load data from a CSV file into a DataFrame.
    :param spark_session: Spark session instance
    :param csv_location: Path to the CSV file
    :return: DataFrame containing the CSV data
    """
    return spark_session.read.option("inferSchema", "true").csv(csv_location, header=True)

def read_yaml_config(config_path):
    """
    Load configuration from a YAML file.
    :param config_path: Path to the YAML configuration file
    :return: Dictionary containing configuration details
    """
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def save_dataframe(df, output_path, format_type):
    """
    Save a DataFrame to a file in the specified format.
    :param df: DataFrame to save
    :param output_path: Path where the output file will be saved
    :param format_type: Format of the output file
    :return: None
    """
    df.repartition(1).write.format(format_type).mode("overwrite").option("header", "true").save(output_path)
