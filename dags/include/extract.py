import os
import logging
import typing as t

import pandas as pd
import apache_beam as beam


class ExtractData(beam.DoFn):
    """A DoFn for extracting data from an Excel file and converting it to a DataFrame.

    Args:
        sheet_name (str): The name of the sheet to extract.
    """

    def __init__(self, sheet_name: str) -> t.Union[pd.DataFrame, None]:
        self.sheet_name = sheet_name

    def process(self, element):
        """Reads an Excel file and yields the DataFrame extracted from the specified sheet.

        Args:
            element: The file path of the Excel file.

        Yields:
            pd.DataFrame: The DataFrame extracted from the Excel file.
        """
        file_path = element
        try:
            df = pd.read_excel(file_path, sheet_name=self.sheet_name)
            yield df

        except Exception as e:
            logging.error(f"Error converting to DataFrame: {e}")
            yield None


class ConvertAndWriteCSV(beam.DoFn):
    """A DoFn for converting and writing DataFrames to CSV.

    Args:
        schema: The schema to apply to the DataFrames.
        raw_file_path (str): The path where the CSV files will be written.
    """

    def __init__(self, schema, raw_file_path: str):
        self.schema = schema
        self.raw_file_path = raw_file_path

    def process(self, element: pd.DataFrame) -> None:
        """Converts and writes DataFrames to CSV files.

        Args:
            element: The DataFrame to be converted and written.

        Yields:
            None
        """
        records = element
        if records is not None:
            records = records.astype(self.schema)
            records.to_csv(self.raw_file_path, index=False)
        else:
            logging.error("records is None")


def get_records(file_path: str, sheet_name: str, raw_file_path: str) -> None:
    """Extract data from an Excel file, apply a schema, and write to a CSV file.

    Args:
        file_path: The path to the Excel file.
        sheet_name: The name of the sheet to extract.
        raw_file_path: The path for the CSV file to be written.

    Returns:
        None
    """
    schema = {
        "COMBUSTÍVEL": str,
        "ANO": str,
        "REGIÃO": str,
        "ESTADO": str,
        "Jan": float,
        "Fev": float,
        "Mar": float,
        "Abr": float,
        "Mai": float,
        "Jun": float,
        "Jul": float,
        "Ago": float,
        "Set": float,
        "Out": float,
        "Nov": float,
        "Dez": float,
        "TOTAL": float,
    }

    with beam.Pipeline() as p:
        records = (
            p
            | "Read Excel File" >> beam.Create([file_path])
            | "Extract Data" >> beam.ParDo(ExtractData(sheet_name))
        )

    if os.path.exists(raw_file_path):
        records | "Write to CSV" >> beam.ParDo(
            ConvertAndWriteCSV(schema, raw_file_path)
        )
    else:
        logging.error(f"Invalid destination file path: {raw_file_path}")

    logging.info(f"loading data in {raw_file_path}")
    return None
