from datetime import datetime


TABLES = {"dim_fuels": "DPCache_m3", "dim_diesel": "DPCache_m3_2"}


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 27),
}

REMOVE_VALUES = ["ETANOL HIDRATADO", "GLP", "OLEO DISEL"]
ID_VARS = ["COMBUSTÍVEL", "ANO", "ESTADO"]

MONTHS = {
    "Jan": "01",
    "Fev": "02",
    "Mar": "03",
    "Abr": "04",
    "Mai": "05",
    "Jun": "06",
    "Jul": "07",
    "Ago": "08",
    "Set": "09",
    "Out": "10",
    "Nov": "11",
    "Dez": "12",
}
