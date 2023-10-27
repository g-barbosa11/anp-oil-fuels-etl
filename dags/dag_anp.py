from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from include.transform import structure_dim_tables
from include.extract import get_records
from include.constants import TABLES, DEFAULT_ARGS, REMOVE_VALUES, ID_VARS, MONTHS

default_args = DEFAULT_ARGS

with DAG(
    "dag-anp",
    default_args=default_args,
    description="Extract data from ANP source",
    schedule_interval=None,
    catchup=False,
    tags=["Raizen Data Challenge"],
) as dag:
    for table_name, sheet_name in TABLES.items():
        file_path = "dags/dataset/vendas-combustiveis-m3.xls"
        raw_file_path = f"dags/datalake/raw_data/{'{{ds}}'}_{table_name}.csv"
        extract_data_from_anp_source = PythonOperator(
            task_id=f"extract_data_from_{table_name}_anp_source",
            python_callable=get_records,
            op_kwargs={
                "file_path": file_path,
                "sheet_name": sheet_name,
                "raw_file_path": raw_file_path,
            },
        )

        if table_name == "dim_diesel":
            REMOVE_VALUES = None
        output_path = f"dags/datalake/clean_data/{'{{ds}}'}_{table_name}.parquet"
        structure_dim_fuels_table = PythonOperator(
            task_id=f"structure_{table_name}_table",
            python_callable=structure_dim_tables,
            op_kwargs={
                "file_path": raw_file_path,
                "output_path": output_path,
                "remove_values": REMOVE_VALUES,
                "id_vars": ID_VARS,
                "months": MONTHS,
            },
        )

        extract_data_from_anp_source >> structure_dim_fuels_table
