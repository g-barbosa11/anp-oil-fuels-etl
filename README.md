## Project Description

The project's primary objective is to perform data extraction from the data source provided by the "Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP)." This data will undergo comprehensive processing to structure and format it as specified below:

### Data Structure

The extracted data will be organized into a structured format with the following columns and corresponding data types:

- **year_month**: Date
- **uf**: String
- **product**: String
- **unit**: String
- **volume**: Double
- **created_at**: Timestamp

### On-premises architecture
Following an ELT approach, we first extract raw data without any modifications to a 'raw_data' folder using Apache Beam. Subsequently, the data is transformed using Dask and loaded into the 'cleaned_data' folder

![Project architecture](docs/architeture.png)

**DISCLAMER**: The Apache Beam is being used in Direct Runner mode. For a cloud environment, it may not be advantageous to use it unless you are working with the GCP (Google Cloud Platform) provider.

### How to run the project

To start the project runs the following command in the terminal:

```sh
bash scripts/setup_docker_compose.sh start_airflow
```

If everything goes well, wait a few seconds and see the airflow in: http://localhost:8080/.

To login:

- **username**: airflow
- **password**: airflow

If everything goes smoothly, it will display the dag-anp on your screen:

![Airflow UI](docs/airflow_ui.png)

Inside the DAG, there are the following tasks:

![Airflow Tasks](docs/airflow_tasks.png)

**Disclaimer**: To execute the tasks, the trigger has been configured to be manually initiated..

To stop the project, just execute:
```sh
bash scripts/setup_docker_compose.sh stop_airflow
```

