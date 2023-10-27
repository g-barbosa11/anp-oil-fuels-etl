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

**DISCLAMER**: The Apache Beam is being used in direct runner mode. For a cloud environment, it may not be advantageous to use it unless you are working with the GCP (Google Cloud Platform) provider.