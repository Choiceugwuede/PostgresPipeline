# Streamlining Property Records Management: Efficient PostgreSQL ETL Pipeline for Zipco

## Project Overview

This project examines the evolution of data management in the real estate sector. It focuses on building an efficient ETL (Extraction, Transformation, and Loading) pipeline for property records using a PostgreSQL database for Zipco Real Estate Agency.

## Key Features

- Extraction Layer: pulls data from Realty Mole API.

- Transformation Layer: Cleans the data and transforms it into facts and dimension tables for easy analysis.

- Loading Layer: Efficiently loads the transformed data into PostgreSQL database.

## Skills Demonstrated

- Data Engineering: Designed and implemented an end-to-end ETL pipeline.

- Database Management: Utilized PostgreSQL for efficient data storage and retrieval.

- Python Programming: Wrote scripts for data extraction, transformation, and loading processes.
  
- Problem-Solving: Addressed real-world data management challenges in the real estate sector.

## Tools Used

- Programming Language: Python
  
- Anaconda Powershell Prompt (powered jupyter notebook on local system)
  
- PostgreSQL

- Visual Studio Code

- Ananconda Prompt

## Intallations

-  Download Ananconda [here](https://www.anaconda.com/download).

   Open and run the Jupyter Notebook by entering *jupyter notebook* then click enter.

- Download PostgreSQL [here](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads).

- Download Visual Studio Code [here](https://code.visualstudio.com/download)

## Configuration

1. **Set up PostgreSQL Database:**
    - Download [Postgres pipeline script](postgres_pipeline.py). Update the PostgreSQL connection settings in database connection function in Loading Layer to match your local setup:
      ```python
      connection=psycopg2.connect(
      host='localhost',
      database='postgres',
      port='5432',
      user='your-username',
      password='your-password'
      )
      ```
      
2. **Update File Paths:**
   - The ETL pipeline may require local file paths to be updated to match the directories on your local system.
      Example in Loading Layer 
      ```python
     fact_csv_path=r'C:\Users\USER\PostgresPipeline Project\property_fact.csv'
     load_data_from_csv_to_table(fact_csv_path,'zapbank.fact_table'):
     ```

## Usage

**Run the ETL pipeline using Anaconda Prompt:**

- Open Anaconda Prompt.
    - Navigate to the project directory:
      ```bash
      cd path\to\your\project
      ```
    
    - Run the ETL pipeline:
      ```bash
      python postgres_pipeline.py
      ```

## Documentation

For more details on the processes, refer to the [PostgresPipeline Jupyter Notebook](Postgres_Pipeline.ipynb).


## Acknowledgements
Special thanks to Amdari for the support to make this project possible.
    


