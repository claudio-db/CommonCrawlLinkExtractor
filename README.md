# CommonCrawl Link Extractor Pipeline


This is a toy pipeline to extract external links from CommonCrawl and analyze them. It comprises the following steps:
* `extract`, that extract external links from recent Common Crawl segments into a target database 
* `analyze`, that parses the extracted links and generates metrics/attributes at the website level

A PostgreSQL database is used to collect external links.
The pipeline is Spark-based and orchestrated using Apache Airflow.

## Project Structure

```
root/
├── Dockerfile                  # Dockerfile for the PySpark application
├── docker-compose.yml          # Docker Compose file with Spark app, PostgreSQL database, and Airflow services
├── env-entry-point.sh          # Entry point script to manage the environment
├── requirements.txt            # Python dependencies for the extraction pipeline
├── setup.py                    # Package setup file
├── config-default/             # Core configuration of the extraction pipeline
├── dags/                       # Airflow DAGs directory
│   └── cc_extractor_dag.py     # DAG for the extraction pipeline
├── init-scripts/               # PostgreSQL initialization scripts
│   └── init.sql                # Creates a new database and user (make sure this is specified in the config)

```
The following additional subdirectories will be generated upon executing Airflow:
```
├── configs/                    # Directory storing the full YAML configuration files used as input for the pipeline
├── dist/                       # Directory storing the Python wheel packages
├── output/                     # Directory storing the pipeline outputs
├── logs/                       # Airflow logs
└── plugins/                    # Airflow plugins
```

## Usage

### Initial Setup
As a prerequisite, make sure Docker and Docker Compose are installed in the local environment.
From the repository root directory:
   ```bash   
   # Make the entry point script executable
   chmod +x env-entry-point.sh
   
   # Start the environment
   ./env-entry-point.sh start
   ```
Now, to access Airflow, just go to http://localhost:8080 and login with username `airflow` and password `airflow`

### Using the Extraction Pipeline

#### Option 1: Airflow UI
1. Navigate to the Airflow UI
2. Find the `cc_link_extractor` DAG
3. Click on the DAG and trigger it manually
4. You can customize parameters by using the "Trigger DAG w/ config" option

#### Option 2: CLI
```bash
# Trigger with default parameters
./env-entry-point.sh trigger cc_link_extractor

# Trigger with custom Spark parameters
./env-entry-point.sh trigger cc_link_extractor --conf '{"master": "yarn", "executor_memory": "8g"}'
```

### Pipeline Parameters

TBA

### Manage the Airflow environment

Use the `env-entry-point.sh` script to control the environment:

```bash
# Start the environment
./env-entry-point.sh start

# Check status
./env-entry-point.sh status

# View logs
./env-entry-point.sh logs -f

# Stop the environment
./env-entry-point.sh stop

# Clean up everything
./env-entry-point.sh clean
```

### Connecting to Components

- **Airflow**: http://localhost:8080
- **PostgreSQL**:
  - Host: localhost
  - Port: 5432
  - Databases: 
    - `cc_raw_links_db` (pipeline data)
    - `airflow` (Airflow metadata)
