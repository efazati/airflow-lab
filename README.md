# Airflow lab

A collection of Airflow DAGs and configurations to demonstrate essential functionalities and common patterns. This repository serves as a hands-on learning tool for Airflow concepts, including task dependencies, parallelism, XCom, custom plugins, and more.

## Quick Start
Start the Airflow environment with Docker Compose:
    ```bash
    docker compose up -d  # Access Airflow at localhost:8080 (default user: airflow, password: airflow)
    ```

## What's New in This Version

- **Upgraded to Apache Airflow 3.0.6**: Latest stable release with improved performance and new features
- **DAG Factory Integration**: All DAGs converted to use YAML-based declarative configuration
- **Organized Structure**: Each DAG now has its own folder with consistent naming (`dag_name/dag_name.yaml` + `dag_name/dag_name.py`)
- **Separation of Concerns**: Custom Python functions are in dedicated modules
- **Environment Configuration**: Proper `.env` file setup for user permissions

![Airflow Dashboard](https://github.com/user-attachments/assets/6c544f96-1fd1-441e-8dba-452f2aecadfa)

## Monitoring with Flower

To enable and access the Flower monitoring tool for Airflow workers:
```bash
docker compose --profile flower up -d  # Access Flower at localhost:5555
```


## DAGs Overview

This project now uses **DAG Factory** with **Apache Airflow 3.0.6** to create DAGs declaratively from YAML configuration files. Each DAG is organized in its own folder with the structure: `dag_name/dag_name.yaml` and `dag_name/dag_name.py`.

### YAML-Based DAGs (using DAG Factory)

| Folder         | Description                                      | Files                                                                |
| -------------- | ------------------------------------------------ | -------------------------------------------------------------------- |
| `parallel_dag/` | A simple DAG that runs tasks in parallel         | `parallel_dag.yaml`, `parallel_dag.py`                              |
| `user_process/` | Demonstrates an HTTP sensor to retrieve and process data | `user_process.yaml`, `user_process.py`, `user_functions.py` (includes database connection) |
| `producer/`     | Demonstrates dataset changes                     | `producer.yaml`, `producer.py`, `producer_functions.py` (works with consumer DAG) |
| `consumer/`     | Processes data when a dataset change is detected | `consumer.yaml`, `consumer.py`, `consumer_functions.py` (pairs with producer DAG) |
| `group_dag/`    | Aggregates multiple tasks into groups for easier management | `group_dag.yaml`, `group_dag.py` (demonstrates task groups) |
| `xcom_dag/`     | Demonstrates data passing between tasks via XComs | `xcom_dag.yaml`, `xcom_dag.py`, `xcom_functions.py` (shows branching and XCom) |
| `elastic_dag/`  | Demonstrates the use of a custom plugin in a DAG | `elastic_dag.yaml`, `elastic_dag.py`, `elastic_functions.py` (requires Elasticsearch connection) |

### Traditional Python DAG

| Folder         | Description                                      | Files                                                                |
| -------------- | ------------------------------------------------ | -------------------------------------------------------------------- |
| `sample-dag/`  | Example of traditional Python DAG creation      | `sample-dag.py` (old-fashioned Airflow DAG without DAG Factory)     |

### Key Features

- **DAG Factory 1.0.0**: All DAGs (except sample-dag) use YAML configuration for declarative DAG creation
- **Apache Airflow 3.0.6**: Latest stable version with improved performance and features
- **Organized Structure**: Each DAG has its own folder with YAML config, Python loader, and custom functions
- **Custom Functions**: Python functions are separated into dedicated modules for better organization
- **Task Groups**: Demonstrated in group_dag for better task organization
- **Datasets**: Producer/Consumer pattern shows data-driven scheduling
- **Custom Plugins**: Elastic DAG shows integration with custom hooks


## DAGs Running History
![image](https://github.com/user-attachments/assets/b2f7a78b-a404-4cb1-ad95-14eabe971a48)



## Dataset Flow
![image](https://github.com/user-attachments/assets/0a4302a3-a171-4d6d-9671-77c264186dc7)


## Execution Records
![image](https://github.com/user-attachments/assets/d3443385-3c3a-49c2-8fa9-768c62e064c3)


## Task Execution Flow (Manual Task Skipping & Triggering)
Airflow allows for manual intervention, enabling users to skip specific tasks and trigger subsequent tasks as needed.
![image](https://github.com/user-attachments/assets/43b7ba21-a488-401f-931c-101cbbd872af)

## XCom Value Inspection
Inspect XCom values by selecting task logs to track data passed between tasks.
![image](https://github.com/user-attachments/assets/53f3e423-7b80-4319-9a4f-7e07d3437002)

##  Worker Performance Monitoring
Track worker performance in real time to optimize DAG scheduling and resource allocation.
![image](https://github.com/user-attachments/assets/74529cf0-7afb-451a-8678-47c0f042e5a4)




