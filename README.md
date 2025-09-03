# Airflow lab

A collection of Airflow DAGs and configurations to demonstrate essential functionalities and common patterns. This repository serves as a hands-on learning tool for Airflow concepts, including task dependencies, parallelism, XCom, custom plugins, and more.

## Quick Start
Start the Airflow environment with Docker Compose:
    ```bash
    docker compose up -d  # Access Airflow at localhost:8080 (default user: airflow, password: airflow)
    ```

![Airflow Dashboard](https://github.com/user-attachments/assets/6c544f96-1fd1-441e-8dba-452f2aecadfa)

## Monitoring with Flower

To enable and access the Flower monitoring tool for Airflow workers:
```bash
docker compose --profile flower up -d  # Access Flower at localhost:5555
```


## DAGs Overview

| Python File    | Description                                      | Notes                                                                |
| -------------- | ------------------------------------------------ | -------------------------------------------------------------------- |
| `parallel_dag` | A simple DAG that runs tasks in parallel         |                                                                      |
| `user_process` | Demonstrates an HTTP sensor to retrieve and process data | Includes a database connection configured via the Airflow UI         |
| `producer`     | Demonstrates dataset changes                     | Works in conjunction with the `consumer` DAG                         |
| `consumer`     | Processes data when a dataset change is detected | Pairs with the `producer` DAG                                        |
| `group_dag`    | Aggregates multiple tasks into a single group for easier management | Custom functions are located in the `group` directory               |
| `xcom_dag`     | Demonstrates data passing between tasks via XComs | Uses custom functions with parameters `ti` and returns task IDs to trigger the next task |
| `elastic_dag`  | Demonstrates the use of a custom plugin in a DAG | Requires a connection setup in the Airflow UI; custom plugin functions are located in the `plugins` directory |


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




