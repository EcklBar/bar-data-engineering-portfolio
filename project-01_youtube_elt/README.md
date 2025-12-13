# **Youtube API - ELT**

## **Architecture** 

<p align="center">
  <img width="500" height="400" src="images/project_architecture.png">
</p>

## **Motivation** 

The aim of this project is to get familiar with data engineering tools such as Python, Docker & Airflow to produce an ELT data pipeline. To make the pipeline more robust, best practices of unit & data quality testing and continuous integration/continuous deployment (CI-CD) are also implemented.

## **Dataset** 

As a data source, the Youtube API is used. The data of this project is pulled from a popular channel - 'MrBeast'.
It is good to note that this project can be replicated for any other Youtube channel you would simply need to change the the Youtube Channel ID/ HandleS. 

## **Summary**

This ELT project uses Airflow as an orchestration tool, packaged inside docker containers. The steps that make up the project are as follows:

1. Data is **extracted** using the Youtube API with Python scripts 
2. The data is initially **loaded** into a `staging schema` which is a dockerized PostgreSQL database
3. From there, a python script is used for minor data **transformations** where the data is then loaded into the `core schema` (also a dockerized PostgreSQL database) 

The first (initial) API pull loads the data - this is the initial **full upload**. 
Successive pulls **upserts** the values for certain variables (columns). Once the core schema is populated and both unit and data quality tests have been implemented, the data is then ready for analysis. 

The following seven variables are extracted from the API: 
* *Video ID*, 
* *Video Title*, 
* *Upload Date*, 
* *Duration*,
* *Video Views*,
* *Likes Count*, 
* *Comments Count*

## **Tools & Technologies**

* *Containerization* - **Docker**, **Docker-Compose**
* *Orchestration* - **Airflow**
* *Data Storage* - **Postgres**
* *Languages* - **Python, SQL**
* *Testing* - **SODA**, **pytest**
* *CI-CD* - **Github Actions**

## **Containerization**

To deploy Airflow on Docker, the official [docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml) file is used with some changes:

1. The image used is an extended image, built using a Dockerfile. This image is pulled/pushed from/to to Docker Hub using the Github Actions CI-CD workflow yaml file. Once the image is created, the docker-compose yaml file can be executed to run the multiple containers - This is also done in the CI-CD workflow.

2. Database Connection and Variables are specified as environment variables. 

The Connection is given in a URI format and has the following naming convention: `AIRFLOW_CONN_{CONN_ID}` 

while the Variables are specified as such: `AIRFLOW_VAR_{VARIABLE_NAME}`

3. A Fernet key is used to encrypt passwords in the connection and variable configuration.

## **Orchestration**

Three DAGs exist, triggered one after the other. These can be accessed using the Airflow UI through http://localhost:8080. The DAGs are as follows;

* *produce_json* - DAG to produce JSON file with raw data
* *update_db* - DAG to process JSON file and insert data into booth staging and core schemas
* *data_quality* - DAG to check the data quality on both layers in the database

## **Data Storage**

To access the Youtube API data, you can either access the postgres docker container and use psql to interact with the database or access a database management tool like Dbeaver and run your queries from there.

## **Testing**

Both unit and data quality testing are implemented in this project using pystest and SODA core respectively.

## **CI-CD**

The CI-CD part of this project is needed for when you make a change the Airflow code, docker image, packages, etc and want to test that the DAGs are still working as expected. CI-CD is implemented using Github Actions.

## **License**

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

