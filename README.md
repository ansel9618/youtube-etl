# **Youtube API - ETL**

## **Motivation** 

---

The aim of this project is to get familiar with data engineering tools such as Python, Docker & Airflow to produce an ETL data pipeline. To make the pipeline more robust, best practices of; unit & data quality testing and CI/CD are also implemented.

## **Dataset** 

---

As a data source, the Youtube API is used. This API is used instead of, for example, Spotify or Twitter API as these two in particular have been used repeatedly for similar projects. 
The data of this project is pulled from the channel 'Mike Thurston' which is a popular fitness Youtube channel.

## **Summary**

---

This ETL project uses Airflow as an orchestration tool, packaged inside docker containers. The steps that make up the project is as follows:

* Data is **extracted** using the Youtube API with Python scripts. 
* The data is initially loaded into a `staging schema` which is a dockerized PostgreSQL database
* From there, a python script is used for minor data **transformations**. 
* Once transformations are completed the data is **loaded** into the `core schema` (also a dockerized PostgreSQL database) 

The first (initial) API pull loads the data and successive pulls update the values for certain variables (columns). 
Once the core schema is populated and both unit and data quality tests have been implemented, the data is then ready for analysis.
Querying of the data is performed by first accessing the postgres container and then using SQL logic.

The following six variables are extracted from the API: 
1. *Video ID*, 
2. *Video Title*, 
3. *Upload Date*, 
4. *Video Views*,
5. *Likes Count*, 
6. *Comments Count*

## **Architecture** 

---

<p align="center">
  <img width="500" height="400" src="images/project_architecture.png">
</p>

## **Tools & Technologies**

---

* *Containerization* - **Docker**, **Docker Compose**
* *Orchestration* - **Airflow**
* *Data Storage* - **Postgres**
* *Languages* - **Python, SQL**
* *Testing* - **Great Expectations**, **pytest**
* *CI/CD* - **Github Actions**

## **Containerization**

---

To deploy Airflow on Docker, the official [docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/2.6.2/docker-compose.yaml) file is used with some changes.

1. The image used is an extended image, built using a Dockerfile. The below command is used to build an extended image called `extending-image`

```
docker build . -f Dockerfile --pull --tag extending-image:0.0.1
```

Once the image is created the docker compose yaml file can be executed to run the multiple containers with the below command

```
docker compose up -d
```

2. Database Connection and Variables are specified as environment variables. 

The Connection is given in a URI format and has the following naming convention: `AIRFLOW_CONN_{CONN_ID}` 

while the Variables are specified as such: `AIRFLOW_VAR_{VARIABLE_NAME}`

3. A Fernet key is used to encrypt passwords in the connection and variable configuration

## **Orchestration**

---

The DAG can be visualized using the Airflow UI through http://localhost:8080. The DAG can be broken down as follows

* Youtube data pull
* Branching into either Staging Schema Creation or Updating
* Staging schema data quality checks
* Branching into either Core Schema Creation or Updating
* Core schema data quality checks

### **Staging/ Core schema creation**

<p align="center">
  <img width="2000" height="125" src="images/orchestration/schema_creation.png">
</p>

### **Staging/ Core schema updating**

<p align="center">
  <img width="2000" height="125" src="images/orchestration/schema_updating.png">
</p>

One could argue that the DAG design need not have the branching components since the staging/core schema creation is a one time event and successive pulls will always branch along the updating route. This is true however this design was chosen to get accustomed with using the `@task.branch` (BranchPythonOperator) in Airflow and also to use trigger rules such as `none_failed_min_one_success`

## **Testing**

---

Both unit and data quality testing are implemented in this project. Note that data quality tests are performed per each column 

### **Unit tests**

* No DAG import errors,
* DAG exists,
* Number of tasks in DAG is correct
* DAG starts at the correct date and runs once a day at midnight
* DAG tasks are ordered correctly
* DAG triggering on the correct rules
* Airflow Variables & Connections exist
* `TOKENS` list has the correct properties to loop through the YT API data
* Data pull exports a json file for each date 
* API URL gives status 200 code

### **Data quality tests** 

**Staging**

<p align="center">
  <img width="1200" height="200" src="images/orchestration/staging_quality_checks.png">
</p>

* **video_id_check**
  1. Not Null 
  2. Unique
  3. Type Object
  4. Length == 11
* **video_title_check**
  1. Not Null
  2. Type Object
* **upload_date_staging_check**
  1. Not Null
  2. Type Object
* **video_views_check**
  1. Not Null
  2. Type Integer
  3. Value greater than both Likes and Comments
* **likes_count_check**
  1. Not Null
  2. Type Integer
* **comments_count_check**
  1. Not Null
  2. Type Integer
* **rows_check**
  1. Number of rows == Number of Videos

**Core**

<p align="center">
  <img width="300" height="200" src="images/orchestration/core_quality_checks.png">
</p>

* **upload_date_core_check**
  1. No video upload date that predates Youtube Channel joined date

## **CI/CD**

---

WIP

## **Data Storage**

---

To access the postgres container that contains the final data, the below docker command is used

```
docker exec -it youtube_api-postgres-1 bash
```
The staging and core schemas can be observed below after accessing the postgres container. The only difference between the staging and core schema in this project is that the time of upload is removed from the `upload_date` column

**Staging Schema**

<p align="center">
  <img width="900" height="300" src="images/database/staging_schema.png">
</p>

**Core Schema**

<p align="center">
  <img width="900" height="300" src="images/database/core_schema.png">
</p>


Querying can be done using SQL logic. In the below query only the videos that have more than 1 million views are queried and results are sorted descending by `video_views` count

```sql
SELECT 
  *
FROM 
  core.yt_api 
WHERE 
  video_views> 1000000 
ORDER BY 
  video_views DESC;
```

<p align="center">
  <img width="900" height="500" src="images/database/query_videos_above_one_million_views.png">
</p>
