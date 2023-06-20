# Big Data
This repository contains three different project of the Big Data course I took as part of the MSc in Fundamental Principles of Data Science at Universitat de Barcelona

## Assignment 1: Docker and MongoDB
The assignment comprises several tasks, including creating keys to collect data from the Weather, dockerizing the application to collect data from a Weather API, publishing the Docker image in Docker Hub, and modifying the application to store data in MongoDB.

By completing this assignment, students will gain hands-on experience in:

Obtaining credentials to access data from the Weather API and collecting data from public sources.
Dockerizing their Python application to ensure portability and ease of deployment.
Publishing their Docker image in Docker Hub to make it accessible to others.
Adapting the application to save data in MongoDB and connecting to it using the PyMongo library.
Utilizing Docker Compose to orchestrate the simultaneous execution of the updated application and MongoDB.
Overall, the assignment provides students with practical knowledge and skills in data ingestion, preprocessing, containerization, and working with popular technologies such as Docker and MongoDB in the big data domain.

## Assignment 2: PySpark
In the second assignment of the Big Data course, the focus is on analyzing data using Apache Spark. The assignment requires writing code to find answers to specific questions using different approaches: RDD, DataFrames without temporary tables, and Spark SQL with temporary tables.

The assignment aims to provide hands-on experience in performing big data analytics using different approaches available in Apache Spark. By completing this assignment, students will gain proficiency in RDD operations, DataFrame manipulations, and Spark SQL queries for data analysis.

## Assignment 3: Airflow
In the third assignment of the Big Data course, the objective is to create a simple batch mode ML model pipeline using Apache Airflow. The pipeline will train a machine learning (ML) model based on data stored in an S3 bucket and print the predictions.

The assignment tasks can be summarized as follows:

- Write a DAG (Directed Acyclic Graph) code:
- Create a DAG in Apache Airflow to define the workflow of the ML model pipeline.
- The DAG should consist of the following tasks:
  - Task 1 (T1): Download all CSV files from the specified S3 bucket and store them locally.
  - Task 2 (T2): Read the downloaded CSV files, train the ML model, and save the trained model locally.
  - Task 3 (T3): Download the prediction.csv file from S3 and save it locally.
  - Task 4 (T4): Load the locally stored ML model, read the downloaded prediction CSV, and save a new CSV file with predictions for each input.

Define the DAG schedule:
  - Set the DAG schedule to run at 8 PM every Monday.
  - Utilize Airflow variables:
      - Use Airflow variables to define the S3 path for data storage and retrieval.
      - Use another Airflow variable to specify the local path for storing the downloaded files and other outputs.

The assignment aims to provide hands-on experience in creating an ML model pipeline in a production environment using Apache Airflow. By completing this assignment, students will gain proficiency in building and scheduling data processing tasks, performing ML model training and inference, and integrating various data sources and storage systems.
