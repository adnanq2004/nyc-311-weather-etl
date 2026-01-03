# nyc-311-weather-etl

This repository contains a monthly ETL (Extract, Transform, Load) pipeline for integrating NYC 311 service request data with weather data. The project leverages Python, Airflow, and Google BigQuery to automate data extraction, cleaning, transformation, and loading for analytical purposes.





# Project Description



### In this project, we aimed to create a data warehouse, focusing on complaint data from the NYC 311 Service relating to water problems, and weather reports over NYC from the Open Meteo API. We followed the Kimball Life Cycle methodology, starting with Project Planning, then gathering Business Requirements, then Dimensional Modeling, deciding on the Technical Architecture, performing ETL Programming, and finally Designing and Developing the Business Intelligence Application.



## 1. Project Planning



### In this section, we drafted project ideas, including information about our selected data sources and an initial list of KPIs related to our topic.



## 2. Business Requirements



### In this section, we confirmed and began downloading our data sources, and formalized the KPIs we planned to use.



## 3. Dimensional Modeling



### In this section, we designed a data mart schema for each data source, then created an integrated dimensional model for the enterprise data warehouse.



## 4. Technical Architecture



### In this section, we selected our DBMS, Google BigQuery, and our hosting environment, Google Cloud. We also chose our ETL tool, which we would build by programming in Python.



## 5. ETL Programming



### In this section, we used python libraries, such as ETLLogger and Polaris, to extract data. We created appropriate schemas covering the KPIs we planned to use. We divide the data source into chunks before downloading it and reading it in memory using the Polaris library, which is faster and more memory-efficient than standard tools for large datasets. 
### We transformed the data to fit our schemas using Polaris library and Dataframe objects. We cleaned the data to handle missing or incorrect values in certain columns and filtered for relevant data.
### We loaded the data into our DBMS, Google BigQuery using ETLLogger and BigQuery libraries.



## 6. BI Application Design and Development



### In this section we used Tableau to visualize our data and represent our KPIs using different graphs, such as bar charts, heat maps, density maps, and others, and organized them into a dashboard for user convenience.
### Here is the link to the functioning visualization: 
