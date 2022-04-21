# DWL-Project_SCJ
Repository for DWL-Project of group SCJ

## TwitterAPI_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/TwitterAPI_HistoricalData.ipynb)
Code for extracting and loading the data of the Twitter API within the period 1/1/2021 - 5/4/2022. Goal of this script was to load the data into the RDS from the beginning of the period, we want to start the analyses, till the start of the daily data load. The code was executed once, as part of the script, the database tables were created.

Requirements:
- Packages mentioned in the first cell should be installed
- Access to a Twitter Academic research account
- Database is prepared
- Database credentials and Twittwer Bearer token are stored in an .env-file

## TwitterAPI_Lambda_dailyload - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/TwitterAPI_Lambda_dailyload.ipynb)
Code of a AWS Lambda function for extracting and loading the data of the Twitter API of the last day on a daily basis. The function is executed every day.

- Packages mentioned in the first cell should be part of a layer in the Lambda function
- Access to a Twitter developer account
- Database is prepared
- Database credentials and Twittwer Bearer token are stored as environmental variable of the lambda function


## YahoofinanceAPI_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/YahoofinanceAPI_HistoricalData.ipynb)
Code for extracting and loading the data of the YahooFinance API within the period 1/1/2021 - 31/3/2022. Goal of this script was to load the data into the RDS from the beginning of the period, we want to start the analyses, till the start of the daily data load. The code was executed once, as part of the script, the database tables were created.

Requirements:
- Packages mentioned in the first cell should be installed
- Database is prepared
- Database credentials are stored in an .env-file

## YahoofinanceAPI_Lambda_dailyload - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/YahoofinanceAPI_Lambda_dailyload.ipynb)
Code of a AWS Lambda function for extracting and loading the data of the YahooFinance API of the last day on a daily basis. The function is executed every day.

- Packages mentionned in the first cell should be part of a layer in the Lambda function
- Database is prepared
- Database credentials are stored as environmental variable of the lambda function

## Reddit_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/Reddit_HistoricalData.py)
Code for extracting, light transforming and load comment data from Reddit to S3 bucket from 1/1/2021 to about 5/4/2022.

Requirements:
- Packages mentioned in the first cells of code may be need to be installed. Alternative:
  - Use the [requirements.txt](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/airflow-docker/requirements.txt) in the airflow-docker folder
- .env-File in the same folder like the script with the credentials of at least AWS (Reddit is only necessary if *praw*-library is used, e.g. for looking for certain subreddits). The naming of the variables can be take out of the script.
- an S3 bucket on AWS (IMPORTANT: put in the right bucket name in [Line 106](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/d396004f4b6df0a8e84ea78e1184e003f31e66da/Reddit_HistoricalData.py#L106))
- enough time if you're looking for bitcoin-comments for a long period 😉

## ApacheAirflow / Reddit_PeriodicalData_Airflow - [Folder](https://github.com/DataBauHeini/DWL-Project_SCJ/tree/main/airflow-docker)
All necessary files for getting periodical Reddit data with Apache Airflow. The current configuration was run on an Windows10 operating system inside a Docker Container. It is also possible to run the DAGs outside of a Docker container. 
It is intended to run the code every three days. If a different period is desired, [Line 25](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/d396004f4b6df0a8e84ea78e1184e003f31e66da/airflow-docker/dags/Reddit_PeriodicalData_Airflow.py#L25) and [Line 116](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/d396004f4b6df0a8e84ea78e1184e003f31e66da/airflow-docker/dags/Reddit_PeriodicalData_Airflow.py#L116) have to be changed.

Requirements:

