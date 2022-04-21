# DWL-Project_SCJ
Repository for DWL-Project of group SCJ

## TwitterAPI_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/TwitterAPI_HistoricalData.ipynb)
Code for extracting and loading the data of the Twitter API within the period 1/1/2021 - 5/4/2022. Goal of this script was to load the data into the RDS from the beginning of the period, we want to start the analyses, till the start of the daily data load. The code was executed once, as part of the script, the database tables were created.

Requirements:
- Packages mentionned in the first cell should be installed
- Access to a Twitter Academic research account
- Database is prepared
- Database credentials and Twittwer Bearer token are stored in an .env-file

## TwitterAPI_Lambda_dailyload - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/TwitterAPI_Lambda_dailyload.ipynb)
Code of a AWS Lambda function for extracting and loading the data of the Twitter API of the last day on a daily basis. The function is executed every day.

- Packages mentionned in the first cell should be part of a layer in the Lambda function
- Access to a Twitter developer account
- Database is prepared
- Database credentials and Twittwer Bearer token are stored as environmental variable of the lambda function


## YahoofinanceAPI_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/YahoofinanceAPI_HistoricalData.ipynb)
Code for extracting and loading the data of the YahooFinance API within the period 1/1/2021 - 31/3/2022. Goal of this script was to load the data into the RDS from the beginning of the period, we want to start the analyses, till the start of the daily data load. The code was executed once, as part of the script, the database tables were created.

Requirements:
- Packages mentionned in the first cell should be installed
- Database is prepared
- Database credentials are stored in an .env-file

## YahoofinanceAPI_Lambda_dailyload - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/YahoofinanceAPI_Lambda_dailyload.ipynb)
Code of a AWS Lambda function for extracting and loading the data of the YahooFinance API of the last day on a daily basis. The function is executed every day.

- Packages mentionned in the first cell should be part of a layer in the Lambda function
- Database is prepared
- Database credentials are stored as environmental variable of the lambda function

## Reddit_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/Reddit_HistoricalData.py)
blabla

## ApacheAirflow / Reddit_PeriodicalData_Airflow - [Folder](https://github.com/DataBauHeini/DWL-Project_SCJ/tree/main/airflow-docker)
blabla
