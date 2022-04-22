# DWL-Project_SCJ
Repository for DWL-Project of group SCJ.
It is expected, that the user of this repository has some basic knowledge about:
- Python
- AWS Services [RDS](https://aws.amazon.com/rds/?nc1=h_ls), [Lambda](https://aws.amazon.com/lambda/?nc1=h_ls), [S3](https://aws.amazon.com/s3/?nc1=h_ls)
- Apache Airflow [Docs](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- (Docker [Docs](https://docs.docker.com/), depends on the operating system)
<br/>

## About the Project
The idea that you can add value to your finances through a smart investment strategy is something most individuals understand relatively early in life. It is the case, however, that the decision to invest a portion of one's assets is not made until later for many individuals. The main purpose for the whole project  is, to analyse and present the performance of various investment opportunities over the last few months, in order to subsequently provide an overview of investment strategies and opportunities for newcomers.


## Data Source
Data was extracted from four API
- Binance: prices and other key figures of top 10 crypto currencies (according to market cap)
- YahooFinance: prices and other key figures of four indices and two precious metals
- Reddit: posts in which the investment assets are mentionned
- Twitter: Count of tweets which have hashtags of the investment assets


## TwitterAPI_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/TwitterAPI_HistoricalData.ipynb)
Code for extracting and loading the data of the Twitter API within the period 1/1/2021 - 5/4/2022. Goal of this script was to load the data into the RDS from the beginning of the period, we want to start the analyses, till the start of the daily data load. The code was executed once, as part of the script, the database tables were created.

*Requirements:*
- Packages mentioned in the first cell should be installed
- Access to a Twitter Academic research account
- Database is prepared
- Database credentials and Twittwer Bearer token are stored in an .env-file

## TwitterAPI_Lambda_dailyload - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/TwitterAPI_Lambda_dailyload.ipynb)
Code of a AWS Lambda function for extracting and loading the data of the Twitter API of the last day on a daily basis. The function is executed every day.

*Requirements:*
- Packages mentioned in the first cell should be part of a layer in the Lambda function
- Access to a Twitter developer account
- Database is prepared
- Database credentials and Twittwer Bearer token are stored as environmental variable of the lambda function
<br/>

## YahoofinanceAPI_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/YahoofinanceAPI_HistoricalData.ipynb)
Code for extracting and loading the data of the YahooFinance API within the period 1/1/2021 - 31/3/2022. Goal of this script was to load the data into the RDS from the beginning of the period, we want to start the analyses, till the start of the daily data load. The code was executed once, as part of the script, the database tables were created.

*Requirements:*
- Packages mentioned in the first cell should be installed
- Database is prepared
- Database credentials are stored in an .env-file

## YahoofinanceAPI_Lambda_dailyload - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/YahoofinanceAPI_Lambda_dailyload.ipynb)
Code of a AWS Lambda function for extracting and loading the data of the YahooFinance API of the last day on a daily basis. The function is executed every day.

*Requirements*
- Packages mentionned in the first cell should be part of a layer in the Lambda function
- Database is prepared
- Database credentials are stored as environmental variable of the lambda function
<br/>

## BinanceAPI_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/BinanceAPI_HistoricalData.py)
Code for extracting and loading historical cryptocurrency data from Binance within the period 1/1/2017 - today. Goal of this script is the same like for YahooFinance: Load the data into the RDS from beginning of the period and create the database tables.

*Requirements:*
- Packages mentioned in the first cells of code may be needed to be installed. 
- Database is prepared
- [Binance](https://www.binance.com/en) account is necessary incl. creating an API-Connection on Binance account
- Credentials stored in an .env-File

## BinanceAPI_Lambda_dailyload - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/BinanceAPI_Lambda_dailyload.py)
Code of a AWS Lambda function for extracting and loading daily the data of the Binance API of the previous day. The function is executed every day.

*Requirements:*
- Packages mentionned in the first cell should be part of a layer in the Lambda function
- Database is prepared
- [Binance](https://www.binance.com/en) account is necessary incl. creating an API-Connection on Binance account
- Database credentials are stored as environmental variable of the lambda function
<br/>

## Reddit_HistoricalData - [File](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/Reddit_HistoricalData.py)
Code for extracting, light transforming and load comment data from Reddit to S3 bucket from 1/1/2021 to circa 5/4/2022.

*Requirements:*
- Packages mentioned in the first cells of code may be needed to be installed. Alternative:
  - Use the [requirements.txt](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/airflow-docker/requirements.txt) in the airflow-docker folder
- .env-File in the same folder like the script with the credentials of at least AWS (Reddit is only necessary if *praw*-library is used, e.g. for looking for certain subreddits). The naming of the variables can be take out of the script.
- an S3 bucket on AWS (IMPORTANT: put in the right bucket name in [Line 106](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/d396004f4b6df0a8e84ea78e1184e003f31e66da/Reddit_HistoricalData.py#L106))
- enough time if you're looking for bitcoin-comments for a long period 😉

## ApacheAirflow / Reddit_PeriodicalData_Airflow - [Folder](https://github.com/DataBauHeini/DWL-Project_SCJ/tree/main/airflow-docker)
All necessary files for getting periodical Reddit data with Apache Airflow. The current configuration was run on an Windows10 operating system inside a Docker Container. It is also possible to run the DAGs outside of a Docker container.<br/>
It is intended to run the code every three days. If a different period is desired, [Line 25](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/d396004f4b6df0a8e84ea78e1184e003f31e66da/airflow-docker/dags/Reddit_PeriodicalData_Airflow.py#L25) and [Line 116](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/d396004f4b6df0a8e84ea78e1184e003f31e66da/airflow-docker/dags/Reddit_PeriodicalData_Airflow.py#L116) have to be changed.

*Requirements: (assuming Apache Airflow will be run in Docker on Windows10)*
The installation of Docker + Airflow is very well explained [here - text](https://naiveskill.com/install-airflow/) and [here - Video](https://www.youtube.com/watch?v=aTaytcxy2Ck)
- Install Docker Engine (incl. Docker Compose)
- Copy the project-Folder [airflow-docker](https://github.com/DataBauHeini/DWL-Project_SCJ/tree/main/airflow-docker) to the desired location
- Run the command below to ensure the container and host computer have matching file permissions:
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```
- Run docker-compose with following command -> Apache Airflow will be started, missing packages should be installed (using those two files: [requirements.txt](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/airflow-docker/requirements.txt) & [Dockerfile](https://github.com/DataBauHeini/DWL-Project_SCJ/blob/main/airflow-docker/Dockerfile))
```
docker-compose up
```
- Specify following variables in Apache Airflow:
  - AWS credentials (*ACCESS_KEY*, *SECRET_KEY*, *SESSION_TOKEN*)
  - S3 bucket name (*bucket* = 'Bucketname of S3-Bucket')
- Run the DAG *Reddit_PeriodicalData_Airflow.py*
