# Search engine using Hadoop Mapreduce and Cassandra

Before running the search engine:
- ensure you have either:
  - loaded k.parquet file to app directory
  - loaded different parquet file, and changed file name in prepare_data.sh and prepare_data.py
  - commented out the ```bash prepare_data.sh``` in app.sh and are using local files as input for indexer
- ensure that the path you put after ```bash index.sh``` points to either:
  - local file with name in format ```{id}_{title}.txt```
  - directory with files in the same format
- ensure that you have given some input after ```bash search.sh``` in app.sh
- ensure you have docker and docker compose on your machine
- ensure you have internet access

To run the search engine, simply run
```commandline
docker compose up
```
in the project folder.

## Repository structure

### app
Repository with files needed for search engine

#### mapreduce
Repository with files for mapreduce pipeline

##### mapper1.py
This python script gets input from hadoop with documents, submits document data to Cassandra table, and transfers input about terms to reducer1.py

##### reducer1.py
This python script calculates statistics for terms and puts them in Cassandra database

#### app.sh
Script that runs the whole project. It starts services, prepares the data, creates cassandra table, runs indexer and ranker

#### index.sh
Script for indexing. Preprocesses local file using process_input.py, and initiates mapreduce pipeline

#### prepare_data.py
Python script for data preparation, example script taken from homework task

#### prepare_data.sh
Bash script for data preparation, example script taken from homework task

#### process_input.py
Python script to transform user input (file or directory) into csv file input for mapreduce, where each line contains doc_id, title and content

#### query.py
Python script for ranker. Searches among all documents, rankes them using BM25 and outputs the best 10.

#### requirements.txt
File with python libraries needed to install for python script to work

#### search.sh
Bash script for ranker. Checks that input exists and processes it to query.py

#### set_cassandra.sh
Script for database creation. Initializes the tables needed for search engine.

#### start-services.sh
Script which starts services. Example script taken from homework task.

### docker-compose.yml
File to start all services. Example taken from homework task

### README.md
File containing information about repository and how to start the project. You are reading it now)

### report.pdf
My report for Introduction to Bigdata Assignment 2. 
