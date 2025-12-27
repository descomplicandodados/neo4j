
## 📊 Data Engineer/Neo4j Code Challenge
ClinicalTrials.gov → Neo4j Knowledge Graph

This project implements a complete end-to-end data pipeline that ingests public data from ClinicalTrials.gov, transforms it into structured entities, and loads everything into a Neo4j graph database.

The main goal is to demonstrate how to build a Knowledge Graph that connects:

- Clinical trials
- Drugs
- Companies / Organizations (Sponsors and Collaborators)
- Diseases / Conditions
- Route of administration
- Dosage form

The entire pipeline is automated, re-runnable, and runs locally using Docker, without requiring manual database setup.

## 🧰 Technologies Used

- Neo4j – Graph database
- Python – Data ingestion and transformation scripts
- Apache Airflow – Pipeline orchestration (controls execution order)
- Docker & Docker Compose – Run everything locally with minimal setup
- APOC – Neo4j library for high-performance batch loading

## Prerequisites

Before start, you will need docker installed on your computer:

- [Docker](https://docs.docker.com/get-docker/)

## Requisites

- neo4j
- psycopg2-binary
- pandas

## Steps

- Clone the repository to your local machine.

```
git clone https://github.com/descomplicandodados/neo4j.git
```

- Open de project folder

```
cd neo4j
```
- Execute the commands
```
mkdir bases_neo4j
```
```
mkdir import_raw
```

- Download data from [Clinical-Trials](https://ctti-aact.nyc3.digitaloceanspaces.com/6scsc1rzihfyr5gdh3fxglrs8sro)
- Extract data from zip/rar, copy all txt files and past on neo4j/bases_neo4j folder

- Execute the commands:
```
docker compose up -d --build 
```

- Wait until neo4j and airflow starts to working

## Run this commands to execute the tests
```
 tests/run_tests.sh all
 ```
 ```
 tests/run_tests.sh integration
 ```

## Airflow
- Use this address [Airflow](http://localhost:8080/) to access airflow, use de user:admin and password:admin, both can be changed on .env file. On Airflow you will active this 3 dags.
![alt text](image.png)

You will wait until de Runs column status be "sucess". This process will have between 40 and 50 minutes.

## Neo4j
Use this address[Neo4j](http://localhost:7474/) to access neo4j, use user:neo4j and password:teste123, both can be changed on .env file.
Once neo4j works, you can run queries like this
![alt text](image-1.png)

like this

![alt text](image-2.png)

like this
![alt text](image-3.png)

or like this
![alt text](image-4.png)