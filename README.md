## comando para converter entrypoint para unix
dos2unix entrypoint.sh

## comando para executar os testes
 tests/run_tests.sh all
 tests/run_tests.sh integration

## 📊 Data Engineer Code Challenge
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

