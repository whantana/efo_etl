efo_etl by Dimitris Bousis
---

Performing ETL task on the EFO ontology.Part of the interview process with inteligencia.ai
for the role of data engineer. This assignments objective is to develop a data ingestion pipeline 
in order to consume terms and relative synonyms from the Experimental Factor Ontology , EFO into a locally deployed 
postgres data base. Furthermore, a bonus objective of this excercise is to support incremental updates of the database
with each time inserting and updating data instead of bulk loading them.   

The Challenge & Response
---
This challenge forces us to retrieve data via a public API (https://www.ebi.ac.uk/ols/index) 
that serves this ontology, instead of ingesting files or streams. In order to fetch the entirety 
of the ontology one has to perform a number of time consuming HTTP requests that greatly varies with 
the number of terms (names, identities, descriptions that belong to the ontology ) to be ingested.

Furthermore, one can quickly understand that the data are not extremely large in terms of in-memory size,
however the process must be efficient in terms of running time and perform a reasonable, 
but not exhaustive, number of API requests in order to fetch all the data.

One approach is of course to brute force the problem by running the requests on a multithreaded environment. 
This of course can show run time improvements of the process, yet it does not reduce the number of requests
made by a naive approach.

A smarter alternative is to treat the ontology as a directed graph and travese through the terms relations 
(parents/children, owl:subClassOf) with either a breadth-first or depth-first manner. In this way the algorithm 
performs much less requests for fetching term-relationship information, vastly improves running time in comparison 
with the naive approach, while still can apply multi-threading techniques since the algorithm encourages parallelism.



Development environment
---
Requirements : Docker & docker-compose  

1. Fetch the latest version of the code `git clone https://github.com/whantana/efo_etl.git`

2. Into the efo_etl directory : `docker build . -t whantana/efo_etl:latest`
3. This for creating a postgres container `docker-compose up -d postgres`
4. And this in order to launch a bash shell on a development container with all the necessary code `docker-compose run efo_etl bash`

efo-etl cmd line
---
```
Usage: python -m efo_etl [OPTIONS] COMMAND [ARGS]...

Options:
  -d, --db-endpoint TEXT          [required]
  --bulk / --incremental
  -c, --cutoff-term_count INTEGER
  -w, --worker-count INTEGER      [default: 1]
  -s, --page-size INTEGER         [default: 20]
  --help                          Show this message and exit.

Commands:
  breadth-first
  depth-first
  naive
```
