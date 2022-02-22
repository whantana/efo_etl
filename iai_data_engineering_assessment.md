# Task Description

The goal is to retrieve specific data provided by the Ontology Lookup Service repository https://www.ebi.ac.uk/ols/index

We need to retrieve:
* EFO terms
* EFO term synonyms
* EFO term ontology (parent links)

You should implement a data pipeline that consumes the OLS repository, taking into account the following:

* data should be retrieved through the API (https://www.ebi.ac.uk/ols/docs/api)
* the implemenation should be able to retrieve *all* records (although you don't have to necessarily retrieve all the records yourself); keep in mind that the API returns paginated results
* the retrieved data should be stored in table(s) of a PostgreSQL database; the table design is up to you
* you're limited to implementing this with any/all of the following: Python, SQL, bash
* there should be clear instructions on how to setup and run the data pipeline in a new development environment
* please provide the solution as a git code repository in your preferred free git hosting service

The implementation will be evaluated against:

* efficiency of data movement, i.e. using bulk retrievals and inserts
* code readability and quality
* database design; you should store data in a normalized schema that eliminates data duplication and enforces data integrity

Bonus:
* retrieve MeSH term references (xref with MSH database)
* make the data pipeline capable of incremental updates