import io
import threading
import urllib.parse
from math import ceil

import click
import numpy as np
import psycopg2
import requests
import csv

ONTOLOGY_BASE_API_URL = "https://www.ebi.ac.uk/ols/api/ontologies/efo"
ONTOLOGY_BASE_IRI = "http://www.ebi.ac.uk/efo"

api_call_count = 0
is_multithreaded = False
parsed_term_cnt = 0


def extract_fields_from_term_json(json_dict):
    """

    :param json_dict:
    :return:
    """

    synonyms = None
    if "synonyms" in json_dict and isinstance(json_dict["synonyms"], list):
        json_dict['synonyms'].sort()
        synonyms = "{" + f"{','.join(json_dict['synonyms'])}" + "}" if len(json_dict['synonyms']) > 0 else None

    mesh_ref = None
    if "obo_xref" in json_dict and isinstance(json_dict["obo_xref"], list):
        mesh_refs = list(filter(lambda x: x['database'] == 'MeSH', json_dict["obo_xref"]))
        if len(mesh_refs) > 0:
            mesh_ref = mesh_refs[0]["url"]

    return [json_dict["iri"], json_dict["label"],
            synonyms if synonyms is not None else "NULL",
            mesh_ref if mesh_ref is not None else "NULL"]


def extract_parents_from_graph_json(json_dict, child_iri):
    """

    :param json_dict:
    :param child_iri:
    :return:
    """
    return list(map(lambda y: y["target"],
                    list(filter(lambda x: x["source"] == child_iri and
                                          x["uri"] == "http://www.w3.org/2000/01/rdf-schema#subClassOf",
                                json_dict['edges']))))


def retrieve_json_from_url(url):
    """

    :param url:
    :return:
    """
    global api_call_count
    try:
        r = requests.get(url)
        r.raise_for_status()
        assert r.status_code == 200
        if is_multithreaded:
            with threading.Lock():
                api_call_count += 1
        else:
            api_call_count += 1
        return r.json()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


def retrieve_efo_ontology_info():
    """
    Retrieve ontology information.

    :return:
    """
    json_dict = retrieve_json_from_url(ONTOLOGY_BASE_API_URL)
    return (
        json_dict["ontologyId"],
        json_dict["updated"],
        json_dict["loaded"],
        json_dict["numberOfTerms"]
    )


def efo_etl_process(function):
    def wrapper(*args, **kwargs):

        # get context from args
        ctx = args[0]

        # get info from api
        ontology, updated_timestamp, loaded_timestamp, total_term_count = retrieve_efo_ontology_info()
        click.echo(f"Ontology: {ontology}")
        ctx.obj['ontology'] = ontology
        click.echo(f"Term count: {total_term_count}")
        ctx.obj['total_term_count'] = total_term_count
        click.echo(f"Loaded at {loaded_timestamp}")
        ctx.obj['loaded_timestamp'] = loaded_timestamp
        click.echo(f"Updated at {updated_timestamp}")
        ctx.obj['updated_timestamp'] = updated_timestamp
        worker_count = ctx.obj['worker_count']
        global is_multithreaded
        is_multithreaded = worker_count > 1

        # set buffers
        terms_csv = io.StringIO()
        ctx.obj['terms_csv_buffer'] = terms_csv
        parent_terms_csv = io.StringIO()
        ctx.obj['parent_terms_csv_buffer'] = parent_terms_csv

        if ctx.obj['cutoff_term_count']:
            click.echo(f"Cut-off term count : {ctx.obj['cutoff_term_count']}")

        try:
            click.echo(f"---EFO {function.__name__} {f'({worker_count})---' if is_multithreaded else '---'}")

            # run traversal method
            function(*args, **kwargs)

            # data base connection details
            db_params = {
                "host": f"{ctx.obj['db_endpoint']}",
                "dbname": "efo_etl",
                "user": "efo_etl",
                "password": "efo_etl"
            }

            # connect to database
            conn = psycopg2.connect(**db_params)

            cursor = conn.cursor()

            try:
                if ctx.obj['bulk']:

                    # drop tables if exist
                    cursor.execute("DROP TABLE IF EXISTS terms;")
                    cursor.execute("DROP TABLE IF EXISTS parent_terms;")
                    conn.commit()

                    # create tables if not exists
                    cursor.execute("CREATE TABLE IF NOT EXISTS terms "
                                   "(iri VARCHAR ,label VARCHAR, synonyms VARCHAR [], mesh_iri VARCHAR);")
                    cursor.execute("CREATE TABLE IF NOT EXISTS parent_terms "
                                   "(iri VARCHAR , parent_iri VARCHAR);")
                    conn.commit()

                    # do copy
                    terms_csv.seek(0)
                    cursor.copy_expert(sql=
                                       """COPY terms FROM STDIN DELIMITER ',' QUOTE '"' NULL AS 'NULL' CSV;""",
                                       file=terms_csv)
                    parent_terms_csv.seek(0)
                    cursor.copy_expert(sql=
                                       """COPY parent_terms FROM STDIN DELIMITER ',' QUOTE '"' NULL AS 'NULL' CSV;""",
                                       file=parent_terms_csv)
                    conn.commit()

                    # apply constraints
                    cursor.execute("ALTER TABLE terms ADD CONSTRAINT pk_terms PRIMARY KEY (iri);")
                    cursor.execute("ALTER TABLE parent_terms ADD CONSTRAINT pk_parent_terms "
                                   "PRIMARY KEY (iri,parent_iri);")
                    cursor.execute("ALTER TABLE parent_terms ADD CONSTRAINT fk_terms_1 "
                                   "FOREIGN KEY (iri) REFERENCES terms (iri);")
                    cursor.execute("ALTER TABLE parent_terms ADD CONSTRAINT fk_terms_2 "
                                   "FOREIGN KEY (parent_iri) REFERENCES terms (iri);")
                    conn.commit()
                else:
                    import pandas as pd
                    from sqlalchemy import create_engine
                    engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}"
                                           f"@{db_params['host']}/{db_params['dbname']}")

                    # load buffers to dfs
                    term_column_names = ['iri', 'label', 'synonyms', 'mesh_iri']
                    terms_csv.seek(0)
                    terms_df = pd.read_csv(terms_csv, names=term_column_names, keep_default_na=False, na_values=['NaN'])
                    terms_df['synonyms'] = terms_df['synonyms'].str.strip('{}').str.split(',')
                    terms_df['from_api'] = True
                    terms_df['_index'] = terms_df.index
                    terms_df.replace(np.nan, pd.NA, regex=True)

                    # load PG tables to dfs
                    terms_table_df = pd.read_sql("SELECT * FROM terms", engine)
                    terms_table_df['from_api'] = False
                    terms_table_df['_index'] = terms_table_df.index

                    # concatenate the two terms dfs and find duplicate iris (assuming irs are unique within a df)
                    concat_terms_df = pd.concat(
                        [terms_df[['iri', 'from_api', '_index']], terms_table_df[['iri', 'from_api', '_index']]])
                    concat_terms_df["is_duplicated"] = concat_terms_df.duplicated(subset=['iri'], keep=False)

                    # can drop added columns now
                    terms_df.drop('from_api', axis=1, inplace=True)
                    terms_table_df.drop('from_api', axis=1, inplace=True)

                    # statement lists
                    term_table_inserts = []
                    term_table_deletes = []
                    term_table_updates = []

                    # retrieve new terms (INSERTs for terms_table)
                    new_terms = concat_terms_df[~concat_terms_df["is_duplicated"] & concat_terms_df["from_api"]]
                    for idx in list(new_terms["_index"]):
                        column_names_str = ','.join(term_column_names)
                        iri = terms_df.iloc[idx]['iri']
                        llabel = terms_df.iloc[idx]['label']
                        synonyms = terms_df.iloc[idx]["synonyms"]
                        synonyms = f'"{{{",".join(synonyms)}}}"' if synonyms is not pd.NA and synonyms is not None else 'NULL'
                        mesh_iri = terms_df.iloc[idx]['mesh_iri']
                        mesh_iri = f"'{mesh_iri}'" if mesh_iri is not pd.NA and mesh_iri is not np.NaN else 'NULL'
                        term_table_inserts.append(
                            f"INSERT INTO terms(iri,label,synonyms,mesh_iri) VALUES('{iri}', '{llabel}', {synonyms}, {mesh_iri});")

                    # retrieve discarded terms (DELETES for terms_table)
                    discarded_terms_df = concat_terms_df[
                        ~concat_terms_df["is_duplicated"] & ~concat_terms_df["from_api"]]
                    for idx in list(discarded_terms_df["_index"]):
                        iri = terms_table_df.iloc[idx]['iri']
                        term_table_deletes.append(f"DELETE FROM terms WHERE iri = '{iri}' ;")

                    merged_df = terms_df.merge(terms_table_df,
                                               how='inner', on='iri', suffixes=("_api", "_tbl"))
                    for _, row in merged_df.iterrows():
                        for column_name in term_column_names:
                            if column_name != 'iri' and (row[f"{column_name}_api"] != row[f"{column_name}_tbl"]):
                                updated_value = row[f"{column_name}_api"]
                                term_table_updates.append(f"UPDATE terms "
                                                          f"SET {column_name} = '{updated_value}'"
                                                          f" WHERE iri = '{row['iri']}';")

                    parent_terms_column_names = ['iri', 'parent_iri']
                    parent_terms_csv.seek(0)
                    parent_terms_df = pd.read_csv(parent_terms_csv, names=parent_terms_column_names,
                                                  keep_default_na=False, na_values=['NaN'])
                    parent_terms_df['from_api'] = True
                    parent_terms_df['_index'] = parent_terms_df.index

                    parent_terms_table_df = pd.read_sql("SELECT * FROM parent_terms", engine)
                    parent_terms_table_df['from_api'] = False
                    parent_terms_table_df['_index'] = parent_terms_table_df.index

                    # concatenate the two parent terms dfs and find duplicate iri,parent_iri pairs between tabes
                    concat_terms_parents_df = pd.concat([parent_terms_df[['iri', 'parent_iri', 'from_api', '_index']],
                                                         parent_terms_table_df[
                                                             ['iri', 'parent_iri', 'from_api', '_index']]])
                    concat_terms_parents_df["is_duplicated"] = concat_terms_parents_df.duplicated(
                        subset=parent_terms_column_names,
                        keep=False)

                    # can drop added columns now
                    parent_terms_df.drop('from_api', axis=1, inplace=True)
                    parent_terms_table_df.drop('from_api', axis=1, inplace=True)

                    # statement lists
                    parent_term_table_inserts = []
                    parent_term_table_deletes = []

                    # retrieve new term-relations (INSERT for parent_terms_table)
                    new_parents_terms_df = concat_terms_parents_df[
                        ~concat_terms_parents_df["is_duplicated"] & concat_terms_parents_df["from_api"]]
                    for idx in list(new_parents_terms_df["_index"]):
                        iri = parent_terms_df.iloc[idx]['iri']
                        parent_iri = parent_terms_df.iloc[idx]['parent_iri']
                        parent_term_table_inserts.append(
                            f"INSERT INTO parent_terms(iri,parent_iri) "
                            f"VALUES('{iri}','{parent_iri}');"
                        )

                    # retrieve discarded term-relations (DELETES for parent_terms_table)
                    discarded_parent_terms_df = concat_terms_parents_df[
                        ~concat_terms_parents_df["is_duplicated"] & ~concat_terms_parents_df["from_api"]]
                    for idx in list(discarded_parent_terms_df["_index"]):
                        iri = parent_terms_table_df.iloc[idx]['iri']
                        parent_iri = parent_terms_table_df.iloc[idx]['parent_iri']
                        term_table_deletes.append(
                            f"DELETE FROM terms WHERE iri = '{iri}' AND parent_iri = '{parent_iri}' ;")

                    # run statements
                    # delete discarded term-relationships
                    for stmnt in parent_term_table_deletes:
                        cursor.execute(stmnt)
                    click.echo("\n".join(parent_term_table_deletes))
                    for stmnt in parent_term_table_deletes:
                        cursor.execute(stmnt)

                    # delete discarded terms
                    click.echo("\n".join(term_table_deletes))
                    for stmnt in term_table_deletes:
                        cursor.execute(stmnt)

                    # insert new terms
                    click.echo("\n".join(term_table_inserts))
                    for stmnt in term_table_inserts:
                        cursor.execute(stmnt)

                    # update terms with updated values
                    click.echo("\n".join(term_table_updates))
                    for stmnt in term_table_updates:
                        cursor.execute(stmnt)

                    # insert new term-relationships
                    click.echo("\n".join(parent_term_table_inserts))
                    for stmnt in parent_term_table_inserts:
                        cursor.execute(stmnt)
                    conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                conn.rollback()
                raise error
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            terms_csv.close()
            parent_terms_csv.close()
            raise e
            # raise SystemExit(e)
        finally:
            click.echo("---DONE---")
            click.echo(f"Parsed term count : {parsed_term_cnt}")
            click.echo(f"Total API calls count : {api_call_count}")
        return

    return wrapper


@efo_etl_process
def naive_traversal(ctx):
    """

    :param ctx:
    :return:
    """
    # get useful stuff from context
    cutoff_term_count = ctx.obj['cutoff_term_count']
    term_csv_writer = csv.writer(ctx.obj['terms_csv_buffer'])
    parent_term_csv_writer = csv.writer(ctx.obj['parent_terms_csv_buffer'])
    page_size = ctx.obj['page_size']
    total_term_count = ctx.obj['total_term_count']
    worker_count = ctx.obj['worker_count']

    # calculate total pages (no need to retrieve it from API, one call less)
    total_page_nr = ceil(total_term_count / page_size)
    click.echo(f"Total number of page to parse : {total_page_nr}")

    #
    cutoff_term_count_reached = False

    def retrieve_terms_from_page(url):
        """
        :param url:
        :return:
        """
        global parsed_term_cnt
        nonlocal cutoff_term_count_reached
        page_json_dict = retrieve_json_from_url(url)
        if "_embedded" not in page_json_dict:
            return
        assert "terms" in page_json_dict["_embedded"]
        for term_json_dict in page_json_dict["_embedded"]["terms"]:
            if cutoff_term_count and parsed_term_cnt >= cutoff_term_count:
                if is_multithreaded:
                    with threading.Lock():
                        cutoff_term_count_reached = True
                else:
                    cutoff_term_count_reached = True
            if cutoff_term_count_reached:
                return
            term_extract = extract_fields_from_term_json(term_json_dict)
            if is_multithreaded:
                with threading.Lock():
                    term_csv_writer.writerow(term_extract)
                    parsed_term_cnt += 1
            else:
                term_csv_writer.writerow(term_extract)
                parsed_term_cnt += 1
            if parsed_term_cnt % 10 == 0:
                click.echo(f"Parsed term count : {parsed_term_cnt}")
            if not term_json_dict["is_root"]:
                iri = term_extract[0]
                graph_json_dict = retrieve_json_from_url(term_json_dict["_links"]["graph"]["href"])
                parent_term_iris = []
                for parent_term_iri in extract_parents_from_graph_json(graph_json_dict, iri):
                    parent_term_iris.append(parent_term_iri)
                if is_multithreaded:
                    with threading.Lock():
                        for parent_term_iri in parent_term_iris:
                            parent_term_csv_writer.writerow([iri, parent_term_iri])
                else:
                    for parent_term_iri in parent_term_iris:
                        parent_term_csv_writer.writerow([iri, parent_term_iri])

    if is_multithreaded:
        from .taskqueue import TaskQueue
        task_queue = TaskQueue(worker_count)

        for page_nr_blc_start in range(0, total_page_nr, worker_count):
            page_nr_blc_end = total_page_nr if (page_nr_blc_start + worker_count) > total_page_nr else (
                    page_nr_blc_start + worker_count)
            for page_nr in range(page_nr_blc_start, page_nr_blc_end):
                task_queue.add_task(retrieve_terms_from_page,
                                    f"{ONTOLOGY_BASE_API_URL}/terms?page={page_nr}&size={page_size}")
            task_queue.join()
            if cutoff_term_count_reached:
                break
    else:
        for page_nr in range(0, total_page_nr):
            retrieve_terms_from_page(f"{ONTOLOGY_BASE_API_URL}/terms?page={page_nr}&size={page_size}")
            if cutoff_term_count_reached:
                break


@efo_etl_process
def breadth_first_traversal(ctx):
    """

    :param ctx:
    :return:
    """

    # get useful stuff from context
    cutoff_term_count = ctx.obj['cutoff_term_count']
    term_csv_writer = csv.writer(ctx.obj['terms_csv_buffer'])
    parent_term_csv_writer = csv.writer(ctx.obj['parent_terms_csv_buffer'])
    worker_count = ctx.obj['worker_count']

    parsed_terms_iris = set()
    if is_multithreaded:
        from .taskqueue import TaskQueue
        task_queue = TaskQueue(worker_count)

    def retrieve_root_term():
        global parsed_term_cnt
        nonlocal task_queue
        root_iri = f'{ONTOLOGY_BASE_IRI}/EFO_0000001'
        term_json_dict = retrieve_json_from_url(
            f"{ONTOLOGY_BASE_API_URL}/terms/"
            f"{urllib.parse.quote(urllib.parse.quote(root_iri, safe=''))}")
        assert term_json_dict["is_root"]
        term_extract = extract_fields_from_term_json(term_json_dict)
        assert root_iri == term_extract[0]
        term_csv_writer.writerow(term_extract)
        parsed_terms_iris.add(root_iri)
        parsed_term_cnt += 1
        if term_json_dict["has_children"]:
            if is_multithreaded:
                task_queue.add_task(retrieve_terms_from_page, root_iri, term_json_dict["_links"]["children"]["href"])
            else:
                retrieve_terms_from_page(root_iri, term_json_dict["_links"]["children"]["href"])

    def retrieve_terms_from_page(parent_term_iri, url):
        """
        :param parent_term_iri:
        :param url:
        :return:
        """
        global parsed_term_cnt
        nonlocal task_queue
        page_json_dict = retrieve_json_from_url(url)
        if "_embedded" not in page_json_dict:
            return
        assert "terms" in page_json_dict["_embedded"]
        term_iris_children_links = []

        for term_json_dict in page_json_dict["_embedded"]["terms"]:
            if cutoff_term_count and (parsed_term_cnt >= cutoff_term_count):
                return
            term_extract = extract_fields_from_term_json(term_json_dict)
            iri = term_extract[0]
            if is_multithreaded:
                with threading.Lock():
                    parent_term_csv_writer.writerow([iri, parent_term_iri])
            else:
                parent_term_csv_writer.writerow([iri, parent_term_iri])
            if iri not in parsed_terms_iris:
                if is_multithreaded:
                    with threading.Lock():
                        term_csv_writer.writerow(term_extract)
                        parsed_terms_iris.add(iri)
                        parsed_term_cnt += 1
                else:
                    term_csv_writer.writerow(term_extract)
                    parsed_terms_iris.add(iri)
                    parsed_term_cnt += 1
                if parsed_term_cnt % 10 == 0:
                    click.echo(f"Parsed term count : {parsed_term_cnt}")
                if term_json_dict["has_children"]:
                    term_iris_children_links.append((iri, term_json_dict["_links"]["children"]["href"]))

        for term_iri, term_children_urls in term_iris_children_links:
            if is_multithreaded:
                task_queue.add_task(retrieve_terms_from_page, term_iri, term_children_urls)
            else:
                retrieve_terms_from_page(term_iri, term_children_urls)

    retrieve_root_term()
    if is_multithreaded:
        task_queue.join()


@efo_etl_process
def depth_first_traversal(ctx):
    """

    :param ctx:
    :return:
    """

    # get useful stuff from context
    cutoff_term_count = ctx.obj['cutoff_term_count']
    term_csv_writer = csv.writer(ctx.obj['terms_csv_buffer'])
    parent_term_csv_writer = csv.writer(ctx.obj['parent_terms_csv_buffer'])
    worker_count = ctx.obj['worker_count']

    parsed_terms_iris = set()
    if is_multithreaded:
        from .taskqueue import TaskQueue
        task_queue = TaskQueue(worker_count)

    def retrieve_root_term():
        global parsed_term_cnt
        root_iri = f'{ONTOLOGY_BASE_IRI}/EFO_0000001'
        term_json_dict = retrieve_json_from_url(
            f"{ONTOLOGY_BASE_API_URL}/terms/"
            f"{urllib.parse.quote(urllib.parse.quote(root_iri, safe=''))}")
        assert term_json_dict["is_root"]
        term_extract = extract_fields_from_term_json(term_json_dict)
        assert root_iri == term_extract[0]
        term_csv_writer.writerow(term_extract)
        parsed_terms_iris.add(root_iri)
        parsed_term_cnt += 1
        if term_json_dict["has_children"]:
            if is_multithreaded:
                task_queue.add_task(retrieve_terms_from_page, root_iri, term_json_dict["_links"]["children"]["href"])
            else:
                retrieve_terms_from_page(root_iri, term_json_dict["_links"]["children"]["href"])

    def retrieve_terms_from_page(parent_term_iri, url):
        """
        :param parent_term_iri:
        :param url:
        :return:
        """
        global parsed_term_cnt
        page_json_dict = retrieve_json_from_url(url)
        if "_embedded" not in page_json_dict:
            return
        assert "terms" in page_json_dict["_embedded"]
        for term_json_dict in page_json_dict["_embedded"]["terms"]:
            if cutoff_term_count and (parsed_term_cnt >= cutoff_term_count):
                return
            term_extract = extract_fields_from_term_json(term_json_dict)
            iri = term_extract[0]
            if is_multithreaded:
                with threading.Lock():
                    parent_term_csv_writer.writerow([iri, parent_term_iri])
            else:
                parent_term_csv_writer.writerow([iri, parent_term_iri])
            if iri not in parsed_terms_iris:
                if is_multithreaded:
                    with threading.Lock():
                        term_csv_writer.writerow(term_extract)
                        parsed_terms_iris.add(iri)
                        parsed_term_cnt += 1
                else:
                    term_csv_writer.writerow(term_extract)
                    parsed_terms_iris.add(iri)
                    parsed_term_cnt += 1
                if parsed_term_cnt % 10 == 0:
                    click.echo(f"Parsed term count : {parsed_term_cnt}")
                if term_json_dict["has_children"]:
                    if is_multithreaded:
                        task_queue.add_task(retrieve_terms_from_page, iri, term_json_dict["_links"]["children"]["href"])
                    else:
                        retrieve_terms_from_page(iri, term_json_dict["_links"]["children"]["href"])

    retrieve_root_term()
    if is_multithreaded:
        task_queue.join()
