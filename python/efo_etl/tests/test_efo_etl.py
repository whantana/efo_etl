import json

import click
import requests
import urllib.parse

from efo_etl.efo_etl import ONTOLOGY_BASE_IRI, ONTOLOGY_BASE_API_URL, retrieve_efo_ontology_info, \
    extract_fields_from_term_json


def test_ontology_fetching():
    click.echo(retrieve_efo_ontology_info())
    assert True


def do_simple_call(short_form):
    iri = f"{ONTOLOGY_BASE_IRI}/{short_form}"
    test_url = f"{ONTOLOGY_BASE_API_URL}/terms/{urllib.parse.quote(urllib.parse.quote(iri, safe=''))}"
    r = requests.get(test_url)
    assert r.status_code == 200
    json_dict = r.json()
    click.echo(f'{json.dumps(json_dict, indent=4, separators=(",", ": "))}')
    click.echo(extract_fields_from_term_json(json_dict))
    return json_dict


def test_do_simple_call_1():
    do_simple_call("EFO_1000864")


def test_do_simple_call_2():
    do_simple_call("EFO_0000588")


def test_do_simple_call_3():
    do_simple_call("EFO_0000001")