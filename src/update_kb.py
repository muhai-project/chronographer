"""
Updating HDT files with new data
Usage: if a new node appears over time, query it with and endpoint and save it
"""
import os
import subprocess
from SPARQLWrapper import SPARQLWrapper

def query_kb(sparql_endpoint, uri):
    """ Query a KG from its SPARQL endpoint, and returns 
    the ingoing and outgoing nodes of the input node uri"""
    sparql = SPARQLWrapper(sparql_endpoint)

    query_template = """
    CONSTRUCT {
        ?s ?p ?o .
    }
    WHERE {
        VALUES ?s {<[1]>}
        ?s ?p ?o .
    } 
    """
    query = query_template.replace("[1]", uri)

    sparql.setQuery(query)
    return sparql.queryAndConvert()

def main(sparql_endpoint, uri, folder_out, hdt_cpp):
    """ Query the KG and serializes it as a .ttl file,
    then converts it to HDT compressed format """
    results = query_kb(sparql_endpoint, uri)
    node = uri.split('/')[-1]

    if not os.path.exists(folder_out):
        os.makedirs(folder_out)
    nested_folder = os.path.join(folder_out, node)
    if not os.path.exists(nested_folder):
        os.makedirs(nested_folder)

    ttl_file = os.path.join(nested_folder, f"{node}.ttl")
    results.serialize(destination=ttl_file)

    command_ttl_to_hdt = f"""
    {os.path.join(hdt_cpp, 'libhdt/tools/rdf2hdt')} \
    -f ttl -p -v -i {ttl_file} {os.path.join(nested_folder, node)}.hdt
    """
    subprocess.call(command_ttl_to_hdt, shell=True)
    subprocess.call(f"rm -rf {ttl_file}", shell=True)

if __name__ == '__main__':
    SPARQL_ENDPOINT = "http://dbpedia.org/sparql"
    URI = "http://dbpedia.org/resource/Russo-Ukrainian_War"

    FOLDER_OUT = "dbpedia-snapshot-2021-09/updated"
    HDT_CPP = "/Users/ines/Projects/hdt-cpp"

    main(SPARQL_ENDPOINT, URI, FOLDER_OUT, HDT_CPP)
