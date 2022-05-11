[WIP]

# **Graph Search Framework**

## **Data**

### **Knowledge Bases for search**
It is possible to use two types of data sources to query DBPedia:
* Triply and [DBpedia 2021-09](https://triplydb.com/DBpedia-association/snapshot-2021-09)
    * Calls to this interface were made through API calls
    * The link is <https://api.triplydb.com/datasets/DBpedia-association/snapshot-2021-09/fragments/?limit=10000> 

* HDT and [DBPedia 2016-10](https://www.kaggle.com/bsteenwi/dbpedia)
    * The two files from the dataset were first downloaded: `dbpedia2016-10.hdt` and `dbpedia2016-10.hdt.index.v1-1`.
    * They were added in a `dbpedia-archive` folder in the root directory of this project. The code now runs with such a structure.
    * The dataset is queried using [pyHDT](https://github.com/Callidon/pyHDT)

### **Knowledge Base for ground truth**

#### **SPARQL endpoint**
The ground truth events used for calculating the metrics were taken from the [EventKG dataset](https://eventkg.l3s.uni-hannover.de)

The [EventKG SPARQL Query Endpoint](http://eventkginterface.l3s.uni-hannover.de/sparql) was used to query the dataset and to retrieve a csv output with the following columns: `startTime`, `callret-1` and `linkDBpediaEn`.

The SPARQL example below show an example query for retrieving all events that are part of World War II.

```
SELECT ?startTime SAMPLE(STR(?description)) (?sa AS ?linkDBpediaEn)
WHERE {
 ?event owl:sameAs dbr:World_War_II .
 ?event sem:hasSubEvent* ?subEvent .
 ?subEvent sem:hasBeginTimeStamp ?startTime .

 OPTIONAL {
  GRAPH eventKG-g:wikipedia_en { ?subEvent dcterms:description ?description . } .
  FILTER(LANGMATCHES(LANG(?description), "en")) .
 }

 OPTIONAL { GRAPH eventKG-g:dbpedia_en { ?subEvent owl:sameAs ?sa . } . } .

 FILTER(BOUND(?sa) || BOUND(?description)) .
}   
GROUP BY ?startTime ?subEvent ?sa
ORDER BY ASC(?startTime)
```

#### **Downloading dataset**

To ensure that EventKG was always queryable, another option was to download it locally and query it using GraphDB. 

A bit of preprocessing was done for two main reasons:
1. Some parsing errors were preventing the graph from being loaded locally
2. Not all the graph was necessary for this experiment

The notebook `eventkg-filtering.ipynb` selects a subgraph of EventKG and discards content that is not correctly parsed by GraphDB.

EventKG (v3.0) was downloaded from [the website](https://eventkg.l3s.uni-hannover.de/data.html) and all files were put in an `eventkg` folder in this repository directory.

Dask and pandas were used to load and process the data. The preprocessed files were then saved back to an `.nq` format.

Loading into GraphDB + querying from there

```
PREFIX dbr: <http://dbpedia.org/resource/> 
PREFIX sem: <http://semanticweb.cs.vu.nl/2009/11/sem/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT DISTINCT(?subEventKG as ?linkDBpediaEn)
WHERE {
    
 ?event owl:sameAs dbr:Cold_War .
 ?event sem:hasSubEvent* ?subEvent .
 ?subEvent owl:sameAs ?subEventKG .
    
 ?event sem:hasBeginTimeStamp ?startTimeEvent .
 ?event sem:hasEndTimeStamp ?endTimeEvent .

 ?subEvent sem:hasBeginTimeStamp ?startTimeSubEvent .
 ?subEvent sem:hasEndTimeStamp ?endTimeSubEvent .
    
 FILTER( strStarts( str(?subEventKG), "http://dbpedia" ) ) .
 FILTER (?endTimeSubEvent >= ?startTimeEvent) .
 FILTER (?startTimeSubEvent <= ?endTimeEvent) .
}
```

Then downloading as csv and moving to folder `./data/gs_events/`

### **URI mapping**

cf. `./src/get_equivalent_url.py`

## **Installation**

Python 3.9.7 was used, and a conda virtual environment.

To have the virtual environment running:
```bash
pip install -r requirements.txt
python setup.py install
```

If you want __pycache__ content or other removed, occasionnally run:
```
sh clean.sh
```

Create a `private.py` file in the settings folder and add the followings:
* AGENT (of computer, for sparql interface)
* TOKEN (for Triply)
* FOLDER_PATH (of project)
* WANDB_USER (for weights and biases)

## **Run one search**

1. Get ground truth
2. Get URI mapping (to account for different versions in DBPedia)
3. Set config file (node to start with, filters, start and end dates)
4. Run experiments
5. Visualise results (wandb recommended)

## **Experiments**

French Revolution
start node: http://dbpedia.org/resource/French_Revolution
start date: 1789-01-01
end date: 1804-12-31

WWII
start node: http://dbpedia.org/resource/World_War_II
start date: 1939-01-01
end date: 1945-12-31

Russian Revolution
start node: http://dbpedia.org/resource/Russian_Revolution
start date: 1917-01-01
end date: 1923-12-31

American Revolution
start node: http://dbpedia.org/resource/American_Revolution
start date: 1765-01-01
end date: 1783-12-31

## **Weights and biases**

It is also possible to run scripts using the [Weights and biases platform](https://wandb.ai/site) and its Python module, wandb.

To use these scripts, first create a wandb account, then follow the [quickstart](https://docs.wandb.ai/quickstart).

* To run one wandb script, refer to `./run_experiment_wandb.py`
* To run a wandb sweep (~grid search for hyperparameters), run the following:
    ```bash
    wandb sweep graph_search_sweep.yaml
    ```
    An agent ID will then be created, and the next command to run will be displayed in the terminal.

## **Tests**

Python unittests were created to test out different components of the graph search framework. To run them all, run in terminal (from root directory of the repository):

```
coverage run -m unittest discover -s src/tests/
coverage html
open -a Safari htmlcov/index.html
```

## **Citations**

For retrieving data, we made use of the following resources:

```
@inproceedings{gottschalk2019eventkg,
   title={{EventKG - the Hub of Event Knowledge on the Web - and Biographical Timeline Generation}},
   author={Gottschalk, Simon and Demidova, Elena},
   year={2019},
   publisher={IOS Press},
   volume={10},
   number={6},
   pages={1039--1070},
   journal={Semantic Web Journal (SWJ)}
}
```
