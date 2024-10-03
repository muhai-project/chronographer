# **ChronoGrapher: Event-centric KG Construction via Informed Graph Traversal**

# Acknowledgement 
This work was funded by the European MUHAI project (Horizon 2020 research and innovation program) under grant agreement number 951846 and the Sony Computer Science Laboratories-Paris. We thank Frank van Harmelen for fruitful discussions.
# Application domain 
Graphs, Natural language processing, Semantic web.
# Code repository 
git@github.com:muhai-project/chronographer.git
# Contact 
Inès Blin
# Contributors
Inès Blin

Ilaria Tiddi

Remi van Trijp

Annette ten Teije
# Creation date
13-12-2021
# Description 
ChronoGrapher is a 2-step method to build an event-centric knowledge graph automatically from another knowledge graph. The first step is a semantically-informed search in a generic knowledge graph, that is novel. The second step constructs event-centric knowledge graphs automatically.
# Download URL 
The main data format we used are the HDT compressed versions of the datasets.

Some pointers for downloading the datasets:
* Some datasets can be downloaded [here](https://www.rdfhdt.org/datasets/).
* DBpedia-2016-10 can also be downloaded [here](https://www.kaggle.com/bsteenwi/dbpedia).

It's faster to download direct the `.hdt` file and the `.hdt.index` files.

**Within our experiments**, we used the followings: 
* [Triply DB](https://triply.cc)'s HDT version of DBpedia (snapshot 2021-09)
* [Wikidata](https://www.rdfhdt.org/datasets/) (2021-03-05)
* [YAGO4](https://yago-knowledge.org/downloads/yago-4) downloaded from the website. We later used [hdt-cpp](https://github.com/rdfhdt/hdt-cpp) to convert it to HDT format.

We put the datasets in the root directory of the repository, under the names `dbpedia-snapshot-2021-09`, `wikidata-2021-03-05` and `yago-2020-02-24` respectively. We query the HDT datasets using [pyHDT](https://github.com/Callidon/pyHDT).

We occasionnaly worked with Triply DB's data:
* [DBpedia 2021-09](https://triplydb.com/DBpedia-association/snapshot-2021-09). We used API calls using <https://api.triplydb.com/datasets/DBpedia-association/snapshot-2021-09/fragments/?limit=10000>.

# Full name 
Inès Blin
# Full title 
chronographer
# Installation instructions 
First clone the repo
```bash
git clone git@github.com:muhai-project/chronographer.git
```

We used Poetry and conda for virtual environment and dependency management.

Interface and traversal implemented with **Python 3.10.6**.

First set up your virtual environment and then download [Poetry](https://python-poetry.org) for dependencies.

To [install dependencies only](https://python-poetry.org/docs/basic-usage/#installing-dependencies-only)
```bash
poetry install --no-root
```

Alternatively, you can use the [full path to the `poetry` binary](https://python-poetry.org/docs/#installation)
> * `~/Library/"Application Support"/pypoetry/venv/bin/poetry` on MacOS.
> * `~/.local/share/pypoetry/venv/bin/poetry` on Linux/Unix.
> * `%APPDATA%\pypoetry\venv\Scripts\poetry` on Windows.
> * `$POETRY_HOME/venv/bin/poetry` if `$POETRY_HOME` is set.

If you work on an Apple Silicon Machine + conda, you might later be prompted to download again `grpcio`, you can do it using:
```bash
pip uninstall grpcio
conda install grpcio
```

Create a `private.py` file in the settings folder and add the followings:
* AGENT (of computer, for sparql interface) [optional]
* TOKEN (for Triply) [optional]
* FOLDER_PATH (of git repository on your machine)


[For submission] We use an external package that is currently WIP, for the purpose of this submission we include it directly into this code. To run its dependencies, run: 
```bash
cd kglab && python setup.py install
```
 
Then run the following for setting up the packages
```bash
python setup.py install
```
---
# Invocation 
We include some sample data in the `sample-data` folder. 

Before running the search, you need to extrac domain, range and superclasses information from the dataset you downloaded. See file `src/extract_domain_range.py` for further information and command lines to run that file, depending on your dataset.

You can run one search using this sample data **from the root directory**, by running:
```bash
python src/framework.py -j sample-data/French_Revolution_config.json
```

The results will be saved in the `experiments` folder in the root directory, in a folder starting by `<date>-<dataset_type>-<name_exp>`.

You can change the content of this configuration file. Some changes can be immediate, some others will require some additional data download (c.f. Section 4 to add further data for the search).

<details>
<summary>Click here to know more about the config file</summary>

##

Parameters that don't require additional data to be downloaded:
* `rdf_type`: the type of nodes you want to retrieve. Keys should be a string, and values the string URI of that node type. In our experiments, we are mainly interested about events.
* `predicate_filter`: list of predicates that are not taken into account for the search
* `start`: node to start the search from
* `start_date`: starting date of that `start` node
* `end_date`: ending date of that `start` node
* `iterations`: number of iterations for the search. The higher the number, the longer it will take to run.
* `type_ranking`: the type of ranking to use for paths.
* `type_interface`: type of interface used, in practice `hdt` only.
* `type_metrics`: the metrics that are computed, should be a sub-list of `["precision", "recall", "f1"]`
* `ordering` and `domain_range`: boolean, to activate or not this parameter
* `filtering`: same than above
* `name_exp`: name of your experiment, for the saving folder
* `dataset_type`: type of dataset, depending on the one you have
* `dataset_path`: path the the dataset folder 
* `nested_dataset`: boolean, whether your dataset is nested (decomposed in smaller chunks) or not

Parameters that require additional data to be downloaded - c.f. section 4 for further details:
* `gold_standard`: .csv path to the gold standard events
* `referents`: .json path to the URI referents
</details>

---

# License
GPL 3.0
# Name 
chronographer
# Owner
Inès Blin
# Owner type
User
# Programming languages 
Python
# Repository Status
Inactive
# Requirements
Cf. `requirements.txt`
# Scripts: Snippets of code contained in the repository
## Download data for ground truth comparison
### - Preprocessing EventKG and loading it into GraphDB
Corresponding notebook: `eventkg-filtering.ipynb`

* **Pre-requisites.** Download EventKG & GraphDB (links in paragraphs above).
* **Main motivation.** Problems when parsing EventKG data to GraphDB + working with a smaller subset of EventKG.
* **How.**: Using dask and pandas to read the data and only select the parts we were interested for our queries + some preprocessing
* **Main usage.** Load the newly saved data into GraphDB to set up a local SPARQL endpoint.

Before running one of the two notebooks below, you need to make sure that the data was downloaded into a GraphDB repositories and that the endpoint is active.

### - Extracting info for one events in a dataset
Corresponding notebook: `eventkg-info-one-event.ipynb`

* **Pre-requisites** Data loaded into GraphDB and SPARQL endpoint active. If you additionnally want to run the search at the end of the notebook, you need to have the dataset for search downloaded as well.
* **Main motivation.** Running the search with more events than the one in `sample-data`.
* **How**: SPARQL queries to extract ground truth events, referents, start and end dates for an event and to generate a config file to run a search.
* **Main usage.** Config file for the graph search.

### - Extracting info for all events in a dataset
Corresponding notebook: `eventkg-retrieving-events.ipynb`

* **Prerequisites.** Data loaded into GraphDB and SPARQL endpoint active. If you additionnally want to run the search at the end of the notebook, you need to have the dataset for search downloaded as well.
* **Main motivation.** Running the search with all events in a dataset.
* **How.** SPARQL queries.
* **Main purpose** Config files for the graph search.



### Other

If you want __pycache__ content or other removed, you can run:
```
sh clean.sh
```

Python unittests were created to test out different components of the graph search framework. To run them all, run in terminal (from root directory of the repository):

```
coverage run -m unittest discover -s src/tests/
coverage html
open -a Safari htmlcov/index.html
```
### Reproducibility

There are different scripts to run to reproduce the experiments described in the paper. First make sure that you have downloaded the data (cf. Sections above). 

All the experiments are described in a separate [README](./experiments_run/README.md) in the [`experiments_run`](./experiments_run/) folder, please refer to it for additional information.

---


# Usage examples 
If you have downloaded DBpedia, Wikidata or YAGO, it is possible to run the search with any of the events that is both in [EventKG](https://eventkg.l3s.uni-hannover.de) and in your dataset. We used [EventKG 3.1.](https://zenodo.org/record/4720078#.Y0bn-S8Rr0o) in our experiments.

We propose 3 notebooks in the `notebooks` folder to extract additional data to run the search. You will also need to download [GraphDB](./https://graphdb.ontotext.com) to set up a local SPARQL endpoint.



