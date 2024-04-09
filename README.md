# **Informed Graph Traversal**

This is the code for the paper submitted to ISWC 2024.


First clone the repo
```bash
git clone <link-ommitted-for-submission>
```
---
## 1. Set Up Virtual Environment

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

## 2. Download data for the graph traversal

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

---

## 3. Run the search

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

## 4. Download data for ground truth comparison

If you have downloaded DBpedia, Wikidata or YAGO, it is possible to run the search with any of the events that is both in [EventKG](https://eventkg.l3s.uni-hannover.de) and in your dataset. We used [EventKG 3.1.](https://zenodo.org/record/4720078#.Y0bn-S8Rr0o) in our experiments.

We propose 3 notebooks in the `notebooks` folder to extract additional data to run the search. You will also need to download [GraphDB](./https://graphdb.ontotext.com) to set up a local SPARQL endpoint.

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

---

## 5. Reproducibility

There are different scripts to run to reproduce the experiments described in the paper. First make sure that you have downloaded the data (cf. Sections above). 


### 5.1 Relevant Event Retrieval
For the search experiments, we created a main folder with the data in the root directory: `data-test`, with two sub-folders: `dbpedia` and `wikidata`. Each sub-folder had three folders: `config`, `gs_events` and `referents`.

All scripts are in the `experiments_run` folder. All example commands are run from root directory.

* Parameter selection for the search (subset of 12 events)
    * Python script: `run_all_grid_search.py`
    * Example command:
        ```bash
        python experiments_run/run_all_grid_search.py -t <type-system> -e experiments_run/grid-search-events.csv 
        ```
* Main results for the search (all events)
    * Python script: `run_all_search.py`
    * Example command:
        ```bash
        python experiments_run/run_all_search.py -t <type-system> -e experiments_run/all-search-events.csv 
        ```

### 5.2 Event-centric KG Population

For the event-centric KG generation experiments, we extracted the data that we needed from the search experiments, and created a new folder, `data_ng_building`.

* Extracting data
    * Python script: `get_data_ng_building.py`. There are some parameters to change in PARAMS (start and end date of experiments, folder_gs if different)
    * Example command:
        ```bash
        python experiments_run/get_data_ng_building.py 
        ```
    * The data was saved in a new folder: `data_ng_building`

* Event-centric KG population (EC-KG-P) from KG
    * This includes EC-KG-P from the output of graph search, EC-KG-P with our system from ground truth events, EC-KG-P from EventKG
    * Python script: `build_ng_from_search.py`
    * Example command:
        ```bash
        python experiments_run/build_ng_from_search.py --folder data_ng_building/ 
        ``` 
* Metrics for EC-KG-P from KG
    * Python script: `get_metrics.py`
    * Example command (comparing with all ground truth events):
        ```bash
        python experiments_run/get_metrics.py --folder data_ng_building/ --output_name eventkg_vs_generation.json --graph_c_path generation_ng.ttl --graph_gs_path eventkg_ng.ttl
        ```
    * Example command (comparing with output of graph search):
        ```bash
        python experiments_run/get_metrics.py --folder data_ng_building/ --output_name eventkg_vs_search.json --graph_c_path search_ng.ttl --graph_gs_path eventkg_ng.ttl
        ``` 

* Aggregating results (as in the paper):
    * Python script: `get_table_results.py`
    * Example command (comparing with all ground truth events):
        ```bash
        python experiments_run/get_table_results.py --folder data_ng_building/ --metric eventkg_vs_generation.json --label <label>
        ```
    * Example command (comparing with output of graph search):
        ```bash
        python experiments_run/get_table_results.py --folder data_ng_building/ --metric eventkg_vs_search.json --label <label>
        ```

* Event-centric KG Population from text
    * These are the experiments to generate KGs from the DBpedia abstracts
    * First you need to set up a local DBpedia, you can follow the steps on this link: <https://github.com/MartinoMensio/spacy-dbpedia-spotlight>
    * Python script: `build_kg_with_frames.py`
    * Example command:
        ```bash
        python experiments_run/build_kg_with_frames.py --folder data_ng_building/dbpedia/
        ```

* Annotating and analysing (causation) frames
    * Analysing
        * Python script: `get_csv_analyse_frame.py`
        * Example command
            ```bash
            python experiments_run/get_csv_analyse_frame.py --folder_input data_ng_building/dbpedia/ --folder_output experiments_run/ng_analysis
            ```
    * Extracting causation frames for manual annotation
        * Python script: `extract_causation_for_annot.py`
        * Example command
            ```bash
            python experiments_run/extract_causation_for_annot.py --csv experiments_run/ng_analysis/df_causation.csv --folder experiments_run/ng_analysis/
            ```

Furthermore, the manually annotated causation frames can be found in the `experiments_run/annotated` folder.

### 5.3 User study

All the content related to the user studies are in the `experiments_run/usage_ng` folder:
* The scripts permit to get the prompts, the answers, and the grounding triples, for all types of prompting.
* `experiments_run/usage_ng/human_evaluation` contains the final results from the forms, and a notebook to analyse the results.
* `experiments_run/usage_ng/qa_prompt_answer` contains the saved prompts and answers for the three prompting techniques.


## 6. Run the interface

--- 

We also implemented an interface to compare the impact of the filters and parameters on the search - `ordering` and `filtering` from the config description in Section 3. of the README. By comparing two sets of parameters, you will also run the search in the backend.

To run a search, you might need to extract some additional information (c.f. Section 4. of the README).

In the terminal command, go to the app folder.
```bash
cd app
```

First open the `variables.py` file in that folder. You can add information on the dataset(s) you are using (`VARIABLES_DATASET`)). As specified in that file, you need to enter details about `dataset_path`, `data_files_path` (folder where are stored ground truth, referents and config files), `start_uri` and `nested_dataset`. You can also change the default values that will be displayed (`DEFAULT_VARIABLES`).

Then run the following to run the interface:
```bash
streamlit run app.py
```

Depending on the parameter and event that you choose, running the search can be slow. Likewise, displaying the HTML graphs can be slow.



---

## 7. Other

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


