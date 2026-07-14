# Datalinker
An [Airflow](https://airflow.apache.org/) plugin that leverages its workflows creating through templating ready to use data operations as Directed Acyclic Graphs (DAGs) for knowledge graph construction and validation from raw data and a Single Source of Truth diagram in [Chowlk notation](https://chowlk.linkeddata.es/notation.html).

## To install the plugin:
1. Make sure you have Apache Airflow (>=2.10.4) installed.
2. Clone the current repository and copy its contents.
3. Paste the repository contents in the root of your Airflow installation (in the same folder where the `dags` folder resides).
4. Configure and run Airflow web server according to your setup by following the [documentation](https://airflow.apache.org/docs/apache-airflow/stable/start.html).
5. As a result you should be able to access to the Datalinker tab inside the Airflow web user interface.

_The plugin was developed using the 2.10.4 version of Airflow (yet to test with other versions)_

## Usage workflow

1. Create a new project by providing its metadata
2. Inside the project's view in the Datasets tab you can upload as many datasets you like. Each of the dataset upload action creates and triggers a DAG with a data profiling operation (currently only available for csv files)
3. Upload in the SSoT tab a single `.drawio` diagram containing the corresponding transformation operations + mappings + ontology + validation shapes of the datasets provided in in Chowlk notation. It will display the diagram below and enable the Construct and validate knowledge graph button.
4. By pressing the Construct and validate knowledge graph button the plugin generates and triggers the DAGS for the diagram conversion to triples, plus for the knowledge graph materialisation and validation following what has been declared in the diagram.
5. The outputs of such operations will be available in the `plugins/datalinker/data/<project_id>` folder.
6. The Endpoint tab contains two options: **Enable SPARQL endpoint** (_which creates and triggers a new DAG that serves the knowledge graph in the port 7878_) and **Download complete Knowledge Graph (NT serialisation)** (_with the hyperlink to the asserted data_)
