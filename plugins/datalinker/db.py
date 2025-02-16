from flask import current_app, g, flash
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, FOAF

# SQLAlchemy rdf store
def get_db():
    if 'db' not in g:
        g.db = Graph(store="SQLAlchemy", identifier="my_triplestore")
        g.db.open("sqlite:///rdf_store.db", create=True) 
    return g.db

def get_projects(id="?project_id"):
    db = get_db()    
    # Convert to pass a string argument
    if id != "?project_id":
        id = f'"{id}"'
    # Query using SPARQL
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dl: <http://datalinker.net/ldp/ontology#>
    PREFIX dcterms: <http://purl.org/dc/terms/> 
    SELECT * WHERE{
        ?project_uri a foaf:Project;
            rdfs:label ?title;
            rdfs:comment ?description;
            dcterms:created ?created;
            dcterms:identifier %s.
    }""" % (id)
    projects = db.query(query)
    # For a specific results the "projects" is a single output instead of a set

    if id != "?project_id":
        for project in projects:
            projects = project

    return projects


def get_datasets(pj_id="?project_id", id="?dataset_id"):
    db = get_db()
    # Convert to pass a string argument
    if pj_id != "?project_id":
        pj_id = f'"{pj_id}"'
    if id != "?dataset_id":
        id = f'"{id}"'
    # Query using SPARQL
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dl: <http://datalinker.io/ld/ontology#>
    PREFIX dcterms: <http://purl.org/dc/terms/> 
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT * WHERE {
        ?project_uri a foaf:Project;
            dcterms:identifier %s;
            dcat:dataset ?dataset_uri.            
        ?dataset_uri a dcat:Dataset;
            dl:filename ?filename;
            dcterms:title ?title;
            rdfs:comment ?description;
            dcterms:created ?created;
            dcterms:identifier %s.
            OPTIONAL {
            ?dataset_uri dl:preprocessingOperations ?preproc_ops.
            ?preproc_ops dl:filename ?preproc_ops_filename;
                        dcterms:identifier ?preproc_ops_id.
            }
            OPTIONAL {
            ?dataset_uri dl:mappings ?mappings.
            ?mappings dl:filename ?mappings_filename;
                        dcterms:identifier ?mappings_id.
            }
            OPTIONAL {
            ?dataset_uri dl:shapes ?shapes.
            ?shapes dl:filename ?shapes_filename;
                        dcterms:identifier ?shapes_id.
            }
    }"""% (pj_id, id) 
    datasets = db.query(query)
    # For a specific results the "datasets" is a single output instead of a set
    if id != "?dataset_id":
        for dataset in datasets:
            datasets = dataset

    return datasets

### SQLITE3 DB
# def get_db():
#     if 'db' not in g:
#         g.db = sqlite3.connect(
#             current_app.config['DATABASE'],
#             detect_types=sqlite3.PARSE_DECLTYPES
#         )
#         g.db.row_factory = sqlite3.Row

#     return g.db

# def close_db(e=None):
#     db = g.pop('db', None)

#     if db is not None:
#         db.close()

# def init_db():
#     db = get_db()

#     with current_app.open_resource('schema.sql') as f:
#         db.executescript(f.read().decode('utf8'))

# def init_app(app):
#     app.teardown_appcontext(close_db)
#     app.cli.add_command(init_db_command)

# @click.command('init-db')
# def init_db_command():
#     """Clear the existing data and create new tables."""
#     init_db()
#     click.echo('Initialized the database.')


# # %%

# # Define namespaces
# EX = Namespace("http://example.org/")

# # Load or create an RDF triplestore using SQLite
# store = "SQLAlchemy"
# g = Graph(store=store, identifier="rdf_store")
# g.open("sqlite:///rdf_store.db", create=True)

# # Add triples
# g.add((EX.Alice, RDF.type, FOAF.Person))
# g.add((EX.Alice, FOAF.name, Literal("Alice")))

# # Commit changes
# g.commit()

# # Query using SPARQL
# query = """
#     SELECT ?s ?p ?o WHERE { ?s ?p ?o }
# """
# for row in g.query(query):
#     print(row)

# # Close the store
# g.close()