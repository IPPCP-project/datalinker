# %%
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, FOAF

db = Graph(store="SQLAlchemy", identifier="my_triplestore")
db.open("sqlite:///rdf_store.db", create=True)
# %%
query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dl: <http://datalinker.io/ld/ontology#>
    PREFIX dcterms: <http://purl.org/dc/terms/> 
    SELECT * WHERE{
        ?project_uri a dl:Project;
            dcterms:identifier ?project_id.
            OPTIONAL {
            ?project_uri rdfs:label ?title;
                dl:useCaseDefinition ?use_case;
                dl:dataSource ?data_source ;
                dl:dataRequirement ?data_requirement ;
                dcterms:created ?created.}
            OPTIONAL {
            ?project_uri dcat:dataset ?dataset_uri.}
            
            OPTIONAL {
            ?project_uri dl:ontology ?ontology_uri.
            ?ontology_uri dl:filename ?ontology_filename;
                        dcterms:identifier ?ontology_id.
            }
    }"""
projects = db.query(query)

for pj in projects.bindings:
    print(pj)
# for project in projects:
#     print(project["project_uri"], project["project_id"], project["use_case"], project["data_source"], project["data_requirement"])
# %%
db.update("""DELETE WHERE {
                <http://datalinker.io/ld/resources/Dataset/bigg_ign> ?property ?value.
            }""")

# %%
