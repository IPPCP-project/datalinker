import os, shutil
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, flash, request, redirect, url_for, send_from_directory
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose
from datalinker.dag import generate, generate_one, trigger
from datalinker.db import get_db, get_projects, get_datasets
from datalinker.utils import allowed_file
from airflow.www.app import csrf
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
from airflow import configuration as conf
from rdflib import Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, FOAF, XSD
from datetime import datetime

class DL(AppBuilderBaseView):
    default_view = "index"

    @expose("/", methods=["GET", "POST"])
    @csrf.exempt
    def index(self):
        datasets = get_datasets()

        if request.method == 'POST': 
            try:
                generate()
            except Exception as e:
                file_url = None
                error = e
                flash(error)
            return redirect(url_for("Airflow.index"))
        return self.render_template("/index.html", datasets=datasets )

    @expose("/create", methods=["GET", "POST"])
    @csrf.exempt
    def create(self):
        datasets = get_datasets()
        if request.method == 'POST':
            dataset_id = request.form['dataset_id']
            title = request.form['title']
            body = request.form['body']

            error = None
            if 'file' not in request.files:
                error = "No file part"
            file = request.files['file']
            if file.filename == '':
                error = "No selected file"
            if not dataset_id:
                error = 'dataset ID is required.'
            if not title:
                error = 'Title is required.'
            if not body:
                error = 'Description is required.'
            if file and not allowed_file(file.filename):
                error = "File with not allowed extension."
            if error is not None:
                flash(error)
            else:
                try:
                    exist = False
                    for dataset in datasets:
                        if dataset["id"].eq(dataset_id) :
                            exist =  True
                    if exist:
                        error = "Dataset ID already in use"
                        flash(error)
                    else:
                        db = get_db()                        
                        update = """
                        PREFIX dl: <http://datalinker.io/ld/ontology#>
                        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                        PREFIX dcterms: <http://purl.org/dc/terms/>
                        PREFIX dcat: <http://www.w3.org/ns/dcat#>

                        INSERT {
                        <http://datalinker.io/ld/resources/Dataset#%s> a dcat:Dataset;
                            dcterms:title "%s";
                            rdfs:comment "%s";
                            dcterms:created ?created;
                            dcterms:identifier "%s";
                            dl:filename "%s".
                        } 
                        WHERE {
                            BIND(NOW() as ?created)
                        }""" % ( dataset_id, title, body, dataset_id, file.filename)
                        db.update(update)

                        # DL = Namespace("http://datalinker.io/ld/ontology")
                        # DCTERMS = Namespace("http://purl.org/dc/terms/")
                        # DCAT = Namespace("http://www.w3.org/ns/dcat#")
                        # dataset = URIRef(f"http://datalinker.io/ld/resources/Dataset#{dataset_id}")

                        # db.add((dataset, RDF.type, DCAT.Dataset))
                        # db.add((dataset, DCTERMS.title, Literal(title)))
                        # db.add((dataset, DCTERMS.identifier, Literal(dataset_id)))
                        # db.add((dataset, DCTERMS.created, Literal(datetime.now().strftime('%Y-%m-%dT%H:%M:%S') , datatype=XSD.dateTime)))
                        # db.add((dataset, RDFS.comment, Literal(body)))
                        # db.add((dataset, DL.filename, Literal(file.filename)))

                        # db.commit()
                        db.close()

                        # Set upload folders
                        raw_data_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{dataset_id}/raw/")
                        refined_data_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{dataset_id}/refined/")
                        os.makedirs(raw_data_folder, exist_ok=True)
                        os.makedirs(refined_data_folder, exist_ok=True)

                        filename = secure_filename(file.filename)
                        # Copy raw data in the dataset folder
                        file.save(os.path.join(raw_data_folder, filename))
                        return redirect(url_for('.index'))
                except Exception as e:
                    error = e
                    flash(error)
        return self.render_template("/create.html" )

    @expose('/<dataset_id>/edit', methods=('GET', 'POST'))
    @csrf.exempt
    def edit(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        print(dataset)
        if request.method == 'POST':
            title = request.form['title']
            body = request.form['body']
            error = None

            if not title:
                error = 'Title is required.'

            if not body:
                error = 'Body is required.'

            if error is not None:
                flash(error)
            else:
                try:
                    db = get_db()
                    update = """
                    PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/> 
                    DELETE { <%s> dcterms:title ?title;
                                rdfs:comment ?body. }
                    INSERT { <%s> dcterms:title "%s";
                                rdfs:comment "%s";
                                dcterms:modified ?modified. }
                    WHERE
                    { <%s> dcterms:title ?title;
                        rdfs:comment ?body.
                    BIND(NOW() as ?modified)
                    }""" % (dataset['dataset_uri'], dataset['dataset_uri'], title, body, dataset['dataset_uri'])

                    db.update(update)
                    db.commit()
                    db.close()

                except Exception as e:
                    error = e
                    flash(error)

                return redirect(url_for('.index'))

        return self.render_template('/edit.html', dataset_id=dataset_id, dataset=dataset)

    @expose('/<dataset_id>/delete', methods=('POST',))
    @csrf.exempt
    def delete(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        if request.method == 'POST':
            try:
                db = get_db()
                delete = """
                    DELETE WHERE {
                        <%s> ?property ?value.
                    }""" % (dataset['dataset_uri'])
                db.update(delete)
                db.commit()
                db.close()

                dataset_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{dataset_id}/")

                shutil.rmtree(dataset_folder, ignore_errors=True)

            except Exception as e:
                error = e
                flash(error)
            return redirect(url_for('.index'))


    @expose('/<dataset_id>/config', methods=('GET', 'POST'))
    @csrf.exempt
    def config(self, dataset_id):
        dataset = get_datasets(dataset_id)
        # dataset_name = dataset_id + "/raw"
        # filename = dataset['filename']
        try:
            file_url = url_for('.index')
            preproc_file_url = url_for('.index')
            # if "preproc_ops_filename" in dataset:
            #     dataset_name = dataset_id + "/preprocessing"
            #     filename = dataset['preproc_ops_filename']
            #     preproc_file_url = get_file_url(get_obs_client(),
            #                     bucket_name=project_id,
            #                     dataset_name=dataset_name,
            #                     filename=filename)
            # else:
            #     preproc_file_url = None

        except Exception as e:
            file_url = None
            error = e
            flash(error)

        # if request.method == 'POST':
        #     title = request.form['title']
        #     body = request.form['body']
        #     error = None

        #     if not title:
        #         error = 'Title is required.'

        #     if error is not None:
        #         flash(error)
        #     else:
        #         db = get_db()
        #         db.execute(
        #             'UPDATE project SET title = ?, body = ?'
        #             ' WHERE id = ?',
        #             (title, body, id)
        #         )
        #         db.commit()
        #         return redirect(url_for('dataset.index'))
        return self.render_template('/config.html', dataset_id=dataset_id, dataset=dataset, file_url=file_url, preproc_file_url=preproc_file_url )

    @expose('/<dataset_id>/load-preproc-ops', methods=('GET', 'POST'))
    @csrf.exempt
    def load_preproc_ops(self, dataset_id):
        dataset = get_datasets(dataset_id)

        if request.method == 'POST':

            error = None

            if 'preproc_ops_file' not in request.files:
                error = "No file part"

            file = request.files['preproc_ops_file']
            if file.filename == '':
                error = "No selected file"

            if file and not allowed_file(file.filename, extensions_dict={'json'}):
                error = "File with not allowed extension. Expected an OpenRefine operations JSON file."
            if error is not None:
                flash(error)
            else:
                try:
                    preproc_ops_id = dataset_id + "-preproc-ops"
                    db = get_db()
                    update = """
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>

                    INSERT {
                    <%s> dl:preprocessingOperations
                                <http://datalinker.io/ld/resources/PreprocessingOperations#%s>.
                    <http://datalinker.io/ld/resources/PreprocessingOperations#%s> a dl:PreprocessingOperations;
                        dcterms:created ?created;
                        dcterms:identifier "%s";
                        dl:filename "%s".
                    } 
                    WHERE {
                        BIND(NOW() as ?created)
                    }""" % ( dataset['dataset_uri'], preproc_ops_id, preproc_ops_id, preproc_ops_id, file.filename)
                    db.update(update)
                    db.commit()
                    db.close()

                    # Set upload folder
                    upload_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{dataset_id}/preprocessing/")
                    os.makedirs(upload_folder, exist_ok=True)

                    filename = secure_filename(file.filename)
                    # Copy raw data in the dataset folder
                    file.save(os.path.join(upload_folder, filename))
                    flash("Pre-processing operations file loaded.")
                except Exception as e:
                    error = e
                    flash(error)

        return redirect(url_for('.config',dataset_id=dataset_id))

    @expose('/<dataset_id>/load-mappings', methods=('GET', 'POST'))
    @csrf.exempt
    def load_mappings(self, dataset_id):
        dataset = get_datasets(dataset_id)

        if request.method == 'POST':

            error = None

            if 'mappings_file' not in request.files:
                error = "No file part"

            file = request.files['mappings_file']
            if file.filename == '':
                error = "No selected file"

            if file and not allowed_file(file.filename, extensions_dict={'ttl', 'xml', 'drawio'}):
                error = "File with not allowed extension. ttl, xml or drawio file expected."
            if error is not None:
                flash(error)
            else:
                try:
                    mappings_id = dataset_id + "-mappings"
                    db = get_db()
                    update = """
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>

                    INSERT {
                    <%s> dl:mappings
                                <http://datalinker.io/ld/resources/Mappings#%s>.
                    <http://datalinker.io/ld/resources/Mappings#%s> a dl:Mappings;
                        dcterms:created ?created;
                        dcterms:identifier "%s";
                        dl:filename "%s".
                    } 
                    WHERE {
                        BIND(NOW() as ?created)
                    }""" % ( dataset['dataset_uri'], mappings_id, mappings_id, mappings_id, file.filename)
                    db.update(update)
                    db.commit()
                    db.close()

                    # Set upload folder
                    upload_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{dataset_id}/mappings/")
                    os.makedirs(upload_folder, exist_ok=True)

                    filename = secure_filename(file.filename)
                    # Copy raw data in the dataset folder
                    file.save(os.path.join(upload_folder, filename))
                    flash("Mappings file loaded.")
                except Exception as e:
                    error = e
                    flash(error)

        return redirect(url_for('.config',dataset_id=dataset_id))

    @expose('/<dataset_id>/load-shapes', methods=('GET', 'POST'))
    @csrf.exempt
    def load_shapes(self, dataset_id):
        dataset = get_datasets(dataset_id)

        if request.method == 'POST':
            error = None
            if 'shapes_file' not in request.files:
                error = "No file part"
            file = request.files['shapes_file']
            if file.filename == '':
                error = "No selected file"
            if file and not allowed_file(file.filename, extensions_dict={'ttl', 'xml', 'drawio'}):
                error = "File with not allowed extension. ttl, xml or drawio file expected."
            if error is not None:
                flash(error)
            else:
                try:
                    shapes_id = dataset_id + "-shapes"
                    db = get_db()
                    update = """
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>

                    INSERT {
                    <%s> dl:shapes
                                <http://datalinker.io/ld/resources/Shapes#%s>.
                    <http://datalinker.io/ld/resources/Shapes#%s> a dl:Shapes;
                        dcterms:created ?created;
                        dcterms:identifier "%s";
                        dl:filename "%s".
                    } 
                    WHERE {
                        BIND(NOW() as ?created)
                    }""" % ( dataset['dataset_uri'], shapes_id, shapes_id, shapes_id, file.filename)
                    db.update(update)
                    db.commit()
                    db.close()

                    # Set upload folder
                    upload_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{dataset_id}/shapes/")
                    os.makedirs(upload_folder, exist_ok=True)

                    filename = secure_filename(file.filename)
                    # Copy raw data in the dataset folder
                    file.save(os.path.join(upload_folder, filename))
                    flash("Shapes file loaded.")
                except Exception as e:
                    error = e
                    flash(error)

        return redirect(url_for('.config',dataset_id=dataset_id))

    @expose("/<dataset_id>/generate-dag", methods=["GET", "POST"])
    @csrf.exempt
    def generate_dag(self, dataset_id):
        dataset = get_datasets(dataset_id)
        template = request.form['template']
        if request.method == 'POST': 
            try:
                generate_one(template, dataset_id)
                flash("DAG generated")

            except Exception as e:
                file_url = None
                error = e
                flash(error)
            # return redirect(url_for("Airflow.index"))
        # return self.render_template("/index.html", datasets=datasets )
        return redirect(url_for('.config',dataset_id=dataset_id))

    @expose("/<dataset_id>/trigger-dag", methods=["GET", "POST"])
    @csrf.exempt
    def trigger_dag(self, dataset_id):
        dataset = get_datasets(dataset_id)
        dag_id = dataset_id + "_preprocessing"
        refined_data_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{dataset_id}/refined/")

        if request.method == 'POST': 
            try:
                shutil.rmtree(refined_data_folder)
                os.makedirs(refined_data_folder, exist_ok=True)

                trigger(dag_id)
                flash("Pre-processing DAG triggered")

            except Exception as e:
                file_url = None
                error = e
                flash(error)
        return redirect(url_for('.config',dataset_id=dataset_id))


# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "dl_plugin",
    __name__,
    template_folder="templates",   # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
)

v_appbuilder_view = DL()
v_appbuilder_package = {
    "name": "Datalinker",
    "category": "DL Plugin",
    "view": v_appbuilder_view,
}
class AirflowLDPlugin(AirflowPlugin):
    name = "datalinker"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]