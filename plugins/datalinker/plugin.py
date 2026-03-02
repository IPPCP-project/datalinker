import os, shutil
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, flash, request, redirect, url_for, send_file, g
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_appbuilder import expose
from datalinker.dag import generate, generate_one, trigger, unpause, parse_dags
from datalinker.db import get_db, get_projects, get_datasets, get_data_sources, get_data_requirements
from datalinker.utils import allowed_file
from datalinker.profile import compact_profile, get_variables
from airflow.www.app import csrf
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
from airflow import configuration as conf

class LD(AppBuilderBaseView):
    default_view = "pj_index"
    # Projects
    @expose("/", methods=["GET", "POST"])
    @csrf.exempt
    def pj_index(self):
        projects = get_projects()
        # for project in projects:
        #     for ds in get_data_sources(project["project_id"]):
        #         flash(ds["data_source"])
        #     for dr in get_data_requirements(project["project_id"]):
        #         flash(dr["data_requirement"])

        return self.render_template("/projects/index.html", projects=projects )

    @expose("/create-project", methods=["GET", "POST"])
    @csrf.exempt
    def pj_create(self):
        if request.method == 'POST':
            project_id = request.form['project_id']
            title = request.form['title']
            use_case = request.form['body']
            d_reqs = request.form.getlist("d_reqs")
            if len(d_reqs) > 1:
                d_reqs = '", "'.join(d_reqs)
            elif len(d_reqs) == 1:
                d_reqs = "".join(d_reqs)
            d_sources = request.form.getlist("d_sources")
            if len(d_sources) > 1:
                d_sources = "<"+">, <".join(d_sources)+">"
            elif len(d_sources) == 1:
                d_sources = "<"+"".join(d_sources)+">"
            error = None
            if not project_id:
                error = 'Project ID is required.'
            if not title:
                error = 'Title is required.'
            if not use_case:
                error = 'Use case definition is required.'
            if not d_reqs:
                error = 'At least one data requirement is required.'
            if not d_sources:
                error = 'At least one data source is required.'
            if error is not None:
                flash(error)
            else:
                try:
                    # account_name = g.user.split(" ")[0]
                    db = get_db()
                    update = """
                    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    INSERT {
                    <http://datalinker.io/ld/resources/Project/%s> a dl:Project ;
                        rdfs:label "%s" ;
                        dl:useCaseDefinition "%s";
                        dl:dataSource %s ;
                        dl:dataRequirement "%s" ;
                        dcterms:created ?created ;
                        dcterms:identifier "%s".
                    } 
                    WHERE {
                        BIND(NOW() as ?created)
                    }""" % (project_id, title, use_case, d_sources, d_reqs, project_id)
                    db.update(update)
                    db.commit()
                    # Set upload folders
                    project_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/")
                    os.makedirs(project_folder, exist_ok=True)
                    return redirect(url_for('.pj_index'))
                except Exception as e:
                    error = e
                    flash(error)
        return self.render_template('projects/create.html')

    @expose("/resources/Project/<project_id>/edit", methods=["GET", "POST"])
    @csrf.exempt
    def pj_edit(self, project_id):
        project = get_projects(project_id)
        data_sources = get_data_sources(project_id)
        data_requirements = get_data_requirements(project_id)

        if request.method == 'POST':
            title = request.form['title']
            use_case = request.form['body']
            d_reqs = request.form.getlist("d_reqs")
            if len(d_reqs) > 1:
                d_reqs = '", "'.join(d_reqs)
            elif len(d_reqs) == 1:
                d_reqs = "".join(d_reqs)
            d_sources = request.form.getlist("d_sources")
            if len(d_sources) > 1:
                d_sources = "<"+">, <".join(d_sources)+">"
            elif len(d_sources) == 1:
                d_sources = "<"+"".join(d_sources)+">"
            error = None
            if not title:
                error = 'Title is required.'
            if not use_case:
                error = 'Use case definition is required.'
            if not d_reqs:
                error = 'At least one data requirement is required.'
            if not d_sources:
                error = 'At least one data source is required.'
            if error is not None:
                flash(error)
            else:
                try:
                    db = get_db()
                    update = """
                    PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/> 
                    DELETE { <%s> rdfs:label ?title;
                                dl:useCaseDefinition ?use_case ;
                                dl:dataSource ?data_source ;
                                dl:dataRequirement ?data_requirement . }
                    INSERT { <%s> rdfs:label "%s" ;
                                dl:useCaseDefinition "%s" ;
                                dl:dataSource %s ;
                                dl:dataRequirement "%s" . }
                    WHERE { <%s> rdfs:label ?title;
                                dl:useCaseDefinition ?use_case ;
                                dl:dataSource ?data_source ;
                                dl:dataRequirement ?data_requirement .
                    }""" % (project['project_uri'], project['project_uri'], title, use_case, d_sources, d_reqs, project['project_uri'] )
                    db.update(update)
                    db.commit()

                except Exception as e:
                    error = e
                    flash(error)

                return redirect(url_for('.pj_index'))

        return self.render_template('projects/edit.html', project_id=project_id, project=project, data_requirements=data_requirements, data_sources=data_sources)

    @expose('/resources/Project/<project_id>/delete', methods=('POST',))
    @csrf.exempt
    def pj_delete(self, project_id):
        project = get_projects(project_id)
        if request.method == 'POST':
            try:
                db = get_db()
                delete = """
                    DELETE WHERE {
                        <%s> ?property ?value.
                    }""" % (project['project_uri'])
                db.update(delete)
                db.commit()
                db.close()

                project_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/")

                shutil.rmtree(project_folder, ignore_errors=True)

            except Exception as e:
                error = e
                flash(error)
            return redirect(url_for('.pj_index'))

    # Datasets
    @expose("/resources/Project/<project_id>", methods=["GET", "POST"])
    @csrf.exempt
    def ds_index(self, project_id):
        project = get_projects(project_id)
        datasets = get_datasets(pj_id=project_id)
        data_sources = get_data_sources(project_id)
        data_requirements = get_data_requirements(project_id)

        if request.method == 'POST': 
            try:
                generate()
            except Exception as e:
                file_url = None
                error = e
                flash(error)
            return redirect(url_for("Airflow.index"))
        return self.render_template("/datasets/index.html", project=project, project_id=project_id, datasets=datasets, data_requirements=data_requirements, data_sources=data_sources )


    @expose('/resources/Dataset/<dataset_id>/profile', methods=('GET', 'POST'))
    @csrf.exempt
    def ds_profile(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        profile_path = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/profile/{dataset_id}_profile.html")
        with open(profile_path) as f:
            html_data = f.read()
        return compact_profile(html_data)

    @expose('/resources/Dataset/<dataset_id>/full-profile', methods=('GET', 'POST'))
    @csrf.exempt
    def ds_full_profile(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        profile_path = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/profile/{dataset_id}_profile.html")
        with open(profile_path) as f:
            html_data = f.read()
        return html_data


    @expose("/resources/Project/<project_id>/create", methods=["GET", "POST"])
    @csrf.exempt
    def ds_create(self, project_id):
        project = get_projects(project_id)
        datasets = get_datasets(pj_id=project_id)
        data_sources = get_data_sources(project_id)
        if request.method == 'POST':
            dataset_id = project_id + "_" + request.form['dataset_id']
            title = request.form['title']
            body = request.form['body']
            data_source = request.form['data_source']
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
                    if len(datasets.bindings) >= 1:
                        for dataset in datasets:
                            if dataset["dataset_id"].eq(dataset_id) :
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
                        <%s> dcat:dataset <http://datalinker.io/ld/resources/Dataset/%s>.
                        <http://datalinker.io/ld/resources/Dataset/%s> a dcat:Dataset;
                            dcterms:title "%s";
                            rdfs:comment "%s";
                            dcterms:created ?created;
                            dcterms:identifier "%s";
                            dl:filename "%s";
                            dcat:landingPage <%s>.
                        } 
                        WHERE {
                            BIND(NOW() as ?created)
                        }""" % ( project['project_uri'], dataset_id, dataset_id, title, body, dataset_id, file.filename, data_source)
                        db.update(update)
                        db.commit()
                        # db.close()

                        # Set upload folders
                        profile_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/profile/")
                        os.makedirs(profile_folder, exist_ok=True)
                        raw_data_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/raw/")
                        os.makedirs(raw_data_folder, exist_ok=True)
                        refined_data_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/refined/")
                        os.makedirs(refined_data_folder, exist_ok=True)

                        filename = secure_filename(file.filename)
                        # Copy raw data in the dataset folder
                        file.save(os.path.join(raw_data_folder, filename))

                        # Generate and run data profiling DAG
                        if filename.split(".")[1] == "csv":
                            generate_one('data_profiling', dataset_id)
                            dag_id = dataset_id + "_data_profiling"
                            trigger(dag_id)
                            unpause(dag_id)
                            flash("Exploratory data analysis DAG created and running.")
                        with open(os.path.join(profile_folder,f"{dataset_id}_profile.html"), "w") as file:
                            if filename.split(".")[1] == "csv":
                                file.write(""" <style> h4, p {font-family: Helvetica}</style>
                                        <h4>Generating data profile...</h4> 
                                        <p>Please reload when the exploratory data analysis is completed.</p> 
                                        """)
                            else:
                                file.write(""" <style> h4, p {font-family: Helvetica; color:gray}</style>
                                        <small><p>Data profile not yet supported for the file type provided.</p></small>  
                                        """)
                        file.close()
                        return redirect(url_for('.ds_index', project_id=project_id))
                except Exception as e:
                    error = e
                    flash(error)
        return self.render_template("/datasets/create.html", project=project, project_id=project_id, data_sources=data_sources)

    @expose('/resources/Dataset/<dataset_id>/edit', methods=('GET', 'POST'))
    @csrf.exempt
    def ds_edit(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        project = get_projects(project_id)
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

                return redirect(url_for('.ds_index', project_id=project_id))

        return self.render_template('/datasets/edit.html', dataset_id=dataset_id, project=project, project_id=project_id, dataset=dataset)

    @expose('/resources/Dataset/<dataset_id>/delete', methods=('POST',))
    @csrf.exempt
    def ds_delete(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        project = get_projects(project_id)
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

                dataset_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/")

                shutil.rmtree(dataset_folder, ignore_errors=True)

            except Exception as e:
                error = e
                flash(error)
            return redirect(url_for('.ds_index', project_id=project_id))

    @expose("/resources/Project/<project_id>/config-ontology", methods=["GET", "POST"])
    @csrf.exempt
    def ont_config(self, project_id):
        project = get_projects(project_id)
        datasets = get_datasets(pj_id=project_id)
        try:
            variables = {}
            for dataset in datasets:
                dataset_id = dataset['dataset_id']
                profile_path = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/profile/{dataset_id}_profile.html")
                with open(profile_path) as f:
                    html_data = f.read()
                variables[dataset] = get_variables(html_data)
        except Exception as e:
            error = e
            flash(error)
        if request.method == 'POST':
            project_id = request.form['project_id']
            title = request.form['title']
            use_case = request.form['body']
            d_reqs = request.form.getlist("d_reqs")
            if len(d_reqs) > 1:
                d_reqs = '", "'.join(d_reqs)
            elif len(d_reqs) == 1:
                d_reqs = "".join(d_reqs)
            d_sources = request.form.getlist("d_sources")
            if len(d_sources) > 1:
                d_sources = "<"+">, <".join(d_sources)+">"
            elif len(d_sources) == 1:
                d_sources = "<"+"".join(d_sources)+">"
            error = None
            if not project_id:
                error = 'Project ID is required.'
            if not title:
                error = 'Title is required.'
            if not use_case:
                error = 'Use case definition is required.'
            if not d_reqs:
                error = 'At least one data requirement is required.'
            if not d_sources:
                error = 'At least one data source is required.'
            if error is not None:
                flash(error)
            else:
                try:
                    # account_name = g.user.split(" ")[0]
                    db = get_db()
                    update = """
                    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    INSERT {
                    <http://datalinker.io/ld/resources/Project/%s> a dl:Project ;
                        rdfs:label "%s" ;
                        dl:useCaseDefinition "%s";
                        dl:dataSource %s ;
                        dl:dataRequirement "%s" ;
                        dcterms:created ?created ;
                        dcterms:identifier "%s".
                    } 
                    WHERE {
                        BIND(NOW() as ?created)
                    }""" % (project_id, title, use_case, d_sources, d_reqs, project_id)
                    db.update(update)
                    db.commit()
                    # Set upload folders
                    project_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/")
                    os.makedirs(project_folder, exist_ok=True)
                    return redirect(url_for('.ds_index'))
                except Exception as e:
                    error = e
                    flash(error)
        return self.render_template('ontology/config.html', project=project, project_id=project_id)

    @expose("/resources/Project/<project_id>/edit-ontology", methods=["GET", "POST"])
    @csrf.exempt
    def ont_edit(self, project_id):
        project = get_projects(project_id)
        data_sources = get_data_sources(project_id)
        data_requirements = get_data_requirements(project_id)

        if request.method == 'POST':
            title = request.form['title']
            use_case = request.form['body']
            d_reqs = request.form.getlist("d_reqs")
            if len(d_reqs) > 1:
                d_reqs = '", "'.join(d_reqs)
            elif len(d_reqs) == 1:
                d_reqs = "".join(d_reqs)
            d_sources = request.form.getlist("d_sources")
            if len(d_sources) > 1:
                d_sources = "<"+">, <".join(d_sources)+">"
            elif len(d_sources) == 1:
                d_sources = "<"+"".join(d_sources)+">"
            error = None
            if not title:
                error = 'Title is required.'
            if not use_case:
                error = 'Use case definition is required.'
            if not d_reqs:
                error = 'At least one data requirement is required.'
            if not d_sources:
                error = 'At least one data source is required.'
            if error is not None:
                flash(error)
            else:
                try:
                    db = get_db()
                    update = """
                    PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/> 
                    DELETE { <%s> rdfs:label ?title;
                                dl:useCaseDefinition ?use_case ;
                                dl:dataSource ?data_source ;
                                dl:dataRequirement ?data_requirement . }
                    INSERT { <%s> rdfs:label "%s" ;
                                dl:useCaseDefinition "%s" ;
                                dl:dataSource %s ;
                                dl:dataRequirement "%s" . }
                    WHERE { <%s> rdfs:label ?title;
                                dl:useCaseDefinition ?use_case ;
                                dl:dataSource ?data_source ;
                                dl:dataRequirement ?data_requirement .
                    }""" % (project['project_uri'], project['project_uri'], title, use_case, d_sources, d_reqs, project['project_uri'] )
                    db.update(update)
                    db.commit()

                except Exception as e:
                    error = e
                    flash(error)

                return redirect(url_for('.ds_index'))

        return self.render_template('ontology/edit.html', project_id=project_id, project=project, data_requirements=data_requirements, data_sources=data_sources)

    @expose('/resources/Project/<project_id>/delete-ontology', methods=('POST',))
    @csrf.exempt
    def ont_delete(self, project_id):
        project = get_projects(project_id)
        if request.method == 'POST':
            try:
                db = get_db()
                delete = """
                    DELETE WHERE {
                        <%s> ?property ?value.
                    }""" % (project['project_uri'])
                db.update(delete)
                db.commit()
                db.close()

                project_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/")

                shutil.rmtree(project_folder, ignore_errors=True)

            except Exception as e:
                error = e
                flash(error)
            return redirect(url_for('.ds_index'))

    @expose('/resources/Project/<project_id>/load-ontology', methods=('GET', 'POST'))
    @csrf.exempt
    def load_ontology(self, project_id):
        project = get_projects(project_id)
        if request.method == 'POST':
            error = None
            if 'ontology_file' not in request.files:
                error = "No file part"
            file = request.files['ontology_file']
            if file.filename == '':
                error = "No selected file"
            if file and not allowed_file(file.filename, extensions_dict={'ttl', 'drawio'}):
                error = "File with not allowed extension. ttl or drawio file expected."
            if error is not None:
                flash(error)
            else:
                try:
                    ontology_id = project_id + "_ontology"
                    db = get_db()
                    update = """
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>
                    PREFIX owl: <http://www.w3.org/2002/07/owl#>

                    INSERT {
                    <%s> dl:ontology
                                <http://datalinker.io/ld/resources/Ontology/%s>.
                    <http://datalinker.io/ld/resources/Ontology/%s> a dl:Ontology;
                        dcterms:created ?created;
                        dcterms:identifier "%s";
                        dl:filename "%s".
                    } 
                    WHERE {
                        BIND(NOW() as ?created)
                    }""" % ( project['project_uri'], ontology_id, ontology_id, ontology_id, file.filename)
                    db.update(update)
                    db.commit()
                    db.close()

                    # Set upload folder
                    upload_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/ontology/")
                    os.makedirs(upload_folder, exist_ok=True)

                    filename = secure_filename(file.filename)
                    # Copy raw data in the dataset folder
                    file.save(os.path.join(upload_folder, filename))
                    flash("Ontology file loaded.")
                except Exception as e:
                    error = e
                    flash(error)
        return redirect(url_for('.ds_index',project_id=project_id))

    @expose('/download/<path:file_path>')
    def download(self, file_path):
        path = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{file_path}")
        return send_file(path, as_attachment=False)

    @expose('/resources/Dataset/<dataset_id>/config', methods=('GET', 'POST'))
    @csrf.exempt
    def ds_config(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        project = get_projects(project_id)
        # dataset_name = dataset_id + "/raw"
        # filename = dataset['filename']
        try:
            file_url = url_for('.ds_index', project_id=project_id)
            preproc_file_url = url_for('.ds_index', project_id=project_id)
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
        #         return redirect(url_for('dataset.ds_index'))
        return self.render_template('/datasets/config.html', project_id=project_id, project=project, dataset_id=dataset_id, dataset=dataset, file_url=file_url, preproc_file_url=preproc_file_url )

    @expose('/resources/Dataset/<dataset_id>/load-preproc-ops', methods=('GET', 'POST'))
    @csrf.exempt
    def load_preproc_ops(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        project = get_projects(project_id)

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
                    preproc_ops_id = dataset_id + "_preproc_ops"
                    db = get_db()
                    update = """
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>

                    INSERT {
                    <%s> dl:preprocessingOperations
                                <http://datalinker.io/ld/resources/PreprocessingOperations/%s>.
                    <http://datalinker.io/ld/resources/PreprocessingOperations/%s> a dl:PreprocessingOperations;
                        dcterms:created ?created;
                        dcterms:identifier "%s";
                        dl:filename "%s".
                    } 
                    WHERE {
                        BIND(NOW() as ?created)
                    }""" % ( dataset['dataset_uri'], preproc_ops_id, preproc_ops_id, preproc_ops_id, file.filename)
                    db.update(update)
                    db.commit()
                    # db.close()

                    # Set upload folder
                    upload_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/preprocessing/")
                    os.makedirs(upload_folder, exist_ok=True)

                    filename = secure_filename(file.filename)
                    # Copy raw data in the dataset folder
                    file.save(os.path.join(upload_folder, filename))
                    flash("Pre-processing operations file loaded.")
                except Exception as e:
                    error = e
                    flash(error)

        return redirect(url_for('.ds_config',dataset_id=dataset_id))

    @expose('/resources/Dataset/<dataset_id>/load-mappings', methods=('GET', 'POST'))
    @csrf.exempt
    def load_mappings(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        project = get_projects(project_id)

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
                    mappings_id = dataset_id + "_mappings"
                    db = get_db()
                    update = """
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>

                    INSERT {
                    <%s> dl:mappings
                                <http://datalinker.io/ld/resources/Mappings/%s>.
                    <http://datalinker.io/ld/resources/Mappings/%s> a dl:Mappings;
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
                    upload_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/mappings/")
                    os.makedirs(upload_folder, exist_ok=True)

                    filename = secure_filename(file.filename)
                    # Copy raw data in the dataset folder
                    file.save(os.path.join(upload_folder, filename))
                    flash("Mappings file loaded.")
                except Exception as e:
                    error = e
                    flash(error)

        return redirect(url_for('.ds_config',dataset_id=dataset_id))

    @expose('/resources/Dataset/<dataset_id>/load-shapes', methods=('GET', 'POST'))
    @csrf.exempt
    def load_shapes(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        project = get_projects(project_id)

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
                    shapes_id = dataset_id + "_shapes"
                    db = get_db()
                    update = """
                    PREFIX dl: <http://datalinker.io/ld/ontology#>
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    PREFIX dcterms: <http://purl.org/dc/terms/>
                    PREFIX dcat: <http://www.w3.org/ns/dcat#>

                    INSERT {
                    <%s> dl:shapes
                                <http://datalinker.io/ld/resources/Shapes/%s>.
                    <http://datalinker.io/ld/resources/Shapes/%s> a dl:Shapes;
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
                    upload_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/shapes/")
                    os.makedirs(upload_folder, exist_ok=True)

                    report_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/report/")
                    os.makedirs(upload_folder, exist_ok=True)

                    filename = secure_filename(file.filename)
                    # Copy raw data in the dataset folder
                    file.save(os.path.join(upload_folder, filename))
                    flash("Shapes file loaded.")
                except Exception as e:
                    error = e
                    flash(error)

        return redirect(url_for('.ds_config',dataset_id=dataset_id))

    @expose("/resources/Dataset/<dataset_id>/generate-dag", methods=["GET", "POST"])
    @csrf.exempt
    def generate_dag(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        project = get_projects(project_id)
        template = request.form['template']
        if request.method == 'POST': 
            try:
                generate_one(template, dataset_id)
                flash(f"{template} DAG generated")

            except Exception as e:
                file_url = None
                error = e
                flash(error)
            # return redirect(url_for("Airflow.ds_index"))
        # return self.render_template("/datasets/index.html", datasets=datasets )
        return redirect(url_for('.ds_config',dataset_id=dataset_id))

    @expose("/resources/Dataset/<dataset_id>/trigger-dag", methods=["GET", "POST"])
    @csrf.exempt
    def trigger_dag(self, dataset_id):
        dataset = get_datasets(id=dataset_id)
        project_id = dataset['project_id']
        project = get_projects(project_id)
        dag_id = dataset_id + "_preprocessing"
        refined_data_folder = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/data/{project_id}/{dataset_id}/refined/")

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
        return redirect(url_for('.ds_config',dataset_id=dataset_id))


# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "dl_plugin",
    __name__,
    template_folder="templates",   # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
)

v_appbuilder_view = LD()
v_appbuilder_package = {
    "name": "Projects",
    "category": "Datalinker",
    "view": v_appbuilder_view,
}
class AirflowLDPlugin(AirflowPlugin):
    name = "datalinker"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]