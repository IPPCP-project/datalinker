from jinja2 import Environment, FileSystemLoader
from airflow import configuration as conf
from datalinker.db import get_datasets, get_ssots
import yaml
import os, subprocess

def generate():
    file_dir = os.path.dirname(os.path.abspath(f"{__file__}/"))
    env = Environment(loader=FileSystemLoader(file_dir))
    template = env.get_template('dag_templates/dag_template.jinja2')

    for filename in os.listdir(f"{file_dir}/dag_inputs"):
        print(filename)
        if filename.endswith('.yaml'):
            with open(f"{file_dir}/dag_inputs/{filename}", "r") as input_file:
                inputs = yaml.safe_load(input_file)
                with open(f"dags/preprocessing_{inputs['id']}.py", "w") as f:
                    f.write(template.render(inputs))

def generate_one(template, dataset_id):
    dataset = get_datasets(id=dataset_id)
    plugin_dir = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/")
    env = Environment(loader=FileSystemLoader(plugin_dir))
    template_obj = env.get_template(f'dag_templates/{template}.jinja2')
    
    inputs = { 'dataset_id': dataset_id,
               'project_id' : dataset['project_id'],
               'filename': dataset['filename'], #.split(".")[0],
               'mappings_filename': dataset['mappings_filename'],
               'shapes_filename': dataset['shapes_filename'] }

    with open(f"dags/{dataset_id}_{template}.py", "w") as f:
        f.write(template_obj.render(inputs))
    
    parse_dags()

def diagram_to_rdf(ssot_id):
    ssot = get_ssots(id=ssot_id)
    plugin_dir = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/")
    env = Environment(loader=FileSystemLoader(plugin_dir))
    template_obj = env.get_template(f'dag_templates/diagram_to_rdf.jinja2')
    
    inputs = { 'ssot_id': ssot_id,
               'project_id' : ssot['project_id'],
               'ssot_filename': ssot['ssot_filename'] }

    with open(f"dags/{ssot_id}_diagram_to_rdf.py", "w") as f:
        f.write(template_obj.render(inputs))
    
    parse_dags()

def materialisation(ssot_id):
    ssot = get_ssots(id=ssot_id)
    plugin_dir = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/")
    env = Environment(loader=FileSystemLoader(plugin_dir))
    template_obj = env.get_template(f'dag_templates/materialisation.jinja2')
    
    inputs = { 'ssot_id': ssot_id,
               'project_id' : ssot['project_id'],
               'ssot_filename': ssot['ssot_filename'] }

    with open(f"dags/{ssot_id}_materialisation.py", "w") as f:
        f.write(template_obj.render(inputs))
    
    parse_dags()


def validation(ssot_id):
    ssot = get_ssots(id=ssot_id)
    plugin_dir = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/")
    env = Environment(loader=FileSystemLoader(plugin_dir))
    template_obj = env.get_template(f'dag_templates/validation.jinja2')
    
    inputs = { 'ssot_id': ssot_id,
               'project_id' : ssot['project_id'],
               'ssot_filename': ssot['ssot_filename'] }

    with open(f"dags/{ssot_id}_validation.py", "w") as f:
        f.write(template_obj.render(inputs))
    
    parse_dags()


def generate_ssot(ssot_id):
    ssot = get_ssots(id=ssot_id)
    plugin_dir = os.path.join(conf.AIRFLOW_HOME, f"plugins/datalinker/")
    env = Environment(loader=FileSystemLoader(plugin_dir))
    template_obj = env.get_template(f'dag_templates/ssot.jinja2')
    
    inputs = { 'ssot_id': ssot_id,
               'project_id' : ssot['project_id'],
               'ssot_filename': ssot['ssot_filename'] }

    with open(f"dags/{ssot_id}_ssot.py", "w") as f:
        f.write(template_obj.render(inputs))
    
    parse_dags()


def parse_dags():
    command = ["airflow", "dags", "reserialize"]
    result = subprocess.run(command, capture_output=True, text=True)

def unpause(dag_id):
    command = ["airflow", "dags", "unpause", dag_id]
    result = subprocess.run(command, capture_output=True, text=True)

def trigger(dag_id):
    command = ["airflow", "dags", "trigger", dag_id]
    result = subprocess.run(command, capture_output=True, text=True)