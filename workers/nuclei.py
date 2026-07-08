
import os
import yaml
from pathlib import Path
from celery import Celery
from celery.signals import worker_process_init
import utils
import subprocess

app=Celery('atlasbrowser_task',broker='amqp://guest:guest@localhost:5672',backend='redis://localhost:6379')


@worker_process_init.connect()
def on_worker_init(**_):
    print("Nuclei Worker initiated")

@app.task(bind=True)
def task_list(self, *args, **kwargs):
    metafilename = Path(__file__).stem+".yml"
    taskobject = yaml.safe_load(open(metafilename,'r'))
    return taskobject


@app.task(bind=True)
def generate_position_files(self, qcparams, **kwargs):
    self.update_state(state="STARTED")
    self.update_state(state="PROGRESS", meta={"position": "preparation" , "progress" : 0})
    config=utils.load_configuration()
    ldataDirectory = Path(config['LDATA_DIRECTORY'])
    ############################################
    # Pull parameters from qcparams file
    
    root_dir_spatial = ldataDirectory.joinpath(qcparams['root_dir_spatial'])
    run_id = qcparams['run_id']
    dapi_path = ldataDirectory.joinpath(qcparams["dapi_path"])
    tixel_width = qcparams["tixel_width"]
    user_given_count = qcparams["user_given"]
    color_chan = qcparams['color_chan']
    min_area_in_tixel = qcparams["min_area_in_tixel"]
    
    spatial_dir = Path(root_dir_spatial).joinpath(run_id, 'spatial')

    ### output directories (S3)
    og_tissue_positions_filename = spatial_dir.joinpath('tissue_positions_list.csv')
    tissue_position = ldataDirectory.joinpath(og_tissue_positions_filename)

    self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 20})
    command = [
        "python",
        "/root/nuclei-count/concat_cell_counts.py",
        str(dapi_path),
        str(tissue_position),
        str(tixel_width),
        str(user_given_count),
        str(color_chan),
        str(min_area_in_tixel),
        str(run_id),
    ]
    subprocess.run(command)
    

        
    if os.path.exists(f"/root/nuclei-count/{run_id}/tissue_positions_count.csv"):
        self.update_state(state="SUCCESS", meta={"position": "Finished" , "progress" : 100})
        out = 'Success'
    else:
        self.update_state(state="FAILURE", meta={"position": "Failed" , "progress" : 100})
        out = 'Fail'
        
    return out
