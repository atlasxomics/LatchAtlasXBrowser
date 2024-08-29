
import os
from threading import local
import yaml,json,csv
import PIL
from PIL import Image
from pathlib import Path
import shutil
import pandas as pd
from matplotlib.image import imread
import json
from pathlib import Path
Image.MAX_IMAGE_PIXELS = None
from celery import Celery
from celery.signals import worker_process_init
import utils
import cv2
import math
import subprocess
import time

app=Celery('nuclei_task',broker='amqp://'+os.environ['RABBITMQ_HOST'],backend='redis://'+os.environ['REDIS_HOST'])

def sleeper():
    time.sleep(20)

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
    
    ############################################
    # Pull parameters from qcparams file
    
    temp_dir = config['TEMP_DIRECTORY'] 
    upload_list=[]
    
    root_dir_spatial = qcparams['root_dir_spatial']
    bucket_name_spatial = qcparams.get("bucket_name_spatial")
    run_id = qcparams['run_id']
    dapi_path = qcparams["dapi_path"]
    tixel_width = qcparams["tixel_width"]
    user_given_count = qcparams["user_given"]
    color_chan = qcparams['color_chan']
    min_area_in_tixel = qcparams["min_area_in_tixel"]
    
    config["S3_BUCKET_NAME"] =  bucket_name_spatial
    aws_s3=utils.AWS_S3(config)

    temp_path = Path(temp_dir).joinpath(root_dir_spatial, run_id)


    spatial_dir = Path(root_dir_spatial).joinpath(run_id, 'spatial')
    figure_dir = Path(root_dir_spatial).joinpath(run_id, 'spatial', 'figure')
    ### local temp directories
    local_spatial_dir = Path(temp_dir).joinpath(spatial_dir)
    local_figure_dir = Path(temp_dir).joinpath(figure_dir)
    local_spatial_dir.mkdir(parents=True, exist_ok=True)
    local_figure_dir.mkdir(parents=True, exist_ok=True)

    ### output directories (S3)
    og_tissue_positions_filename = spatial_dir.joinpath('tissue_positions_list.csv')
    tissue_positions_filename = spatial_dir.joinpath('tissue_positions_count.csv')
    local_tissue_positions_filename = local_spatial_dir.joinpath('tissue_positions_count.csv')
    barcode_only_filename = spatial_dir.joinpath('single-cell_list.csv')
    local_barcode_only_filename = local_spatial_dir.joinpath('single-cell_list.csv')

    tissue_pos_path = aws_s3.getFileObject(og_tissue_positions_filename)
    image_object = aws_s3.getFileObject(dapi_path)
    
    

    
    command = f"echo 'python /home/ubuntu/nuclei-count/concat_cell_counts.py {image_object} {tissue_pos_path} {tixel_width} {local_tissue_positions_filename} {local_barcode_only_filename} {user_given_count} {color_chan} {min_area_in_tixel} {run_id} ' > /home/ubuntu/pipe/mypipe"
    print(command)
    subprocess.run(command, shell=True)
    
    self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 20})
    
    count = 0
    begin = time.time()
    while not os.path.exists(local_tissue_positions_filename) and count < 14400:
        sleeper()
        check = time.time()
        count = check - begin
        
    if os.path.exists(local_tissue_positions_filename):
        self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 60})
        upload_list.append([local_tissue_positions_filename, tissue_positions_filename])
        upload_list.append([local_barcode_only_filename, barcode_only_filename])
        
        for local_filename, output_key in upload_list:
            aws_s3.uploadFile(str(local_filename), str(output_key))
        
        self.update_state(state="SUCCESS", meta={"position": "Finished" , "progress" : 100})
        out=[list(map(str, x)) for x in upload_list]
    else:
        self.update_state(state="FAILURE", meta={"position": "Failed" , "progress" : 100})
        out = 'Fail'
        
    return out

