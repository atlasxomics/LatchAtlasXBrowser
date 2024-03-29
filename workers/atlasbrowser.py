
import os
import subprocess
import yaml,json,csv
from PIL import Image
from pathlib import Path
import json
from pathlib import Path
Image.MAX_IMAGE_PIXELS = None
from celery import Celery
from celery.signals import worker_process_init
import time
import shutil
import math
import numpy as np 
import utils
import cv2

app=Celery('atlasbrowser_task',broker='amqp://guest:guest@localhost:5672',backend='redis://localhost:6379')

@worker_process_init.connect()
def on_worker_init(**_):
    print("AtlasBrowser Worker initiated")

@app.task(bind=True)
def task_list(self, *args, **kwargs):
    metafilename = Path(__file__).stem+".yml"
    taskobject = yaml.safe_load(open(metafilename,'r'))
    return taskobject

def rotate_image_no_cropping(img, degree):
    (h, w) = img.shape[:2]
    (cX, cY) = (w // 2, h // 2)
    # rotate our image by 45 degrees around the center of the image
    M = cv2.getRotationMatrix2D((cX, cY), degree, 1.0)
    abs_cos = abs(M[0,0]) 
    abs_sin = abs(M[0,1])
    bound_w = int(h * abs_sin + w * abs_cos)
    bound_h = int(h * abs_cos + w * abs_sin)
    M[0, 2] += bound_w/2 - cX
    M[1, 2] += bound_h/2 - cY
    rotated = cv2.warpAffine(img, M, (bound_w, bound_h))
    return rotated
@app.task(bind=True)
def generate_spatial(self, qcparams, **kwargs):
    self.update_state(state="STARTED")
    self.update_state(state="PROGRESS", meta={"position": "preparation" , "progress" : 0})
    config=utils.load_configuration()
    ## config
    temp_dir = config['TEMP_DIRECTORY']
    latch_dir = '/ldata/spatials/'
    ## parameter parsing
    root_dir = '/root/LatchAtlasXBrowser/Images'
    metadata = qcparams['metadata']
    oldFiles = qcparams['files']
    scalefactors = qcparams['scalefactors']
    run_id = qcparams['run_id']
    tixel_positions = qcparams['mask']
    crop_coordinates = qcparams['crop_area']
    orientation = qcparams['orientation']
    rotation = (int(orientation['rotation']) % 360)
    bsa_path = qcparams['bsa_path']
    barcode_path = qcparams['barcode_path']
    postB_flag = qcparams['postB_flag']
    postb_path = "{}{}/".format(latch_dir,run_id)
    
    updating_existing = qcparams.get('updating_existing', False)

    next_gen_barcodes = True
    
    metadata["replaced_24_barcodes"] = next_gen_barcodes

    ### source image path
    allFiles = [i for i in oldFiles if '.json' not in i and 'spatial' not in i]
    ### output directories (S3)
    ##Images
    spatial_dir = Path(root_dir).joinpath(run_id, 'spatial')
    figure_dir = Path(root_dir).joinpath(run_id, 'spatial', 'figure')
    # if not figure_dir.exists(): figure_dir.mkdir(parents=True, exist_ok=True)
    if not spatial_dir.exists(): spatial_dir.mkdir(parents=True, exist_ok=True)
    if not figure_dir.exists(): figure_dir.mkdir(parents=True, exist_ok=True)

    ### read barcodes information 
    

    self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 20})
    barcodes = []
    try:
        with open(latch_dir + barcode_path,'r') as f:
            barcodes = f.read().splitlines()
    except:
        pass
    row_count = math.sqrt(len(barcodes))

    ### save metadata & scalefactors
    local_metadata_filename = spatial_dir.joinpath('metadata.json')
    local_scalefactors_filename = spatial_dir.joinpath('scalefactors_json.json')
    # json.dump(metadata, open(local_metadata_filename,'w'), indent=4,sort_keys=True)
    json.dump(metadata, open(local_metadata_filename,'w'), indent=4,sort_keys=True)
    # adding metadata and scalefactors to the list to be uploaded to S3 Bucket
    
    if not updating_existing:
    ### load image from s3
        for i in allFiles:
          vals = i.split("/")
          name = vals[len(vals) - 1]
          if "flow" in i.lower() or "fix" in i.lower():
            try:
                os.rename("{}/{}/{}".format(latch_dir,run_id,name), str(figure_dir.joinpath(name)))
            except:
                pass
          elif i in bsa_path:
              bsa_original = Image.open(bsa_path)
              bsa_img_arr = np.array(bsa_original, np.uint8)
              bsa_original.close()
              if rotation != 0 :
                  bsa_img_arr = rotate_image_no_cropping(bsa_img_arr, rotation)
                  
              postB_img_arr = bsa_img_arr[:, :, 2]
              postB_source = Image.fromarray(postB_img_arr)
              bsa_source = Image.fromarray(bsa_img_arr)
              

        self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 45})

        ## generate cropped images using crop parameters
        cropped_bsa = bsa_source.crop((crop_coordinates[0], crop_coordinates[1], crop_coordinates[2], crop_coordinates[3]))
        cropped_postB = postB_source.crop((crop_coordinates[0], crop_coordinates[1], crop_coordinates[2], crop_coordinates[3]))
        ## high resolution
        tempName_bsa = figure_dir.joinpath("postB_BSA.tif")
        tempName_postB = figure_dir.joinpath("postB.tif")
        
        cropped_bsa.save(tempName_bsa.__str__())
        cropped_postB.save(tempName_postB.__str__())
        
        height = cropped_postB.height
        width = cropped_postB.width
        self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 65})
        if width > height:
            factorHigh = 2000/width
            factorLow = 600/width
            high_res = cropped_postB.resize((2000, int(height * factorHigh)), Image.LANCZOS)
            low_res = cropped_postB.resize((600, int(height * factorLow)), Image.LANCZOS)
        else:
            factorHigh = 2000/height
            factorLow = 600/height
            high_res = cropped_postB.resize((int(width*factorHigh), 2000), Image.LANCZOS)
            low_res = cropped_postB.resize((int(width*factorLow), 600), Image.LANCZOS)
            
        local_hires_image_path = spatial_dir.joinpath('tissue_hires_image.png')
        local_lowres_image_path = spatial_dir.joinpath('tissue_lowres_image.png')
        
        high_res.save(local_hires_image_path.__str__())
        low_res.save(local_lowres_image_path.__str__())
        
        scalefactors["tissue_hires_scalef"] = factorHigh
        scalefactors["tissue_lowres_scalef"] = factorLow
    
        json.dump(scalefactors, open(local_scalefactors_filename,'w'), indent=4,sort_keys=True)
        
    self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 75})
    ### generate tissue_positions_list.csv
    local_tissue_positions_filename= spatial_dir.joinpath('tissue_positions_list.csv')
    tissue_positions_list = []
    tixel_pos_list= [x['position'] for x  in tixel_positions]
    f=open(local_tissue_positions_filename, 'w')
    csvwriter = csv.writer(f, delimiter=',',escapechar=' ',quoting=csv.QUOTE_NONE)
    for idx, b in enumerate(barcodes):
        colidx = int(idx/row_count)
        rowidx = int(idx % row_count)
        keyindex = tixel_pos_list.index([rowidx,colidx])
        coord_x = int(round(tixel_positions[keyindex]['coordinates']['x']))
        coord_y = int(round(tixel_positions[keyindex]['coordinates']['y']))
        val = 0
        if tixel_positions[keyindex]['value'] : val = 1
        datarow = [b, val, rowidx, colidx, coord_y , coord_x ]
        tissue_positions_list.append(datarow)    
        csvwriter.writerow(datarow)
    f.close()
    self.update_state(state="PROGRESS", meta={"position": "Finishing" , "progress" : 80})
    ### concatenate tissue_positions_to gene expressions
    command = f"latch cp /root/LatchAtlasXBrowser/Images/{run_id}/spatial latch:///spatials/{run_id} && rm -rf /root/LatchAtlasXBrowser/Images/{run_id}"
    subprocess.run(command, shell=True)
    self.update_state(state="PROGRESS", meta={"position": "Finished" , "progress" : 100})
    return 'Finished'
