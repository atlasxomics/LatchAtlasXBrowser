
import os
import yaml,json,csv
from PIL import Image
from pathlib import Path
import json
from pathlib import Path
Image.MAX_IMAGE_PIXELS = None
from celery import Celery
from celery.signals import worker_process_init

import numpy as np 
import utils
import cv2

app=Celery('atlasbrowser_task',broker='amqp://'+os.environ['RABBITMQ_HOST'],backend='redis://'+os.environ['REDIS_HOST'])

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
    latch_dir = '/ldata/'
    ## parameter parsing
    root_dir = qcparams['root_dir']
    metadata = qcparams['metadata']
    oldFiles = qcparams['files']
    scalefactors = qcparams['scalefactors']
    run_id = qcparams['run_id']
    tixel_positions = qcparams['mask']
    crop_coordinates = qcparams['crop_area']
    orientation = qcparams['orientation']
    rotation = (int(orientation['rotation']) % 360)
    bsa_path = qcparams['bsa_path']
    barcode_root_dir = qcparams['barcode_dir']
    barcode_path = qcparams['barcode_path']
    postB_flag = qcparams['postB_flag']
    latch_flag = qcparams['latch_flag']
    postb_path = "{}{}/{}/".format(temp_dir,root_dir, run_id) if not latch_flag else "{}{}/".format(latch_dir,run_id)
    
    updating_existing = qcparams.get('updating_existing', False)

    next_gen_barcodes = True
    
    metadata["replaced_24_barcodes"] = next_gen_barcodes

    ### source image path
    allFiles = [i for i in oldFiles if '.json' not in i and 'spatial' not in i]
    ### output directories (S3)
    ##Images
    spatial_dir = Path(root_dir).joinpath(run_id, 'spatial')
    figure_dir = Path(root_dir).joinpath(run_id, 'spatial', 'figure')
    La_spatial_dir = Path(latch_dir).joinpath(run_id, 'spatial')
    La_figure_dir = Path(latch_dir).joinpath(run_id, 'spatial', 'figure')
    ### local temp directories
    ##/root/Images
    local_spatial_dir = Path(temp_dir).joinpath(spatial_dir)
    La_local_spatial_dir = Path(latch_dir).joinpath(La_spatial_dir)
    if not local_spatial_dir.exists(): local_spatial_dir.mkdir(parents=True, exist_ok=True)
    try:
        if not La_local_spatial_dir.exists(): La_local_spatial_dir.mkdir(parents=True, exist_ok=True)
    except:
        pass
    
    local_figure_dir = Path(temp_dir).joinpath(figure_dir)
    La_local_figure_dir = Path(latch_dir).joinpath(La_figure_dir)
    if not local_figure_dir.exists(): local_figure_dir.mkdir(parents=True, exist_ok=True)
    try:
        if not La_local_figure_dir.exists(): La_local_figure_dir.mkdir(parents=True, exist_ok=True)
    except:
        pass

    ### read barcodes information 
    row_count = 50

    self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 20})
    barcodes = None
    if not latch_flag:
        with open(temp_dir + barcode_path,'r') as f:
            barcodes = f.read().splitlines()
    else:
        try:
            with open(latch_dir + barcode_path,'r') as f:
                barcodes = f.read().splitlines()
        except:
            pass
    ### save metadata & scalefactors
    local_metadata_filename = local_spatial_dir.joinpath('metadata.json')
    La_local_metadata_filename = La_local_spatial_dir.joinpath('metadata.json')
    local_scalefactors_filename = local_spatial_dir.joinpath('scalefactors_json.json')
    La_local_scalefactors_filename = La_local_spatial_dir.joinpath('scalefactors_json.json')
    json.dump(metadata, open(local_metadata_filename,'w'), indent=4,sort_keys=True)
    try:
        json.dump(metadata, open(La_local_metadata_filename,'w'), indent=4,sort_keys=True)
    except:
        pass
        
    # adding metadata and scalefactors to the list to be uploaded to S3 Bucket
    
    if not updating_existing:
    ### load image from s3
        for i in allFiles:
          vals = i.split("/")
          name = vals[len(vals) - 1]
          if "flow" in i.lower() or "fix" in i.lower():
            if not latch_flag: os.rename("{}/{}/{}/{}".format(temp_dir,root_dir,run_id,name), str(figure_dir.joinpath(name)))
            else:
                try:
                    os.rename("{}/{}/{}".format(latch_dir,run_id,name), str(figure_dir.joinpath(name)))
                except:
                    pass
          elif "bsa" in i.lower():
              if not latch_flag: bsa_original = Image.open(temp_dir + bsa_path)
              else: bsa_original = Image.open(bsa_path)
              bsa_img_arr = np.array(bsa_original, np.uint8)
              if rotation != 0 :
                  bsa_img_arr = rotate_image_no_cropping(bsa_img_arr, rotation)
              if not postB_flag:
                postB_img_arr = bsa_img_arr[:, :, 2]
                postB_source = Image.fromarray(postB_img_arr)
              bsa_source = Image.fromarray(bsa_img_arr)
          elif "postb" in i.lower():
              postB_original = Image.open(postb_path+name)
              postB_img_arr = np.array(postB_original, np.uint8)
              if rotation != 0 :
                postB_img_arr = rotate_image_no_cropping(postB_img_arr, rotation)
              postB_source = Image.fromarray(postB_img_arr)
              

        self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 45})

        ## generate cropped images using crop parameters
        cropped_bsa = bsa_source.crop((crop_coordinates[0], crop_coordinates[1], crop_coordinates[2], crop_coordinates[3]))
        cropped_postB = postB_source.crop((crop_coordinates[0], crop_coordinates[1], crop_coordinates[2], crop_coordinates[3]))

        ## high resolution
        tempName_bsa = local_figure_dir.joinpath("postB_BSA.tif")
        tempName_postB = local_figure_dir.joinpath("postB.tif")
        La_tempName_bsa = La_local_figure_dir.joinpath("postB_BSA.tif")
        La_tempName_postB = La_local_figure_dir.joinpath("postB.tif")
        cropped_bsa.save(tempName_bsa.__str__())
        cropped_postB.save(tempName_postB.__str__())
        try:
            cropped_bsa.save(La_tempName_bsa.__str__())
        except:
            pass
        try:
            cropped_postB.save(La_tempName_postB.__str__())
        except:
            pass

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

        local_hires_image_path = local_spatial_dir.joinpath('tissue_hires_image.png')
        local_lowres_image_path = local_spatial_dir.joinpath('tissue_lowres_image.png')
        La_local_hires_image_path = La_local_spatial_dir.joinpath('tissue_hires_image.png')
        La_local_lowres_image_path = La_local_spatial_dir.joinpath('tissue_lowres_image.png')
        high_res.save(local_hires_image_path.__str__())
        low_res.save(local_lowres_image_path.__str__())
        try:
            high_res.save(La_local_hires_image_path.__str__())   
        except:
            pass
        try:
          low_res.save(La_local_lowres_image_path.__str__())  
        except:
            pass
        
        scalefactors["tissue_hires_scalef"] = factorHigh
        scalefactors["tissue_lowres_scalef"] = factorLow
    
        json.dump(scalefactors, open(local_scalefactors_filename,'w'), indent=4,sort_keys=True)
        try:
          json.dump(scalefactors, open(La_local_scalefactors_filename,'w'), indent=4,sort_keys=True)  
        except:
            pass
        
    self.update_state(state="PROGRESS", meta={"position": "running" , "progress" : 75})
    ### generate tissue_positions_list.csv
    local_tissue_positions_filename= local_spatial_dir.joinpath('tissue_positions_list.csv')
    La_local_tissue_positions_filename= local_spatial_dir.joinpath('tissue_positions_list.csv')
    tissue_positions_list = []
    tixel_pos_list= [x['position'] for x  in tixel_positions]
    f=open(local_tissue_positions_filename, 'w')
    csvwriter = csv.writer(f, delimiter=',',escapechar=' ',quoting=csv.QUOTE_NONE)
    for idx, b in enumerate(barcodes):
        colidx = int(idx/row_count)
        rowidx = idx % row_count
        keyindex = tixel_pos_list.index([rowidx,colidx])
        coord_x = int(round(tixel_positions[keyindex]['coordinates']['x']))
        coord_y = int(round(tixel_positions[keyindex]['coordinates']['y']))
        val = 0
        if tixel_positions[keyindex]['value'] : val = 1
        datarow = [b, val, rowidx, colidx, coord_y , coord_x ]
        tissue_positions_list.append(datarow)    
        csvwriter.writerow(datarow)
    f.close()
    try:
      f1=open(La_local_tissue_positions_filename, 'w')
    except:
        pass
    
    csvwriter = csv.writer(f1, delimiter=',',escapechar=' ',quoting=csv.QUOTE_NONE)
    for idx, b in enumerate(barcodes):
        colidx = int(idx/row_count)
        rowidx = idx % row_count
        keyindex = tixel_pos_list.index([rowidx,colidx])
        coord_x = int(round(tixel_positions[keyindex]['coordinates']['x']))
        coord_y = int(round(tixel_positions[keyindex]['coordinates']['y']))
        val = 0
        if tixel_positions[keyindex]['value'] : val = 1
        datarow = [b, val, rowidx, colidx, coord_y , coord_x ]
        tissue_positions_list.append(datarow)    
        csvwriter.writerow(datarow)
    f1.close()
    self.update_state(state="PROGRESS", meta={"position": "Finishing" , "progress" : 80})
    ### concatenate tissue_positions_to gene expressions
    

    self.update_state(state="PROGRESS", meta={"position": "Finished" , "progress" : 100})
    return 'Finished'
