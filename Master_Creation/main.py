from frame_extracting import extract_asset_frames
from master_sheet_creation import create_master_sheet
from linear_master_creation import linear_master
from json_modification import combine_json_files,copy_to_linear_folder
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Insert.fixed_master import upload_to_database_master_F
from Insert.linear_master import upload_linear_master
from Insert.linear_anomaly import upload_linear_anomaly
from Preprocessing.json_correction import validate_and_modify_json
from kml.create_json import create_kml
from kml.linear_json import linear_kml
from Insert.fixed_anomaly import upload_to_database_anomaly_F
from Insert.fixed_master import load_config
from images.copy_to_sr_storage import upload_fixed_images

user_option=input("Uploading options:\n" 
"1.fixed and linear in same json\n"
"2.fixed\n" 
"3.linear\n"
"4.fixed and linear separate json"
"Select an option 1 or 2 or 3 :").strip().lower()
if user_option not in ['1','2','3','4']:
    print("Invalid option selected. Please choose 1, 2, or 3 or 4.")
    sys.exit(1)


combined_flag=False



if user_option in ['1','2','4']:

    if user_option =='1':
        json_location=combine_json_files()
    else:  
        config=load_config()
        json_location=config['json_folder']

    validate_and_modify_json(json_location)
    if user_option =='1':
        copy_to_linear_folder(json_location)

    extract_asset_frames(json_location)

    master_file=create_master_sheet(json_location)

    first_id,last_id=upload_to_database_master_F(master_file)
    print("** Master file uploaded to database **")

    upload_to_database_anomaly_F(master_file)

    print("** Anomaly file uploaded to database **")

    if first_id is not None and last_id is not None:
        print(f"First ID in DB: {first_id}, Last ID in DB: {last_id}")
        create_kml(first_id, last_id)
        
    else:
        print("Failed to upload master file to database.")

if user_option in ['1','3']:
    ## Linear master upload
    linear_sheets=linear_master()

    firts_id_L,last_id_L=upload_linear_master(linear_sheets)
    linear_kml(firts_id_L,last_id_L)
    upload_linear_anomaly(linear_sheets)

if user_option in ['1','2','4']:
    upload_fixed_images(json_location)
