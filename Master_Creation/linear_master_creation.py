import json
import pandas as pd
import mysql.connector
import ast
import glob
import utils
import os
import numpy as np
import cv2
import geo
from opencv_draw_annotation import draw_bounding_box
import re


# from normalised_code import normalised_csv
# from meta_data_safecam import get_gps

# from LinearCode.Linear_assets import skip_count

# section = utils.getsection(x)
# site_name = 'sr_lnt'
# base_path = config['linear_folder']
# # csv_folder="/home/saran/Projects/TEL/aNOMALY/Linear_MCW/RHS/Videowise"
# # video_base_path="/run/user/1000/gvfs/smb-share:server=anton.local,share=roadis_phase4/ml_support/Kerla_TEL_Videos/MCW & SR_SET_2/LHS/MCW/MCW_Videos"
# image_folder = f"{base_path}/IMAGES"

# # file = "/home/saran/POC/Invision_2nd_phase/Metadata/LHS/Linear/LHS_metadata.csv"
# root_dir = '/run/user/1000/gvfs/smb-share:server=anton.local,share=roadis_phase4/ml_support/sept+2025/invision/SECTION_V_EVZONOI/V_EVZONOI_MAIN/V_EVZONOI_MAIN_LEFT'
# outpath_path = ""



def split_video_wise(output_file,path):
    df = pd.read_csv(output_file)
    if not os.path.exists(path):
        os.mkdir(path)
    grouped = df.groupby('video_name')
    for group_name, group_data in grouped:
        group_data.to_csv(path+f'/{group_name}.csv', index=False)  
        print(f"Saved {group_name}.csv")

from math import radians, cos, sin, asin, sqrt

def calculateDistance(latlon1, latlon2):
    """
    Calculate the great circle distance between two points
    given in 'Nxx.xxxxxExx.xxxxx' format.
    Returns distance in meters.
    """
    def parse_latlon(latlon):
        try:
            lat = float(latlon.split('E')[0][1:])
            lon = float(latlon.split('E')[1])
        except:
            lat = float(latlon.split('W')[0][1:])
            lon = float(latlon.split('W')[1])
        return lat, lon

    lat1, lon1 = parse_latlon(latlon1)
    lat2, lon2 = parse_latlon(latlon2)

    # Convert to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000  # Radius of earth in meters
    return c * r



def extract_frame(video_file, output_file, frame_number,video_name,image_folder,contract_no):
    print("###############################")
    print(video_file)
    print(output_file)
    print(frame_number,video_name)
    video_name = video_name.replace('.MP4','')
    cap = cv2.VideoCapture(video_file)
    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number) # Set video position in milliseconds
    directory = f"{image_folder}/{contract_no}/{video_name}/"
    ret, frame_number = cap.read()
    try:
        print("1111111111111",video_file)
        if ret:
            if not os.path.exists(directory):
                os.makedirs(directory)
            # print("11222123334ui935789405856890")
            print("2222222222")
            print(output_file)
                # print(f"Directory '{directory}' created successfully")
            # else:
            #     print(f"Directory '{directory}' already exists")

            cv2.imwrite(output_file, frame_number)
            print(f"Frame extracted and saved as {output_file}")
        else:
            print("Error: Frame not extracted")
    except ex as Exception:
        print("ex",ex)

    cap.release()

def extract_all_frames(video_file):


    video_file_name = f"{base_path}/{video_file}.MP4"
    outpath = video_file_name.replace(".MP4", "/")
    print(video_file_name,outpath)
    print("####OUTPATH####")
    # print(outpath)

    # Create the output folder if it doesn't exist
    os.makedirs(outpath, exist_ok=True)

    # Open the video file
    cap = cv2.VideoCapture(video_file_name)

    # Get video properties
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    print(frame_count)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # Loop through each frame and save them
    frame_number = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break

        frame_filename = os.path.join(outpath, f'{outpath}frame_{frame_number:04d}.jpeg')
        # cv2.imwrite(frame_filename, frame)

        frame_number += 1

        # Show progress
        print(f"Extracting frame {frame_number}/{frame_count}")

        # Break the loop when all frames are extracted
        if frame_number == frame_count:
            break

    # Release the video capture object
    cap.release()

    print("All frames extracted and saved.")
    return total_frames


def split_ltng(cord):
    return float(cord.split("E")[0][1:]), float(cord.split("E")[1])


def interpolate(v1, v2, c1, c2, f1, f2, F):
    lat1, lon1 = split_ltng(v1)
    lat2, lon2 = split_ltng(v2)
    lat1 = lat1 + (F - f1) * (lat2 - lat1) / (f2 - f1)
    lon1 = lon1 + (F - f1) * (lon2 - lon1) / (f2 - f1)
    c1 = c1 + (F - f1) * (c2 - c1) / (f2 - f1)
    return lat1, lon1, c1


def draw_bounding_box(img, bbox, labels=None, color='green', font_scale=1, font_thickness=2):
    # Define the color dictionary
    color_dict = {
        'red': (0, 0, 255),
        'green': (0, 255, 0),
        'blue': (255, 0, 0)
    }

    # Get the color
    color = color_dict.get(color, (0, 255, 0))

    # Unpack the bounding box coordinates
    x_min, y_min, x_max, y_max = bbox

    # Draw the bounding box
    cv2.rectangle(img, (x_min, y_min), (x_max, y_max), color, 2)

    # Draw the label if provided
    if labels:
        for label in labels:
            # Define the position for the text
            text_position = (x_min, y_min - 10)
            label=label.replace("_"," ")
            # Put the text on the image
            cv2.putText(img, label, text_position, cv2.FONT_HERSHEY_SIMPLEX,
                        font_scale, color, font_thickness, cv2.LINE_AA)

    return img

config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
config_file = os.path.normpath(config_path) 
with open(config_file) as f:
    config = json.load(f)
if config['server']=="anton":
    sql = "SELECT asset_id,asset_name,asset_type,asset_synonyms FROM seekright_v3_poc.tbl_asset;"
    aws_host = "takeleap.in"
    aws_user = "seekright"
    aws_password = "Takeleap@123"
    aws_database = "seekright_v3_poc"
    aws_port = 3307

    asset_mydb = mysql.connector.connect(
        host=aws_host,
        user=aws_user,
        password=aws_password,
        database=aws_database,
        port=aws_port
    )
if config['server']=="production":
    asset_mydb = mysql.connector.connect(host='seekright-db.ce3lsmnwzkln.ap-south-1.rds.amazonaws.com',user='admin',password='BXWUCSpjRxEqzxXYTF9e',port='3306') 
    sql = "SELECT asset_id,asset_name,asset_type,asset_synonyms FROM seekright_v3.tbl_asset;"

mycursor = asset_mydb.cursor(dictionary=True)
mycursor.execute(sql)
myresult = mycursor.fetchall()
# print(myresult)
d = {'asset_id':[],'asset_name':[],'asset_type':[],'asset_synonyms':[]}
for j in myresult:
    d['asset_id'].append(j['asset_id'])
    d['asset_name'].append(j['asset_name'])
    d['asset_type'].append(j['asset_type'])
    d['asset_synonyms'].append(j['asset_synonyms'])

d2 = {}
for j in range(len(d['asset_name'])):
    # print(d['asset_synonyms'][j])
    new_name = d['asset_name'][j]
    # print(type(new_synonyms))
    d2[new_name] = d['asset_id'][j],d['asset_type'][j]
    # for k in range(len(new_name)):
    #     # print(new_synonyms[k])
    #     d2[new_synonyms[k]]=d['asset_id'][j]
#
# print(d2)


# new_files = []
# for file in glob.glob(f"{base_path}/*.json"):
#     new_files.append(file)
# new_files.sort()
# print(new_files)

# #adding columns in csv and changing names of bounding box inside images


# print(new_data['video_name'])



def search_folders_with_103_and_json(contract_no,root_dir,file):
    # Walk through the directory structure
    # print("########")
    # print("1111111")
    # print(root_dir)
    # try:
    video_path = None

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Check if "103" is in the directory path

        if contract_no in dirpath:
            # print(dirpath)
            # Search for JSON files in the directory
            json_files = [f for f in filenames if f.endswith('.MP4')]
            if json_files:
                print(f"Directory: {dirpath}")
                for json_file in json_files:
                    print(f"Video file: {json_file}")
                    print(file)
                    if re.sub('_([0-9]{4})?F',"",file) in re.sub('_([0-9]{4})?F',"",json_file ):
                        if not json_file.startswith('Output'):
                            return f"{dirpath}/{json_file}"

                    if file  in  json_file:
                        if not json_file.startswith('Output'):
                            print("found exact match",json_file)
                            video_path = f"{dirpath}/{json_file}"
                            
                            return video_path
                                # print(video_path)
                        
                    
    return video_path



def process_asset_pair(start_asset,end_asset,i,video_name,video_file,final_data,site_id,asset_id,asset_count,d1,base_path,filtered_data,site_name,image_folder,contract_no,df2,main_asset_name,image_saving_folder):
    temp_data = []
    print("for Start Asset")
    print(start_asset,end_asset)
    side = start_asset[0].split("_")[0]
    print("side",side)
    skip_count = start_asset[-2]
    print("skip_count",skip_count)
    new_asset_name = i
    print("asset_name",new_asset_name)
    if type(skip_count) == int:
        skip_count = skip_count
    else:
        skip_count = 0
    # print("skip_count", skip_count)
    new_asset_name = side + '_' + new_asset_name
    # print(new_asset_name, "asset_name_new")
    remark = start_asset[5][1]
    comment = start_asset[5][0]
    frame = start_asset[2]
    # print(remark,comment,frame)
    for k in range(len(final_data['Position'])):
        # print("index",k)
        # print(final_data['video_name'][k])
        # print("11111111111111",k,frame)
        # if video_name == final_data['video_name'][k]:
            # print("index",k)
            if (frame >= final_data['Start_frame'][k]) and (frame <= final_data['End_frame'][k]) or (frame - 1 == final_data['End_frame'][k]):
                    # print("video_name", video_name, final_data['video_name'][k])
                    # print("1111122222222222", k,frame)

                    chainage = round(float(final_data['Chainage'][k]), 3)
                    master_id = f"{site_id}_{asset_id}_{chainage}_{asset_count}"
                    currentFrame = frame
                    print("currentFrame", frame)

                    speed = final_data['Speed'][k]
                    if speed == 0:
                        print("speed is 0", frame, video_name, speed)
                        speed = max(speed, 5)
                        # break
                    F = currentFrame
                    if new_asset_name.startswith('LEFT'):
                        F = currentFrame + int(1080 / speed)
                        # print("left assets#########")
                    elif new_asset_name.startswith('RIGHT'):
                        F = currentFrame + int(1080 / speed)
                        # print("right assets#########")
                    else:
                        print(f"Warning: Asset name '{new_asset_name}' does not start with 'LEFT' or 'RIGHT'")
                    # print("new geotag value", F, type(F),video_name)
                    # print(d1[final_data['video_name'][k]][-1])
                    # print(d1[final_data['video_name'][k]])
                    aaaa = F - d1[final_data['video_name'][k]][-1]
                    print("nnnnnnn", final_data['video_name'][k],aaaa)
                    # print(d1[final_data['video_name'][k]][-1])
                    if F - d1[final_data['video_name'][k]][-1] >= 0:

                        print("ssssssssssssssssjojgdiofgn", k)
                        F = F - d1[final_data['video_name'][k]][-1]

                        # print("GGGGGGGGGGGGG1111111111", F)
                        index1 = df2.index(final_data['video_name'][k])
                        print("index1", df2.index(final_data['video_name'][k]))
                        # print("aaaaaaaaaaaaaa", df2[index1 + 1])
                        try:
                            d = final_data[final_data['video_name'] == df2[index1 + 1]]
                            print("d", d)
                            for l in range(len(d['Position'])):
                                # print("lll")
                                if (F >= d.iloc[l]['Start_frame']) and (F <= d.iloc[l]['End_frame']):
                                    v1 = d.iloc[l]['Position']
                                    v2 = d.iloc[l + 1]['Position']
                                    c1 = d.iloc[l]['Chainage']
                                    c2 = d.iloc[l + 1]['Chainage']
                                    if skip_count == 1:
                                        f1 = d.iloc[l + 1]['Start_frame']
                                        f2 = d.iloc[l + 1]['End_frame'] + 2
                                    else:
                                        f1 = d.iloc[l]['Start_frame']
                                        f2 = d.iloc[l]['End_frame'] + 2
                                    print("111",v1,v2,c1,c2,f1,f2,F)
                                    break
                        except Exception as ex:
                            # print("jshdpOHSDSIODHIOSDHW")
                            print(ex)
                            # print("djhnsdghsklghklsf")
                            try:
                                # print("aaasffffffffffffffffffffffdgfggh")
                                print(final_data,
                                      "###@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%%%%%&&*@@@@@@@@@")
                                v1 = final_data.iloc[-2]['Position']
                                v2 = final_data.iloc[-1]['Position']
                                c1 = final_data.iloc[-2]['Chainage']
                                c2 = final_data.iloc[-1]['Chainage']
                                f1 = final_data.iloc[-1]['Start_frame']
                                f2 = final_data.iloc[-1]['End_frame']
                                print("CCCCCCCCCCCCCCCCCCCCCCCCCCCCccsfgfghgh")
                                break
                            except Exception as ex:
                                print(ex, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@################")


                    elif F - d1[final_data['video_name'][k]][-1] < 0:
                        F = F
                        print("gggggggggggg", F)
                        video_name = final_data['video_name'][k]
                        d = final_data[final_data['video_name'] == video_name]
                        print(d['video_name'])
                        # d2 = final_data[]
                        for l in range(len(d['Position'])):
                            # print("$$$$$$$$$$$$$$$$$$$")

                            if (F >= d.iloc[l]['Start_frame']) and (F <= d.iloc[l]['End_frame']+1):
                                print("aaa")
                                print(d.iloc[l]['Start_frame'], d.iloc[l]['End_frame'])
                                v1 = d.iloc[l]['Position']
                                c1 = d.iloc[l]['Chainage']

                                inddd = d.index[-1]
                                print(inddd, "ASSD")
                                try:

                                    c1 = d.iloc[l]['Chainage']
                                    if skip_count == 1:
                                        f1 = d.iloc[l + 1]['Start_frame']
                                        f2 = d.iloc[l + 1]['End_frame'] + 2
                                    else:
                                        f1 = d.iloc[l]['Start_frame']
                                        f2 = d.iloc[l]['End_frame'] + 2
                                    c2 = d.iloc[l + 1]['Chainage']
                                    v2 = d.iloc[l + 1]['Position']
                                    print("222", v1, v2, c1, c2, f1, f2, F)
                                    break
                                except Exception as ex:
                                    # print(ex,v1,v2,c1,c2,f1,f2)
                                    print("fdsfsddfgg")
                                    try:
                                        v2 = final_data['Position'][inddd + 1]
                                        c2 = final_data['Chainage'][inddd + 1]
                                        print("v2,c2", v1, c1, v2, c2, f1, f2)
                                    except Exception as ex:
                                        print("exception")
                                        print(ex)
                                        try:
                                            print("last values")
                                            v1 = final_data.iloc[-2]['Position']
                                            v2 = final_data.iloc[-1]['Position']
                                            c1 = final_data.iloc[-2]['Chainage']
                                            c2 = final_data.iloc[-1]['Chainage']
                                            f1 = final_data.iloc[-1]['Start_frame']
                                            f2 = final_data.iloc[-1]['End_frame']
                                            print("111111111111adanaldk")
                                        except Exception as ex:
                                            print(ex)
                                            print(frame, F)
                    else:
                        print("nothing found")
                        aa = F - d1[final_data['video_name'][k]][-1]
                        print(F,d1[final_data['video_name'][k]][-1])
                    try:

                        a = interpolate(v1, v2, c1, c2, f1, f2, F)
                    except Exception as ex:
                        print(ex)
                        try:
                            a= float(v1.split("E")[0][1:]),float(v1.split("E")[-1]),c1
                        except:
                            a= float(v1.split("W")[0][1:]),float(v1.split("W")[-1]),c1
                    lat, long, ch = a

                    ch = round(float(ch), 3)
                    # print(type(ch))
                    print(lat, long, ch)

                    geotag = f"N{lat}E{long}"
                    print("GEOTAG", geotag)
                    # count = count + 1
                    new_master = master_id.replace(master_id.split("_")[-2], str(ch))
                    new_img_path = f"{base_path}/{filtered_data[i]['Assets'][0][2]}.jpeg"
                    if calculateDistance(final_data['Position'][k], geotag) > 10:
                        # print("distance is more than 10 mtrs")
                        geotag = final_data['Position'][k]
                        ch = final_data['Chainage'][k]

                    elif ch - final_data['Chainage'][k] > 0.050:
                        # print("chainage difference", ch - final_data['Chainage'][k])
                        geotag = final_data['Position'][k]
                        ch = final_data['Chainage'][k]
                    print(new_master, final_data['Position'][k], 0, ch, geotag, frame, video_name,
                          remark, comment)

                    temp_data.append({'Start_Frame': [new_master, final_data['Position'][k], ch, geotag, frame, video_name,
                                                      remark, comment,(start_asset[3],start_asset[4])]})

    print("for End Asset")

    skip_count = end_asset[-2]
    new_asset_name = i
    if type(skip_count) == int:
        # print("##",skip_count)
        skip_count = skip_count
    else:
        skip_count = 0
    print("skip_count", skip_count)
    new_asset_name = side + '_' + new_asset_name
    print(new_asset_name, "asset_name_new")
    remark = end_asset[5][1]
    comment = end_asset[5][0]
    frame = end_asset[2]
    bbox = (end_asset[3],end_asset[4])
    print("end_asset",end_asset)
    print(bbox)
    for k in range(len(final_data['Position'])):
        # print("index",k)
        # print(final_data['video_name'][k])
        # print("11111111111111",k,frame)
        # if video_name == final_data['video_name'][k]:
            # print("index",k)
            if (frame >= final_data['Start_frame'][k]) and (frame <= final_data['End_frame'][k]) or (frame - 1 == final_data['End_frame'][k]):

                chainage = round(float(final_data['Chainage'][k]), 3)
                master_id = f"{site_id}_{asset_id}_{chainage}_{asset_count}"
                currentFrame = frame
                print("currentFrame", frame)
#
                speed = final_data['Speed'][k]
                if speed == 0:
                    print("speed is 0", frame, video_name, speed)
                    speed = max(speed, 5)
                    # break


                F = currentFrame
                if new_asset_name.startswith('LEFT'):
                    F = currentFrame + int(1080 / speed)
                    # print("left assets#########")
                elif new_asset_name.startswith('RIGHT'):
                    F = currentFrame + int(1080 / speed)
                    # print("right assets#########")
                else:
                    print(f"Warning: Asset name '{new_asset_name}' does not start with 'LEFT' or 'RIGHT'")

                # print("new geotag value", F, type(F))
                print("nnnnnnn", final_data['video_name'][k])
                print(d1[final_data['video_name'][k]][-1])
                aaa = F- d1[final_data['video_name'][k]][-1]
                # print(aaa)
                if F - d1[final_data['video_name'][k]][-1] >= 0:

                    print("ssssssssssssssssjojgdiofgn", k)
                    F = F - d1[final_data['video_name'][k]][-1]

                    print("GGGGGGGGGGGGG1111111111", F)
                    index1 = df2.index(final_data['video_name'][k])
                    print("index1", df2.index(final_data['video_name'][k]))
                    # print("aaaaaaaaaaaaaa", df2[index1 + 1])
                    try:
                        d = final_data[final_data['video_name'] == df2[index1 + 1]]
                        print("d",d)
                        condition_satisfied = False
                        for l in range(len(d['Position'])):
                            # print("lll")
                            if (F >= d.iloc[l]['Start_frame']) and (
                                    F <= d.iloc[l]['End_frame']+1):
                                v1 = d.iloc[l]['Position']
                                v2 = d.iloc[l + 1]['Position']
                                c1 = d.iloc[l]['Chainage']
                                c2 = d.iloc[l + 1]['Chainage']
                                if skip_count == 1:
                                    f1 = d.iloc[l + 1]['Start_frame']
                                    f2 = d.iloc[l + 1]['End_frame'] + 2
                                else:
                                    f1 = d.iloc[l]['Start_frame']
                                    f2 = d.iloc[l]['End_frame'] + 2
                                condition_satisfied = True
                                break
                        if  not  condition_satisfied:
                            try:
                                # print("aaasffffffffffffffffffffffdgfggh")
                                print(final_data,
                                      "###@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%%%%%&&*@@@@@@@@@")
                                v1 = final_data.iloc[-2]['Position']
                                v2 = final_data.iloc[-1]['Position']
                                c1 = final_data.iloc[-2]['Chainage']
                                c2 = final_data.iloc[-1]['Chainage']
                                f1 = final_data.iloc[-1]['Start_frame']
                                f2 = final_data.iloc[-1]['End_frame']
                                print("CCCCCCCCCCCCCCCCCCCCCCCCCCCCccsfgfghgh")
                            except Exception as ex:
                                print(ex, "@@@@@@@@@@@@@@@@@")
                    except Exception as ex:
                        print("jshdpOHSDSIODHIOSDHW")
                        print(ex)
                        print("djhnsdghsklghklsf")
                        try:
                            print("aaasffffffffffffffffffffffdgfggh")
                            # print(final_data,
                            #       "###@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%%%%%&&*@@@@@@@@@")
                            v1 = final_data.iloc[-2]['Position']
                            v2 = final_data.iloc[-1]['Position']
                            c1 = final_data.iloc[-2]['Chainage']
                            c2 = final_data.iloc[-1]['Chainage']
                            f1 = final_data.iloc[-1]['Start_frame']
                            f2 = final_data.iloc[-1]['End_frame']
                            print("CCCCCCCCCCCCCCCCCCCCCCCCCCCCccsfgfghgh")
                            print(v1,v2,c1,c2,f1,f2)
                        except Exception as ex:
                            print(ex, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@################")


                elif F - d1[final_data['video_name'][k]][-1] < 0:
                    F = F
                    print("gggggggggggg", F)
                    video_name = final_data['video_name'][k]
                    # print(video_name)
                    d = final_data[final_data['video_name'] == video_name]
                    # d2 = final_data[]
                    for l in range(len(d['Position'])):
                        if (F >= d.iloc[l]['Start_frame']) and (
                                F <= d.iloc[l]['End_frame']+1):
                            v1 = d.iloc[l]['Position']
                            c1 = d.iloc[l]['Chainage']

                            inddd = d.index[-1]
                            # print(inddd, "ASSD")
                            try:

                                c1 = d.iloc[l]['Chainage']
                                if skip_count == 1:
                                    f1 = d.iloc[l + 1]['Start_frame']
                                    f2 = d.iloc[l + 1]['End_frame'] + 2
                                else:
                                    f1 = d.iloc[l]['Start_frame']
                                    f2 = d.iloc[l]['End_frame'] + 2
                                c2 = d.iloc[l + 1]['Chainage']
                                v2 = d.iloc[l + 1]['Position']
                            except Exception as ex:
                                # print(ex,v1,v2,c1,c2,f1,f2)
                                print("fdsfsddfgg")
                                try:
                                    v2 = final_data['Position'][inddd + 1]
                                    c2 = final_data['Chainage'][inddd + 1]
                                    print("v2,c2", v1, c1, v2, c2, f1, f2)
                                except Exception as ex:
                                    # print("exception")
                                    print(ex)
                                    try:
                                        print("last values")
                                        v1 = final_data.iloc[-2]['Position']
                                        v2 = final_data.iloc[-1]['Position']
                                        c1 = final_data.iloc[-2]['Chainage']
                                        c2 = final_data.iloc[-1]['Chainage']
                                        f1 = final_data.iloc[-1]['Start_frame']
                                        f2 = final_data.iloc[-1]['End_frame']
                                        print()
                                    except Exception as ex:
                                        print(ex)
                                        print(frame, F)

                # a = interpolate(v1, v2, c1, c2, f1, f2, F)
                try:

                        a = interpolate(v1, v2, c1, c2, f1, f2, F)
                except Exception as ex:
                        print(ex,"interpolate has issues")
                        try:
                            a= float(v1.split("E")[0][1:]),float(v1.split("E")[-1]),c1
                        except:
                            a= float(v1.split("W")[0][1:]),float(v1.split("W")[-1]),c1
                lat, long, ch = a

                ch = round(float(ch), 3)
                # print(type(ch))
                print(lat, long, ch)

                geotag = f"N{lat}E{long}"
                print("GEOTAG", geotag)
                # count = count + 1
                new_master = master_id.replace(master_id.split("_")[-2], str(ch))
                new_img_path = f"{base_path}/{filtered_data[i]['Assets'][0][2]}.jpeg"
                if calculateDistance(final_data['Position'][k], geotag) > 10:
                    # print("distance is more than 10 mtrs")
                    geotag = final_data['Position'][k]
                    ch = final_data['Chainage'][k]

                elif ch - final_data['Chainage'][k] > 0.050:
                    # print("chainage difference", ch - final_data['Chainage'][k])
                    geotag = final_data['Position'][k]
                    ch = final_data['Chainage'][k]
                print(new_master, final_data['Position'][k], ch,
                      geotag, frame, video_name, remark, comment)
                print("End_Frame Added")

                temp_data.append({'End_Frame': [new_master, final_data['Position'][k], ch, geotag,
                                                frame, video_name, remark, comment,(end_asset[3],end_asset[4])]})

    print(temp_data)
    try:

        print("Temp_data started")
        print(temp_data)
        # Remove duplicates from list of dicts
        def deep_freeze(obj):
            """
            Recursively convert lists in dicts to tuples to make them hashable.
            """
            if isinstance(obj, dict):
                return tuple(sorted((k, deep_freeze(v)) for k, v in obj.items()))
            elif isinstance(obj, list):
                return tuple(deep_freeze(x) for x in obj)
            else:
                return obj

        # Remove duplicates from list of dicts with nested lists
        # temp_data = [dict(t) for t in {deep_freeze(d) for d in temp_data}]
        # Keep only first Start_Frame and first End_Frame
        start_frame_data = next((d for d in temp_data if 'Start_Frame' in d), None)
        end_frame_data = next((d for d in temp_data if 'End_Frame' in d), None)

        # Reset temp_data to only have 1 Start_Frame and 1 End_Frame
        temp_data = []
        if start_frame_data:
            temp_data.append(start_frame_data)
        if end_frame_data:
            temp_data.append(end_frame_data)


        # temp_data = [dict(t) for t in {tuple(sorted(d.items())) for d in temp_data}]

        print(len(temp_data))
        if len(temp_data) > 1:
            print(temp_data)
            print("########issue########")
            print(temp_data[0]['Start_Frame'])
            print("START",len(temp_data[0]['Start_Frame']))
            print("eND",len(temp_data[1]['End_Frame']))
            if len(temp_data[0]['Start_Frame']) > 5 and len(temp_data[1]['End_Frame']) > 5:
                print(temp_data[0]['Start_Frame'][4], temp_data[1]['End_Frame'][4],
                      "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$&&&&&&&&&&&&&")
                print(temp_data[0]['Start_Frame'][4], temp_data[1]['End_Frame'][4])
                middle_frame = int((int(temp_data[0]['Start_Frame'][4]) + int(temp_data[1]['End_Frame'][4])) / 2)

                print(middle_frame)
                print("for Middle Asset")
                # side = (filtered_data[i]['Assets'][0][0].split('_'))[0]
                side = start_asset[0][:4]
                skip_count = 0
                print("skip_count", skip_count)
                # new_asset_name = side + '_' + new_asset_name
                print(new_asset_name, "asset_name_new")
                remark = filtered_data[i]['Assets'][0][5][1]
                comment = filtered_data[i]['Assets'][0][5][0]
                frame = middle_frame
                for k in range(len(final_data['Position'])):
                    # print("index",k)
                    # print(final_data['video_name'][k],video_name)
                    # print("11111111111111",k,frame)
                    # if video_name == final_data['video_name'][k]:
                        # print(video_name,frame,"121212")
                        # print("index",k)
                        if (frame >= final_data['Start_frame'][k]) and (
                                frame <= final_data['End_frame'][k]) or (
                                frame - 1 == final_data['End_frame'][k]):

                            # print("video_name", video_name, final_data['video_name'][k])
                            # print("1111122222222222", k,frame)
                            # if video_name==final_data['video_name'][k]:
                            # print("333333333333",frame,final_data['Position'][k],final_data['video_name'][k],new_asset_name)
                            #     print(final_asset_name)
                            chainage = round(float(final_data['Chainage'][k]), 3)
                            master_id = f"{site_id}_{asset_id}_{chainage}_{asset_count}"
                            currentFrame = frame
                            print("currentFrame", frame)

                            speed = final_data['Speed'][k]
                            if speed == 0:
                                print("speed is 0", frame, video_name, speed)
                                speed = max(speed, 5)
                                # break
                            if new_asset_name.startswith('LEFT'):
                                F = currentFrame + 1080 / speed
                            elif new_asset_name.startswith('RIGHT'):
                                F = currentFrame + 1080 / speed

                            # print("new geotag value", F, type(F))
                            print("nnnnnnn", final_data['video_name'][k])
                            # print(d1[final_data['video_name'][k]][-1])
                            if F - d1[final_data['video_name'][k]][-1] >= 0:

                                # print("ssssssssssssssssjojgdiofgn", k)
                                F = F - d1[final_data['video_name'][k]][-1]

                                # print("GGGGGGGGGGGGG1111111111", F)
                                index1 = df2.index(final_data['video_name'][k])
                                print("index1", df2.index(final_data['video_name'][k]))
                                # print("aaaaaaaaaaaaaa", df2[index1 + 1])
                                try:
                                    d = final_data[final_data['video_name'] == df2[index1 + 1]]
                                    for l in range(len(d['Position'])):
                                        # print("lll")
                                        if (F >= d.iloc[l]['Start_frame']) and (
                                                F <= d.iloc[l]['End_frame']+1):
                                            v1 = d.iloc[l]['Position']
                                            v2 = d.iloc[l + 1]['Position']
                                            c1 = d.iloc[l]['Chainage']
                                            c2 = d.iloc[l + 1]['Chainage']
                                            if skip_count == 1:
                                                f1 = d.iloc[l + 1]['Start_frame']
                                                f2 = d.iloc[l + 1]['End_frame'] + 2
                                            else:
                                                f1 = d.iloc[l]['Start_frame']
                                                f2 = d.iloc[l]['End_frame'] + 2
                                            break
                                except Exception as ex:
                                    # print("jshdpOHSDSIODHIOSDHW")
                                    print(ex)
                                    # print("djhnsdghsklghklsf")
                                    try:
                                        # print("aaasffffffffffffffffffffffdgfggh")
                                        print(final_data,
                                              "###@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@%%%%%&&*@@@@@@@@@")
                                        v1 = final_data.iloc[-2]['Position']
                                        v2 = final_data.iloc[-1]['Position']
                                        c1 = final_data.iloc[-2]['Chainage']
                                        c2 = final_data.iloc[-1]['Chainage']
                                        f1 = final_data.iloc[-1]['Start_frame']
                                        f2 = final_data.iloc[-1]['End_frame']
                                        print("CCCCCCCCCCCCCCCCCCCCCCCCCCCCccsfgfghgh")
                                    except Exception as ex:
                                        print(ex,
                                              "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@################")


                            elif F - d1[final_data['video_name'][k]][-1] < 0:
                                F = F
                                # print("gggggggggggg", F)
                                video_name = final_data['video_name'][k]
                                d = final_data[final_data['video_name'] == video_name]
                                # d2 = final_data[]
                                for l in range(len(d['Position'])):
                                    if (F >= d.iloc[l]['Start_frame']) and (
                                            F <= d.iloc[l]['End_frame']+1):
                                        v1 = d.iloc[l]['Position']
                                        c1 = d.iloc[l]['Chainage']

                                        inddd = d.index[-1]
                                        print(inddd, "ASSD")
                                        try:

                                            c1 = d.iloc[l]['Chainage']
                                            if skip_count == 1:
                                                f1 = d.iloc[l + 1]['Start_frame']
                                                f2 = d.iloc[l + 1]['End_frame'] + 2
                                            else:
                                                f1 = d.iloc[l]['Start_frame']
                                                f2 = d.iloc[l]['End_frame'] + 2
                                            c2 = d.iloc[l + 1]['Chainage']
                                            v2 = d.iloc[l + 1]['Position']
                                        except Exception as ex:
                                            # print(ex,v1,v2,c1,c2,f1,f2)
                                            print("fdsfsddfgg")
                                            try:
                                                v2 = final_data['Position'][inddd + 1]
                                                c2 = final_data['Chainage'][inddd + 1]
                                                print("v2,c2", v1, c1, v2, c2, f1, f2)
                                            except Exception as ex:
                                                # print("exception")
                                                print(ex)
                                                try:
                                                    print("last values")
                                                    v1 = final_data.iloc[-2]['Position']
                                                    v2 = final_data.iloc[-1]['Position']
                                                    c1 = final_data.iloc[-2]['Chainage']
                                                    c2 = final_data.iloc[-1]['Chainage']
                                                    f1 = final_data.iloc[-1]['Start_frame']
                                                    f2 = final_data.iloc[-1]['End_frame']
                                                    print()
                                                except Exception as ex:
                                                    print(ex)
                                                    print(frame, F)

                            # a = interpolate(v1, v2, c1, c2, f1, f2, F)
                            try:

                                a = interpolate(v1, v2, c1, c2, f1, f2, F)
                            except Exception as ex:
                                print(ex)
                                try:
                                    a= float(v1.split("E")[0][1:]),float(v1.split("E")[-1]),c1
                                except:
                                    a= float(v1.split("W")[0][1:]),float(v1.split("W")[-1]),c1
                            lat, long, ch = a

                            ch = round(float(ch), 3)
                            # print(type(ch))
                            print(lat, long, ch)

                            geotag = f"N{lat}E{long}"
                            # print("GEOTAG", geotag)
                            # count = count + 1
                            new_master = master_id.replace(master_id.split("_")[-2], str(ch))
                            # new_img_path = f"/media/suman/03c87680-c3b2-493d-aa2f-67f0a007357c/home/maxi/suman_folder/saudi_new/27_103/verified_data/{filtered_data[i]['Assets'][0][2]}.jpeg"
                            if calculateDistance(final_data['Position'][k], geotag) > 10:
                                # print("distance is more than 10 mtrs")
                                # if geo.calculateDistance(final_data['Position'][k], geotag) > 0.05:
                                geotag = final_data['Position'][k]
                                ch = final_data['Chainage'][k]

                            elif ch - final_data['Chainage'][k] > 0.050:
                                # print("chainage difference", ch - final_data['Chainage'][k])
                                geotag = final_data['Position'][k]
                                ch = final_data['Chainage'][k]
                            print(new_master, final_data['Position'][k], ch,
                                  geotag, frame, video_name, remark, comment)

                temp_data.append(
                    {'Middle_Frame': [new_master, final_data['Position'][k], ch, geotag,
                                      frame, video_name, remark, comment]})
                print(f"temp_data added")

                print(temp_data[2]['Middle_Frame'])
                print("$$$#############Middle frame done##########$$$")
                if 'LEFT_' in start_asset[0]:
                    lhs_rhs = 1
                elif 'RIGHT_' in start_asset[0]:
                    lhs_rhs = 2
                if 'Bad_' in start_asset[0]:
                    bad_or_not = 2
                else:
                    bad_or_not = 1


                output_file = f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{middle_frame}_{bad_or_not}.jpeg"
                print(output_file)

                extract_frame(video_file, output_file, int(frame), video_name,image_folder,site_id)

                new_master = f"{site_id}_{asset_id}_{temp_data[2]['Middle_Frame'][3]}_{asset_count}_{temp_data[2]['Middle_Frame'][5]}"
                print(new_master)

                master = f"{video_name.replace(video_name.split('_')[-1], '')}{temp_data[2]['Middle_Frame'][5]}"
                print(master)
                print("before checking bad ")
                print(new_asset_name)
                try:
                    if 'Bad_' in new_asset_name and temp_data[0]['Start_Frame'][-3] == "":
                        temp_data[0]['Start_Frame'][-3] = "Damaged"
                    new_tuple = (temp_data[0]['Start_Frame'][3], temp_data[1]['End_Frame'][3], temp_data[2]['Middle_Frame'][3],
                                 temp_data[0]['Start_Frame'][4], temp_data[1]['End_Frame'][4], temp_data[2]['Middle_Frame'][4],
                                 str(temp_data[0]['Start_Frame'][2]), str(temp_data[1]['End_Frame'][2]), str(asset_id),
                                 video_name, temp_data[0]['Start_Frame'][-3], temp_data[0]['Start_Frame'][-2],temp_data[0]['Start_Frame'][-1],temp_data[1]['End_Frame'][-1])
                    print(new_tuple)
                except Exception as ex:
                    print("ex for bad",ex)

    #             print("############RENAME################")
                # if  os.path.exists(f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[0]['Start_Frame'][4]}.jpeg"):
                print("path_does_not_exist")
                
                print(f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{asset_id}_{temp_data[0]['Start_Frame'][2]}_{temp_data[0]['Start_Frame'][4]}.jpeg")
                # exit()
                    
                if os.path.exists(f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[0]['Start_Frame'][4]}_{bad_or_not}.jpeg"):
                    os.rename(f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[0]['Start_Frame'][4]}_{bad_or_not}.jpeg",
                          f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[0]['Start_Frame'][2]}_{temp_data[0]['Start_Frame'][4]}.jpeg")
                else:
                    print(f"Start File not found: {image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[0]['Start_Frame'][4]}_{bad_or_not}.jpeg")

                if os.path.exists(f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[1]['End_Frame'][4]}_{bad_or_not}.jpeg"):
                    os.rename(f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[1]['End_Frame'][4]}_{bad_or_not}.jpeg",
                          f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[0]['Start_Frame'][2]}_{temp_data[1]['End_Frame'][4]}.jpeg")
                else:
                    print(f"End File not found: {image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[1]['End_Frame'][4]}_{bad_or_not}.jpeg")
                if os.path.exists(f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{temp_data[2]['Middle_Frame'][4]}_{bad_or_not}.jpeg"):
                    os.rename(f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{temp_data[2]['Middle_Frame'][4]}_{bad_or_not}.jpeg",
                          f"{image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{asset_id}_{temp_data[0]['Start_Frame'][2]}_{temp_data[2]['Middle_Frame'][4]}.jpeg")
                else:
                    print(f"Middle File not found: {image_saving_folder}/{site_name}/{contract_no}/{video_name}/{lhs_rhs}_{temp_data[2]['Middle_Frame'][4]}_{bad_or_not}.jpeg")

                # print("Start frame extraction")
                # output_file_start = f"{image_folder}/{video_name}/{temp_data[0]['Start_Frame'][4]}.jpeg"
                # print(f"{base_path}/{video_name}.MP4", output_file_start, temp_data[0]['Start_Frame'][4], video_name)
                # extract_frame(f"{base_path}/{video_name}.MP4", output_file_start, temp_data[0]['Start_Frame'][4], video_name)
                # print("Middle frame extraction")
                # output_file_middle = f"{image_folder}/{video_name}/{temp_data[2]['Middle_Frame'][4]}.jpeg"
                # extract_frame(f"{base_path}/{video_name}.MP4", output_file_middle, temp_data[2]['Middle_Frame'][4], video_name)
                # print("End frame extraction")
                # output_file_end = f"{image_folder}/{video_name}/{temp_data[1]['End_Frame'][4]}.jpeg"
                # extract_frame(f"{base_path}/{video_name}.MP4", output_file_end, temp_data[1]['End_Frame'][4], video_name)
                # print("##########################")
                print("########THE OUTPUT########")
                # print("##########################")
                # image_path_start = f"{base_path}/{filtered_data[i]['Assets'][0][2]}.jpeg"
                print(new_asset_name,"name of the column")

                try:
                    if new_asset_name  in final_data:
                        print("column_name exists", new_asset_name,i)
                        print(new_tuple)
                        # Ensure "Bad_" assets go only into their respective columns
                        if 'Bad_' in main_asset_name:  # `i` should be the asset name being processed
                            if f"Bad_{new_asset_name}" in final_data:  
                                target_col = f"Bad_{new_asset_name}"
                            else:
                                target_col = new_asset_name
                        else:
                            target_col = new_asset_name  # Normal asset update

                        if final_data[target_col][k] == '[]':
                            final_data[target_col][k] = [new_tuple]
                        else:
                            final_data[target_col][k].append(new_tuple)
                        print(target_col, "ADDED")

                        # if final_data[new_asset_name][k] == '[]':
                        #     final_data[new_asset_name][k] = [new_tuple]
                        # else:
                        #     final_data[new_asset_name][k].append(new_tuple)
                        # print(new_asset_name,"ADDED")
                    # break
                except Exception as error:
                    print(error)

            # if not found_end:
            #     print("No corresponding End found for", asset[0])

            # else:
            #     print("One of the lists is too short.")
        else:
            print("temp_data does not have enough elements.")
    except Exception as error:
        print(error)
        # print(temp_data[0]['Start_Frame'][5],temp_data[1]['End_Frame'][5],"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$&&&&&&&&&&&&&")


# import tqdm

# video_path_dict = utils.allvideos()
# for jsons in tqdm.tqdm(glob.glob("/media/groot/New Volume1/Suman/Linear_upload_code_modified/{image_saving_folder}/Al-baha/**/*final.json",recursive=True)):
def Linear_master_main(site_id,contract_no,site_name,jsons,file,video_path,image_saving_folder,count_file):
    asset_count = 0
    error_count = 00
    contract_no = contract_no
    # site_id = '1201'
    # try:
    #     video_path_dict[contract_no][utils.basen(jsons)]
    # except Exception as ex:
    #     continue
    # if os.path.exists(f"{site_id}/{contract_no}/{utils.basen(jsons)}_master_Linear.csv"):
    #     continue
    # site_name = jsons.split("/")[-3]
    base_path = f"{image_saving_folder}/"
    try:
        if not os.path.exists("out"):
            os.makedirs("out")
            print("creating image_folder")
        image_folder = f"{image_saving_folder}/{site_name}/"
        if not os.path.exists(image_folder):
            os.makedirs(image_folder)
    except Exception as ex:
        print(ex)
    
    
    print(contract_no,jsons)

    # contract_no = "502"
    # try:
    #     print(video_path_dict[contract_no][utils.basen(jsons)])
    #     # if  file == '/run/user/1000/gvfs/smb-share:server=anton.local,share=saudi_video_sync/Jazan/pending videos_11-12-2024/902/11-25-2024/2024_1125_100257_F_new.csv'
    #     file = (video_path_dict[contract_no][utils.basen(jsons)]).replace(".MP4","_new.csv")
    #     new_data = pd.read_csv(file)
    # except Exception as ex:
    #     print(ex)
        
        # file = file.replace(".csv","_normalised.csv")
    
    new_data = pd.read_csv(file)

    # new_data.to_csv("temp.csv")
    # root_dir = '/run/user/1000/gvfs/smb-share:server=enigma.local,share=saudi_video_sync/Qassim/'
    print(new_data)
    video_name = (jsons.split("/")[-1]).replace('_final.json','')
    new_data['video_name'] = video_name
    
    data = new_data.loc[:,['Position','Start_frame','End_frame','Speed','video_name','Chainage']]
    final_data = data
    # print(len(final_data['Position']))
    d1 = {}
    column_values = final_data[['video_name']].values
    # column_values = column_values.astype(str)
    df2 = np.unique(column_values)
    print(df2)
    df2.sort()

    df2 = list(df2)

    # print(len(df2))
    for i in df2:
        rslt_df = final_data[final_data['video_name'] == i]
        l1 = len(rslt_df['video_name'])
        d1[i] = [int(rslt_df.iloc[-1]['Start_frame']), int(rslt_df.iloc[-1]['End_frame'])]

    assets = []
    # video_name_anton_path = {}
    #part1
    print(jsons)
    j1 = jsons.split("/")
    # print(j1)
    # base_path = ('/'.join(j1[1:-1]))
    # print("$$$$$$$$$$$$$$$",base_path)
    # print("$$$$$$$$$$$$$$$",j2)
    #LHS
    direction = 1
    # for i in range(len(d1['direction'])):
    #     if direction==d1['direction'][i]:
    #         site_id = d1['site_id'][i]
    # site_id = '210'
    json_file = jsons

    video_name = json_file.split("/")[-1]
    print(json_file)
    


    video_name1 = video_name.replace("_final.json",".MP4")
    video_file = video_path
    video_name = video_name.replace("_final.json","")
    # if not os.path.exists(f"{site_id}/{contract_no}/"):
    #     os.makedirs(f"{site_id}/{contract_no}/")
    outpath_path = file.replace('.csv','_output.csv')
    # video_name_anton_path[video_name1] = video_name
    print("^^^^^^",video_name)
    # video_file = f"/{base_path}/{video_name}.MP4"
    # print(video_file)
    f = open(json_file,'r')
    f = f.read()
    # print(f)
    data1 = ast.literal_eval(f)
    # print(data1['Assets'])

    for i in range(len(data1['Assets'])):
        try:
            print("orginal_name",data1['Assets'][i][0])
            asset_name = data1['Assets'][i][0]
            print("asset_name check",asset_name)
            if 'Bad_' in asset_name:
                bad_or_not = 2
            else:
                bad_or_not = 1
            count = data1['Assets'][i][1]
            frame = data1['Assets'][i][2]
            # print("#####################",data1['Assets'][i][3][0],data1['Assets'][i][3][1],data1['Assets'][i][4][0],data1['Assets'][i][4][1])

            image_path = f"{video_name}_{frame}_{asset_name}_{count}.jpeg"
            new_asset_name = data1['Assets'][i][0].replace('LEFT_', '')
            new_asset_name = new_asset_name.replace('RIGHT_', '')
            if not any(sub in new_asset_name for sub in ['_Start', '_End', '_END', '_START']):
                continue

            # print(new_asset_name,"$")
            map_assets = {"Chevron_Board": "Signboard_Chevron_Board", "Hazard_Board": "Signboard_Hazard_Board",
                            "High_Mast_Light": "High_Mast", "Signboard_Hazard_board": "Signboard_Hazard_Board",
                            "Signboard_Information_Board_B": "Signboard_Information_Board",
                            "Signboard_Information_Board_N": "Signboard_Information_Board",
                            "Signboard_Information_Board_G": "Signboard_Information_Board",
                            "Hoardings":"Hoarding","Signboard_Mandatory_Board_White":"Signboard_Mandatory_Board",
                            "Street_Light_Slanding": "Street_Light","Pot_Holes":"Potholes",
                            "Single_Arm_Light_Slanting": "Street_Light",
                            "Signboard_Mandatory_ Board_2":"Signboard_Mandatory_Board",
                            "Signboard_Information_Board_1": "Signboard_Information_Board",
                            "Signboard_Information_Board_2": "Signboard_Information_Board",
                            "Signboard_Mandatory_Board_Stop": "Signboard_Mandatory_Board",
                            "Signboard_Mandatory_Board_2": "Signboard_Mandatory_Board",
                            "Singboard_Mandatory _Board _2":"Signboard_Mandatory_Board",
                            "Single_Arm_Street_Light": "Street_Light", "Single_Arm_Street_Light_2": "Street_Light",
                            "Double_Arm_Street_Light": "Street_Light","Single_Arm_Light_Slanting": "Street_Light",
                            "Litter_Bin": "Litter_bin","Litterbin": "Litter_bin","Bollard":"Bollard",
                            "Street_Light_3": "Street_Light", "Street_Light_2": "Street_Light",
                            "Street_Light":"Street_Light","Ductlight_Light":"Ductlight_W",
                            "Signboard_Hazard_Board.3": "Signboard_Hazard_Board",
                            "Signboard_Hazard_Board.2": "Signboard_Hazard_Board",
                            "Signboard_Hazard_Board.1": "Signboard_Hazard_Board",
                            "Hazard_Marker_2": "Hazard_Marker",
                            "DB_Box": "DB_box",
                            "Signboard_Mandatory_Board_STOP": "Signboard_Mandatory_Board",
                            'Hectometer_stone': 'Hectometer_Stone',
                            'Solar_blinker': 'Solar_Blinker','Signboard_Gantry_Board_2':'Signboard_Gantry_Board',""
                            'High_Mast_Light':'High_Mast','Low_Mast_Light': 'Low_Mast',"Telecommunication_Tower":"Telecommunication_Tower","Underpass_Luminaire":"Under_Pass_Luminaire"}


            # l = len(new_asset_name)
            if new_asset_name=="Underpass_Luminaire_End" or new_asset_name=="Underpass_Luminaire_Start":
                new_asset_name=new_asset_name.replace("Underpass_Luminaire","Under_Pass_Luminaire")
            new_asset_name=new_asset_name.replace("Anti_Glare","Anti-Glare").replace("Bridge -Technical_Metal_Barrier","Bridge_-Technical_Metal_Barrier")
            if new_asset_name in map_assets.keys():
                new_asset_name = map_assets[new_asset_name]
            print(new_asset_name,"asset_name_after_checking")
            start_end_flag=False
            if "_start" in new_asset_name.lower() or "_end" in new_asset_name.lower():
                start_end_flag=True
            linear_name = new_asset_name.replace("_END","").replace("_START","").replace("_Start","").replace("_End","")
            temp_asset_name = linear_name.replace("Bad_", "")
            print(temp_asset_name,"1111111111111111111")
            if temp_asset_name == 'Lane':
                temp_asset_name = 'Road_Markings'
            print(temp_asset_name,d2[temp_asset_name][1])
            if start_end_flag:
                print(f"temp_asset_name: {temp_asset_name}, Value in d2: {d2[temp_asset_name]}")
                if d2[temp_asset_name][1] in [1,2,3,4]: #CHANGED also checks fixed because we are passing only linear master json (Retaining_Walls is 2 in asset_type)
                    print("Condition met:", d2[temp_asset_name][1])
                    if temp_asset_name == "Hoarding" or temp_asset_name == "Hoardings":
                        continue
            # if d2[temp_asset_name][1] == 3 or d2[temp_asset_name][1]==4:
            #     if linear_name == "Hoarding":
            #         continue
                    # print(linear_name, "22222",d2[linear_name])
                    assets.append(linear_name)
                    # temp_asset_name = linear_name.replace("Bad_", "")
                    print(new_asset_name)
                    if 'LEFT' in asset_name:
                        lhs_rhs = 1
                    elif 'RIGHT' in asset_name:
                        lhs_rhs = 2
                    
                    asset_id_for_img = d2[temp_asset_name][0]
                    print("222222222222222")
                    if not os.path.exists(f"{image_folder}/{contract_no}/{video_name}/"):
                        print(video_name,"video_folder creating")
                        os.makedirs(f"{image_folder}/{contract_no}/{video_name}/")

                    output_file = f"{image_folder}{contract_no}/{video_name}/{lhs_rhs}_{asset_id_for_img}_{frame}_{bad_or_not}.jpeg"
                    print("3333333333333333333333333",output_file)
                    if not os.path.exists(f"{image_folder}/{contract_no}/{video_name}/"):
                        os.makedirs(f"{image_folder}/{contract_no}/{video_name}/")
                    # print(output_file,video_file)
                    print("111111111111111check11111111111")
                    extract_frame(video_file, output_file, frame,video_name,image_folder,site_id)
                    print(output_file)
                    img = cv2.imread(output_file)
                    bbox = (
                    data1['Assets'][i][3][0], data1['Assets'][i][3][1], data1['Assets'][i][4][0], data1['Assets'][i][4][1])
                    label = linear_name
                    font_scale = 1  # Increase the font size
                    font_thickness = 2  # Increase the font thickness

                    img_with_bbox = draw_bounding_box(img, bbox, labels=[label], color='green', font_scale=font_scale,
                                                        font_thickness=font_thickness)
                    cv2.imwrite(output_file, img)

        except Exception as  ex:
            print("error",ex)
    #     print(assets)
    unique_values = list(set(assets))
    print("unique",unique_values)
    for h in range(len(unique_values)):
        if (f"LEFT_{unique_values[h]}" not in data) or (f"RIGHT_{unique_values[h]}" not in data):
            final_data['LEFT_' + unique_values[h]] = [[] for _ in range(len(data))]
            final_data['RIGHT_' + unique_values[h]] = [[] for _ in range(len(data))]
    # print(len(column_values))
    unique_names = list(set(arr[0] for arr in column_values))
    # print(unique_names)
    # print(final_data.columns)
    # last_frame_count = {}
    # for i in unique_names:
    #     total_frames = extract_all_frames(i)
    #     last_frame_count[i]  = total_frames

    # print(last_frame_count)
    # print("############################")
    print("######PART-1 DONE###########")
    # print("############################")



    #part2
    print(jsons)
    j1 = jsons.split("/")
    # print(j1)
    # base_path = ('/'.join(j1[1:-1]))
    print("$$$$$$$$$$$$$$$",base_path)
    # print("$$$$$$$$$$$$$$$",j2)
    # LHS
    direction = 1
    # for i in range(len(d1['direction'])):
    #     if direction==d1['direction'][i]:
    #         site_id = d1['site_id'][i]

    json_file = jsons
    video_name = ((json_file.split("/"))[-1])
    video_name = video_name.replace("_final.json","")
    print("^^^^^^",video_name)
    video_name1 = f"{video_name}.MP4"
    # video_file = search_folders_with_103_and_json(contract_no,root_dir,video_name1)
    # video_name1 = video_file = video_path_dict[contract_no][utils.basen(jsons)]

    f = open(json_file, 'r')
    f = f.read()
    # print(f)
    data1 = ast.literal_eval(f)
    data1['Assets'] = sorted(data1['Assets'], key=lambda x: x[1])
    print(data1['Assets'])
    print("###########################")
    

    # print(data1['Assets'])
    assets = []
    temp_data = []
    filtered_data = {key: {"Assets": []} for key in unique_values}
    print(filtered_data)
    # sorted_data = sorted(data, key=lambda x: x[2])
    for i in range(len(unique_values)):
            if "Underpass_Luminaire" in unique_values[i] and "_NW" not in unique_values[i]:
                unique_values[i]=unique_values[i].replace("Underpass_Luminaire","Under_Pass_Luminaire")
    print("unique_values after editing",unique_values)
    # Iterate through the assets and filter those matching any criteria in unique_values
    for asset in data1['Assets']:
        asset_name = asset[0] # Assuming the asset name is the first element
        asset_name=asset_name.replace("Anti_Glare","Anti-Glare").replace("Bridge -Technical_Metal_Barrier","Bridge_-Technical_Metal_Barrier")
        for criterion in unique_values:
            clean_asset_name = asset_name.replace('LEFT_', '').replace('RIGHT_', '').replace('_Start', '').replace('_End', '')
            print("clean_asset_name",clean_asset_name)
            if not "NW" in clean_asset_name:
                clean_asset_name=clean_asset_name.replace("Underpass_Luminaire","Under_Pass_Luminaire")
            # Ensure exact match, so "Bad_Fence" does not match "Fence"
            if criterion == clean_asset_name:
            # if criterion in asset_name:
                print("criterion",criterion)
                print(asset)
                filtered_data[criterion]['Assets'].append(asset)
    print(filtered_data)
    for i in unique_values:
        print(i)
        filtered_data[i]['Assets'] = sorted(filtered_data[i]['Assets'], key=lambda x: x[2])
        assets = filtered_data[i]['Assets']
        print("$$$$$assets$$$$$")
        # print(a)
        for val in assets:
            if "Underpass_Luminaire" in val:
                val=val.replace("Underpass_Luminaire","Under_Pass_Luminaire")
        print("assets",assets)
        main_asset_name = assets[0][0].replace('LEFT_','').replace('RIGHT_','').replace('_Start','').replace('_End','')
        print(main_asset_name)
        print("$$$$$assets$$$$$")
        if filtered_data[i]['Assets'] != []:
            # print("$$$$",filtered_data[i]['Assets'])
            for index, asset in enumerate(assets):
                print(asset, video_name,"  %%%$$$$$$%%%  ")
                temp_asset_name = i.replace("Bad_", "")
                if temp_asset_name == 'Lane':
                    temp_asset_name = 'Road_Markings'
                asset_id = d2[temp_asset_name][0]
                print("assets present",assets)
                if len(filtered_data[i]['Assets']) == 1:
                    continue


                elif len(assets) > 1:
                    print(asset, video_name, "##################!!!!!!!!#############")
                    print("assets")
                    print(assets)
                    if 'Cracks' in asset[0] or 'Bad_Lane' in asset[0] or 'Sand_Accumulation' in asset[0] or 'Patch' in asset[0] or 'Potholes' in asset[0] or 'Earth_Works' in asset[0]:
                        if '_Start' in asset[0]:
                            print("Start is present in", asset[0], index)
                            # side = (filtered_data[i]['Assets'][0][0].split('_'))[0]
                            # print(filtered_data[i]['Assets'][0][0])
                            side = asset[0][:4]
                            print(side)
                            found_end = False
                            start_asset = asset
                            print(start_asset)
                            print("sumanhc")
                            for j in range(index + 1, len(assets)):
                                if '_End' in assets[j][0] :
                                    print("End is present in", assets[j][0], "at index", j, "after", asset[0],assets[j])
                                    found_end = True
                                    end_asset = assets[j]

                                    # print(j,index)
                                    # if (j > 1 and j-index ==1) or (j ==1 and j-index == 1):
                                    temp_data = []
                                    print(len(assets),"len of assets",j,index)
                                    asset_count = asset_count + 1
                                    # print("#######processing asset#######")
                                    print(start_asset,end_asset)
                                    print("#######processing asset#######")
                                    print(start_asset, end_asset,i,video_name,video_file)
                                    # exit()
                                    process_asset_pair(start_asset, end_asset,i,video_name,video_file,final_data,site_id,asset_id,asset_count,d1,base_path,filtered_data,site_name,image_folder,contract_no,df2,main_asset_name,image_saving_folder)
                                    break
                    else:


                        if '_Start' in asset[0]:
                            print("Start is present in", asset[0], index)
                            # side = (filtered_data[i]['Assets'][0][0].split('_'))[0]
                            # print(filtered_data[i]['Assets'][0][0])
                            side = asset[0][:4]
                            print(side)
                            found_end = False
                            start_asset = asset
                            print(start_asset)
                            for j in range(index + 1, len(assets)):
                                if '_End' in assets[j][0] and side in assets[j][0]:
                                    print("End is present in", assets[j][0], "at index", j, "after", asset[0],assets[j])
                                    found_end = True
                                    end_asset = assets[j]

                                    # print(j,index)
                                    # if (j > 1 and j-index ==1) or (j ==1 and j-index == 1):
                                    temp_data = []
                                    print(len(assets),"len of assets",j,index)
                                    asset_count = asset_count + 1
                                    # print("#######processing asset#######")
                                    print(start_asset,end_asset)
                                    print("#######processing asset#######")
                                    print(start_asset, end_asset,i,video_name,video_file)
                                    # exit()
                                    process_asset_pair(start_asset, end_asset,i,video_name,video_file,final_data,site_id,asset_id,asset_count,d1,base_path,filtered_data,site_name,image_folder,contract_no,df2,main_asset_name,image_saving_folder)
                                    break

    final_data.to_csv(outpath_path.replace(".csv",f"_{count_file}.csv"),index=False)
    return final_data


def linear_master():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    config_file = os.path.normpath(config_path) 
    with open(config_file) as f:
        config = json.load(f)
    site_name = config['db_name']
    # jsons="/home/saran/POC/Invision_2nd_phase/Metadata/LHS/Linear/2024_0727_161649_00259F_final.json"
    video_base_path=config['video_folder']
    # video_path="/run/user/1000/gvfs/smb-share:server=anton.local,share=roadis_phase4/ml_support/May-2025/Greece-invision-may-07-2025/B_MAIN/B_MAIN_LEFT/2024_0727_161649_00259F.MP4"
    image_saving_folder=config['linear_folder']+"/IMAGES"
    # csv_file="/home/saran/POC/Invision_2nd_phase/Metadata/RHS/Linear/RHS_metadata.csv"
    csv_folder=config['linear_folder']+"/Videowise"
    print("CSV Folder:", csv_folder)
    split_video_wise(config['chainage_file'],csv_folder)
    site_id=config['site_id']
    count=0
    contract_no=1
    for file in glob.glob(config['linear_folder']+"/json/**/*.json"):
        # file_name=file.split("/")[-1].split("F_")[0]+"F"
        file_name=file.split("/")[-1].split("_final.json")[0]
        print(file_name)
        video_path=f"{video_base_path}/{file_name}.MP4"
        jsons=file
        csv_file=csv_folder+f"/{file_name}.csv"
        Linear_master_main(site_id,contract_no,site_name,jsons,csv_file,video_path,image_saving_folder,count)
        count=count+1

    def add_dist_column(folder):
        os.makedirs(folder+"/output",exist_ok=True)
        for file in glob.glob(folder + "*.csv"):
            if "output" not in str(file):
                continue
            df = pd.read_csv(file)
            
            if "Distance" not in df.columns and "Chainage" in df.columns:
                df["Distance"] = df["Chainage"].diff().fillna(0)  # First row gets 0
                output_path = os.path.join(folder+"/output/", "with_distance_" + os.path.basename(file))
                df.to_csv(output_path, index=False)
                print(f"Added 'Distance' column and saved to: {output_path}")
            else:
                print(f"Skipped (already has 'Distance' or missing 'Chainage'): {file}")
        return folder+"output/"
    loc = add_dist_column(csv_folder+"/")
    return loc