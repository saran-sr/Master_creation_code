import json
import pandas as pd
import mysql.connector
import ast
import glob
import os
import numpy as np
import cv2
from opencv_draw_annotation import draw_bounding_box

def get_asset_mapping():
    """Returns the asset mapping dictionary"""
    
    return {
        'Duck_Light': 'Ductlight'
    }

def setup_database_connection():
    """Establishes database connection and returns cursor"""
    aws_host = "takeleap.in"
    aws_user = "seekright"
    aws_password = "Takeleap@123"
    aws_database = "seekright_v3_poc"
    
    asset_mydb = mysql.connector.connect(
        host=aws_host,
        user=aws_user,
        password=aws_password,
        port=3307,
        database=aws_database
    )
    
    return asset_mydb.cursor(dictionary=True)

def fetch_asset_data():
    aws_host = "takeleap.in"
    aws_user = "seekright"
    aws_password = "Takeleap@123"
    aws_database = "seekright_v3_poc"
    
    asset_mydb = mysql.connector.connect(
        host=aws_host,
        user=aws_user,
        password=aws_password,
        port=3307,
        database=aws_database
    )

    mycursor=asset_mydb.cursor(dictionary=True)
    """Fetches asset data from database and returns asset mapping dictionary"""
    sql = "SELECT asset_id,asset_name,asset_type,asset_synonyms FROM seekright_v3_poc.tbl_asset;"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    
    d = {'asset_id':[],'asset_name':[],'asset_type':[],'asset_synonyms':[]}
    for j in myresult:
        d['asset_id'].append(j['asset_id'])
        d['asset_name'].append(j['asset_name'])
        d['asset_type'].append(j['asset_type'])
        d['asset_synonyms'].append(j['asset_synonyms'])

    d2 = {}
    for j in range(len(d['asset_name'])):
        new_name = d['asset_name'][j]
        d2[new_name] = d['asset_id'][j]
    
    return d2

def get_json_files(json_path):
    """Returns sorted list of JSON files"""
    new_files = []
    print("json_path:", json_path)
    for file in glob.glob(json_path):
        new_files.append(file)
    new_files.sort()
    return new_files

def load_csv_data(file_path):
    """Loads and prepares CSV data"""
    new_data = pd.read_csv(file_path)
    data = new_data.loc[:,['Position','Start_frame','End_frame','Speed','video_name','Distance','Chainage']]
    return data

def normalize_asset_name(asset_name, map_assets):
    """Normalizes asset name according to mapping rules"""
    new_asset_name = asset_name.replace('LEFT_', '')
    new_asset_name = new_asset_name.replace('RIGHT_', '')
    if "Bad_Lane" not in new_asset_name:
        new_asset_name = new_asset_name.replace('RIGHT_', '').replace("Bad_","")
    if new_asset_name.endswith(("_C", "_LM", "_SM", "_RM","_Set","_set","_Other","_Temp","_LED")):
        new_asset_name="_".join(new_asset_name.split("_")[:-1])
    if new_asset_name.endswith(("_Other_Lane","Other_lane")):
        new_asset_name="_".join(new_asset_name.split("_")[:-2])
    
    if new_asset_name in map_assets.keys():
        new_asset_name = map_assets[new_asset_name]
    
    return new_asset_name

def should_skip_asset(asset_name):
    """Determines if asset should be skipped based on name"""
    skip_keywords = ["Electric_Power_Mast", "Pothole", "Start", "End", "start", "end", "Drains", "Tunnel_Reflector"]
    return any(keyword in asset_name for keyword in skip_keywords)

def process_json_for_columns(json_files, data, map_assets):
    print("json_files:", json_files)
    """Processes JSON files to add columns to data"""
    for jsons in json_files:
        json_file = jsons
        f = open(json_file,'r')
        f = f.read()
        data1 = ast.literal_eval(f)
        
        assets = []
        for i in range(len(data1['Assets'])):
            data1['Assets'][i][0] = data1['Assets'][i][0].replace("\n","")
            
            if not should_skip_asset(data1['Assets'][i][0]):
                asset_name = data1['Assets'][i][0]
                asset_name = asset_name.replace(" ","")
                new_asset_name = normalize_asset_name(data1['Assets'][i][0], map_assets)
                assets.append(new_asset_name)
        
        unique_values = list(set(assets))
        for h in range(len(unique_values)):
            if (f"LEFT_{unique_values[h]}" not in data) or (f"RIGHT_{unique_values[h]}" not in data):
                data['LEFT_' + unique_values[h]] = [[] for _ in range(len(data))]
                data['RIGHT_' + unique_values[h]] = [[] for _ in range(len(data))]
    
    return data

def split_ltng(cord):
    """Splits latitude longitude coordinates"""
    try:
        return float(cord.split("E")[0][1:]), float(cord.split("E")[1])
    except:
        return float(cord.split("W")[0][1:]), float(cord.split("W")[1])

def interpolate(v1, v2, c1, c2, f1, f2, F):
    """Interpolates position and chainage"""
    lat1, lon1 = split_ltng(v1)
    lat2, lon2 = split_ltng(v2)
    lat1 = lat1 + (F - f1) * (lat2 - lat1) / (f2 - f1)
    lon1 = lon1 + (F - f1) * (lon2 - lon1) / (f2 - f1)
    c1 = c1 + (F - f1) * (c2 - c1) / (f2 - f1)
    return lat1, lon1, c1

def get_video_frame_mapping(final_data):
    """Creates mapping of video names to frame ranges"""
    d1 = {}
    column_values = final_data[['video_name']].values
    df2 = np.unique(column_values)
    df2.sort()
    df2 = list(df2)

    for i in df2:
        rslt_df = final_data[final_data['video_name'] == i]
        l1 = len(rslt_df['video_name'])
        d1[i] = [rslt_df.iloc[-1]['Start_frame'], rslt_df.iloc[-1]['End_frame']]
    
    return d1, df2

def calculate_frame_offset(new_asset_name, currentFrame, speed):
    """Calculates frame offset based on asset type and side"""
    if new_asset_name.startswith('LEFT'):
        F = currentFrame + int((108*5)/speed)
        if 'Street_Light' in new_asset_name:
            F = currentFrame + int((108*13)/speed)
        if "High_Mast" in new_asset_name:
            F = currentFrame + int((108*5)/speed)
        if "Gantry" in new_asset_name or "ITS_Structure" in new_asset_name:
            F = currentFrame + int((108*13)/speed)
    elif new_asset_name.startswith('RIGHT'):
        F = currentFrame + int((108*5)/speed)
        if 'Street_Light' in new_asset_name:
            F = currentFrame + int((108*13)/speed)
        if "High_Mast" in new_asset_name:
            F = currentFrame + int((108*13)/speed)
        if "Gantry" in new_asset_name or "ITS_Structure" in new_asset_name:
            F = currentFrame + int((108*13)/speed)
    
    if F%2==1:
        F=F+1
    
    return F

def handle_frame_overflow(F, final_data, d1, df2, skip_count):

    """Handles cases where frame exceeds video boundaries"""

    print("OVERFLOW CHECK",F - d1[final_data['video_name'][0]][-1])  #debugging
    try:
        
        if F - d1[final_data['video_name'][0]][-1] >= 0:
            F = F - d1[final_data['video_name'][0]][-1]
            index1 = df2.index(final_data['video_name'][0])
            try:
                d = final_data[final_data['video_name'] == df2[index1 + 1]]
                for l in range(len(d['Position'])):
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
                        return v1, v2, c1, c2, f1, f2, F
            except Exception as ex:
                try:
                    v1 = final_data.iloc[-2]['Position']
                    v2 = final_data.iloc[-1]['Position']
                    c1 = final_data.iloc[-2]['Chainage']
                    c2 = final_data.iloc[-1]['Chainage']
                    f1 = final_data.iloc[-1]['Start_frame']
                    f2 = final_data.iloc[-1]['End_frame']
                    return v1, v2, c1, c2, f1, f2, F
                except Exception as ex:
                    print(ex)
        else:
            # Handle normal case within video bounds
            video_name = final_data['video_name'][0]
            d = final_data[final_data['video_name'] == video_name]
            for l in range(len(d['Position'])):
                if (F >= d.iloc[l]['Start_frame']) and (F <= d.iloc[l]['End_frame']):
                    v1 = d.iloc[l]['Position']
                    c1 = d.iloc[l]['Chainage']
                    inddd = d.index[-1]
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
                        return v1, v2, c1, c2, f1, f2, F
                    except Exception as ex:
                        try:
                            v2 = final_data['Position'][inddd + 1]
                            c2 = final_data['Chainage'][inddd + 1]
                            return v1, v2, c1, c2, f1, f2, F
                        except Exception as ex:
                            try:
                                v1 = final_data.iloc[-2]['Position']
                                v2 = final_data.iloc[-1]['Position']
                                c1 = final_data.iloc[-2]['Chainage']
                                c2 = final_data.iloc[-1]['Chainage']
                                f1 = final_data.iloc[-1]['Start_frame']
                                f2 = final_data.iloc[-1]['End_frame']
                                return v1, v2, c1, c2, f1, f2, F
                            except Exception as ex:
                                print(ex)
    except:
        pass
    
    # Default fallback
    return None, None, None, None, None, None, F

def create_asset_tuple(new_master, final_data, k, ch, geotag, frame, video_name, 
                      asset_info, remark_bad, remark_nw, bounding_box, image_path):
    """Creates asset tuple based on remark flags"""
    if remark_bad == False and remark_nw == False:
        return (new_master, final_data['Position'][k], float(final_data['Distance'][k]), 
                ch, geotag, frame, video_name, asset_info[0], asset_info[1], 
                bounding_box, image_path)
    elif remark_bad == True:
        return (new_master, final_data['Position'][k], float(final_data['Distance'][k]), 
                ch, geotag, frame, video_name, asset_info[0], "Bad", 
                bounding_box, image_path)
    elif remark_nw == True:
        return (new_master, final_data['Position'][k], float(final_data['Distance'][k]), 
                ch, geotag, frame, video_name, asset_info[0], "Not Working", 
                bounding_box, image_path)

def process_master_data(json_files, final_data, d2, map_assets, site_id, 
                       rename_flag, service_road_flag, missing_images):
    """Processes JSON files to create master data"""
    asset_count = 0
    d1, df2 = get_video_frame_mapping(final_data)
    
    for jsons in json_files:
        print("Processing JSON:", jsons)
        j1 = jsons.split("/")
        base_path = ('/'.join(j1[1:-1]))
        direction = 1
        
        json_file = jsons
        video_name = ((json_file.split("/"))[-2])
        f = open(json_file, 'r')
        f = f.read()
        data1 = ast.literal_eval(f)
        
        for i in range(len(data1['Assets'])):
            data1['Assets'][i][0] = data1['Assets'][i][0].replace("\n","")
            asset_name = data1['Assets'][i][0]
            asset_name = asset_name.replace(" ","")
            count = data1['Assets'][i][1]
            frame = data1['Assets'][i][2]
            print(asset_name, count, frame)
            if not should_skip_asset(data1['Assets'][i][0]):
                remark_bad = False
                remark_nw = False
                if "Bad_" in data1['Assets'][i][0] and data1['Assets'][i][5][1]=="":
                    remark_bad = True
                if "_NW" in data1['Assets'][i][0] and data1['Assets'][i][5][1]=="":
                    remark_nw = True
                
                image_path = f"{video_name}_{frame}_{asset_name}_{count}.jpeg"
                side = (data1['Assets'][i][0].split('_'))[0]
                skip_count = data1['Assets'][i][-2]
                
                if type(skip_count) == int:
                    skip_count = skip_count
                else:
                    skip_count = 0
                
                new_asset_name = normalize_asset_name(data1['Assets'][i][0], map_assets)
                if new_asset_name=="Signboard_Signboard_Caution_Board":
                    new_asset_name="Signboard_Caution_Board"
                
                asset_id = d2[new_asset_name]
                new_asset_name = side + '_' + new_asset_name
                asset_count = asset_count + 1
                
                if side=="LEFT":
                    s_m=1
                if side=="RIGHT":
                    s_m=2
                
                for k in range(len(final_data['Position'])):
                    if video_name == final_data['video_name'][k]:
                        # print("video_name match", video_name, final_data['video_name'][k])  #debugging
                        if (frame >= final_data['Start_frame'][k]) and (frame <= final_data['End_frame'][k]):
                            chainage = round(float(final_data['Chainage'][k]), 3)
                            master_id = f"{site_id}_{asset_id}_{chainage}_{asset_count}"
                            
                            currentFrame = frame
                            speed = final_data['Speed'][k]
                            if speed == 0:
                                speed = max(speed, 5)
                            
                            F = calculate_frame_offset(new_asset_name, currentFrame, speed)
                            print("F:", F)  #debugging
                            # Handle frame overflow/underflow
                            result = handle_frame_overflow(F, final_data.iloc[k:k+1], d1, df2, skip_count)
                            print("Result:", result)  #debugging
                            if result[0] is not None:
                                v1, v2, c1, c2, f1, f2, F = result
                                a = interpolate(v1, v2, c1, c2, f1, f2, F)
                                lat, long, ch = a
                                ch = round(float(ch), 3)
                                
                                if ch - final_data['Chainage'][k] > 0.015:
                                    print("chainage difference", ch - final_data['Chainage'][k])
                                
                                if "S" not in final_data['Position'][5]:
                                    geotag = f"N{lat}E{long}"
                                else:
                                    geotag = f"S{lat}W{long}"
                                
                                count = count + 1
                                new_master = master_id.replace(master_id.split("_")[2], str(ch))
                                if service_road_flag==True:
                                    new_master = "SR_" + new_master
                                
                                new_img_path = f"/{base_path}/{new_master}.jpeg"
                                print("new_img_path:", new_img_path)  #debugging
                                if os.path.exists(f"/{base_path}/{image_path}") or not os.path.exists(f"/{base_path}/{image_path}"):
                                    if os.path.exists(new_img_path):
                                        continue
                                    else:
                                        if rename_flag==True:
                                            if "Regulatory/" in image_path:
                                                image_path=image_path.replace("Regulatory/","")
                                            if os.path.exists(f"/{base_path}/{image_path}"):
                                                os.rename(f"/{base_path}/{image_path}", f"/{base_path}/{new_master}.jpeg")
                                            else:
                                                missing_images.append(f"/{base_path}/{image_path}")
                                    
                                    new_tuple = create_asset_tuple(
                                        new_master, final_data, k, ch, geotag, frame, video_name,
                                        data1['Assets'][i][5], remark_bad, remark_nw,
                                        (data1['Assets'][i][3],data1['Assets'][i][4]), image_path
                                    )
                                    
                                    if final_data[new_asset_name][k] == '[]':
                                        final_data[new_asset_name][k] = [new_tuple]
                                    else:
                                        final_data[new_asset_name][k].append(new_tuple)
                                else:
                                    print("error,path present in json but image not present", image_path)
    
    return final_data

def separate_latlong(position):
    """Separates latitude and longitude from position string"""
    temp = position.replace("N","_").replace("E","_")
    temp = temp.split("_")
    lat = temp[1]
    long = temp[2]
    return lat, long

def create_modified_sheet(master_file):
    """Creates modified sheet with separated coordinates"""
    latitudes = []
    longitudes = []
    row_id = []
    asset_name = []
    video_frame = []
    image_paths = []
    s_m = []
    
    sheet = pd.read_csv(master_file)
    for column in sheet.columns:
        if column.startswith("LEFT_") or column.startswith("RIGHT_"):
            for row in sheet[column]:
                row = eval(row)
                for set_ in row:
                    lt, lng = separate_latlong(set_[4])
                    latitudes.append(lt)
                    longitudes.append(lng)
                    row_id.append(set_[0])
                    asset_name.append(column.replace("LEFT_","").replace("RIGHT_","").replace("_"," "))
                    v_n = str(set_[-3])+"_"+str(set_[-4])+"_"+column
                    video_frame.append(v_n)
                    if "LEFT_" in column:
                        s_m.append("Shoulder")
                    if "RIGHT_" in column:
                        s_m.append("Median")
    
    dictionary = {"Row_id":row_id,"Latitude":latitudes,"Longitude":longitudes,
                 "Asset":asset_name,"Shoulder_or_Median":s_m}
    df = pd.DataFrame(dictionary)
    modified = master_file.replace("_master.csv","_master_modified.csv")
    df.to_csv(modified,index=False)
    df.to_excel(modified.replace(".csv",".xlsx"))

def create_master_sheet():
    """Main function to orchestrate the entire process"""
    # Configuration
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    config_file = os.path.normpath(config_path) 
    with open(config_file) as f:
        config = json.load(f)

    # json_path = "/home/saran/Projects/adani/KKRPL_NEw/Fixed/LHS/mcw_gantry_missed/json/**/*.json"
    # csv_file = "/home/saran/Projects/adani/KKRPL_NEw/Fixed/LHS/mcw_gantry_missed/KKRPL_LHS.csv"
    # site_id = '280'

    json_path = config["json_folder"] + "/**/*.json"
    csv_file = config["chainage_file"]
    site_id = config["site_id"]
    rename_flag = True
    service_road_flag = False
    
    # Initialize
    missing_images = []
    map_assets = get_asset_mapping()
    
    # Setup database
    # mycursor = setup_database_connection()
    d2 = fetch_asset_data()
    
    # Get files and data
    new_files = get_json_files(json_path)
    print(new_files)
    
    # Load and prepare data
    data = load_csv_data(csv_file)
    print(data.columns)
    
    # Process JSON files to add columns
    data = process_json_for_columns(new_files, data, map_assets)
    print(data.columns)
    
    # Save skeleton file
    file_name = csv_file.replace("_chainage","").replace(".csv","_skel.csv")
    data.to_csv(file_name, index=False)
    
    # Process master data
    final_data = pd.read_csv(file_name)
    final_data = process_master_data(new_files, final_data, d2, map_assets, 
                                   site_id, rename_flag, service_road_flag, missing_images)
    
    # Save master file
    master_file = csv_file.replace("_chainage","").replace(".csv","_master.csv")
    final_data.to_csv(master_file, index=False)
    
    # Save missing images
    missing_images_df = pd.DataFrame({"missing_images":missing_images})
    missing_images_df.to_csv(csv_file.replace(".csv","_missing_images.csv"), index=False)
    
    # Create modified sheet (commented out in original)
    # create_modified_sheet(master_file)

if __name__ == "__main__":
    create_master_sheet()