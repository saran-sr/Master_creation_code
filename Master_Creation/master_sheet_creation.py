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
        'Litter_Bin': 'Litter_bin', 
        'DB_Box': 'DB_box', 
        'Pot_Holes': 'Potholes', 
        'Signboard_Additional_Board': 'Signboard_Additional_board'
    }

def setup_database_connection(server):

    """Establishes database connection and returns connection object"""
    if server=="anton":   
        conx=mysql.connector.connect(host='takeleap.in',user='seekright',password='Takeleap@123',port='3307')
    elif server=="production":
        conx= mysql.connector.connect(host='seekright-db.ce3lsmnwzkln.ap-south-1.rds.amazonaws.com',user='admin',password='BXWUCSpjRxEqzxXYTF9e',port='3306')
    elif server=="enigma":
        conx = mysql.connector.connect(host='mariadb.seekright.ai', user='enigma', password='Takeleap@123', port='3307')
    
    return conx

def fetch_asset_data(server):
    """Fetches asset data from database and returns asset mapping dictionary"""
    asset_mydb = setup_database_connection(server)
    mycursor = asset_mydb.cursor(dictionary=True)
    
    try:
        if server =="anton":
            assets_db_name="seekright_v3_poc"
        elif server=="production":
            assets_db_name="seekright_v3"
        elif server=="enigma":
            assets_db_name="seekright_v3_enigma"
        else:
            print("Invalid server name")        
        sql = f"SELECT asset_id,asset_name,asset_type,asset_synonyms FROM {assets_db_name}.tbl_asset;"
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
    finally:
        mycursor.close()
        asset_mydb.close()

def get_json_files(json_path):
    """Returns sorted list of JSON files"""
    new_files = []
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
    skip_keywords = ["Start", "End", "start", "end"]
    return any(keyword in asset_name for keyword in skip_keywords)

def process_json_for_columns(json_files, data, map_assets):
    """Processes JSON files to add columns to data"""
    for jsons in json_files:
        print(jsons)
        j1 = jsons.split("/")
        # print(j1)
        base_path = ('/'.join(j1[1:-1]))
        # print("$$$$$$$$$$$$$$$",base_path)
        
        direction = 1
        json_file = jsons
        video_name = ((json_file.split("/"))[-2])
        # print("^^^^^^",video_name)
        
        try:
            f = open(json_file,'r')
            f = f.read()
            data1 = ast.literal_eval(f)
        except (ValueError, SyntaxError) as e:
            print(f"Error parsing JSON file {json_file}: {e}")
            continue
        
        assets = []
        for i in range(len(data1['Assets'])):
            data1['Assets'][i][0] = data1['Assets'][i][0].replace("\n","")
            
            if not should_skip_asset(data1['Assets'][i][0]):
                # print("orginal_name",data1['Assets'][i][0])
                asset_name = data1['Assets'][i][0]
                asset_name = asset_name.replace(" ","")
                count = data1['Assets'][i][1]
                frame = data1['Assets'][i][2]
                # print("#####################",data1['Assets'][i][3][0],data1['Assets'][i][3][1],data1['Assets'][i][4][0],data1['Assets'][i][4][1])
                
                new_asset_name = normalize_asset_name(data1['Assets'][i][0], map_assets)
                assets.append(new_asset_name)
        
        unique_values = list(set(assets))
        # print("unique",unique_values)
        for h in range(len(unique_values)):
            if (f"LEFT_{unique_values[h]}" not in data.columns) or (f"RIGHT_{unique_values[h]}" not in data.columns):
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
    # print(len(df2))

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

def handle_frame_overflow(F, final_data, d1, df2, skip_count, current_index):
    """Handles cases where frame exceeds video boundaries"""
    current_video_name = final_data.iloc[current_index]['video_name']
    # print("OVERFLOW CHECK",F - d1[current_video_name][-1])
    
    try:
        if F - d1[current_video_name][-1] >= 0:
            F = F - d1[current_video_name][-1]
            # print("GGGGGGGGGGGGG1111111111", F)
            index1 = df2.index(current_video_name)
            # print("index1", df2.index(current_video_name))
            
            try:
                d = final_data[final_data['video_name'] == df2[index1 + 1]]
                for l in range(len(d['Position'])):
                    print("lll")
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
                        print("v1",v1,v2,c1,c2,f1,f2)
                        return v1, v2, c1, c2, f1, f2, F
            except Exception as ex:
                print("jshdpOHSDSIODHIOSDHW")
                print(ex)
                print("djhnsdghsklghklsf")
                try:
                    print("aaasffffffffffffffffffffffdgfggh")
                    v1 = final_data.iloc[-2]['Position']
                    v2 = final_data.iloc[-1]['Position']
                    c1 = final_data.iloc[-2]['Chainage']
                    c2 = final_data.iloc[-1]['Chainage']
                    f1 = final_data.iloc[-1]['Start_frame']
                    f2 = final_data.iloc[-1]['End_frame']
                    print("CCCCCCCCCCCCCCCCCCCCCCCCCCCCccsfgfghgh")
                    return v1, v2, c1, c2, f1, f2, F
                except Exception as ex:
                    print(ex, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@################")
        else:
            # Handle normal case within video bounds
            print("gggggggggggg", F)
            video_name = current_video_name
            d = final_data[final_data['video_name'] == video_name]
            
            for l in range(len(d['Position'])):
                if (F >= d.iloc[l]['Start_frame']) and (F <= d.iloc[l]['End_frame']):
                    v1 = d.iloc[l]['Position']
                    c1 = d.iloc[l]['Chainage']
                    inddd = d.index[-1]
                    print(inddd, "ASSD")
                    try:
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
                        print("fdsfsddfgg")
                        try:
                            v2 = final_data.iloc[inddd + 1]['Position']
                            c2 = final_data.iloc[inddd + 1]['Chainage']
                            print("v2,c2", v1, c1, v2, c2, f1, f2)
                            return v1, v2, c1, c2, f1, f2, F
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
                                return v1, v2, c1, c2, f1, f2, F
                            except Exception as ex:
                                print(ex)
                                print(frame, F)
    except:
        pass
    
    # Default fallback
    return None, None, None, None, None, None, F

def create_asset_tuple(new_master, final_data, k, ch, geotag, frame, video_name, 
                      asset_info, remark_bad, remark_nw, bounding_box, image_path):
    """Creates asset tuple based on remark flags"""
    base_tuple = (new_master, final_data.iloc[k]['Position'], float(final_data.iloc[k]['Distance']), 
                  ch, geotag, frame, video_name, asset_info[0])
    
    if remark_bad:
        return base_tuple + ("Bad", bounding_box, image_path)
    elif remark_nw:
        return base_tuple + ("Not Working", bounding_box, image_path)
    else:
        return base_tuple + (asset_info[1], bounding_box, image_path)

def process_master_data(json_files, final_data, d2, map_assets, site_id, 
                       rename_flag, service_road_flag, missing_images):
    """Processes JSON files to create master data"""
    asset_count = 0
    d1, df2 = get_video_frame_mapping(final_data)
    
    print(f"Processing {len(json_files)} JSON files...")
    print(f"Video frame mapping: {len(d1)} videos")
    
    processed_assets = 0
    
    for json_idx, jsons in enumerate(json_files):
        print(f"Processing JSON {json_idx + 1}/{len(json_files)}: {jsons}")
        j1 = jsons.split("/")
        base_path = ('/'.join(j1[1:-1]))
        direction = 1
        
        json_file = jsons
        video_name = ((json_file.split("/"))[-2])
        
        try:
            f = open(json_file, 'r')
            f = f.read()
            data1 = ast.literal_eval(f)
            print(f"  Found {len(data1.get('Assets', []))} assets in JSON")
        except (ValueError, SyntaxError) as e:
            print(f"Error parsing JSON file {json_file}: {e}")
            continue
        
        if 'Assets' not in data1:
            print(f"  No 'Assets' key in JSON file {json_file}")
            continue
        
        for i in range(len(data1['Assets'])):
            data1['Assets'][i][0] = data1['Assets'][i][0].replace("\n","")
            asset_name = data1['Assets'][i][0]
            asset_name = asset_name.replace(" ","")
            count = data1['Assets'][i][1]
            frame = data1['Assets'][i][2]
            
            if not should_skip_asset(data1['Assets'][i][0]):
                processed_assets += 1
                print(f"    Processing asset {processed_assets}: {asset_name} at frame {frame}")
                
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
                
                if new_asset_name not in d2:
                    print(f"    WARNING: Asset '{new_asset_name}' not found in database")
                    continue
                    
                asset_id = d2[new_asset_name]
                new_asset_name = side + '_' + new_asset_name
                asset_count = asset_count + 1
                
                if side=="LEFT":
                    s_m=1
                if side=="RIGHT":
                    s_m=2
                
                matched = False
                for k in range(len(final_data)):
                    if video_name == final_data.iloc[k]['video_name']:
                        if (frame >= final_data.iloc[k]['Start_frame']) and (frame <= final_data.iloc[k]['End_frame']):
                            matched = True
                            print(f"      Matched with row {k}")
                            
                            chainage = round(float(final_data.iloc[k]['Chainage']), 3)
                            master_id = f"{site_id}_{asset_id}_{chainage}_{asset_count}"
                            
                            currentFrame = frame
                            speed = final_data.iloc[k]['Speed']
                            if speed == 0:
                                print(f"      Speed is 0, setting to 5")
                                speed = 5  # Direct assignment instead of max()
                            
                            F = calculate_frame_offset(new_asset_name, currentFrame, speed)
                            
                            # Handle frame overflow/underflow
                            result = handle_frame_overflow(F, final_data, d1, df2, skip_count, k)
                            if result[0] is not None:
                                v1, v2, c1, c2, f1, f2, F = result
                                a = interpolate(v1, v2, c1, c2, f1, f2, F)
                                lat, long, ch = a
                                ch = round(float(ch), 3)
                                
                                if ch - final_data.iloc[k]['Chainage'] > 0.015:
                                    print(f"      Large chainage difference: {ch - final_data.iloc[k]['Chainage']}")
                                print("final_data.iloc[5]['Position']",final_data.iloc[5]['Position'])
                                if "N" in final_data.iloc[5]['Position'] and "E" in final_data.iloc[5]['Position']:
                                    geotag = f"N{lat}E{long}"
                                    print("geotag", geotag)
                                if "S" in final_data.iloc[5]['Position'] and "E" in final_data.iloc[5]['Position']:
                                    geotag = f"S{lat}E{long}"
                                    print("geotag", geotag)
                                if "N" in final_data.iloc[5]['Position'] and "W" in final_data.iloc[5]['Position']:
                                    geotag = f"N{lat}W{long}"
                                    print("geotag", geotag)
                                if "S" in final_data.iloc[5]['Position'] and "W" in final_data.iloc[5]['Position']:
                                    geotag = f"S{lat}W{long}"
                                    print("geotag", geotag)
                                
                                count = count + 1
                                new_master = master_id.replace(master_id.split("_")[2], str(ch))
                                if service_road_flag==1:
                                    new_master = "SR_" + new_master
                                
                                new_img_path = f"/{base_path}/{new_master}.jpeg"
                                
                                if os.path.exists(f"/{base_path}/{image_path}") or not os.path.exists(f"/{base_path}/{image_path}"):
                                    if os.path.exists(new_img_path):
                                        print(f"      Skipping - image already exists: {new_img_path}")
                                        
                                    else:
                                        if rename_flag==True:
                                            if os.path.exists(f"/{base_path}/{image_path}"):
                                                os.rename(f"/{base_path}/{image_path}", f"/{base_path}/{new_master}.jpeg")
                                                print(f"      Renamed image: {image_path} -> {new_master}.jpeg")
                                            else:
                                                missing_images.append(f"/{base_path}/{image_path}")
                                                print(f"      Missing image: {image_path}")
                                    
                                    new_tuple = create_asset_tuple(
                                        new_master, final_data, k, ch, geotag, frame, video_name,
                                        data1['Assets'][i][5], remark_bad, remark_nw,
                                        (data1['Assets'][i][3],data1['Assets'][i][4],data1['Assets'][i][2]), image_path
                                    )
                                    
                                    # Fix the list comparison issue - handle string representations
                                    current_value = final_data.at[k, new_asset_name]
                                    print(f"      Current value type: {type(current_value)}, value: {current_value}")
                                    
                                    # Convert current_value to a list if it's not already
                                    if isinstance(current_value, str):
                                        try:
                                            if current_value.strip() == '[]' or current_value.strip() == '':
                                                current_list = []
                                            else:
                                                current_list = ast.literal_eval(current_value)
                                                if not isinstance(current_list, list):
                                                    current_list = []
                                        except (ValueError, SyntaxError):
                                            current_list = []
                                    elif isinstance(current_value, list):
                                        current_list = current_value
                                    else:
                                        current_list = []
                                    
                                    # Always append the new tuple to the list
                                    current_list.append(new_tuple)
                                    final_data.at[k, new_asset_name] = current_list
                                    print(f"      Updated list with new tuple (list now has {len(current_list)} items)")
                                else:
                                    print(f"      ERROR: Path present in json but image not present: {image_path}")
                            else:
                                print(f"      ERROR: Could not handle frame overflow for frame {F}")
                            break
                
                if not matched:
                    print(f"    WARNING: No matching row found for video {video_name}, frame {frame}")
            else:
                print(f"    Skipping asset: {asset_name} (filtered out)")
    
    print(f"Processed {processed_assets} assets total")
    return final_data

def initialize_dataframe_lists(df):
    """Initialize DataFrame columns that should contain lists"""
    for col in df.columns:
        if col.startswith('LEFT_') or col.startswith('RIGHT_'):
            # Convert string representations back to actual lists
            for i in range(len(df)):
                value = df.at[i, col]
                if isinstance(value, str):
                    try:
                        if value.strip() == '[]' or value.strip() == '':
                            df.at[i, col] = []
                        else:
                            df.at[i, col] = ast.literal_eval(value)
                    except (ValueError, SyntaxError):
                        df.at[i, col] = []
                elif pd.isna(value):
                    df.at[i, col] = []
    return df

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

def create_master_sheet(json_folder):
    """Main function to orchestrate the entire process"""
    # Configuration from config file
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    config_file = os.path.normpath(config_path) 
    
    try:
        with open(config_file) as f:
            config = json.load(f)
        
        json_path = json_folder + "/**/*.json"
        csv_file = config["chainage_file"]
        site_id = config["site_id"]
        rename_flag = True
        service_road_flag = config['service_road_flag']
        server=config['server']
        
        print(f"Configuration loaded from: {config_file}")
        print(f"JSON path: {json_path}")
        print(f"CSV file: {csv_file}")
        print(f"Site ID: {site_id}")
        print(f"Service road flag: {service_road_flag}")
        
    except FileNotFoundError:
        print(f"Config file not found: {config_file}")
        
        # Fallback to default values
        
    except KeyError as e:
        print(f"Missing key in config file: {e}")
        print("Please check your config.json file has all required fields")
        return
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in config file: {e}")
        return
    
    print("Starting create_master_sheet...")
    
    # Initialize
    missing_images = []
    map_assets = get_asset_mapping()
    print(f"Asset mapping loaded: {len(map_assets)} entries")
    
    # Setup database
    try:
        d2 = fetch_asset_data(server)
        print(f"Database data loaded: {len(d2)} assets")
    except Exception as e:
        print(f"Error fetching database data: {e}")
        return
    
    # Get files and data
    new_files = get_json_files(json_path)
    print(f"Found {len(new_files)} JSON files")
    if not new_files:
        print("No JSON files found! Check the path.")
        return
    
    # Load and prepare data
    try:
        data = load_csv_data(csv_file)
        print(f"CSV data loaded: {len(data)} rows")
        print(f"Original columns: {data.columns.tolist()}")
    except Exception as e:
        print(f"Error loading CSV data: {e}")
        return
    
    # Process JSON files to add columns
    try:
        data = process_json_for_columns(new_files, data, map_assets)
        print(f"Columns after processing: {len(data.columns)}")
        print(f"New columns: {data.columns.tolist()}")
    except Exception as e:
        print(f"Error processing JSON for columns: {e}")
        return
    
    # Save skeleton file
    file_name = csv_file.replace("_chainage","").replace(".csv","_skel.csv")
    try:
        data.to_csv(file_name, index=False)
        print(f"Skeleton file saved: {file_name}")
    except Exception as e:
        print(f"Error saving skeleton file: {e}")
        return
    
    # Process master data
    try:
        final_data = pd.read_csv(file_name)
        print(f"Skeleton data loaded: {len(final_data)} rows")
        
        # Initialize list columns properly
        final_data = initialize_dataframe_lists(final_data)
        print("DataFrame list columns initialized")
        
        print("Starting master data processing...")
        final_data = process_master_data(new_files, final_data, d2, map_assets, 
                                       site_id, rename_flag, service_road_flag, missing_images)
        print("Master data processing completed")
        
        # Check if final_data has any data
        print(f"Final data shape: {final_data.shape}")
        
        # Save master file
        master_file = csv_file.replace("_chainage","").replace(".csv","_master.csv")
        print(f"Attempting to save master file: {master_file}")
        
        final_data.to_csv(master_file, index=False)
        print(f"Master file saved successfully: {master_file}")
        
        # Verify the file was created
        if os.path.exists(master_file):
            file_size = os.path.getsize(master_file)
            print(f"Master file exists, size: {file_size} bytes")
        else:
            print("ERROR: Master file was not created!")
            
    except Exception as e:
        print(f"Error in master data processing: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Save missing images
    try:
        missing_images_df = pd.DataFrame({"missing_images":missing_images})
        missing_images_file = csv_file.replace(".csv","_missing_images.csv")
        missing_images_df.to_csv(missing_images_file, index=False)
        print(f"Missing images file saved: {missing_images_file}")
        print(f"Missing images count: {len(missing_images)}")
    except Exception as e:
        print(f"Error saving missing images file: {e}")
    
    # Create modified sheet (commented out in original)
    # create_modified_sheet(master_file)
    
    print("Process completed!")
    return master_file

if __name__ == "__main__":
    create_master_sheet()