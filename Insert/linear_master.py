import mysql.connector 
import pandas as pd
import ast
from datetime import datetime
import numpy as np
import glob,os,json
from . import fixed_master




def upload_linear(lhs_df):
    config=fixed_master.load_config()
    
    server= config['server']
    db_name=config['db_name']
    site_id=config['site_id']
    service_road=config['service_road_flag']
    conx = fixed_master.get_database_connection(server)
    if not conx:
        return
    cursor = conx.cursor(dictionary=True)
    d=fixed_master.fetch_asset_data(cursor,server)
    lhs=pd.read_csv(lhs_df)

    lhs_df=pd.DataFrame(lhs)


    print(lhs_df.columns)
    lhs_df=lhs_df.drop(columns=['Position','Start_frame','End_frame','Speed','video_name','Distance','Chainage'])
    tuple_list = []
    dir_name=config['image_directory'] #changeS

    asset_id_db=0
    for column in lhs_df.columns:
        print("column",column)
        if 'Bad_' in column:
            continue 
        check=str(column)
        check=check.replace("LEFT_","")
        check=check.replace("RIGHT_","")
        print("check",check)
        if "_NW" not in check:
            if column.startswith("RIGHT_") or column.startswith("LEFT_"):
                asset_name = column.split("_", 1)[1]
                print(asset_name)
                if 'Bad_' in asset_name or 'Patch' in asset_name or "Crack" in asset_name:
                    continue  
                if asset_name in d['asset_name']:  # Check if asset_name exists in the dictionary
                    index = d['asset_name'].index(asset_name)  # Get the index of asset_name
                    asset_id = d['asset_id'][index]  # Get the corresponding asset_id
                    asset_type = d['asset_type'][index]  # Get the corresponding asset_type
                    print(f"Found match: {column} -> asset_id: {asset_id}, asset_type: {asset_type}")
                if column.startswith("RIGHT_"):
                    lhs_or_rhs=2
                elif column.startswith("LEFT_"):
                    lhs_or_rhs=1
                
                for j in lhs_df[column]:
                    if j !='[]':
                        a = ast.literal_eval(str(j))
                        if len(a) > 0:
                            for k in a:
                                # print(k)
                                tup = k
                                if not tup in tuple_list:
                                    master_id=tup[9]
                                    print(master_id)
                                    asset_id = tup[-6]
                                    chainage_r=round(float(tup[-7]),3)
                                    if service_road:
                                        master_id="SR_1_" + str(site_id)+"_"+str(asset_id)+"_"+str(chainage_r) + "_" +str(lhs_or_rhs)
                                    else:
                                        master_id="1_" + str(site_id)+"_"+str(asset_id)+"_"+str(chainage_r) + "_" +str(lhs_or_rhs)

                                    
                                    location_str=tup[2]
                                    minus_flag=False
                                    try:
                                        e_index = location_str.index('E')
                                    except:

                                        e_index = location_str.index('W')
                                        minus_flag=True

                                    latitude = float(location_str[1:e_index])
                                    longitude = float(location_str[e_index + 1:])
                                    if minus_flag==True:
                                        latitude="-"+str(latitude)
                                        longitude="-"+str(longitude)
                                    if "S" in location_str:
                                        latitude="-"+str(latitude)
                                    print(f"Latitude: {latitude}, Longitude: {longitude}")
                                    start_latlong = tup[0]
                                    end_latlong = tup[1]
                                    remark = tup[10]
                                    comment = tup[11]
                                    print(type(remark), type(comment))
                                    video_name = tup[9]
                                    print("video_name",video_name)
                                    start_ch = float(tup[6])
                                    end_ch = float(tup[7])
                                    if end_ch > start_ch:
                                        chainage=start_ch+((end_ch-start_ch)/2)
                                    else:
                                        chainage=start_ch-((start_ch-end_ch)/2)
                                    start_frame = tup[3]
                                    end_frame = tup[4]
                                    middle_frame = tup[5]
                                    print(lhs_or_rhs,asset_id)
                                    print(start_ch,start_frame)
                                    # start_ch=float(start_ch)
                                    start_ch=round(start_ch, 3)

                                    images = f"{dir_name}{video_name}/{lhs_or_rhs}_{asset_id}_{start_ch}_{start_frame}.jpeg,{dir_name}{video_name}/{lhs_or_rhs}_{asset_id}_{start_ch}_{middle_frame}.jpeg,{dir_name}{video_name}/{lhs_or_rhs}_{asset_id}_{start_ch}_{end_frame}.jpeg"
                                    print("images")
                                    print(images)
                                    year = video_name[:4]
                                    month = video_name[5:7]
                                    day = video_name[7:9]
                                    hour = video_name[10:12]
                                    minute = video_name[12:14]
                                    second = video_name[14:16]
                                    date_time_str = f"{year}-{month}-{day} {hour}:{minute}:{second}"
                                    print(f"Parsed date_time_str: {date_time_str}")  # Debugging

                                    try:
                                        date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
                                        sql_date_time = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')
                                        add_date=True
                                    except ValueError as e:
                                        sql_date_time=None
                                        print(f"Error parsing date_time_str: {date_time_str}. Error: {e}")
                                        add_date=False
                                    now = datetime.now()

                                    # Format it as "YYYY-MM-DD HH:MM:SS"
                                    updated_on = now.strftime("%Y-%m-%d %H:%M:%S")
                                    
                                    print("remark",remark)

                                    tuple_list.append(tup)

                                    bbox = f"{tup[-2]},{tup[-1]}"
                                    print(bbox,type(bbox))
                                    if add_date:
                                        print(master_id,asset_id,site_id, chainage, latitude, longitude, images,lhs_or_rhs,asset_type,sql_date_time,remark,comment,video_name,start_ch,end_ch,start_latlong,end_latlong,bbox,updated_on)
                                        if service_road:
                                            sql = f"""INSERT INTO {db_name}.tbl_site_asset
                                                        (row_id, asset_id, site_id, Chainage, latitude, longitude, image_path, lhs_rhs,
                                                        number_anomaly, recent_anomaly, current_status, asset_type, created_on, updated_on,
                                                        deleted_on, deleted, recent_anomaly_count, remark, Equipment_id, Description, lane_category,section,material_used,video_name,start_chainage,end_chainage,start_latlong,end_latlong,BBox)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '0', NULL, '1', %s, %s, %s, NULL, '0', '0', %s, NULL, NULL, '3',NULL,%s,%s,%s,%s,%s,%s,%s)"""
                                        else:
                                            sql = f"""INSERT INTO {db_name}.tbl_site_asset
                                                    (row_id, asset_id, site_id, Chainage, latitude, longitude, image_path, lhs_rhs,
                                                    number_anomaly, recent_anomaly, current_status, asset_type, created_on, updated_on,
                                                    deleted_on, deleted, recent_anomaly_count, remark, Equipment_id, Description, lane_category,section,material_used,video_name,start_chainage,end_chainage,start_latlong,end_latlong,BBox)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '0', NULL, '1', %s, %s, %s, NULL, '0', '0', %s, NULL, NULL, '1',NULL ,%s,%s,%s,%s,%s,%s,%s)"""
                                        print(sql)
                                        cursor.execute(sql, (master_id,asset_id,site_id, chainage, latitude, longitude, images,lhs_or_rhs,asset_type,sql_date_time,updated_on,remark,comment,video_name,start_ch,end_ch,start_latlong,end_latlong,bbox))
                                    else:
                                        print(master_id,asset_id,site_id, chainage, latitude, longitude, images,lhs_or_rhs,asset_type,remark,comment,video_name,start_ch,end_ch,start_latlong,end_latlong,bbox,updated_on)
                                        if service_road:
                                            sql = f"""INSERT INTO {db_name}.tbl_site_asset
                                                        (row_id, asset_id, site_id, Chainage, latitude, longitude, image_path, lhs_rhs,
                                                        number_anomaly, recent_anomaly, current_status, asset_type, created_on, updated_on,
                                                        deleted_on, deleted, recent_anomaly_count, remark, Equipment_id, Description, lane_category,section,material_used,video_name,start_chainage,end_chainage,start_latlong,end_latlong,BBox)
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '0', NULL, '1', %s,NULL, %s, NULL, '0', '0', %s, NULL, NULL, '3',NULL,%s,%s,%s,%s,%s,%s,%s)"""
                                        else:
                                            sql = f"""INSERT INTO {db_name}.tbl_site_asset
                                                    (row_id, asset_id, site_id, Chainage, latitude, longitude, image_path, lhs_rhs,
                                                    number_anomaly, recent_anomaly, current_status, asset_type, created_on, updated_on,
                                                    deleted_on, deleted, recent_anomaly_count, remark, Equipment_id, Description, lane_category,section,material_used,video_name,start_chainage,end_chainage,start_latlong,end_latlong,BBox)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '0', NULL, '1', %s, NULL, %s, NULL, '0', '0', %s, NULL, NULL, '1',NULL,%s,%s,%s,%s,%s,%s,%s)"""
                                        print(sql)
                                        cursor.execute(sql, (master_id,asset_id,site_id, chainage, latitude, longitude, images,lhs_or_rhs,asset_type,updated_on,remark,comment,video_name,start_ch,end_ch,start_latlong,end_latlong,bbox))

    conx.commit()
    print("Done")
    conx.close()
        


def upload_linear_master(linear_sheets):
    for file in  glob.glob(f"{linear_sheets}/*.csv",recursive=True):
        upload_linear(file)


    
